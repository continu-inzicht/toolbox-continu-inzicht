from typing import Optional

import numpy as np
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.adapters.validate_dataframe import validate_dataframe
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.exceedance_frequency_curve import (
    ExceedanceFrequencyCurve,
)
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve


@dataclass(config={"arbitrary_types_allowed": True})
class IntegrateFragilityCurve:
    """Integreert een waterniveau overschrijdingsfrequentielijn met een fragility curve


    Options in config
    ------------------
    Bij het combineren van de fragility curves met overschrijdingsfrequentielijn moeten de waterstanden van de curves op elkaar afgestemd worden.
    Dit gebeurt door de waterstanden van de curves te interpoleren naar een nieuwe set waterstanden.
    De volgende opties kunnen worden ingesteld:

    - refine_step_size: float
        De stapgrootte van de waterstanden die gebruikt wordt bij het herschalen van de kansen voor het combineren.
        Default is 0.05
    """

    data_adapter: DataAdapter
    df_exceedance_frequency: Optional[pd.DataFrame] | None = None
    df_fragility_curve: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list, output: str):
        """Runt de integratie van een waterniveau overschrijdingsfrequentielijn met een fragility curve

        Parameters
        ----------
        input: list[str]
               [0] df_exceedance_frequency (pd.DataFrame),
               [1] df_fragility_curve (pd.DataFrame),
        output: str
            output df

        Notes:
        ------
        input: list[str]

               [0] df_exceedance_frequency (pd.DataFrame)
                    DataFrame met waterstand overschrijdingsfrequentie statistiek.
                    Moet de volgende kolommen bevatten:
                    - waterlevels : float
                    - probability_exceedance : float

               [1] df_fragility_curve (pd.DataFrame):
                    DataFrame met df_fragility_curve data.
                    Moet de volgende kolommen bevatten:
                    - waterlevels: float
                    - failure_probabilities: float
        """
        self.df_exceedance_frequency = self.data_adapter.input(input[0])
        self.df_fragility_curve = self.data_adapter.input(input[1])

        exceedance_frequency_curve = ExceedanceFrequencyCurve(self.data_adapter)
        exceedance_frequency_curve.load(input[0])
        fragility_curve = FragilityCurve(self.data_adapter)
        fragility_curve.load(input[1])

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("IntegrateFragilityCurve", {})
        refine_step_size = options.get("refine_step_size", 0.05)

        self.df_out = self.calculate_integration(
            exceedance_frequency_curve,
            fragility_curve,
            refine_step_size,
        )
        self.data_adapter.output(output, self.df_out)

    @staticmethod
    def calculate_integration(
        exceedance_frequency_curve: ExceedanceFrequencyCurve,
        fragility_curve: FragilityCurve,
        refine_step_size: float,
    ) -> pd.DataFrame:
        exceedance_frequency_curve_waterlevels = exceedance_frequency_curve.as_array()[
            :, 0
        ]
        fragility_curve_waterlevels = fragility_curve.as_array()[:, 0]
        min_waterlevel = min(
            exceedance_frequency_curve_waterlevels.min(),
            fragility_curve_waterlevels.min(),
        )
        max_waterlevel = max(
            exceedance_frequency_curve_waterlevels.max(),
            fragility_curve_waterlevels.max(),
        )

        # Create a water level range with the given stepsize from
        # min_waterlevel to at least max_waterlevel. The resulting range might
        # in some cases go slightly over the max_waterlevel.
        new_range_waterlevel = np.arange(
            min_waterlevel, max_waterlevel + refine_step_size, refine_step_size
        )
        # The arange can result in stepsize with floating point errors.
        # Therefore, round the waterlevels to the accuracy of the step_size.
        # Do this by finding the first significant digit of the stepsize,
        # and add 3 more decimals to be safe (and to account for a stepsize
        # such as 0.9999).
        decimals = int(np.ceil(-np.log10(refine_step_size))) + 3
        new_range_waterlevel = new_range_waterlevel.round(decimals)

        exceedance_frequency_curve.refine(new_range_waterlevel)
        fragility_curve.refine(new_range_waterlevel)

        integrated_probability = _integrate_midpoint(
            new_range_waterlevel,
            fragility_curve.as_array()[:, 1],
            exceedance_frequency_curve.as_array()[:, 1],
        )

        return pd.DataFrame(
            list(zip(new_range_waterlevel, integrated_probability)),
            columns=["waterlevels", "probability_contribution"],
        )


class IntegrateFragilityCurveMultiple(IntegrateFragilityCurve):
    """Integreert een waterniveau overschrijdingsfrequentielijn met een fragility curve voor reeks aan secties"""

    data_adapter: DataAdapter
    df_exceedance_frequency: Optional[pd.DataFrame] | None = None
    df_fragility_curve: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list, output: str):
        """Runt de integratie van een waterniveau overschrijdingsfrequentielijn met een fragility curve
        Parameters
        ----------
        input: list[str]
               [0] df_exceedance_frequency (pd.DataFrame),
               [1] df_fragility_curve (pd.DataFrame),
        output: str
            output df

        Notes:
        ------
        input: list[str]

               [0] df_exceedance_frequency (pd.DataFrame)
                    DataFrame met waterstand overschrijdingsfrequentie statistiek.
                    Moet de volgende kolommen bevatten:
                    - waterlevels : float
                    - probability_exceedance : float

               [1] df_fragility_curve (pd.DataFrame):
                    DataFrame met df_fragility_curve data.
                    Moet de volgende kolommen bevatten:
                    - waterlevels: float
                    - failure_probabilities: float"""
        self.df_exceedance_frequency = self.data_adapter.input(input[0])
        self.df_fragility_curve = self.data_adapter.input(input[1])

        exceedance_frequency_curve = ExceedanceFrequencyCurve(self.data_adapter)
        exceedance_frequency_curve.load(input[0])

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("IntegrateFragilityCurveMultiple", {})
        refine_step_size = options.get("refine_step_size", 0.05)

        fragility_curve_multi_section = self.data_adapter.input(input[1])

        results = []
        for section_id in fragility_curve_multi_section["section_id"].unique():
            df_fc = fragility_curve_multi_section.query("`section_id` == @section_id")
            status, message = validate_dataframe(
                df=df_fc, schema=FragilityCurve.fragility_curve_schema
            )
            if status > 0:
                raise UserWarning(message)
            fragility_curve = FragilityCurve(self.data_adapter)
            fragility_curve.from_dataframe(df_fc)
            result = self.calculate_integration(
                exceedance_frequency_curve,
                fragility_curve,
                refine_step_size,
            )
            result["section_id"] = section_id
            results.append(result)

        self.df_out = pd.concat(results)
        self.data_adapter.output(output, self.df_out)


def _integrate_midpoint(
    waterlevel_grid, fragility_curve_grid, exceedance_frequency_grid
):
    """
    Voor de bepaling van de integraal passen we de midpoint rule toe,
    dichtheid van frequentie is midden van verschil tussen twee ov.freq. waarden

    N.B. De stapgrootte van de waterstand dh valt weg in de formule:

    (2*dh/2) * cond_faalkans(h) * (ov.freq.(h-dh)-ov.freq.(h+dh)) / (2*dh)

    prob_fail_grid[i] = fragility_curve_grid[i] * (exceedance_frequency_grid[i-1] - exceedance_frequency_grid[i+1])/2

    """
    prob_fail_grid = np.zeros(len(waterlevel_grid))
    prob_fail_grid[1:-1] = (
        exceedance_frequency_grid[0:-2] - exceedance_frequency_grid[2:]
    ) / 2
    prob_fail_grid[1:-1] = fragility_curve_grid[1:-1] * prob_fail_grid[1:-1]

    return prob_fail_grid
