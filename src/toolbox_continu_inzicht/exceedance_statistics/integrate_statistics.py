import numpy as np
import pandas as pd
from typing import Optional
from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve
from toolbox_continu_inzicht.base.exceedance_frequency_curve import (
    ExceedanceFrequencyCurve,
)


@dataclass(config={"arbitrary_types_allowed": True})
class IntegrateStatisticsPerSection:
    """Integreert een waterniveau overschrijdingsfrequentielijn met een fragility curve


    Options in config
    ------------------
    Bij het combineren van de fragility curves met overschrijdingsfrequentielijn moeten de waterstanden van de curves op elkaar afgestemd worden.
    Dit gebeurt door de waterstanden van de curves te interpoleren naar een nieuwe set waterstanden.
    De volgende opties kunnen worden ingesteld:
    - extend_past_max: float
        Hoever de nieuwe waterstanden verder gaan dan de maximale waterstanden van de input curves.
        Default is 0.01

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
        """
        self.df_exceedance_frequency = self.data_adapter.input(input[0])
        self.df_fragility_curve = self.data_adapter.input(input[1])

        exceedance_frequency_curve = ExceedanceFrequencyCurve(self.data_adapter)
        exceedance_frequency_curve.load(input[0])
        fragility_curve = FragilityCurve(self.data_adapter)
        fragility_curve.load(input[1])

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("IntegrateStatisticsPerSection", {})
        extend_past_max = options.get("extend_past_max", 0.01)
        refine_step_size = options.get("refine_step_size", 0.05)

        self.df_out = self.calculate_integration(
            exceedance_frequency_curve,
            fragility_curve,
            refine_step_size,
            extend_past_max,
        )
        self.data_adapter.output(output, self.df_out)

    @staticmethod
    def calculate_integration(
        exceedance_frequency_curve: ExceedanceFrequencyCurve,
        fragility_curve: FragilityCurve,
        refine_step_size: float,
        extend_past_max: float,
    ):
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

        new_range_waterlevel = np.linspace(
            min_waterlevel,
            max_waterlevel + extend_past_max,
            int((max_waterlevel - min_waterlevel) / refine_step_size + 1),
        )
        exceedance_frequency_curve.refine(new_range_waterlevel)
        fragility_curve.refine(new_range_waterlevel)

        result = _integrate_midpoint(
            new_range_waterlevel,
            fragility_curve.as_array()[:, 1],
            exceedance_frequency_curve.as_array()[:, 1],
        )

        return pd.DataFrame([result.sum()], columns=["result"])


class IntegrateStatistics(IntegrateStatisticsPerSection):
    """Integreert een waterniveau overschrijdingsfrequentielijn met een fragility curve voor een heel gebied"""

    def run(self, input: list, output: str):
        self.df_exceedance_frequency = self.data_adapter.input(input[0])
        self.df_fragility_curves = self.data_adapter.input(input[1])

        # TODO: filter per section and combine back to one df
        self.calculate_integration()

        self.data_adapter.output(output, self.df_out)


def _integrate_midpoint(
    waterlevel_grid, fragility_curve_grid, exceedance_frequency_grid
):
    """Voor de bepaling van de integraal passen we de midpoint rule toe,
    dichtheid van frequentie is midden van verschil tussen twee ov.freq. waarden
    N.B. De stapgrootte van de waterstand dh valt weg in de formule.
    (2*dh/2) * cond_faalkans(h) * (ov.freq.(h-dh)-ov.freq.(h+dh)) / (2*dh)
    prob_fail_grid[i] = fragility_curve_grid[i] * (exceedance_frequency_grid[i-1] - exceedance_frequency_grid[i+1])/2"""
    prob_fail_grid = np.zeros(len(waterlevel_grid))
    prob_fail_grid[1:-1] = (
        exceedance_frequency_grid[0:-2] - exceedance_frequency_grid[2:]
    ) / 2
    prob_fail_grid[1:-1] = fragility_curve_grid[1:-1] * prob_fail_grid[1:-1]

    return prob_fail_grid
