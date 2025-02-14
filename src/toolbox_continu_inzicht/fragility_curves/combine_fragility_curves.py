from dataclasses import field
import numpy as np
from pydantic.dataclasses import dataclass
import pandas as pd
from typing import Optional, Callable

from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve


def combine_independent(lst_fragility_curves, **kwargs):
    """Combineer onafhankelijk: P(fail,comb|h) = 1 - PROD(1 - P(fail,i|h))"""
    array_curves = np.vstack(
        [1 - curve["failure_probability"].to_numpy() for curve in lst_fragility_curves]
    )
    onderschrijdingskans = array_curves.prod(axis=0)
    return 1 - onderschrijdingskans


def combine_dependent(lst_fragility_curves, **kwargs):
    """Combineer afhankelijk:  P(fail,comb|h) = MAX(P(fail,i|h))"""
    array_curves = np.vstack(
        [curve["failure_probability"].to_numpy() for curve in lst_fragility_curves]
    )
    overschrijdingskans = array_curves.max(axis=0)
    return overschrijdingskans


def combine_weighted(lst_fragility_curves, weights=None):
    """Combineer afhankelijk:  P(fail,comb|h) = SUM(w_i * P(fail,i|h))"""
    if weights is None:
        weights = [1 / len(lst_fragility_curves)] * len(lst_fragility_curves)
    lst_curves = []
    for curve, w_i in zip(lst_fragility_curves, weights):
        lst_curves.append(curve["failure_probability"].to_numpy() * w_i)
    array_curves = np.vstack(lst_curves)
    overschrijdingskans = array_curves.sum(axis=0)
    overschrijdingskans = np.clip(overschrijdingskans, 0, 1)
    return overschrijdingskans


@dataclass(config={"arbitrary_types_allowed": True})
class CombineFragilityCurvesIndependent:
    """
    Combineer meerdere fragility curves onafhankelijk tot een enkele fragility curves.

    Args:
        data_adapter (DataAdapter): DataAdapter object

    Options in config
    ------------------
    Bij het combineren van de fragility curves moeten de waterstanden van de curves op elkaar afgestemd worden.
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

    lst_fragility_curves: list[pd.DataFrame] = field(default_factory=list)
    df_out: Optional[pd.DataFrame] | None = None
    combine_func: Callable = combine_independent
    weights = None

    def run(self, input: list[str], output: str) -> None:
        """


        Parameters
        ----------
        input: list[str]

            Lijst van namen van de data adapters met fragility curves.

        output: str

            Naam van de output data adapter.


        Notes:
        ------
        Elke fragility curve moet de volgende kolommen bevatten:
            - waterlevels: float
            - failure_probabilities: float
        """

        for key in input:
            df_in = self.data_adapter.input(key)
            self.lst_fragility_curves.append(df_in)

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("CombineFragilityCurvesIndependent", {})
        extend_past_max = options.get("extend_past_max", 0.01)
        refine_step_size = options.get("refine_step_size", 0.05)

        self.df_out = self.calculate_combined_curve(extend_past_max, refine_step_size)
        self.data_adapter.output(output, self.df_out)

    def calculate_combined_curve(self, extend_past_max, refine_step_size):
        waterlevels_min = []
        waterlevels_max = []
        for df_in in self.lst_fragility_curves:
            waterlevels_min.append(df_in["waterlevels"].min())
            waterlevels_max.append(df_in["waterlevels"].max())

        waterlevels = np.arange(
            min(waterlevels_min),
            max(waterlevels_max) + extend_past_max,
            refine_step_size,
        )

        # interpolate fragility curves to the same waterlevels
        for index, fragility_curve in enumerate(self.lst_fragility_curves):
            fc = FragilityCurve(data_adapter=self.data_adapter)
            fc.from_dataframe(fragility_curve)
            fc.refine(waterlevels)
            self.lst_fragility_curves[index] = fc.as_dataframe()

        overschrijdingskans = self.combine_func(
            self.lst_fragility_curves, weights=self.weights
        )
        return pd.DataFrame(
            {
                "waterlevels": waterlevels,
                "failure_probability": overschrijdingskans,
                "failuremechanismid": 1,
            }
        )


@dataclass(config={"arbitrary_types_allowed": True})
class CombineFragilityCurvesDependent(CombineFragilityCurvesIndependent):
    """
    Combineer meerdere fragility curves afhankelijk tot een enkele fragility curves.

    Args:
        data_adapter (DataAdapter): DataAdapter object

    Options in config
    ------------------
    Bij het combineren van de fragility curves moeten de waterstanden van de curves op elkaar afgestemd worden.
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

    lst_fragility_curves: list[pd.DataFrame] = field(default_factory=list)
    df_out: Optional[pd.DataFrame] | None = None
    combine_func: Callable = combine_dependent
    weights = None


@dataclass(config={"arbitrary_types_allowed": True})
class CombineFragilityCurvesWeightedSum(CombineFragilityCurvesIndependent):
    """
    Combineer meerdere fragility curves met een gewogen som tot een enkele fragility curves.

    Args:
        data_adapter (DataAdapter): DataAdapter object

    Options in config
    ------------------
    Bij het combineren van de fragility curves moeten de waterstanden van de curves op elkaar afgestemd worden.
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

    lst_fragility_curves: list[pd.DataFrame] = field(default_factory=list)
    df_out: Optional[pd.DataFrame] | None = None
    combine_func: Callable = combine_weighted
    weights: list[float] | None = None

    def run(self, input: list[str], output: str):
        """

        Parameters
        ----------
        input: list[str]

            Lijst van namen van de data adapters met fragility curves.
            De laatste lijst hiervan in de gewichten.

        output: str

            Naam van de output data adapter.


        Notes:
        ------
        Elke fragility curve moet de volgende kolommen bevatten:
            - waterlevels: float
            - failure_probabilities: float

        De laatste fragility curve in de input lijst bevat de gewichten.

        Deze moet de volgende kolom bevatten:

            - weights: float

                per curve de gewichten

        """

        for key in input[:-1]:
            df_in = self.data_adapter.input(key)
            self.lst_fragility_curves.append(df_in)

        self.weights = self.data_adapter.input(input[-1])["weights"].to_numpy()

        if len(self.lst_fragility_curves) != len(self.weights):
            raise UserWarning(
                f"De laatste van de lijst van inputs moet de gewichten bevatten {input[-1]}, \
                de lengte van de gewichten ({len(self.weights)}) moet gelijk aan het aan het aantal fragility curves ({len(self.lst_fragility_curves)})"
            )

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("CombineFragilityCurvesIndependent", {})
        extend_past_max = options.get("extend_past_max", 0.01)
        refine_step_size = options.get("refine_step_size", 0.05)

        self.df_out = self.calculate_combined_curve(extend_past_max, refine_step_size)
        self.data_adapter.output(output, self.df_out)
