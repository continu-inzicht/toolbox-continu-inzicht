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
    """Combineer afhankelijk:  P(fail,comb|h) = max( P(fail,i|h))"""
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
    return overschrijdingskans


@dataclass(config={"arbitrary_types_allowed": True})
class CombineFragilityCurvesIndependent:
    """
    Combineer meerdere fragility curves onafhankelijk tot een enkele fragility curves.
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
        self.df_out = self.calculate_combined_curve()
        self.data_adapter.output(output, self.df_out)

    def calculate_combined_curve(self):
        waterlevels_min = []
        waterlevels_max = []
        for df_in in self.lst_fragility_curves:
            waterlevels_min.append(df_in["waterlevels"].min())
            waterlevels_max.append(df_in["waterlevels"].max())

        waterlevels = np.arange(min(waterlevels_min), max(waterlevels_max) + 0.01, 0.05)

        # interpolate fragility curves to the same waterlevels
        for index, fragility_curve in enumerate(self.lst_fragility_curves):
            fc = FragilityCurve(data_adapter=self.data_adapter, df_out=fragility_curve)
            fc.refine(waterlevels)
            self.lst_fragility_curves[index] = fc.df_out

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
        """

        for key in input[:-1]:
            df_in = self.data_adapter.input(key)
            self.lst_fragility_curves.append(df_in)

        # laatste waarde van de input list is de gewichten
        self.weights = self.data_adapter.input(input[-1])

        self.df_out = self.calculate_combined_curve()
        self.data_adapter.output(output, self.df_out)
