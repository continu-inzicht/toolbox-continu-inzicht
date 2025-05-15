from dataclasses import field
from typing import Callable, ClassVar, Optional

import numpy as np
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve
from toolbox_continu_inzicht.utils.interpolate import log_interpolate_1d


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
class CombineFragilityCurvesIndependent(ToolboxBase):
    """
    Combineer meerdere fragility curves onafhankelijk tot een enkele fragility curve.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    lst_fragility_curves: list[pd.DataFrame]
        Lijst van fragility curves die worden gecombineerd
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de gecombineerde fragility curve
    combine_func: Callable
        Functie die wordt gebruikt om de fragility curves te combineren
    weights: None
        Alleen van toepassing bij de weighted sum methode, hier None
    fragility_curve_schema: ClassVar[dict[str, str]]
        Schema waaraan de fragility curve moet voldoen: hydraulicload: float, failure_probability: float
    interp_func: Callable
        Functie waarmee geinterpoleerd wordt in FragilityCurve

    Notes
    -----
    Bij het combineren van de fragility curves moeten de waterstanden van de curves op elkaar afgestemd worden.
    Dit gebeurt door de waterstanden van de curves te interpoleren naar een nieuwe set waterstanden.
    De volgende opties kunnen via de config worden ingesteld:

    1. extend_past_max. Hoever de nieuwe waterstanden verder gaan dan de maximale waterstanden van de inputcurves. Default is 0.01.
    2. refine_step_size. De stapgrootte van de waterstanden die gebruikt wordt bij het herschalen van de kansen voor het combineren. Default is 0.05.
    """

    data_adapter: DataAdapter
    lst_fragility_curves: list[pd.DataFrame] = field(default_factory=list)
    df_out: Optional[pd.DataFrame] | None = None
    combine_func: Callable = combine_independent
    weights: None = None
    fragility_curve_schema: ClassVar[dict[str, str]] = {
        "hydraulicload": "float",
        "failure_probability": "float",
    }
    interp_func: Callable = log_interpolate_1d

    def run(self, input: list[str], output: str) -> None:
        """
        Combineert meerdere fragility curves

        Parameters
        ----------
        input: list[str]
            Lijst van namen van de DataAdapters met fragility curves.
        output: str
            Naam van de output DataAdapter.
        """

        for key in input:
            df_in = self.data_adapter.input(key, self.fragility_curve_schema)
            self.lst_fragility_curves.append(df_in)

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("CombineFragilityCurvesIndependent", {})
        extend_past_max = options.get("extend_past_max", 0.01)
        refine_step_size = options.get("refine_step_size", 0.05)

        self.df_out = self.calculate_combined_curve(extend_past_max, refine_step_size)
        self.data_adapter.output(output, self.df_out)

    def calculate_combined_curve(self, extend_past_max: float, refine_step_size: float):
        hydraulicload_min = []
        hydraulicload_max = []
        for df_in in self.lst_fragility_curves:
            hydraulicload_min.append(df_in["hydraulicload"].min())
            hydraulicload_max.append(df_in["hydraulicload"].max())

        # Maak het grid van hydraulische belastingen aan waarop alle
        # fragility curves geinterpoleerd gaan worden.
        hydraulicload = np.arange(
            min(hydraulicload_min),
            max(hydraulicload_max) + extend_past_max,
            refine_step_size,
        )

        # Detecteer sprongen in de fragility curves en voeg deze toe aan het
        # algemene grid. Om toch een voorspelbaar grid te krijgen waar we op
        # kunnen interpoleren, voegen we ter plaatse van de sprong de
        # hydraulische belasting en de hydraulische belasting plus een kleine
        # offset toe.
        steps = []
        for index, fragility_curve in enumerate(self.lst_fragility_curves):
            fc = FragilityCurve(data_adapter=self.data_adapter)
            fc.from_dataframe(fragility_curve)
            idxs = fc.find_jump_indices()
            if len(idxs) > 0:
                for wl in np.unique(fc.hydraulicload[idxs]):
                    if wl not in steps:
                        steps.append(wl)
                        steps.append(wl + 1e-16)
        hydraulicload = np.sort(np.hstack([hydraulicload, steps]))

        # Interpoleer fragility curves naar dezelfde hydraulicload. Aangezien
        # we de sprongen al hebben gedetecteerd en verwerkt, doe dat hier niet
        # nog een keer.
        for index, fragility_curve in enumerate(self.lst_fragility_curves):
            fc = FragilityCurve(data_adapter=self.data_adapter)
            fc.interp_func = self.interp_func
            fc.from_dataframe(fragility_curve)
            fc.refine(hydraulicload, add_steps=False)
            self.lst_fragility_curves[index] = fc.as_dataframe()

        overschrijdingskans = self.combine_func(
            self.lst_fragility_curves, weights=self.weights
        )
        return pd.DataFrame(
            {
                "hydraulicload": hydraulicload,
                "failure_probability": overschrijdingskans,
                "failuremechanismid": 1,
            }
        )


@dataclass(config={"arbitrary_types_allowed": True})
class CombineFragilityCurvesDependent(CombineFragilityCurvesIndependent):
    """
    Combineer meerdere fragility curves afhankelijk tot een enkele fragility curves.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    lst_fragility_curves: list[pd.DataFrame]
        Lijst van fragility curves die worden gecombineerd
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de gecombineerde fragility curve
    combine_func: Callable
        Functie die wordt gebruikt om de fragility curves te combineren
    weights: None
        Alleen van toepassing bij de weighted sum methode, hier None
    fragility_curve_schema: ClassVar[dict[str, str]]
        Schema waaraan de fragility curve moet voldoen: hydraulicload: float, failure_probability: float
    interp_func: Callable
        Functie waarmee geinterpoleerd wordt in FragilityCurve

    Notes
    -----
    Bij het combineren van de fragility curves moeten de waterstanden van de curves op elkaar afgestemd worden.
    Dit gebeurt door de waterstanden van de curves te interpoleren naar een nieuwe set waterstanden.
    De volgende opties kunnen via de config worden ingesteld:

    1. extend_past_max, Hoever de nieuwe waterstanden verder gaan dan de maximale waterstanden van de inputcurves. Default is 0.01.
    2. refine_step_size, De stapgrootte van de waterstanden die gebruikt wordt bij het herschalen van de kansen voor het combineren. Default is 0.05.
    """

    data_adapter: DataAdapter

    lst_fragility_curves: list[pd.DataFrame] = field(default_factory=list)
    df_out: Optional[pd.DataFrame] | None = None
    combine_func: Callable = combine_dependent
    weights: None = None
    fragility_curve_schema: ClassVar[dict[str, str]] = {
        "hydraulicload": "float",
        "failure_probability": "float",
    }
    interp_func: Callable = log_interpolate_1d


@dataclass(config={"arbitrary_types_allowed": True})
class CombineFragilityCurvesWeightedSum(CombineFragilityCurvesIndependent):
    """
    Combineer meerdere fragility curves met een gewogen som tot een enkele fragility curve.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    lst_fragility_curves: list[pd.DataFrame]
        Lijst van fragility curves die worden gecombineerd
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de gecombineerde fragility curve
    combine_func: Callable
        Functie die wordt gebruikt om de fragility curves te combineren
    weights: list[float] | None
        Gewichten voor de weighted sum methode, in de zelfde volgorde als de lijst van fragility curves
    fragility_curve_schema: ClassVar[dict[str, str]]
        Schema waaraan de fragility curve moet voldoen: hydraulicload: float, failure_probability: float
    interp_func: Callable
        Functie waarmee geinterpoleerd wordt in FragilityCurve

    Notes
    -----
    Bij het combineren van de fragility curves moeten de waterstanden van de curves op elkaar afgestemd worden.
    Dit gebeurt door de waterstanden van de curves te interpoleren naar een nieuwe set waterstanden.
    De volgende opties kunnen via de config worden ingesteld:

    1. extend_past_max. Hoever de nieuwe waterstanden verder gaan dan de maximale waterstanden van de inputcurves. Default is 0.01.
    2. refine_step_size. De stapgrootte van de waterstanden die gebruikt wordt bij het herschalen van de kansen voor het combineren. Default is 0.05.
    """

    data_adapter: DataAdapter

    lst_fragility_curves: list[pd.DataFrame] = field(default_factory=list)
    df_out: Optional[pd.DataFrame] | None = None
    combine_func: Callable = combine_weighted
    weights: list[float] | None = None
    fragility_curve_schema: ClassVar[dict[str, str]] = {
        "hydraulicload": "float",
        "failure_probability": "float",
    }
    interp_func: Callable = log_interpolate_1d

    def run(self, input: list[str], output: str):
        """
        Combineert meerdere fragility curves onafhankelijk

        Parameters
        ----------
        input: list[str]
            Lijst van namen van de DataAdapters met fragility curves.
            De laatste lijst hiervan in de gewichten.
        output: str
            Naam van de output DataAdapter.

        Raises
        ------
        UserWarning
            Als de lengte van de gewichten niet gelijk is aan het aantal fragility curves, de laatste waarde van de input lijst moet de gewichten bevatten.

        """

        for key in input[:-1]:
            df_in = self.data_adapter.input(key, self.fragility_curve_schema)
            self.lst_fragility_curves.append(df_in)

        self.weights = self.data_adapter.input(input[-1])["weights"].to_numpy()

        if len(self.lst_fragility_curves) != len(self.weights):
            raise UserWarning(
                f"De laatste van de lijst van inputs moet de gewichten bevatten {input[-1]}, \
                de lengte van de gewichten ({len(self.weights)}) moet gelijk zijn aan het aantal fragility curves ({len(self.lst_fragility_curves)})"
            )

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("CombineFragilityCurvesIndependent", {})
        extend_past_max = options.get("extend_past_max", 0.01)
        refine_step_size = options.get("refine_step_size", 0.05)

        self.df_out = self.calculate_combined_curve(extend_past_max, refine_step_size)
        self.data_adapter.output(output, self.df_out)
