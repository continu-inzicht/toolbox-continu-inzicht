from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter, FragilityCurve
from toolbox_continu_inzicht.base.base_module import ToolboxBase


@dataclass(config={"arbitrary_types_allowed": True})
class LoadCachedFragilityCurveOneFailureMechanism(ToolboxBase):
    """
    Maakt het mogelijk om een vooraf berekende (cached) fragility curve in te laden.

    Deze class doet het voor een enkele curve.


    ----------
    data_adapter : DataAdapter
        Adapter for handling data input and output operations.
    df_in : Optional[pd.DataFrame] | None
        Input DataFrame containing fragility curves with multiple measure_ids
    df_out : Optional[pd.DataFrame] | None
        Output DataFrame containing the final fragility curve
    loaded_curve : Optional[FragilityCurve] | None
        Ingeladen fragility curve met cached curves
    cache_fragility_curve_schema : ClassVar[dict[str, str]]
        Schema waaraan de fragility curve moet voldoen:

        - measure_id: int,
        - hydraulicload: float,
        - failure_probability: float

    Notes
    -----


    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame] = None
    df_out: Optional[pd.DataFrame] = None
    loaded_curve: Optional[FragilityCurve] = None
    cache_fragility_curve_schema: ClassVar[dict[str, str]] = {
        "measureid": "int",
        "hydraulicload": "float",
        "failure_probability": "float",
    }

    def run(self, input: str, output: str, measure_id: int) -> None:
        """
        Runt de berekening van de fragility curve voor piping

        Parameters
        ----------
        input: str
            Naam van de input dataadapter: fragility curves,
            hierbij zijn de fragility curves voor een faalmechanisme en dijkvak, maar meerdere measureids.

        output: str
            Naam van de dataadapter voor de enkele Fragility curve output.

        measure_id: int
            Measure_id van de fragility curve die ingeladen moet worden.

        Notes
        -----

        """
        self.retrieve_cache(input, output, measure_id)

    def retrieve_cache(self, input: str, output: str, measure_id: int) -> None:
        """ "Functie om hergebruik toe te staan"""
        df_fragility_curves = self.data_adapter.input(
            input, schema=self.cache_fragility_curve_schema
        )
        fragility_curve = FragilityCurve(self.data_adapter)
        # aanname dat altijd op nul staat
        measures = list(df_fragility_curves["measureid"].unique())
        if 0 not in measures:
            raise ValueError(
                "Er is geen basis fragility curve (measureid 0) aanwezig in de input dataadapter"
            )
        self.df_in = df_fragility_curves[df_fragility_curves["measureid"] == 0]
        fragility_curve.from_dataframe(self.df_in)
        measures.remove(0)  # we already loaded the base curve with measureid 0
        cache = {}
        for ids in measures:
            cache[ids] = df_fragility_curves[df_fragility_curves["measureid"] == ids]
        fragility_curve.cached_fragility_curves = cache
        # select the implemented fragility curve based on measureid
        fragility_curve.load_effect_from_dataframe(measure_id)
        self.df_out = fragility_curve.as_dataframe()
        self.data_adapter.output(output, self.df_out)


@dataclass(config={"arbitrary_types_allowed": True})
class LoadCachedFragilityCurve(LoadCachedFragilityCurveOneFailureMechanism):
    """
    Maakt het mogelijk om een vooraf berekende (cached) fragility curve in te laden.

    Deze class doet het voor een enkele curve.


    ----------
    data_adapter : DataAdapter
        Adapter for handling data input and output operations.
    df_in : Optional[pd.DataFrame] | None
        Input DataFrame containing fragility curves with multiple measure_ids
    df_out : Optional[pd.DataFrame] | None
        Output DataFrame containing the final fragility curve
    loaded_curve : Optional[FragilityCurve] | None
        Ingeladen fragility curve met cached curves
    cache_fragility_curve_schema : ClassVar[dict[str, str]]
        Schema waaraan de fragility curve moet voldoen:

        - measure_id: int,
        - hydraulicload: float,
        - failure_probability: float

    Notes
    -----


    """

    data_adapter: DataAdapter
    df_in: Optional[pd.DataFrame] = None
    df_out: Optional[pd.DataFrame] = None
    loaded_curve: Optional[FragilityCurve] = None
    cache_fragility_curve_schema: ClassVar[dict[str, str]] = {
        "measureid": "int",
        "hydraulicload": "float",
        "failure_probability": "float",
    }

    def run(self, input: str, output: str, measure_id: int) -> None:
        """
        Runt de berekening van de fragility curve voor piping

        Parameters
        ----------
        input: str
            Naam van de input dataadapter: fragility curves,
            hierbij zijn de fragility curves voor een faalmechanisme en dijkvak, maar meerdere measureids.

        output: str
            Naam van de dataadapter voor de enkele Fragility curve output.

        measure_id: int
            Measure_id van de fragility curve die ingeladen moet worden.

        Notes
        -----

        """


@dataclass(config={"arbitrary_types_allowed": True})
class LoadCachedFragilityCurveMultiple(FragilityCurve):
    """
    Maakt het mogelijk om een vooraf berekende (cached) fragility curve in te laden.

    Deze class doet het voor meerderere curves.

    ----------
    data_adapter : DataAdapter
        Adapter for handling data input and output operations.
    df_out : Optional[pd.DataFrame] | None
        Output DataFrame containing the final fragility curve

    Notes
    -----


    """

    data_adapter: DataAdapter
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor piping

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input dataadapters: ...
        output: str
            Naam van de dataadapter Fragility curve output

        Notes
        -----

        """
        # TODO implement for a single curve with caching
        pass
