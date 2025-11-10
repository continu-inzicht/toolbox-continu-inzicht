from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter, FragilityCurve
from toolbox_continu_inzicht.base.base_module import ToolboxBase

"""
Load cached fragility curve heeft 3 niveas:

1. Per dijk vak de curve aanpassen voor een mechanisme met `LoadCachedFragilityCurveOneFailureMechanism`.
    Hierbij is het meest overzichtelijk wat er gebeurt, maar het is wel omslachtig als er veel dijkvakken zijn.

1. Voor een dijkvak, maar voor alle faalmechanismen in een keer de curves aanpassen met `LoadCachedFragilityCurve`.

1. Voor alle dijkvakken en faalmechanismes in een keer alle curves aanpassen met `LoadCachedFragilityCurveMultiple`,
    dit is in de huidge implementatie de manier om het te doen.
"""


@dataclass(config={"arbitrary_types_allowed": True})
class LoadCachedFragilityCurveOneFailureMechanism(ToolboxBase):
    """
    Maakt het mogelijk om een vooraf berekende (cached) fragility curve in te laden.

    Deze class doet het voor een enkel vak curve, voor een enkele faalmechanisme.
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
        "measure_id": "int",
        "hydraulicload": "float",
        "failure_probability": "float",
    }

    def run(self, input: str, output: str, measure_id: int) -> None:
        """
        Runt het ophalen van de fragility curve voor een enkel vak, voor een faalmechanisme.

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
        df_fragility_curves = self.data_adapter.input(
            input, schema=self.cache_fragility_curve_schema
        )
        self.df_in, self.df_out = self.retrieve_cache(df_fragility_curves, measure_id)
        self.df_out["measure_id"] = measure_id
        self.df_out = self.df_out[list(self.cache_fragility_curve_schema.keys())]
        self.data_adapter.output(output, self.df_out)

    def retrieve_cache(
        self, df_fragility_curves: pd.DataFrame, measure_id: int
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Functie om hergebruik toe te staan van LoadCachedFragilityCurveOneFailureMechanism door LoadCachedFragilityCurve"""

        fragility_curve = FragilityCurve(self.data_adapter)
        # aanname dat altijd op nul staat
        measures = list(df_fragility_curves["measure_id"].unique())
        if 0 not in measures:
            raise ValueError(
                "Er is geen basis fragility curve (measure_id 0) aanwezig in de input dataadapter"
            )

        df_in = df_fragility_curves[df_fragility_curves["measure_id"] == 0]
        fragility_curve.from_dataframe(df_in)
        measures.remove(0)  # we already loaded the base curve with measure_id 0
        cache = {}
        for ids in measures:
            cache[ids] = df_fragility_curves[df_fragility_curves["measure_id"] == ids]
        fragility_curve.cached_fragility_curves = cache
        # select the implemented fragility curve based on measure_id
        fragility_curve.load_effect_from_dataframe(measure_id)
        df_out = fragility_curve.as_dataframe()
        return df_in, df_out


@dataclass(config={"arbitrary_types_allowed": True})
class LoadCachedFragilityCurve(LoadCachedFragilityCurveOneFailureMechanism):
    """
    Maakt het mogelijk om een vooraf berekende (cached) fragility curve in te laden.

    Deze class doet het voor een enkel vak curve, voor een meerdere faalmechanisme.


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

        - failure_mechanism_id: int,
        - measure_id: int,
        - hydraulicload: float,
        - failure_probability: float

    Notes
    -----


    """

    # inherit everything from LoadCachedFragilityCurveOneFailureMechanism
    cache_fragility_curve_schema: ClassVar[dict[str, str]] = {
        "failuremechanism_id": "int",
        "measure_id": "int",
        "hydraulicload": "float",
        "failure_probability": "float",
    }

    def run(self, input: str, output: str, measure_id: int) -> None:
        """
        Runt de het ophalen van een voorberekende fragility curve voor een enkel vak, voor een meerdere faalmechanisme.

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
        df_fragility_curves = self.data_adapter.input(
            input, schema=self.cache_fragility_curve_schema
        )
        self.df_in, self.df_out = self.retrieve_cache_for_multiple_failure_mechanisms(
            df_fragility_curves, measure_id
        )
        self.df_out = self.df_out[
            list(self.cache_fragility_curve_schema.keys())
        ]  # fix column order
        self.data_adapter.output(output, self.df_out)

    def retrieve_cache_for_multiple_failure_mechanisms(
        self, df_fragility_curves: pd.DataFrame, measure_id: int
    ) -> pd.DataFrame:
        """Functie om hergebruik toe te staan van LoadCachedFragilityCurve door LoadCachedFragilityCurveMultiple"""
        failure_mechanism_ids = list(
            df_fragility_curves["failuremechanism_id"].unique()
        )
        lst_selected_curves, lst_initial_curves = [], []
        for failure_mechanism in failure_mechanism_ids:
            df_fragility_curves_per_mechanism = df_fragility_curves[
                df_fragility_curves["failuremechanism_id"] == failure_mechanism
            ]
            inital_curve, selected_curve = self.retrieve_cache(
                df_fragility_curves_per_mechanism, measure_id
            )
            selected_curve["failuremechanism_id"] = failure_mechanism
            selected_curve["measure_id"] = measure_id

            lst_selected_curves.append(selected_curve)
            lst_initial_curves.append(inital_curve)

        return pd.concat(lst_initial_curves, ignore_index=True), pd.concat(
            lst_selected_curves, ignore_index=True
        )


@dataclass(config={"arbitrary_types_allowed": True})
class LoadCachedFragilityCurveMultiple(LoadCachedFragilityCurve):
    """
    Maakt het mogelijk om een vooraf berekende (cached) fragility curve in te laden.

    Deze class doet het voor een meerdere vakken en meerdere faalmechanisme.

    ----------
    data_adapter : DataAdapter
        Adapter for handling data input and output operations.
    df_out : Optional[pd.DataFrame] | None
        Output DataFrame containing the final fragility curve

    Notes
    -----


    """

    # inherit everything from LoadCachedFragilityCurve
    cache_fragility_curve_schema: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "failuremechanism_id": "int",
        "measure_id": "int",
        "hydraulicload": "float",
        "failure_probability": "float",
    }

    def run(self, input: str, output: str, measure_id: int) -> None:
        """
        Runt de het ophalen van een voorberekende fragility curve voor  meerdere vakken en voor meerdere faalmechanisme.

        Parameters
        ----------
        input: str
            Naam van de input dataadapter: fragility curves,
            Hierbij zijn de fragility curves voor meerdere faalmechanisme, dijkvak en measure_ids.
        output: str
            Naam van de dataadapter Fragility curve output
        measure_id: int
            Measure_id van de fragility curve die ingeladen moet worden.

        Notes
        -----

        """
        df_fragility_curves = self.data_adapter.input(
            input, schema=self.cache_fragility_curve_schema
        )
        section_ids = list(df_fragility_curves["section_id"].unique())
        lst_selected_curves = []
        lst_initial_curves = []
        for section in section_ids:
            df_fragility_curves_per_section = df_fragility_curves[
                df_fragility_curves["section_id"] == section
            ]
            input_per_section, output_per_section = (
                self.retrieve_cache_for_multiple_failure_mechanisms(
                    df_fragility_curves_per_section, measure_id
                )
            )
            output_per_section["section_id"] = section
            output_per_section["measure_id"] = measure_id
            lst_selected_curves.append(output_per_section)
            lst_initial_curves.append(input_per_section)

        self.df_in = pd.concat(lst_initial_curves, ignore_index=True)
        self.df_out = pd.concat(lst_selected_curves, ignore_index=True)
        self.df_out = self.df_out[
            list(self.cache_fragility_curve_schema.keys())
        ]  # fix column order
        self.data_adapter.output(output, self.df_out)
