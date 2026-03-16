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
    dit is in de huidige implementatie van CI de manier om het te doen.
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

    Default waarden te overschrijven in de global variables (let op: `LoadCachedFragilityCurve`):

    - default_measure_id: int
        Standaard measure_id die gebruikt wordt als basis fragility curve, standaard 0.


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
    measure_to_effect_schema: ClassVar[dict[str, str]] = {
        "measure_id": "int",
        "effect": "float",
    }

    def run(self, input: list[str], output: str, measure_id: int) -> None:
        """
        Runt het ophalen van de fragility curve voor een enkel vak, voor een faalmechanisme.

        Parameters
        ----------
        input: list[str]
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
            input[0], schema=self.cache_fragility_curve_schema
        )
        df_measures_to_effect = self.data_adapter.input(
            input[1], schema=self.measure_to_effect_schema
        )
        self.df_in, self.df_out = self.retrieve_cache(
            df_fragility_curves, df_measures_to_effect, measure_id
        )
        self.df_out["measure_id"] = measure_id
        self.df_out = self.df_out[list(self.cache_fragility_curve_schema.keys())]
        self.data_adapter.output(output, self.df_out)

    def retrieve_cache(
        self,
        df_fragility_curves: pd.DataFrame,
        df_measures_to_effect: pd.DataFrame,
        measure_id: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Functie om hergebruik toe te staan van LoadCachedFragilityCurveOneFailureMechanism door LoadCachedFragilityCurve"""

        fragility_curve = FragilityCurve(self.data_adapter)
        # measures zijn configurabel
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("LoadCachedFragilityCurve", {})
        default_measure_value = options.get("default_measure_id", 0)
        measures = list(df_fragility_curves["measure_id"].unique())
        if default_measure_value not in measures:
            raise ValueError(
                f"Er is geen basis fragility curve (measure_id {default_measure_value}) aanwezig. Zorg dat de input data klopt of pas "
                f"de default_measure_id aan in de input data adapter onder LoadCachedFragilityCurve:\ndefault_measure_id:"
            )

        df_in = df_fragility_curves[
            df_fragility_curves["measure_id"] == default_measure_value
        ]
        # trivial case, return the base curve
        if measure_id == default_measure_value:
            df_out = df_in.copy()
        else:
            # load the base fragility curve
            fragility_curve.from_dataframe(df_in)
            measures.remove(
                default_measure_value
            )  # we already loaded the base curve with measure_id 0
            cache = {}
            for ids in measures:
                cache[ids] = df_fragility_curves[
                    df_fragility_curves["measure_id"] == ids
                ]
            fragility_curve.cached_fragility_curves = cache
            # for the cases where the measure_id is not found, we shift the curve based on the effect
            # turn df into a dict
            fragility_curve.measure_to_effect = dict(
                zip(
                    df_measures_to_effect["measure_id"], df_measures_to_effect["effect"]
                )
            )
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
        Schema waaraan de fragility curve moet voldoen.

        - failure_mechanism_id: int,
        - measure_id: int,
        - hydraulicload: float,
        - failure_probability: float

    measure_id_to_failuremechanism_id_schema : ClassVar[dict[str, str]]
        Schema waaraan de koppelingstabel tussen measure_id en failuremechanism_id moet voldoen.

        - failure_mechanism_id: int,
        - measure_id: int
    Notes
    -----

    Default waarden te overschrijven in de global variables:

    - default_measure_id: int
        Standaard measure_id die gebruikt wordt als basis fragility curve, standaard 0.

    """

    data_adapter: DataAdapter

    # inherit everything from LoadCachedFragilityCurveOneFailureMechanism
    cache_fragility_curve_schema: ClassVar[dict[str, str]] = {
        "failuremechanism_id": "int",
        "measure_id": "int",
        "hydraulicload": "float",
        "failure_probability": "float",
    }
    measure_to_effect_schema: ClassVar[dict[str, str]] = {
        "measure_id": "int",
        "effect": "float",
    }
    failuremechanism_id_to_measure_id_schema: ClassVar[dict[str, str]] = {
        "failuremechanism_id": "int",
        "measure_id": "int",
    }

    def run(self, input: list[str], output: str, measure_id: int | None = None) -> None:
        """
        Runt de het ophalen van een voorberekende fragility curve voor een enkel vak, voor een meerdere faalmechanisme.

        Parameters
        ----------
        input: list[str]
            - Als string:
                Naam van de input dataadapter: fragility curves,
                hierbij zijn de fragility curves voor een faalmechanisme en dijkvak, maar meerdere measureids.
            - Als list van strings:
                1. Naam van de input dataadapter: fragility curves,
                hierbij zijn de fragility curves voor een faalmechanisme en dijkvak, maar meerdere measureids.
                2. Naam van de input dataadapter: koppelingstabel tussen measure_id en failuremechanism_id

        output: str
            Naam van de dataadapter voor de enkele Fragility curve output.

        measure_id: int | None
            Measure_id van de fragility curve die ingeladen moet worden.

        Raises
        ------
        AssertionError
            Als `input` een string is, maar `measure_id` is None.

        Notes
        -----

        """
        if measure_id is not None:
            assert len(input) == 2, (
                "Als er een measure_id is opgegeven, moeten er precies 2 input data adapters zijn: "
                "1. fragility curves, 2. measures to effect."
            )
            df_fragility_curves = self.data_adapter.input(
                input[0], schema=self.cache_fragility_curve_schema
            )
            df_measures_to_effect = self.data_adapter.input(
                input[1], schema=self.measure_to_effect_schema
            )
            self.df_in, self.df_out = (
                self.retrieve_cache_for_multiple_failure_mechanisms(
                    df_fragility_curves, df_measures_to_effect, measure_id
                )
            )
        else:
            assert len(input) == 3, (
                "Als er geen measure_id is opgegeven, moeten er precies 3 input data adapters zijn: "
                "1. fragility curves, 2. measures to effect, 3. section_id to measure_id."
            )
            df_fragility_curves = self.data_adapter.input(
                input[0], schema=self.cache_fragility_curve_schema
            )
            df_measures_to_effect = self.data_adapter.input(
                input[1], schema=self.measure_to_effect_schema
            )
            df_failuremechanism_to_measure_id = self.data_adapter.input(
                input[2], schema=self.failuremechanism_id_to_measure_id_schema
            )
            self.df_in, self.df_out = (
                self.retrieve_cache_for_multiple_failure_mechanisms(
                    df_fragility_curves,
                    df_measures_to_effect,
                    df_failuremechanism_to_measure_id,
                )
            )

        self.df_out = self.df_out[
            list(self.cache_fragility_curve_schema.keys())
        ]  # fix column order
        self.data_adapter.output(output, self.df_out)

    def retrieve_cache_for_multiple_failure_mechanisms(
        self,
        df_fragility_curves: pd.DataFrame,
        df_measures_to_effect: pd.DataFrame,
        measure_id: int | pd.DataFrame,
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
            if isinstance(measure_id, pd.DataFrame):
                # als er een koppelingstabel is meegegeven, zoek dan de juiste measure_id per faalmechanisme
                corresponding_measure_id = measure_id[
                    measure_id["failuremechanism_id"] == failure_mechanism
                ]["measure_id"].values[0]
            else:
                corresponding_measure_id = measure_id

            inital_curve, selected_curve = self.retrieve_cache(
                df_fragility_curves_per_mechanism,
                df_measures_to_effect,
                corresponding_measure_id,
            )
            selected_curve["failuremechanism_id"] = failure_mechanism
            selected_curve["measure_id"] = corresponding_measure_id

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

    Default waarden te overschrijven in de global variables (let op: `LoadCachedFragilityCurve`):

    - default_measure_id: int
        Standaard measure_id die gebruikt wordt als basis fragility curve, standaard 0.



    """

    data_adapter: DataAdapter
    # inherit everything from LoadCachedFragilityCurve
    cache_fragility_curve_schema: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "failuremechanism_id": "int",
        "measure_id": "int",
        "hydraulicload": "float",
        "failure_probability": "float",
    }
    measure_to_effect_schema: ClassVar[dict[str, str]] = {
        "measure_id": "int",
        "effect": "float",
    }
    section_id_to_measure_id_schema: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "failuremechanism_id": "int",
        "measure_id": "int",
    }

    def run(self, input: list[str], output: str, measure_id: int | None = None) -> None:
        """
        Runt de het ophalen van een voorberekende fragility curve voor  meerdere vakken en voor meerdere faalmechanisme.

        Parameters
        ----------
        input: list[str]
            Naam van de input dataadapter: fragility curves,
            Hierbij zijn de fragility curves voor meerdere faalmechanisme, dijkvak en measure_ids.

        output: str
            Naam van de dataadapter Fragility curve output

        measure_id: int | None
            Measure_id van de fragility curve die ingeladen moet worden.

        Raises
        ------
        AssertionError
            Als `input` een string is, maar `measure_id` is None.


        Notes
        -----

        """
        if measure_id is not None:
            assert len(input) == 2, (
                "Als er een measure_id is opgegeven, moeten er precies 2 input data adapters zijn: "
                "1. fragility curves, 2. measures to effect."
            )
            df_fragility_curves = self.data_adapter.input(
                input[0], schema=self.cache_fragility_curve_schema
            )
            df_measures_to_effect = self.data_adapter.input(
                input[1], schema=self.measure_to_effect_schema
            )
            corresponding_measure_id = measure_id
        else:
            assert len(input) == 3, (
                "Als er geen measure_id is opgegeven, moeten er precies 3 input data adapters zijn: "
                "1. fragility curves, 2. measures to effect, 3. section_id to measure_id."
            )
            df_fragility_curves = self.data_adapter.input(
                input[0], schema=self.cache_fragility_curve_schema
            )
            df_measures_to_effect = self.data_adapter.input(
                input[1], schema=self.measure_to_effect_schema
            )
            df_section_to_measure_id = self.data_adapter.input(
                input[2], schema=self.section_id_to_measure_id_schema
            )
        section_ids = list(df_fragility_curves["section_id"].unique())
        lst_selected_curves = []
        lst_initial_curves = []
        for section in section_ids:
            df_fragility_curves_per_section = df_fragility_curves[
                df_fragility_curves["section_id"] == section
            ]
            if measure_id is None:
                corresponding_measure_id = df_section_to_measure_id[
                    df_section_to_measure_id["section_id"] == section
                ]

            input_per_section, output_per_section = (
                self.retrieve_cache_for_multiple_failure_mechanisms(
                    df_fragility_curves_per_section,
                    df_measures_to_effect,
                    corresponding_measure_id,
                )
            )
            output_per_section["section_id"] = section
            if measure_id is not None:
                output_per_section["measure_id"] = measure_id

            lst_selected_curves.append(output_per_section)
            lst_initial_curves.append(input_per_section)

        self.df_in = pd.concat(lst_initial_curves, ignore_index=True)
        self.df_out = pd.concat(lst_selected_curves, ignore_index=True)
        self.df_out = self.df_out[
            list(self.cache_fragility_curve_schema.keys())
        ]  # fix column order
        self.data_adapter.output(output, self.df_out)
