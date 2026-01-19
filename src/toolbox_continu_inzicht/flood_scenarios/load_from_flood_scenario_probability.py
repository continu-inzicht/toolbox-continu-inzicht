from typing import ClassVar, Optional
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve


@dataclass(config={"arbitrary_types_allowed": True})
class LoadFromFloodScenarioProbability(ToolboxBase):
    """
    Met deze functie wordt de belasting van een scenario bepaald

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_scenario_failure_probability : Optional[pd.DataFrame] | None
        Dataframe met scenariokansen per deeltraject (segment)
    df_in_section_to_segment : Optional[pd.DataFrame] | None
        Dataframe met koppeling van sectie naar deeltrajecten voor representatieve fragility curves
    df_in_section_fragility_curves : Optional[pd.DataFrame] | None
        Dataframe met fragility curves per sectie en per faalmechanisme
    df_out_scenario_loads : Optional[pd.DataFrame] | None
        Dataframe met belastingen scenario (waarmee gevolgen kunnen worden geselecteerd)
    schema_scenario_failure_probability : ClassVar[dict[str, str]]
        Schema voor de input dataframe met scenariokansen per deeltraject (segment)
    schema_section_to_segment : ClassVar[dict[str, str]]
        Schema voor de input dataframe met koppeling van sectie naar deeltrajecten voor representatieve fragility curves
    schema_section_fragility_curves : ClassVar[dict[str, str]]
        Schema voor de input dataframe met fragility curves per sectie en per faalmechanisme

    Notes
    -----
    schema voor scenario_failure_probability
    - segment_id: int
    - scenario_failure_probability: float

    schema voor sections_to_segment
    - segment_id: int
    - section_id: int
    - length: float

    schema voor section_fragility_curves
    - section_id: int
    - failuremechanism_id: int
    - measure_id: int
    - hydraulicload: float
    - failure_probability: float

    """

    data_adapter: DataAdapter

    df_in_scenario_failure_probability: Optional[pd.DataFrame] | None = None
    df_in_section_to_segment: Optional[pd.DataFrame] | None = None
    df_in_section_fragility_curves: Optional[pd.DataFrame] | None = None
    df_out_scenario_loads: Optional[pd.DataFrame] | None = None

    # schemas voor de input dataframes
    schema_scenario_failure_probability: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "scenario_failure_probability": "float",
    }
    schema_section_to_segment: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "section_id": "int",
        "length": "float",
    }
    schema_section_fragility_curves: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "failuremechanism_id": "int",
        "measure_id": "int",
        "hydraulicload": "float",
        "failure_probability": "float",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Load From Flood Scenario Probability module.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapters
        output: str
            Data adapter voor output van hydraulische belasting per scenario [deeltraject (segment)]
        """

        if not len(input) == 3:
            raise UserWarning("Input variabele moet 3 string waarden bevatten.")

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("LoadFromFloodScenarioProbability", {})
        failuremechanism_id_combined = options.get("failuremechanism_id_combined", 1)

        # scenariokansen per deeltraject (segment)
        self.df_in_scenario_failure_probability = self.data_adapter.input(
            input[0], schema=self.schema_scenario_failure_probability
        )
        # zet index op segment_id
        self.df_in_scenario_failure_probability.set_index("segment_id", inplace=True)

        # koppeling van sectie naar deeltrajecten voor representatieve fragility curves
        self.df_in_section_to_segment = self.data_adapter.input(
            input[1], schema=self.schema_section_to_segment
        )
        df_segment_to_curve = self.df_in_section_to_segment.set_index("segment_id")

        # belasting per moment per meetlocaties
        self.df_in_fragility_curves = self.data_adapter.input(
            input[2], schema=self.schema_section_fragility_curves
        )
        self.df_in_fragility_curves.set_index("section_id", inplace=True)

        segments = self.df_in_section_to_segment["segment_id"].unique()
        load_per_segment = {}
        for segment in segments:
            segment_failure_probability = self.df_in_scenario_failure_probability.loc[
                segment, "scenario_failure_probability"
            ]
            fragility_curve_id = df_segment_to_curve.loc[segment, "section_id"]
            measure_id = df_segment_to_curve.loc[segment, "measure_id"]
            fragility_curves = self.df_in_fragility_curves.loc[fragility_curve_id]
            mechanism_comb_bool = (
                fragility_curves["failuremechanism_id"] == failuremechanism_id_combined
            )
            measure_id_bool = fragility_curves["measure_id"] == measure_id
            fragility_curve_data = fragility_curves[
                mechanism_comb_bool & measure_id_bool
            ]
            if fragility_curve_data.empty:
                raise UserWarning(
                    f"Geen fragility curve data gevonden voor segment {segment} "
                    f"Zorg dat de fragility curves correct zijn ingeladen."
                )
            # fragility curve object aanmaken en vullen met data uit dataframe
            fragility_curve = FragilityCurve(data_adapter=self.data_adapter)
            fragility_curve.lower_limit = (
                1e-500  # FC defailt was 1e-200, we have smaller values here so adjust
            )
            try:
                fragility_curve.from_dataframe(fragility_curve_data)
            except KeyError:
                self.data_adapter.logger.warning(
                    f"Geen fragility curve gevonden voor segment {segment} "
                    f"met fragility_curve_id {fragility_curve_id}. "
                    f"Zorg dat de fragility curves correct zijn ingeladen, en de mapping van segment naar fragility_curve_id klopt."
                )

            # bepaal de hydraulische belasting behorende bij de scenariokans
            hydraulic_load = fragility_curve.hydraulic_load_from_failure_probability(
                segment_failure_probability
            )
            self.data_adapter.logger.debug(
                f"Segment {segment}: failure_probability {segment_failure_probability}, "
                f"hydraulicload {hydraulic_load}"
            )

            load_per_segment[segment] = [hydraulic_load]

        self.df_out_scenario_loads = pd.DataFrame.from_dict(
            load_per_segment, orient="index", columns=["hydraulicload"]
        )
        self.df_out_scenario_loads.index.name = "segment_id"

        self.data_adapter.output(output=output, df=self.df_out_scenario_loads)
