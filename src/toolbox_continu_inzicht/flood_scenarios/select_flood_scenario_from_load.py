from typing import ClassVar, Optional
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class SelectFloodScenarioFromLoad(ToolboxBase):
    """
    Met deze functie wordt een representatief scenario bepaald gegeven een belasting.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_segment_flood_scenario_load : Optional[pd.DataFrame] | None
        Dataframe met belastingen per deeltraject & doobraaklocatie id per deeltraject.
    df_in_flood_scenario_metadata : Optional[pd.DataFrame] | None
        Dataframe met metadata van de Bresen locaties
    df_out : Optional[pd.DataFrame] | None
        Dataframe met de geselecteerde flood scenario's.
    schema_segment_flood_scenario_load : ClassVar[dict[str, str]]
        Schema voor de input dataframe met belastingen & doobraaklocatie id per deeltraject.
    sechema_flood_scenario_metadata : ClassVar[dict[str, str]]
        Schema voor de input dataframe met koppeling van Bresen naar deeltrajecten en maatgevende fragility curves

    Notes
    -----

    schema voor sections_to_segment

    - section_id: int
    - segment_id: int

    schema voor grouped_sections_failure_probability
    - section_id: int
    - failure_probability: float

    """

    data_adapter: DataAdapter

    df_in_segment_flood_scenario_load: Optional[pd.DataFrame] | None = None
    df_in_flood_scenario_metadata: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    schema_segment_flood_scenario_load: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "hydraulicload": "float",
        "breach_id": "int",
    }
    sechema_flood_scenario_metadata: ClassVar[dict[str, str]] = {
        "breach_id": "int",
        "hydaulicload_upperboundary": "float",
        "waterdepth_grid": "str",
        "casualties_grid": "str",
        "damage_grid": "str",
        "flooding_grid": "str",
        "affected_people_grid": "str",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Select Flood Scenario From Load module.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapter voor
        output: str
            Data adapter voor output van scenario kansen per deeltraject
        """

        if not len(input) == 3:
            raise UserWarning("Input variabele moet 2 string waarden bevatten.")

        self.df_in_segment_flood_scenario_load = self.data_adapter.input(
            input[0], schema=self.schema_segment_flood_scenario_load
        )
        # drempelwaarden per meetlocatie``
        self.df_in_flood_scenario_metadata = self.data_adapter.input(
            input[1], schema=self.sechema_flood_scenario_metadata
        )
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("SelectFloodScenarioFromLoad", {})
        return_two_scenarios = options.get("return_two_scenarios", False)

        segments = self.df_in_flood_scenario_metadata["segment_id"].unique()
        self.df_in_segment_flood_scenario_load.set_index("segment_id", inplace=True)
        meta_data = self.df_in_flood_scenario_metadata.copy()
        selected_grids = {}
        for segment in segments:
            segment_series = self.df_in_segment_flood_scenario_load.loc[segment]
            hydraulic_load = segment_series["hydraulicload"]
            breach_id = segment_series["breach_id"]
            current_breach_data = meta_data[meta_data["breach_id"] == breach_id].copy()
            smallest_load = current_breach_data["hydaulicload_upperboundary"].min()
            if hydraulic_load <= smallest_load:
                grids_series = current_breach_data[
                    current_breach_data["hydaulicload_upperboundary"] == smallest_load
                ]
            elif return_two_scenarios:
                pass
            else:
                smaller_loads = (
                    current_breach_data["hydaulicload_upperboundary"] < hydraulic_load
                )
                # current_breach_data[]
                smaller_loads
                pass

            selected_grids[segment] = grids_series.to_numpy()

        self.df_out = pd.DataFrame.from_dict(
            selected_grids, orient="index", columns=["scenario_id"]
        )

        self.data_adapter.output(output=output, df=self.df_out)
