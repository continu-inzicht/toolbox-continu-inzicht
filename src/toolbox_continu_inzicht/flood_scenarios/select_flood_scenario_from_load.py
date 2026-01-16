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
    df_in_scenarios_loads : Optional[pd.DataFrame] | None
        Dataframe met belastingen per scenario/deeltraject (segment)
    df_in_consequences_loads : Optional[pd.DataFrame] | None
        Dataframe met gevolgberekeningen (grids) per hydraulische belastingniveau (voor verschillende type risico's)
    df_out_scenario_consequences_grids : Optional[pd.DataFrame] | None
        Dataframe met de geselecteerde scenario grids.
    schema_scenarios_loads : ClassVar[dict[str, str]]
        Schema voor de input dataframe met belastingen per scenario/deeltraject (segment)
    schema_consequences_loads : ClassVar[dict[str, str]]
        Schema voor de input dataframe met gevolgberekeningen (grids) per hydraulische belastingniveau (voor verschillende type risico's)

    Notes
    -----

    schema voor scenarios_loads
    - segment_id: int
    - hydraulicload: float

    schema voor consequences_loads
    - segment_id: int
    - section_id: int
    - hydraulicload_upperboundary: float
    - waterdepth_grid
    - casualties_grid
    - damage_grid
    - flooding_grid
    - affected_people_grid
    """

    data_adapter: DataAdapter

    df_in_scenarios_loads: Optional[pd.DataFrame] | None = None
    df_in_consequences_loads: Optional[pd.DataFrame] | None = None
    df_out_scenario_consequences_grids: Optional[pd.DataFrame] | None = None

    # schemas voor de input dataframes
    schema_scenarios_loads: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "hydraulicload": "float",
    }
    schema_consequences_loads: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "section_id": "int",
        "hydraulicload_upperboundary": "float",
        # can contain nans so object dtype is used, we dont stricly check on them
        # "waterdepth_grid": "object",
        # "casualties_grid": "object",
        # "damage_grid": "object",
        # "flooding_grid": "object",
        # "affected_people_grid": "object",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Select Flood Scenario From Load module.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapter
        output: str
            Data adapter voor output van gevolgen grids per scenario/deeltraject (segment)
        """

        if not len(input) == 2:
            raise UserWarning("Input variabele moet 2 string waarden bevatten.")

        # hydraulische belastingen per deeltraject (segment) behorende bij scenariokans
        self.df_in_scenarios_loads = self.data_adapter.input(
            input[0], schema=self.schema_scenarios_loads
        )
        # gevolgberekeningen (grids) per hydraulische belastingniveau
        self.df_in_consequences_loads = self.data_adapter.input(
            input[1], schema=self.schema_consequences_loads
        )
        columns_grid = [
            col.replace("_grid", "")
            for col in self.df_in_consequences_loads.columns
            if col.endswith("_grid")
        ]
        if len(columns_grid) < 1:
            raise UserWarning(
                "Er zijn geen grid kolommen gevonden, dit moet er minimaal 1 zijn."
            )

        # lees de opties in
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("SelectFloodScenarioFromLoad", {})
        return_two_scenarios = options.get("return_two_scenarios", False)

        # bepaal de segmenten
        segments = self.df_in_scenarios_loads["segment_id"].unique()
        self.df_in_scenarios_loads.set_index("segment_id", inplace=True)

        # lees de gevolgberekeningen in
        consequences = self.df_in_consequences_loads.copy()

        selected_grids = []
        for segment in segments:
            segment_series = self.df_in_scenarios_loads.loc[segment]
            hydraulic_load = segment_series["hydraulicload"]
            current_consequence_data = consequences[
                consequences["segment_id"] == segment
            ].copy()
            smallest_load = current_consequence_data[
                "hydraulicload_upperboundary"
            ].min()
            if hydraulic_load <= smallest_load:
                # Er wordt altijd vanuit gegaan dat er een hydraulische belasting moet overschreden worden om risico te berekenen (er moet een overstroming/doorbraak zijn), anders is het risico gelijk aan 0
                grids_series = current_consequence_data[
                    current_consequence_data["hydraulicload_upperboundary"]
                    == smallest_load
                ]
            else:
                smaller_loads = (
                    current_consequence_data["hydraulicload_upperboundary"]
                    < hydraulic_load
                )
                subs_selection = current_consequence_data[smaller_loads]
                # standaard de grootste lagere belasting kiezen als representatief scenario
                idx_lowerbound = subs_selection["hydraulicload_upperboundary"].idxmax()
                grids_series = current_consequence_data.loc[[idx_lowerbound]]

                # indien gewenst ook de kleinste hogere belasting kiezen als representatief scenario
                if return_two_scenarios:
                    largest_load = current_consequence_data[
                        "hydraulicload_upperboundary"
                    ].max()
                    if hydraulic_load <= largest_load:
                        larger_loads = (
                            current_consequence_data["hydraulicload_upperboundary"]
                            >= hydraulic_load
                        )
                        larger_sub_selection = current_consequence_data[larger_loads]
                        idx_upperbound = larger_sub_selection[
                            "hydraulicload_upperboundary"
                        ].idxmin()
                        grids_series = current_consequence_data.loc[
                            [idx_lowerbound, idx_upperbound]
                        ]
                    else:
                        pass  # als er geen upper bound is, dan alleen de lower bound teruggeven

            grids_series["segment_id"] = segment
            selected_grids.append(grids_series)

        self.df_out_scenario_consequences_grids = pd.concat(selected_grids, axis=0)

        # herschik de kolommen zodat segment_id vooraan staat
        columns = self.df_out_scenario_consequences_grids.columns.tolist()
        columns.remove("segment_id")
        columns = ["segment_id"] + columns
        self.df_out_scenario_consequences_grids = (
            self.df_out_scenario_consequences_grids[columns]
        )

        self.data_adapter.output(
            output=output, df=self.df_out_scenario_consequences_grids
        )
