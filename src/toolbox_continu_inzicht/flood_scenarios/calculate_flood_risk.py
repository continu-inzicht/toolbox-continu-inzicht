from typing import ClassVar, Optional
from pydantic.dataclasses import dataclass

import pandas as pd
import numpy as np
import geopandas as gpd

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def import_rasterstats():
    try:
        from rasterstats import zonal_stats
    except ImportError:
        zonal_stats = None
        raise ImportError(
            "Rasterio or zonalstats is not installed, use the dev pixi environment or install rasterio and rasterstats"
        )
    return zonal_stats


@dataclass(config={"arbitrary_types_allowed": True})
class CalculateFloodRisk(ToolboxBase):
    """
    Met deze functie wordt een representatief scenario bepaald gegeven een belasting.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_scenario_failure_prob_segments : Optional[pd.DataFrame] | None
        Dataframe met scenariokansen
    df_in_scenario_consequences_grids: Optional[pd.DataFrame] | None
        Dataframe met gevolgengrids behorende bij de scenariokansen per deeltraject (segment)
    gdf_in_areas_to_aggregate : Optional[gpd.GeoDataFrame] | None
        GeoDataframe met gebieden te aggregeren
    df_out_flood_risk_results : Optional[pd.DataFrame] | None
        Dataframe met de risico resultaten
    schema_scenario_failure_prob_segments : ClassVar[dict[str, str]]
        Schema voor de input dataframe met deeltrajectkansen
    schema_scenario_consequences_grids : ClassVar[dict[str, str]]
        Schema voor de input dataframe met gevolgengrids behorende bij de scenariokansen per deeltraject (segment)
    schema_areas_to_aggregate : ClassVar[dict[str, str]]
        Schema voor de input geodataframe met gebieden te aggregeren.


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

    df_in_scenario_failure_prob_segments: Optional[pd.DataFrame] | None = None
    df_in_scenario_consequences_grids: Optional[pd.DataFrame] | None = None
    gdf_in_areas_to_aggregate: Optional[gpd.GeoDataFrame] | None = None
    df_out_flood_risk_results: Optional[pd.DataFrame] | None = None

    # schemas voor de input dataframes
    schema_scenario_failure_prob_segments: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "scenario_failure_probability": "float",
    }
    schema_scenario_consequences_grids: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "section_id": "int",
        "hydraulicload_upperboundary": "float",
        ## Kunnen NAN waarden bevatten en daarom wordt het object dtype gebruikt, we controleren hier niet strikt op
        ## Gebruikers kunnen zelf aangeven welke grids ze willen gebruiken, dus express een niet stricte  controle
        # "waterdepth_grid": "object",
        # "casualties_grid": "object",
        # "damage_grid": "object",
        # "flooding_grid": "object",
        # "affected_people_grid": "object",
    }
    schema_areas_to_aggregate: ClassVar[dict[str, str]] = {
        #### these are useful later on but not used now
        "area_id": "int32",
        # "name": "object",
        # "code": "object",
        # "zip": "int32",
        # these are most needed
        # "people": "int32",
        "geometry": "geometry",
    }

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Calculate Flood Risk module.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapter
        output: str
            Data adapter voor output van overstromingsrisico resultaten
        """

        if not len(input) == 4:
            raise UserWarning("Input variabele moet 4 string waarden bevatten.")

        # inladen van scenariokansen per deeltraject (segment)
        self.df_in_scenario_failure_prob_segments = self.data_adapter.input(
            input=input[0],
            schema=self.schema_scenario_failure_prob_segments,
        )
        self.df_in_scenario_failure_prob_segments.set_index("segment_id", inplace=True)

        # inladen van gevolgengrids behorende bij de scenariokansen per deeltraject (segment)
        self.df_in_scenario_consequences_grids = self.data_adapter.input(
            input=input[1],
            schema=self.schema_scenario_consequences_grids,
        )
        columns_grid = [
            col.replace("_grid", "")
            for col in self.df_in_scenario_consequences_grids.columns
            if col.endswith("_grid")
        ]
        if len(columns_grid) < 1:
            raise UserWarning(
                "Er zijn geen grid kolommen gevonden in de flood scenario grids input, dit moet er minimaal 1 zijn."
            )

        # inladen van gebieden om te aggregeren
        self.gdf_in_areas_to_aggregate = self.data_adapter.input(
            input=input[2],
        )

        # lees de opties in
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("CalculateFloodRisk", {})
        per_hectare = options.get(
            "per_hectare", False
        )  # de mogelijkheid om de output per hectare te krijgen TODO: nog niet getest

        # stel de aggregatie methodes in
        # TODO nog methode voor plaatsgebonden kansen toevoegen
        user_set_aggregate_methods = options.get("aggregate_methods", {})
        aggregate_methods = {
            "casualties": "sum",
            "damage": "sum",
            "flooding": "median",
            "affected_people": "sum",
            "waterdepth": "sum",
        }
        aggregate_methods.update(user_set_aggregate_methods)
        if not set(columns_grid).issubset(
            set(aggregate_methods.keys())
        ):  # controleer of alle grid columns een aggregate methode hebben
            raise UserWarning(
                "Niet alle grid kolommen hebben een bijbehorende aggregate methode in de options."
            )
        # type statistics (`mean`, `sum`, `median`, `min`, `max`, `count`)
        #     see: https://pythonhosted.org/rasterstats/manual.html#zonal-statistics
        #     `sum` for risk based grids
        #     `median` for probability based grids

        # onconventionele manier om ervoor te zorgen dat de bibliotheken alleen worden geïmporteerd wanneer dat nodig is, wat lichtere installaties mogelijk maakt
        zonal_stats = import_rasterstats()
        dict_segments_out = {}
        for _, row in self.df_in_scenario_consequences_grids.iterrows():
            # Grids met gevolgen laden
            # Dynamisch grid_files dict maken van kolommen die eindigen op '_grid'
            grid_files = {
                col.replace("_grid", ""): row[col]
                for col in self.df_in_scenario_consequences_grids.columns
                if col.endswith("_grid")
            }

            # scenariokans voor segment ophalen
            segment_id = row["segment_id"]
            failure_probability_segment = self.df_in_scenario_failure_prob_segments.loc[
                segment_id, "scenario_failure_probability"
            ]

            # maak een kopie van de gebieden om te aggregeren voor dit segment
            dict_segments_out[segment_id] = self.gdf_in_areas_to_aggregate.copy()
            dict_segments_out[segment_id].loc[:, "segment_id"] = segment_id

            # loop door alle grids en bereken het risico
            for grid_name, grid_file in grid_files.items():
                # sla over als er een NAN waarde is voor het grid bestand (dan geen risico berekenen)
                if pd.isna(grid_file):
                    continue
                self.data_adapter.config.data_adapters[input[3]]["grid_file"] = (
                    grid_file
                )

                array_masked_grid, affine = self.data_adapter.input(
                    input=input[3],
                )
                assert np.issubdtype(array_masked_grid.data.dtype, np.floating), (
                    "De grid data moet van float type zijn, zorg dat dit afgevangen wordt in de data adapter."
                )

                # kans vermenigvuldigen met raster
                array_masked_grid *= failure_probability_segment
                stat = aggregate_methods[grid_name]
                zs = zonal_stats(
                    vectors=self.gdf_in_areas_to_aggregate["geometry"],
                    raster=array_masked_grid,
                    affine=affine,
                    stats=[stat],
                    all_touched=False,
                    nodata=np.nan,
                )

                # add the zonal stats to the output geodataframe
                dict_segments_out[segment_id] = pd.concat(
                    (dict_segments_out[segment_id], pd.DataFrame(zs)), axis=1
                )
                dict_segments_out[segment_id].rename(
                    columns={stat: grid_name}, inplace=True
                )
                dict_segments_out[segment_id].set_index("area_id")

        # Concatenate all segment dataframes
        all_segments_df = pd.concat(dict_segments_out.values(), ignore_index=True)

        # Identify numeric columns to sum (excluding geometry and identifier columns)
        exclude_cols = [
            "geometry",
            "area_id",
            "name",
            "code",
            "zip",
            "people",
            "segment_id",
        ]
        numeric_cols = all_segments_df.select_dtypes(
            include=["float64", "float32", "int64", "int32"]
        ).columns
        numeric_cols = [col for col in numeric_cols if col not in exclude_cols]

        # Group by area_id and aggregate
        agg_dict = {col: "sum" for col in numeric_cols}
        # Keep first value for non-numeric columns
        for col in ["name", "code", "zip", "people", "geometry"]:
            if col in all_segments_df.columns:
                agg_dict[col] = "first"

        df_out = all_segments_df.groupby("area_id", as_index=False).agg(agg_dict)
        self.df_out = gpd.GeoDataFrame(df_out)

        # TODO: risico berekenen en evnt. omrekenen per hectare
        # omrekenen naar hectaren (later)
        self.data_adapter.logger.debug(f"Per hectare ingesteld op: {per_hectare}")
        self.data_adapter.logger.debug(f"opties: {options}")
        if per_hectare:
            if "columns_per_hectare" not in options:
                raise UserWarning(
                    "Als per_hectare is ingesteld op True, moet ook columns_per_hectare in de options worden opgegeven."
                )
            columns_per_hectare = options["columns_per_hectare"]
            if not isinstance(columns_per_hectare, list):
                raise UserWarning(
                    "In de options moet bij per_hectare ook columns_per_hectare opgegeven worden als lijst van kolomnamen."
                )
            for column_per_hectare in columns_per_hectare:
                if column_per_hectare in self.df_out.columns:
                    self.df_out[f"{column_per_hectare}_per_ha"] = self.df_out[
                        column_per_hectare
                    ] / (self.gdf_in_areas_to_aggregate["geometry"].area / 10000)
                else:
                    self.data_adapter.logger.warning(
                        f"Kolom {column_per_hectare} niet gevonden in output dataframe, kan niet omrekenen per hectare."
                    )
        self.data_adapter.output(output=output, df=self.df_out)
