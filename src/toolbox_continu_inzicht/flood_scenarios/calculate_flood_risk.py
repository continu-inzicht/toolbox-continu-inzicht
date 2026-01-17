from pathlib import Path
from typing import ClassVar, Optional
from pydantic.dataclasses import dataclass

import pandas as pd
import numpy as np
import geopandas as gpd

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def import_raster_libraries():
    try:
        from rasterstats import zonal_stats
        import rasterio

    except ImportError:
        zonal_stats = None
        rasterio = None
        raise ImportError(
            "Rasterio or zonalstats is not installed, use the dev pixi environment or install rasterio and rasterstats"
        )
    return zonal_stats, rasterio


@dataclass(config={"arbitrary_types_allowed": True})
class CalculateFloodRisk(ToolboxBase):
    """
    Met deze functie wordt een representatief scenario bepaald gegeven een belasting.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.
    df_in_segment_failure_probability : Optional[pd.DataFrame] | None
        Dataframe met deeltrajectkansen
    df_in_flood_scenario_grids: Optional[pd.DataFrame] | None
        Dataframe met namen van grids per trajectedeel.
    gdf_in_areas_to_average : Optional[gpd.GeoDataFrame] | None
        GeoDataframe met gebieden om te middelen.
    df_out : Optional[pd.DataFrame] | None
        Dataframe met de geselecteerde flood scenario's.
    schema_segment_failure_probability : ClassVar[dict[str, str]]
        Schema voor de input dataframe met deeltrajectkansen
    schema_flood_scenario_grids : ClassVar[dict[str, str]]
        Schema voor de input dataframe met namen van grids per trajectedeel.
    schema_areas_to_average : ClassVar[dict[str, str]]
        Schema voor de input geodataframe met gebieden om te middelen.
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

    df_in_segment_failure_probability: Optional[pd.DataFrame] | None = None
    df_in_flood_scenario_grids: Optional[pd.DataFrame] | None = None
    gdf_in_areas_to_average: Optional[gpd.GeoDataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    schema_segment_failure_probability: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "failure_probability": "float",
    }
    schema_flood_scenario_grids: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "breach_id": "int",
        "hydaulicload_upperboundary": "float",
        # can contain nans so object dtype is used, we dont stricly check on them
        # "casualties_grid": "object",
        # "damage_grid": "object",
        # "flooding_grid": "object",
        # "affected_people_grid": "object",
    }
    schema_areas_to_average: ClassVar[dict[str, str]] = {
        # these are useful later on but not used now
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
        De runner van de Select Flood Scenario From Load module.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapter voor
        output: str
            Data adapter voor output van scenario kansen per deeltraject
        """

        if not len(input) == 3:
            raise UserWarning("Input variabele moet 3 string waarden bevatten.")

        self.df_in_segment_failure_probability = self.data_adapter.input(
            input=input[0],
            schema=self.schema_segment_failure_probability,
        )
        self.df_in_segment_failure_probability.set_index("segment_id", inplace=True)
        self.df_in_flood_scenario_grids = self.data_adapter.input(
            input=input[1],
            schema=self.schema_flood_scenario_grids,
        )
        columns_grid = [
            col.replace("_grid", "")
            for col in self.df_in_flood_scenario_grids.columns
            if col.endswith("_grid")
        ]
        if len(columns_grid) < 1:
            raise UserWarning(
                "Er zijn geen grid kolommen gevonden in de flood scenario grids input, dit moet er minimaal 1 zijn."
            )
        self.gdf_in_areas_to_average = self.data_adapter.input(
            input=input[2],
        )

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("CalculateFloodRisk", {})
        per_hectare = options.get("per_hectare", False)

        user_set_averaging_methods = options.get("averaging_methods", {})
        averaging_methods = {
            "casualties": "sum",
            "damage": "sum",
            "flooding": "median",
            "affected_people": "sum",
        }
        averaging_methods.update(user_set_averaging_methods)
        if not set(columns_grid).issubset(
            set(averaging_methods.keys())
        ):  # check of alle grid columns een averaging method hebben
            raise UserWarning(
                "Niet alle grid kolommen hebben een bijbehorende averging method in de options."
            )
        # type statistics (`mean`, `sum`, `median`, `min`, `max`, `count`)
        #     see: https://pythonhosted.org/rasterstats/manual.html#zonal-statistics
        #     `sum` for risk based grids
        #     `median` for probability based grids

        # TODO: meer flexibiliteit voor grids: waarom niet andere bronnen?
        # 4 opties om het pad naar de scenario grids te bepalen:
        # absoluut
        # relatief onder de data dir
        # relatief tov werkdirectory
        # in de data dir (niet aanbevolen)
        scenario_path_abs = Path(options["scenario_path"])
        scenario_path_rel = global_variables["used_root_dir"] / Path(
            options["scenario_path"]
        )
        scenario_path_cwd_rel = Path.cwd() / Path(options["scenario_path"])
        if scenario_path_abs.is_absolute() and scenario_path_abs.exists():
            self.data_adapter.logger.debug(
                f"Gebruik absolute pad voor scenario grids: {scenario_path_abs}"
            )
            scenario_path = scenario_path_abs

        elif scenario_path_rel.exists():
            self.data_adapter.logger.debug(
                f"Gebruik relatieve pad voor scenario grids: {scenario_path_rel}"
            )
            scenario_path = scenario_path_rel
        elif scenario_path_cwd_rel.exists():
            self.data_adapter.logger.debug(
                f"Gebruik relatieve pad tov werkdirectory voor scenario grids: {scenario_path_cwd_rel}"
            )
            scenario_path = scenario_path_cwd_rel
        else:
            self.data_adapter.logger.info("Gebruik data directory voor scenario grids.")
            scenario_path = global_variables["used_root_dir"]

        # unconventional way to ensure that the libraries are imported only when needed allowing for lighter installations
        zonal_stats, rasterio = import_raster_libraries()
        dict_segments_out = {}
        for _, row in self.df_in_flood_scenario_grids.iterrows():
            # load grids
            # Dynamically create grid_files dict from columns ending with '_grid'
            grid_files = {
                col.replace("_grid", ""): row[col]
                for col in self.df_in_flood_scenario_grids.columns
                if col.endswith("_grid")
            }
            segment_id = row["segment_id"]
            failure_probability_segment = self.df_in_segment_failure_probability.loc[
                segment_id, "failure_probability"
            ]
            dict_segments_out[segment_id] = self.gdf_in_areas_to_average.copy()
            dict_segments_out[segment_id].loc[:, "segment_id"] = segment_id
            for grid_name, grid_file in grid_files.items():
                # slaan over als er een nan in de tif waarde staat.
                if pd.isna(grid_file):
                    continue

                grid_path = scenario_path / grid_file
                if not grid_path.exists():
                    raise UserWarning(f"Grid file {grid_path} bestaat niet.")

                with rasterio.open(grid_path) as src:
                    array_msk = src.read(1, masked=True)
                    affine = src.transform

                # kans vermenigvuldigen met raster
                array_msk *= failure_probability_segment
                stat = averaging_methods[grid_name]
                zs = zonal_stats(
                    vectors=self.gdf_in_areas_to_average["geometry"],
                    raster=array_msk,
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
                    ] / (self.gdf_in_areas_to_average["geometry"].area / 10000)
                else:
                    self.data_adapter.logger.warning(
                        f"Kolom {column_per_hectare} niet gevonden in output dataframe, kan niet omrekenen per hectare."
                    )
        self.data_adapter.output(output=output, df=self.df_out)
