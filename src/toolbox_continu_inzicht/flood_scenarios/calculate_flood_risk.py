from pathlib import Path
from typing import ClassVar, Optional
from pydantic.dataclasses import dataclass

import pandas as pd
import numpy as np
import geopandas as gpd

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def import_raster_librariers():
    try:
        from rasterstats import zonal_stats
        import rasterio

    except ImportError:
        zonal_stats = None
        rasterio = None
        raise ImportError(
            "Folium is not installed, use the dev pixi environment or install folium"
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
    df_in_flood_scenario_grids: Optional[pd.DataFrame] | None
        Dataframe met namen van grids per trajectedeel.
    gdf_in_areas_to_average : Optional[gpd.GeoDataFrame] | None
        GeoDataframe met gebieden om te middelen.
    df_out : Optional[pd.DataFrame] | None
        Dataframe met de geselecteerde flood scenario's.
    sechema_flood_scenario_grids : ClassVar[dict[str, str]]
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

    df_in_flood_scenario_grids: Optional[pd.DataFrame] | None = None
    gdf_in_areas_to_average: Optional[gpd.GeoDataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    sechema_flood_scenario_grids: ClassVar[dict[str, str]] = {
        "segment_id": "int",
        "breach_id": "int",
        "hydaulicload_upperboundary": "float",
        # can contain nans so object dtype is used
        "waterdepth_grid": "object",
        "casualties_grid": "object",
        "damage_grid": "object",
        "flooding_grid": "object",
        "affected_people_grid": "object",
    }
    schema_areas_to_average: ClassVar[dict[str, str]] = {
        # these are useful later on but not used now
        # "id": "int32",
        "name": "object",
        "code": "object",
        "zip": "int32",
        # these are most needed
        "people": "int32",
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

        if not len(input) == 2:
            raise UserWarning("Input variabele moet 2 string waarden bevatten.")

        self.df_in_flood_scenario_grids = self.data_adapter.input(
            input=input[0],
            schema=self.sechema_flood_scenario_grids,
        )
        self.gdf_in_areas_to_average = self.data_adapter.input(
            input=input[1],
        )

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("CalculateFloodRisk", {})
        stat = options.get("averging_method", "mean")
        per_hectare = options.get("per_hectare", True)
        # type statistics (`mean`, `sum`, `median`, `min`, `max`, `count`)
        #     see: https://pythonhosted.org/rasterstats/manual.html#zonal-statistics
        #     `sum` for risk based grids
        #     `median` for probability based grids

        # 3 opties om het pad naar de scenario grids te bepalen:
        # absoluut
        # relatief onder de data dir
        # in de data dir (niet aanbevolen)
        scenario_path_abs = Path(options["scenario_path"])
        scenario_path_rel = global_variables["used_root_dir"] / Path(
            options["scenario_path"]
        )
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
        else:
            self.data_adapter.logger.info("Gebruik data directory voor scenario grids.")
            scenario_path = global_variables["used_root_dir"]

        # unconventional way to ensure that the libraries are imported only when needed allowing for lighter installations
        zonal_stats, rasterio = import_raster_librariers()
        dict_segments_out = {}
        for idx, row in self.df_in_flood_scenario_grids.iterrows():
            # load grids
            grid_files = {
                "waterdepth": row["waterdepth_grid"],
                "casualties": row["casualties_grid"],
                "damage": row["damage_grid"],
                "flooding": row["flooding_grid"],
                "affected_people": row["affected_people_grid"],
            }
            segment = row["segment_id"]
            dict_segments_out[segment] = self.gdf_in_areas_to_average.copy()
            dict_segments_out[segment].loc[:, "segment_id"] = segment
            for grid_name, grid_file in grid_files.items():
                if pd.isna(grid_file):
                    continue

                grid_path = scenario_path / grid_file
                if not grid_path.exists():
                    raise UserWarning(f"Grid file {grid_path} bestaat niet.")

                with rasterio.open(grid_path) as src:
                    array_msk = src.read(1, masked=True)
                    affine = src.transform

                zs = zonal_stats(
                    vectors=self.gdf_in_areas_to_average["geometry"],
                    raster=array_msk.data,
                    affine=affine,
                    stats=[stat],
                    all_touched=False,
                    nodata=np.nan,
                )

                # add the zonal stats to the output geodataframe
                dict_segments_out[segment] = pd.concat(
                    (dict_segments_out[segment], pd.DataFrame(zs)), axis=1
                )
                dict_segments_out[segment].rename(
                    columns={stat: grid_name}, inplace=True
                )

        # TODO: risico brekenene en evnt. omrekenen per hectare
        # omrekenen naar hectaren (later)

        # combine the segments
        self.df_out = pd.concat(dict_segments_out, ignore_index=True)
        if per_hectare:
            self.df_out["risk_per_ha"] = self.df_out["risk"] / (
                self.gdf_in_areas_to_average["geometry"].area / 10000
            )
        self.data_adapter.output(output=output, df=self.df_out)
        return dict_segments_out
