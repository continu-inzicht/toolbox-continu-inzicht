from typing import ClassVar, Optional
from pydantic.dataclasses import dataclass

import pandas as pd
import geopandas as gpd

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.utils.import_functions import import_folium


@dataclass(config={"arbitrary_types_allowed": True})
class PostProcessFloodRisk(ToolboxBase):
    """
    Met deze functie wordt een representatief scenario bepaald gegeven een belasting.

    Attributes
    ----------
    data_adapter : DataAdapter
        De data adapter die wordt gebruikt om de data in te laden en op te slaan.

    df_in_sections_failure_probability : Optional[pd.DataFrame] | None
        Dataframe met kansen per sectie en per faalmechanisme
    df_in_sections_in_segment : Optional[pd.DataFrame] | None
        Dataframe met secties per deeltraject (segment)
    gdf_in_flood_risk_results_per_segment: Optional[gpd.GeoDataFrame] | None
        GeoDataframe met de risico resultaten per segment
    gdf_out_areas_to_determining_sections : Optional[gpd.GeoDataFrame] | None
        GeoDataframe koppeling tussen gebieden en secties ddie hoogte bijdragen hebben aan de overstromingskans van het gebied
    higheset_risk_section_id_in_segment_store : dict[str, str] | None
        Store voor de sectie id met de hoogste faalkans in een segment, zodat deze niet steeds opnieuw berekend hoeft te worden
    schema_sections_failure_probability : ClassVar[dict[str, str]]
        Schema voor de input dataframe met kansen per sectie en per faalmechanisme
    schema_sections_in_segment : ClassVar[dict[str, str]]
        Schema voor de input dataframe met koppeling van dijkvakken en deeltrajecten (segmenten)
    schema_flood_risk_results_per_segment : ClassVar[dict[str, str]]
        Schema voor de input dataframe met de risico resultaten per segment
    Notes
    -----

    schema voor sections_failure_probability
    - section_id: int
    - failuremechanism_id: int
    - combined_failure_probability: float

    schema voor sections_in_segment
    - section_id: int
    - segment_id: int

    schema voor scenario failure prob segments

    - segment_id: int
    - scenario_failure_probability: float

    """

    data_adapter: DataAdapter

    df_in_sections_failure_probability: Optional[pd.DataFrame] | None = None
    df_in_sections_in_segment: Optional[pd.DataFrame] | None = None
    gdf_in_flood_risk_results_per_segment: Optional[gpd.GeoDataFrame] | None = None
    gdf_out_areas_to_determining_sections: Optional[gpd.GeoDataFrame] | None = None
    higheset_risk_section_id_in_segment_store: dict[str, str] | None = None

    # schemas voor de input dataframes
    schema_sections_failure_probability: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "failuremechanism_id": "int",
        "failure_probability": "float",
    }
    schema_sections_in_segment: ClassVar[dict[str, str]] = {
        "section_id": "int",
        "segment_id": "int",
    }

    schema_flood_risk_results_per_segment: ClassVar[dict[str, str]] = {
        "area_id": "int",
        "geometry": "O",
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

        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("PostProcessFloodRisk", {})
        risk_metric_columns = options.get("risk_metric_columns", [])
        if not len(input) == 4:
            raise UserWarning("Input variabele moet 4 string waarden bevatten.")
        self.df_in_sections_failure_probability = self.data_adapter.input(
            input=input[0],
            schema=self.schema_sections_failure_probability,
        )

        self.df_in_sections_in_segment = self.data_adapter.input(
            input=input[1],
            schema=self.schema_sections_in_segment,
        )

        self.gdf_in_flood_risk_results_per_segment = self.data_adapter.input(
            input=input[2],
            schema=self.schema_flood_risk_results_per_segment,
        )

        # Alleen de gebieden waar een risico is, worden meegenomen in de analyse.
        self.gdf_in_flood_risk_results_per_segment.dropna(inplace=True)
        self.gdf_out_areas_to_determining_sections = (
            self.gdf_in_flood_risk_results_per_segment.copy().drop_duplicates(
                subset=["area_id"]
            )[["area_id", "geometry"]]
        )
        area_ids = self.gdf_out_areas_to_determining_sections["area_id"].unique()
        # Multi index op segment_id en area_id om makkelijk te selecteren
        self.gdf_in_flood_risk_results_per_segment.set_index(
            ["segment_id", "area_id"], inplace=True
        )

        self.gdf_out_areas_to_determining_sections.set_index("area_id", inplace=True)

        # check op de kolomen die de gebruiker opgeeft
        risk_metric_columns_found = [
            col
            for col in risk_metric_columns
            if col in self.gdf_in_flood_risk_results_per_segment.columns
        ]
        if len(risk_metric_columns_found) != len(risk_metric_columns):
            self.data_adapter.logger.warning(
                f"Niet alle risk_metric_columns {risk_metric_columns} zijn aanwezig in de flood_risk_results_per_segment dataframe (index 3). \
                Gevonden columns: {risk_metric_columns_found}"
            )
        if len(risk_metric_columns_found) == 0:
            raise UserWarning(
                f"Geen van de opgegeven {risk_metric_columns=} zijn aanwezig in de flood_risk_results_per_segment dataframe."
            )

        # loop de gebeiden af
        for area_id in area_ids:
            # bepaal welke segmenten bij dit gebied horen.
            # Zie https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.xs.html voor meer uitleg
            segment_area_id_to_consider = self.gdf_in_flood_risk_results_per_segment.xs(
                area_id, level="area_id", drop_level=False
            )
            for risk_metric in risk_metric_columns_found:
                # Bepaal welke van de segmenten de hoogste risco bijdragen heeft
                highest_failure_segment_id = segment_area_id_to_consider[
                    risk_metric
                ].idxmax()[0]  # waarbij 0 de segment_id index is, 1 area_id index is

                # sla dit segment id op
                self.gdf_out_areas_to_determining_sections.loc[
                    area_id, f"segment_id_{risk_metric}"
                ] = highest_failure_segment_id
                # bepaal binnen het traject (segment), welk vak(section) de hoogste faalkans heeft
                # omdat dit vak de grootste bijdrage levert aan het risico in het gebied
                section_id = self.higheset_risk_section_id_in_segment(
                    highest_failure_segment_id
                )
                self.gdf_out_areas_to_determining_sections.loc[
                    area_id, f"section_id_{risk_metric}"
                ] = section_id

        self.gdf_out_areas_to_determining_sections.reset_index(inplace=True)
        self.data_adapter.output(
            output=output, df=self.gdf_out_areas_to_determining_sections
        )

    def higheset_risk_section_id_in_segment(self, segment_id: int) -> int:
        """functie die de sectie id met de hoogste faalkans in een segment bepaald"""

        if self.higheset_risk_section_id_in_segment_store:
            if segment_id in self.higheset_risk_section_id_in_segment_store:
                return self.higheset_risk_section_id_in_segment_store[segment_id]
        else:  # als de dictionary er nog niet is, maak deze dan aan
            self.higheset_risk_section_id_in_segment_store = {}

        df_sections = self.df_in_sections_in_segment[
            self.df_in_sections_in_segment["segment_id"] == segment_id
        ]

        # haal voor die sections de bijbehorende kansen op
        df_prob = self.df_in_sections_failure_probability.copy()

        # filter in the faalkansen alleen de sections die bij dit segment horen
        df_prob_given_sections = df_prob[
            df_prob["section_id"].apply(lambda x: x in df_sections["section_id"].values)
        ].copy()

        # AANNAME:
        # We combineren de faalmechanismes onafhankelijk binnen een vak.
        # Combineer onafhankelijk: P(fail,comb|h) = 1 - PROD(1 - P(fail,i|h)) v
        failure_prod = 1.0

        failure_per_section = {}
        for section_id in df_sections["section_id"].unique():
            # filter df_prob_segment per faalmechanisme
            df_prob_fm = df_prob_given_sections[
                df_prob_given_sections["section_id"] == section_id
            ]["failure_probability"]
            failure_prod *= (1 - df_prob_fm).prod()
            failure_per_section[section_id] = 1 - failure_prod

        # van de waardes in de dictionary, bepaald de hoogte faalkans.
        highest_risk_section_id = max(failure_per_section, key=failure_per_section.get)

        self.higheset_risk_section_id_in_segment_store[segment_id] = (
            highest_risk_section_id
        )
        return highest_risk_section_id

    def make_map(self, risk_metric: str, crs: str = "EPSG:28992"):
        """Helper functie om de geometrieën in een leaflet (folium) kaart te visualiseren.

        parameters
        ----------
        df : pd.DataFrame | gpd.GeoDataFrame
            (Geo)DataFrame met een ``geometry`` kolom en een ``section_id`` kolom.
            Als ``geometry`` WKT-strings bevat wordt deze omgezet naar geometrieën.
        crs : str
            Het bron-coördinatenstelsel van de geometrieën. Standaard RD New
            (EPSG:28992); folium verwacht WGS84, dus de data wordt herprojecteerd.
        """

        folium = import_folium()

        # zorg dat we met een GeoDataFrame in WGS84 werken
        gdf = self.gdf_out_areas_to_determining_sections.copy()
        if f"section_id_{risk_metric}" not in gdf.columns:
            raise ValueError(f"Risk metric '{risk_metric}' is not in the input data.")

        if not isinstance(gdf, gpd.GeoDataFrame):
            from shapely import wkt

            if gdf["geometry"].dtype == object and isinstance(
                gdf["geometry"].iloc[0], str
            ):
                gdf["geometry"] = gdf["geometry"].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=crs)
        elif gdf.crs is None:
            gdf = gdf.set_crs(crs)

        gdf = gdf.to_crs("EPSG:4326")

        # ken iedere section_id een eigen kleur toe via de tab20 kleurenschaal
        from matplotlib import colormaps
        from matplotlib.colors import to_hex

        cmap = colormaps["tab10"]
        section_ids = sorted(gdf[f"section_id_{risk_metric}"].unique())
        color_map = {sid: to_hex(cmap(i % cmap.N)) for i, sid in enumerate(section_ids)}

        # centreer de kaart op de data
        bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
        center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]
        m = folium.Map(location=center, zoom_start=12)

        folium.GeoJson(
            gdf,
            name="secties",
            style_function=lambda feature: {
                "fillColor": color_map[
                    feature["properties"][f"section_id_{risk_metric}"]
                ],
                "color": color_map[feature["properties"][f"section_id_{risk_metric}"]],
                "weight": 1,
                "fillOpacity": 0.6,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=[f"section_id_{risk_metric}"],
                aliases=[f"Sectie ID voor {risk_metric}f:"],
            ),
        ).add_to(m)

        m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

        return m
