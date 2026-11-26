import time
import warnings
from datetime import timedelta, timezone

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from geolib.models.dstability.internal import (
    AnalysisTypeEnum,
    CalculationSettings,
    Scenario,
    Stage,
)
from matplotlib.patches import Polygon
from pydantic.dataclasses import dataclass
from shapely.geometry import LineString, Point, Polygon as ShapelyPolygon
from shapely.ops import unary_union

from toolbox_continu_inzicht.base.adapters.input.dam_live.stix import (
    StixCollection,
    StixFile,
)
from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter

# The toolbox writes DamLive's input time series with `<timeZone>1.0</timeZone>`;
# DamLive writes its output time series and result file names in that time zone.
DAMLIVE_TIMEZONE = timezone(timedelta(hours=1))

STAGE_COLUMNS = ["stage_id", "stage_label", "scenario_id", "scenario_label"]
MERGED_SOIL_COLUMNS = STAGE_COLUMNS + [
    "geometry_id",
    "layer_id",
    "layer_label",
    "points",
    "soillayers_id",
    "soil_id",
    "name",
    "code",
]
MERGED_WATERNET_COLUMNS = STAGE_COLUMNS + [
    "waternet_id",
    "line_type",
    "line_id",
    "line_label",
    "x",
    "z",
    "top_headline_id",
    "bottom_headline_id",
    "content_version",
]
MERGED_CALCULATIONS_COLUMNS = STAGE_COLUMNS + [
    "calculationsettings_id",
    "analysis_type",
    "calculation_type",
    "model_factor_mean",
    "model_factor_std",
    "circle_center_x",
    "circle_center_z",
    "circle_radius",
]


@dataclass(config={"arbitrary_types_allowed": True})
class CombineDamLiveResults(ToolboxBase):
    """
    Zet één D-Stability `.stix` bestand, zoals een DamLive resultaat, om naar tabellen
    met grondlagen, waterspanningslijnen en glijcirkels, en plot een stage.

    Attributes
    ----------
    data_adapter: DataAdapter
        Data adapter voor in- en output
    stix: StixFile | None
        Het gekozen `.stix` bestand
    df_merged_soils: pd.DataFrame | None
        Grondlagen per stage, met de punten en de grondsoort van elke laag
    df_merged_waternet: pd.DataFrame | None
        Punten van de stijghoogte- en referentielijnen per stage
    df_merged_calculations: pd.DataFrame | None
        Rekeninstellingen per stage, één rij per glijcirkel
    df_colors: pd.DataFrame | None
        Kleuren per grondsoort en waterlijn
    soil_color_map: dict[str, str]
        Kleur per grondsoortnaam
    water_color_map: dict[str, str]
        Kleur per waterlijnnaam
    """

    data_adapter: DataAdapter

    stix: StixFile | None = None
    df_merged_soils: pd.DataFrame | None = None
    df_merged_waternet: pd.DataFrame | None = None
    df_merged_calculations: pd.DataFrame | None = None
    df_colors: pd.DataFrame | None = None
    soil_color_map: dict[str, str] = None
    water_color_map: dict[str, str] = None

    def run(self, input: list[str], output: list[str]) -> None:
        """
        Leest een `.stix` bestand via de DataAdapter, bouwt de tabellen en schrijft
        deze naar de opgegeven outputs.

        Bevat de input meerdere `.stix` bestanden, dan wordt er één gekozen met de
        opties `location` en `timestamp`, en eventueel `calculation_model`, onder
        `CombineDamLiveResults` in de GlobalVariables.

        Parameters
        ----------
        input: list[str]
            Namen van DataAdapter-inputs: [stix, colors]. `stix` is van type
            `damlive_stix`, `colors` een tabel met de kolommen type, name en color.
        output: list[str]
            Namen van DataAdapter-outputs: [merged_soils, merged_waternet,
            merged_calculations], optioneel gevolgd door [damlive_soil,
            damlive_soil_color].

        Raises
        ------
        UserWarning
            Als de stix-input niet van type `damlive_stix` is, of als geen of meerdere
            bestanden bij de opties passen.
        """
        stix_files = self.data_adapter.input(input[0])
        if not isinstance(stix_files, StixCollection):
            raise UserWarning(
                f"Input '{input[0]}' moet van type `damlive_stix` zijn, "
                f"niet {type(stix_files).__name__}"
            )
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("CombineDamLiveResults", {})
        self.stix = self.select_stix(stix_files, options)

        self.df_merged_soils = self.merge_geometries_soils()
        self.df_merged_waternet = self.merge_waternet()
        self.df_merged_calculations = self.merge_calculationsettings()

        self.df_colors = self.data_adapter.input(input[1])
        self.soil_color_map = (
            self.df_colors[self.df_colors["type"] == "soil"]
            .set_index("name")["color"]
            .to_dict()
        )

        self.water_color_map = (
            self.df_colors[self.df_colors["type"] == "water"]
            .set_index("name")["color"]
            .to_dict()
        )

        self.data_adapter.output(output[0], self.df_merged_soils)
        self.data_adapter.output(output[1], self.df_merged_waternet)
        self.data_adapter.output(output[2], self.df_merged_calculations)
        if len(output) > 3:
            self.data_adapter.output(output[3], self.create_df_damlive_soil())
            self.data_adapter.output(output[4], self.create_df_damlive_soil_color())

    @staticmethod
    def select_stix(stix_files: StixCollection, options: dict) -> StixFile:
        """
        Kies het `.stix` bestand: met `location` en `timestamp`, en eventueel
        `calculation_model`, uit de opties, of zonder opties het enige bestand.

        Parameters
        ----------
        stix_files: StixCollection
            De ingelezen `.stix` bestanden
        options: dict
            Opties van `CombineDamLiveResults` uit de GlobalVariables

        Returns
        -------
        StixFile
            Het gekozen `.stix` bestand

        Raises
        ------
        UserWarning
            Als geen of meerdere bestanden passen.
        """
        if "location" in options or "timestamp" in options:
            try:
                return stix_files.select(
                    options.get("location"),
                    options.get("timestamp"),
                    options.get("calculation_model"),
                )
            except KeyError as e:
                raise UserWarning(e.args[0]) from e
        if len(stix_files) == 1:
            return stix_files[next(iter(stix_files))]
        raise UserWarning(
            f"De input bevat {len(stix_files)} stix-bestanden; kies er één met "
            "`location` en `timestamp` onder `CombineDamLiveResults` in de GlobalVariables"
        )

    def merge_calculationsettings(self) -> pd.DataFrame:
        """
        Rekeninstellingen per stage: voor elke berekening in het scenario van de stage
        de instellingen, met één rij per glijcirkel (Bishop één, UpliftVan twee).
        """
        rows = []
        for scenario, stage in self.stix.stages():
            for calculation in scenario.Calculations:
                settings = self.stix.settings_of(calculation)
                for center_x, center_z, radius in _circles(settings):
                    rows.append(
                        {
                            **_stage_row(scenario, stage),
                            "calculationsettings_id": settings.Id,
                            "analysis_type": settings.AnalysisType.value,
                            "calculation_type": settings.CalculationType.value,
                            "model_factor_mean": settings.ModelFactorMean,
                            "model_factor_std": settings.ModelFactorStandardDeviation,
                            "circle_center_x": center_x,
                            "circle_center_z": center_z,
                            "circle_radius": radius,
                        }
                    )
        return pd.DataFrame(rows, columns=MERGED_CALCULATIONS_COLUMNS)

    def merge_geometries_soils(self) -> pd.DataFrame:
        """
        Grondlagen per stage: de lagen uit de geometrie van de stage, met de
        grondsoort uit de eigen grondlagenverzameling van de stage.
        """
        rows = []
        for scenario, stage in self.stix.stages():
            for layer in self.stix.geometry_of(stage).Layers:
                soil = self.stix.soil_of_layer(stage, layer.Id)
                rows.append(
                    {
                        **_stage_row(scenario, stage),
                        "geometry_id": stage.GeometryId,
                        "layer_id": layer.Id,
                        "layer_label": layer.Label,
                        "points": [point.model_dump() for point in layer.Points],
                        "soillayers_id": stage.SoilLayersId,
                        "soil_id": soil.Id if soil else None,
                        "name": soil.Name if soil else None,
                        "code": soil.Code if soil else None,
                    }
                )
        return pd.DataFrame(rows, columns=MERGED_SOIL_COLUMNS)

    def merge_waternet(self) -> pd.DataFrame:
        """
        Punten van de stijghoogte- en referentielijnen uit het waternet van elke stage.
        """
        rows = []
        for scenario, stage in self.stix.stages():
            waternet = self.stix.waternet_of(stage)
            lines = [("Head", line) for line in waternet.HeadLines]
            lines += [("Reference", line) for line in waternet.ReferenceLines]
            for line_type, line in lines:
                for point in line.Points or []:
                    rows.append(
                        {
                            **_stage_row(scenario, stage),
                            "waternet_id": waternet.Id,
                            "line_type": line_type,
                            "line_id": line.Id,
                            "line_label": line.Label,
                            "x": point.X,
                            "z": point.Z,
                            # only reference lines lie between head lines
                            "top_headline_id": getattr(line, "TopHeadLineId", None),
                            "bottom_headline_id": getattr(
                                line, "BottomHeadLineId", None
                            ),
                            "content_version": waternet.ContentVersion,
                        }
                    )
        df_merged = pd.DataFrame(rows, columns=MERGED_WATERNET_COLUMNS)

        # Sorteren zodat lijnen netjes doorlopen
        df_merged = df_merged.sort_values(
            by=["stage_id", "line_type", "line_id", "x"]
        ).reset_index(drop=True)

        return df_merged

    def _circle_arc(self, cx, cz, r, theta_start, theta_end, n=200):
        angles = np.linspace(theta_start, theta_end, n)
        xs = cx + r * np.cos(angles)
        zs = cz + r * np.sin(angles)
        return LineString(zip(xs, zs))

    def _clip_arc_to_soil(self, arc, soil_union):
        coords = list(arc.coords)
        filtered_coords = []

        inside = False

        for x, z in coords:
            point = Point(x, z)

            in_soil = soil_union.buffer(1e-6).intersects(point)

            if in_soil and not inside:
                inside = True
                filtered_coords.append((x, z))

            elif in_soil and inside:
                filtered_coords.append((x, z))

            elif not in_soil and inside:
                break

        return filtered_coords

    def plot_stage(self, stage_id, xlim, ylim):
        """
        Plot de geometrie van een stage inclusief soils,
        waterlijnen en glijcirkels.
        """

        stage_id = str(stage_id)

        df_stage_soils = self.df_merged_soils[
            self.df_merged_soils["stage_id"] == stage_id
        ]

        df_stage_waternets = self.df_merged_waternet[
            self.df_merged_waternet["stage_id"] == stage_id
        ]

        df_stage_calculations = self.df_merged_calculations[
            self.df_merged_calculations["stage_id"] == stage_id
        ]

        if df_stage_soils.empty:
            raise ValueError(f"Geen soil data gevonden voor stage {stage_id}")

        fig, ax = plt.subplots(figsize=(15, 15))

        # --------------------
        # SOILS
        # --------------------
        plotted_soils = {}
        soil_geoms = []
        for _, row in df_stage_soils.iterrows():
            points = row["points"]
            if not points:
                continue

            polygon_coords = [(p["X"], p["Z"]) for p in points]
            if polygon_coords[0] != polygon_coords[-1]:
                polygon_coords.append(polygon_coords[0])
            soil_geoms.append(ShapelyPolygon(polygon_coords))
            soil_name = row["name"]

            if soil_name in self.soil_color_map:
                color = self.soil_color_map[soil_name]
            else:
                warnings.warn(
                    f"Geen kleur gevonden voor soil '{soil_name}' in color map.",
                    stacklevel=2,
                )
                color = "#cccccc"  # fallback grijs

            poly = Polygon(
                polygon_coords,
                closed=True,
                facecolor=color,
                edgecolor="k",
                alpha=1,
            )
            ax.add_patch(poly)

            soil_name = row["name"]
            if soil_name not in plotted_soils:
                plotted_soils[soil_name] = color

        soil_union = unary_union(soil_geoms)
        # --------------------
        # WATERLIJNEN
        # --------------------
        plotted_lines = {}

        for line_id, df_line in df_stage_waternets.groupby("line_id"):
            xs = df_line["x"].tolist()
            zs = df_line["z"].tolist()

            line_label = df_line["line_label"].iloc[0]

            if line_label in self.water_color_map:
                color = self.water_color_map[line_label]
            else:
                warnings.warn(
                    f"Geen kleur gevonden voor waterlijn '{line_label}' in color map. Gebruik fallbackkleur grijs.",
                    stacklevel=2,
                )
                color = "#cccccc"  # fallback grijs

            ax.plot(xs, zs, color=color, linewidth=2)

            if line_label not in plotted_lines:
                plotted_lines[line_label] = color

        # --------------------
        # GLIJCIRKELS
        # --------------------

        circles = df_stage_calculations.dropna(
            subset=["circle_center_x", "circle_center_z"]
        )

        circles = circles.sort_values("circle_center_x")

        if len(circles) == 1:
            row = circles.iloc[0]

            cx = row["circle_center_x"]
            cz = row["circle_center_z"]
            r = row["circle_radius"]

            if pd.notna(r):
                arc = self._circle_arc(cx, cz, r, -np.pi, 0)
                filtered_coords = self._clip_arc_to_soil(arc, soil_union)

                if len(filtered_coords) > 1:
                    xs, zs = zip(*filtered_coords)
                    ax.plot(xs, zs, color="red", linewidth=2)

        elif len(circles) == 2:
            row1 = circles.iloc[0]
            row2 = circles.iloc[1]

            cx1 = row1["circle_center_x"]
            cz1 = row1["circle_center_z"]
            r1 = row1["circle_radius"]

            cx2 = row2["circle_center_x"]
            cz2 = row2["circle_center_z"]

            if pd.notna(r1):
                # diepste punt cirkel 1
                z_tangent = cz1 - r1

                # radius cirkel 2
                r2 = cz2 - z_tangent

                # cirkel 1 (links naar tangent)
                arc1 = self._circle_arc(cx1, cz1, r1, -np.pi, -np.pi / 2)

                # tangent
                tangent = LineString([(cx1, z_tangent), (cx2, z_tangent)])

                # cirkel 2 (van tangent omhoog)
                arc2 = self._circle_arc(cx2, cz2, r2, -np.pi / 2, 0)

                # cirkel 1
                filtered_coords1 = self._clip_arc_to_soil(arc1, soil_union)
                if len(filtered_coords1) > 1:
                    xs, zs = zip(*filtered_coords1)
                    ax.plot(xs, zs, color="red", linewidth=2)

                # tangent
                xs, zs = tangent.xy
                ax.plot(xs, zs, color="red", linewidth=2)

                # cirkel 2
                filtered_coords2 = self._clip_arc_to_soil(arc2, soil_union)
                if len(filtered_coords2) > 1:
                    xs, zs = zip(*filtered_coords2)
                    ax.plot(xs, zs, color="red", linewidth=2)

        # --------------------
        # LEGENDS
        # --------------------

        # --- Soils legend ---
        soil_legend = None
        if plotted_soils:
            soil_handles = [
                plt.Line2D([0], [0], color=c, lw=10) for c in plotted_soils.values()
            ]
            soil_legend = ax.legend(
                soil_handles,
                plotted_soils.keys(),
                title="Soil type",
                loc="upper right",
                bbox_to_anchor=(1.0, 1.0),
            )
            ax.add_artist(soil_legend)

        # --- Water legend ---
        water_legend = None
        if plotted_lines:
            line_handles = [
                plt.Line2D([0], [0], color=c, lw=2) for c in plotted_lines.values()
            ]
            water_legend = ax.legend(
                line_handles,
                plotted_lines.keys(),
                title="Water lines",
                loc="upper right",
                bbox_to_anchor=(1.0, 0.5),
            )
            ax.add_artist(water_legend)

        # --- Slip surface legend ---
        circle_legend = None

        if not circles.empty:
            circle_handles = [plt.Line2D([0], [0], color="red", lw=2)]

            circle_labels = ["Slip surface"]

            circle_legend = ax.legend(
                circle_handles,
                circle_labels,
                title="Slip Surface",
                loc="upper right",
                bbox_to_anchor=(1.0, 0.15),
            )

            ax.add_artist(circle_legend)

        ax.set_xlim(xlim)
        ax.set_ylim(ylim)
        ax.set_xlabel("X")
        ax.set_ylabel("Z")
        ax.set_title(f"Stage {stage_id}")
        ax.set_aspect("equal")
        plt.grid(True)
        plt.tight_layout()
        return fig, ax

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer df_soil vanuit de DAMlive dataframes
    # ---------------------------------------------------------------------------

    def create_df_damlive_soil(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_soil``.

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_soil te schrijven, met kolommen:
            measuringstationid, parameterid, itemid, layername, soils.color,
            datetime, index, x, y, z
        """
        measuringstation_id: int = 1
        parameter_id: int = 200

        # Rekenmoment van DamLive in epoch milliseconden (since 1970-01-01 UTC), zodat
        # de tabel bij dezelfde invoer gelijk blijft. Zonder DamLive-tijd in de
        # bestandsnaam wordt de huidige tijd gebruikt.
        if self.stix.timestamp is not None:
            moment = self.stix.timestamp.replace(tzinfo=DAMLIVE_TIMEZONE)
            epoch_ms = int(moment.timestamp() * 1000)
        else:
            epoch_ms = int(time.time() * 1000)

        rijen: list[dict] = []

        for _, stage in self.stix.stages():
            for layer in self.stix.geometry_of(stage).Layers:
                soil = self.stix.soil_of_layer(stage, layer.Id)
                if soil is None:
                    # Geen grondsoort gevonden; laag overslaan
                    continue

                # Genereer één rij per punt in de geometrie
                for punt_index, punt in enumerate(layer.Points):
                    rijen.append(
                        {
                            "measuringstationid": measuringstation_id,
                            "parameterid": parameter_id,
                            "itemid": int(layer.Id),  # uniek ID per laag
                            "layername": soil.Name,
                            "soils.color": None,  # leeglaten conform specificatie
                            "datetime": epoch_ms,
                            "index": punt_index,
                            "x": float(punt.X),
                            "y": 0.0,  # altijd 0 (2D-profiel)
                            "z": float(punt.Z),
                        }
                    )

        df_soil = pd.DataFrame(rijen)

        # Zorg voor het juiste kolomtype
        if not df_soil.empty:
            df_soil["measuringstationid"] = df_soil["measuringstationid"].astype(
                "int64"
            )
            df_soil["parameterid"] = df_soil["parameterid"].astype("int64")
            df_soil["itemid"] = df_soil["itemid"].astype("int64")
            df_soil["datetime"] = df_soil["datetime"].astype("int64")
            df_soil["index"] = df_soil["index"].astype("int64")
            df_soil["x"] = df_soil["x"].astype("float64")
            df_soil["y"] = df_soil["y"].astype("float64")
            df_soil["z"] = df_soil["z"].astype("float64")

        return df_soil

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer df_soil_color vanuit colors DataFrame
    # ---------------------------------------------------------------------------

    def create_df_damlive_soil_color(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_soil_color``.

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_soil_color te schrijven, met kolommen:
            soil_name, r, g, b, color, stb_name

        Raises
        ------
        ValueError
            Als er geen rijen met type='soil' gevonden worden in het colors DataFrame.
        """
        # Filter op rijen waar type == 'soil'
        df_soil_colors = self.df_colors[self.df_colors["type"] == "soil"].copy()

        if df_soil_colors.empty:
            raise ValueError(
                "Geen rijen met type='soil' gevonden in het colors DataFrame. "
                "Controleer of de kolom 'type' correct is gevuld."
            )

        rijen: list[dict] = []
        for _, rij in df_soil_colors.iterrows():
            hex_code = str(rij["color"]).strip()
            r, g, b = _hex_naar_rgb(hex_code)

            rijen.append(
                {
                    "soil_name": rij["name"],
                    "r": r,
                    "g": g,
                    "b": b,
                    "color": hex_code if hex_code.startswith("#") else f"#{hex_code}",
                    "stb_name": rij["name"],  # zelfde waarde als soil_name
                }
            )

        df_color = pd.DataFrame(rijen)

        if not df_color.empty:
            df_color["r"] = df_color["r"].astype("int64")
            df_color["g"] = df_color["g"].astype("int64")
            df_color["b"] = df_color["b"].astype("int64")

        return df_color


# TODO verplaats naar utils
def _hex_naar_rgb(hex_code: str) -> tuple[int, int, int]:
    """Converteer HEX kleurcode naar (R, G, B) integers."""
    hex_code = str(hex_code).strip().lstrip("#")
    if len(hex_code) != 6:
        return (128, 128, 128)  # fallback grijs bij ongeldige code
    r = int(hex_code[0:2], 16)
    g = int(hex_code[2:4], 16)
    b = int(hex_code[4:6], 16)
    return r, g, b


def _stage_row(scenario: Scenario, stage: Stage) -> dict:
    """De stage-kolommen die elke tabel begint."""
    return {
        "stage_id": stage.Id,
        "stage_label": stage.Label,
        "scenario_id": scenario.Id,
        "scenario_label": scenario.Label,
    }


def _circles(settings: CalculationSettings) -> list[tuple]:
    """De glijcirkels (center x, center z, straal) die DamLive in de instellingen schreef.

    Bishop heeft één cirkel. UpliftVan heeft er twee, waarvan alleen de eerste een
    straal heeft: de tweede raakt dezelfde horizontale lijn (zie `plot_stage`).
    Andere analysetypes geven geen cirkels.
    """
    circles = []
    if (
        settings.AnalysisType == AnalysisTypeEnum.BISHOP
        and settings.Bishop
        and settings.Bishop.Circle
    ):
        circle = settings.Bishop.Circle
        circles.append((circle.Center.X, circle.Center.Z, circle.Radius))
    elif settings.AnalysisType == AnalysisTypeEnum.UPLIFT_VAN and settings.UpliftVan:
        plane = settings.UpliftVan.SlipPlane
        if plane and plane.FirstCircleCenter:
            center = plane.FirstCircleCenter
            circles.append((center.X, center.Z, plane.FirstCircleRadius))
        if plane and plane.SecondCircleCenter:
            center = plane.SecondCircleCenter
            circles.append((center.X, center.Z, None))
    return circles
