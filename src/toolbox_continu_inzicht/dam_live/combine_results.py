import matplotlib
from pydantic.dataclasses import dataclass
import pandas as pd
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from typing import Optional
from toolbox_continu_inzicht.base.base_module import ToolboxBase
import matplotlib.pyplot as plt

matplotlib.use("TkAgg")  # of "Agg" als je geen interactief venster nodig hebt
from matplotlib.patches import Polygon
import numpy as np
from shapely.geometry import Point, Polygon as ShapelyPolygon, LineString
from shapely.ops import unary_union
import warnings
import time


@dataclass(config={"arbitrary_types_allowed": True})
class CombineDamLiveResults(ToolboxBase):
    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None
    soil_color_map: dict[str, str] = None
    water_color_map: dict[str, str] = None

    def run(self, input: list[str], output: list[str]) -> None:
        """
        Haalt stage-gerelateerde data op via de DataAdapter,
        voert de benodigde merges uit en schrijft de
        gecombineerde resultaten terug naar de opgegeven outputs.

        Parameters
        ----------
        input: list[str]
            Lijst met namen van DataAdapter-inputs in de volgorde: [stages, geometries, soils, soillayers, waternets, calculationsettings, scenarios].
        output: list[str]
            Lijst met namen van DataAdapter-outputs in de volgorde: [merged_soils, merged_waternet, merged_calculations].

        Raises
        ------
        KeyError
            Als een verwachte kolom ontbreekt in één van de input-DataFrames.

        ValueError
            Als één van de inputbronnen geen data bevat of niet correct
            kan worden gemerged.
        """

        self.df_stages = self.data_adapter.input(input[0])
        self.df_geometries = self.data_adapter.input(input[1])
        self.df_soils = self.data_adapter.input(input[2])
        self.df_soillayers = self.data_adapter.input(input[3])
        self.df_waternets = self.data_adapter.input(input[4])
        self.df_calculationsettings = self.data_adapter.input(input[5])
        self.df_scenarios = self.data_adapter.input(input[6])
        self.df_colors = self.data_adapter.input(input[7])
        # TODO nu nog hard coded maar eigen uitlezen uit bestanden structuur resultata DAM-Live
        self.df_models = pd.DataFrame(
            [
                {"id": 1, "name": "Bishop", "description": "Bishop"},
                {"id": 2, "name": "Uplift Van", "description": "Uplift Van"},
            ]
        )

        # ValueError: check of alle benodigde DataFrames aanwezig zijn en data bevatten ---
        dataframes = {
            "stages": self.df_stages,
            "geometries": self.df_geometries,
            "soils": self.df_soils,
            "soillayers": self.df_soillayers,
            "waternets": self.df_waternets,
            "calculationsettings": self.df_calculationsettings,
            "scenarios": self.df_scenarios,
        }

        missing_or_empty = [
            name for name, df in dataframes.items() if df is None or df.empty
        ]

        if missing_or_empty:
            raise ValueError(
                "De volgende inputs ontbreken of bevatten geen data: "
                + ", ".join(missing_or_empty)
            )

        # KeyError: check cruciale kolommen ---
        required_columns = {
            "stages": [
                "stage_id",
                "geometry_id",
                "waternet_id",
                "calculationsettings_id",
            ],
            "geometries": ["geometry_id", "layer_id"],
            "soillayers": ["layer_id", "soil_id"],
            "soils": ["soil_id", "name"],
            "waternets": [
                "waternet_id",
                "line_id",
                "x",
                "z",
                "line_label",
                "line_type",
            ],
            "calculationsettings": ["calculationsettings_id"],
            "scenarios": ["scenario_id", "scenario_label"],
        }

        for name, cols in required_columns.items():
            df = dataframes[name]
            missing_cols = [col for col in cols if col not in df.columns]
            if missing_cols:
                raise KeyError(
                    f"Ontbrekende kolommen in '{name}': {', '.join(missing_cols)}"
                )

        self.df_merged_soils = self.merge_geometries_soils()
        self.df_merged_waternet = self.merge_waternet()
        self.df_merged_calculations = self.merge_calculationsettings()

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
            self.data_adapter.output(output[3], self.create_df_damlive_models())
            self.data_adapter.output(output[4], self.create_df_damlive_scenarios())
            self.data_adapter.output(output[5], self.create_df_damlive_soil())
            self.data_adapter.output(output[6], self.create_df_damlive_soil_color())
            self.data_adapter.output(output[7], self.create_df_damlive_lines())
            self.data_adapter.output(
                output[8], self.create_df_damlive_lines_enveloping()
            )
            self.data_adapter.output(output[9], self.create_df_damlive_values())
            # self.data_adapter.output(output[10], self.create_df_damlive_conditions())

    def merge_calculationsettings(self) -> pd.DataFrame:
        """
        Merge stages met calculationsettings op calculationsettings_id.
        """
        df_merged = self.df_stages.merge(
            self.df_calculationsettings,
            how="left",
            left_on="calculationsettings_id",
            right_on="calculationsettings_id",
        )

        columns_order = [
            "stage_id",
            "stage_label",
            "scenario_id",
            "scenario_label",
            "calculationsettings_id",
            "analysis_type",
            "calculation_type",
            "model_factor_mean",
            "model_factor_std",
            "circle_center_x",
            "circle_center_z",
            "circle_radius",
            "content_version",
        ]
        columns_order = [col for col in columns_order if col in df_merged.columns]

        return df_merged[columns_order]

    def merge_geometries_soils(self) -> pd.DataFrame:
        """
        Merge stages met geometry, soillayers en soils.
        """
        df_merged = self.df_stages.merge(
            self.df_geometries,
            how="left",
            left_on="geometry_id",
            right_on="geometry_id",
        )

        df_merged = df_merged.merge(
            self.df_soillayers,
            how="left",
            left_on="layer_id",
            right_on="layer_id",
            suffixes=("", "_soillayers"),
        )

        df_merged = df_merged.merge(
            self.df_soils,
            how="left",
            left_on="soil_id",
            right_on="soil_id",
            suffixes=("", "_soil"),
        )

        columns_order = [
            "stage_id",
            "stage_label",
            "scenario_id",
            "scenario_label",
            "geometry_id",
            "layer_id",
            "layer_label",
            "points",
            "soillayers_id",
            "soil_id",
            "name",
            "code",
        ]
        columns_order = [col for col in columns_order if col in df_merged.columns]

        return df_merged[columns_order]

    def merge_waternet(self) -> pd.DataFrame:
        """
        Merge stages met waternetlijnen en voeg kleuren toe per type lijn.
        """
        # Kolommen uit stages die we willen behouden
        stage_cols = ["stage_id", "stage_label", "scenario_id", "scenario_label"]

        df_stages_light = self.df_stages[stage_cols + ["waternet_id"]].drop_duplicates()

        # Merge met waternet
        df_merged = df_stages_light.merge(
            self.df_waternets, how="left", on="waternet_id"
        )

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
        plt.show()

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer data_damlive_models vanuit de DAMlive
    # ---------------------------------------------------------------------------

    def create_df_damlive_models(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_models``.

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_models te schrijven, met kolommen:
            id, name en description
        """

        create_df_damlive_models = self.df_models

        # Zorg voor het juiste kolomtype
        if not create_df_damlive_models.empty:
            create_df_damlive_models["id"] = create_df_damlive_models["id"].astype(
                "int64"
            )
            create_df_damlive_models["name"] = create_df_damlive_models["name"].astype(
                "string"
            )
            create_df_damlive_models["description"] = create_df_damlive_models[
                "description"
            ].astype("string")

        return create_df_damlive_models

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer data_damlive_scenarios vanuit de DAMlive
    # ---------------------------------------------------------------------------

    def create_df_damlive_scenarios(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_scenarios``.

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_scenarios te schrijven, met kolommen:
            id en name
        """

        create_df_damlive_scenarios = self.df_scenarios.rename(
            columns={"scenario_id": "id", "scenario_label": "name"}
        )[["id", "name"]]

        # Zorg voor het juiste kolomtype
        if not create_df_damlive_scenarios.empty:
            create_df_damlive_scenarios["id"] = create_df_damlive_scenarios[
                "id"
            ].astype("int64")
            create_df_damlive_scenarios["name"] = create_df_damlive_scenarios[
                "name"
            ].astype("string")

        return create_df_damlive_scenarios

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer df_damlive_soil vanuit de DAMlive
    # ---------------------------------------------------------------------------

    def create_df_damlive_soil(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_soil``.

        Gebruikt onderstaande dataFrames:
        ----------
        df_stages met uitvoer van de DAMlive 'scenario'-parser.
            Bevat minimaal: stage_id, geometry_id, soillayers_id.
        df_geometriesmet uitvoer van de DAMlive 'geometries'-parser.
            Bevat minimaal: geometry_id, layer_id, layer_label, points.
        df_soillayers: met uitvoer van de DAMlive 'soillayers'-parser.
            Bevat minimaal: soillayers_id, layer_id, soil_id.
        df_soils met uitvoer van de DAMlive 'soils'-parser.
            Bevat minimaal: soil_id, name.
        TODO: nu nog hard coded gezet
        measuringstation_id met een  waarde voor de kolom measuringstationid.
        parameter_id met een waarde voor de kolom parameterid.

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_soil te schrijven, met kolommen:
            measuringstationid, parameterid, itemid, layername, soils.color,
            datetime, index, x, y, z
        """
        measuringstation_id: int = 322
        parameter_id: int = 200

        # Huidige tijd in epoch milliseconden (since 1970-01-01 UTC)
        epoch_ms = int(time.time() * 1000)

        rijen: list[dict] = []

        for _, stage_row in self.df_stages.iterrows():
            geometry_id = stage_row["geometry_id"]
            soillayers_id = stage_row["soillayers_id"]

            # Filter geometrieën voor deze stage
            geom_stage = self.df_geometries[
                self.df_geometries["geometry_id"] == geometry_id
            ]

            for _, geom_row in geom_stage.iterrows():
                layer_id = geom_row["layer_id"]
                layer_label = geom_row["layer_label"]
                points = geom_row["points"]  # list of dict met 'X' en 'Z' sleutels

                # Zoek de bijbehorende soil_id via soillayers
                soil_match = self.df_soillayers[
                    (self.df_soillayers["soillayers_id"] == soillayers_id)
                    & (self.df_soillayers["layer_id"] == layer_id)
                ]

                if soil_match.empty:
                    # Geen grondsoort koppeling gevonden; laag overslaan
                    continue

                soil_id = soil_match.iloc[0]["soil_id"]

                # Zoek de naam van de grondsoort
                soil_name_match = self.df_soils[self.df_soils["soil_id"] == soil_id]
                layer_name = (
                    soil_name_match.iloc[0]["name"]
                    if not soil_name_match.empty
                    else layer_label
                )

                # Genereer één rij per punt in de geometrie
                for punt_index, punt in enumerate(points):
                    x_waarde = punt.get("X", punt.get("x", 0.0))
                    z_waarde = punt.get("Z", punt.get("z", 0.0))

                    rijen.append(
                        {
                            "measuringstationid": measuringstation_id,
                            "parameterid": parameter_id,
                            "itemid": int(layer_id),  # uniek ID per laag
                            "layername": layer_name,
                            "color": None,  # leeglaten conform specificatie
                            "datetime": epoch_ms,
                            "index": punt_index,
                            "x": float(x_waarde),
                            "y": 0.0,  # altijd 0 (2D-profiel)
                            "z": float(z_waarde),
                        }
                    )

        df_damlive_soil = pd.DataFrame(rijen)

        # Zorg voor het juiste kolomtype
        if not df_damlive_soil.empty:
            df_damlive_soil["measuringstationid"] = df_damlive_soil[
                "measuringstationid"
            ].astype("int64")
            df_damlive_soil["parameterid"] = df_damlive_soil["parameterid"].astype(
                "int64"
            )
            df_damlive_soil["itemid"] = df_damlive_soil["itemid"].astype("int64")
            df_damlive_soil["datetime"] = df_damlive_soil["datetime"].astype("int64")
            df_damlive_soil["index"] = df_damlive_soil["index"].astype("int64")
            df_damlive_soil["x"] = df_damlive_soil["x"].astype("float64")
            df_damlive_soil["y"] = df_damlive_soil["y"].astype("float64")
            df_damlive_soil["z"] = df_damlive_soil["z"].astype("float64")

        return df_damlive_soil

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer df_damlive_soil_color vanuit colors DataFrame
    # ---------------------------------------------------------------------------

    def create_df_damlive_soil_color(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_soil_color``.

        Gebruikt dataFrame df_colors met minimaal de kolommen 'name', 'type' en 'color' afkomstig uit colors.csv.
        De 'color'-kolom bevat HEX-kleurcodes (bijv. '#A0522D' of 'A0522D').

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_soil_color te schrijven, met kolommen:
            soil_name, r, g, b, color, stb_name
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
                    "stb_name": rij["name"],
                }
            )

        df_damlive_color = pd.DataFrame(rijen)

        if not df_damlive_color.empty:
            df_damlive_color["r"] = df_damlive_color["r"].astype("int64")
            df_damlive_color["g"] = df_damlive_color["g"].astype("int64")
            df_damlive_color["b"] = df_damlive_color["b"].astype("int64")

        return df_damlive_color

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer data_damlive_lines vanuit de DAMlive
    # ---------------------------------------------------------------------------

    def create_df_damlive_lines(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_lines``.

        Gebruikt onderstaande dataFrames:
        ----------

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_lines te schrijven, met kolommen:
            measuringstationid, modelid, parameterid, itemid, layername, color, datetime, index, x, y, z, scenario
        """
        # TODO voorlopig hardcoded
        measuringstation_id: int = 322
        # model_id: 1 -> Bischop
        # model_id: 2 -> Uplift Van
        # TODO voorlopig hardcoded
        model_id: int = 2
        # parameter_id: 201 -> Phreatic line
        # parameter_id: 202 -> Glijvlak
        parameter_id: int = 201
        # itemid: uit dataFrame df_waternets, kolom line_id
        # color: uit dataFrame df_waternets, kolom line_label, gemapt naar df_colors
        # scenario: uit dataFrame df_scenarios, kolom scenario_id

        # Huidige tijd in epoch milliseconden (since 1970-01-01 UTC)
        epoch_ms = int(time.time() * 1000)

        color_map = self.df_colors[self.df_colors["type"] == "water"].set_index("name")[
            "color"
        ]

        # WATERLIJNEN

        df_damlive_lines_waterlijnen = pd.DataFrame(
            {
                "measuringstationid": measuringstation_id,
                "modelid": model_id,
                "parameterid": parameter_id,
                "itemid": self.df_waternets["line_id"],
                "layername": self.df_waternets["line_label"],
                "color": self.df_waternets["line_label"].map(color_map),
                "datetime": epoch_ms,
                "index": self.df_waternets.groupby("line_id").cumcount() + 1,
                "x": self.df_waternets["x"],
                "y": self.df_waternets["z"],
                "z": 0,
                "scenario": self.df_scenarios["scenario_id"],
            }
        )

        # GLIJCIRKELS

        # TODO: hoe kom ik aan de glijcirkel data
        df_damlive_lines_glijcirkels = pd.DataFrame()

        # df_damlive_lines_glijcirkels = pd.DataFrame(
        #     {
        #         "measuringstationid": measuringstation_id,
        #         "modelid": model_id,
        #         "parameterid": parameter_id,
        #         "itemid": self.df_glijcirkels["line_id"],
        #         "layername": self.df_glijcirkels["line_label"],
        #         "color": self.df_glijcirkels["line_label"].map(color_map),
        #         "datetime": epoch_ms,
        #         "index": self.df_glijcirkels.groupby("line_id").cumcount() + 1,
        #         "x": self.df_glijcirkels["x"],
        #         "y": self.df_glijcirkels["z"],
        #         "z": 0,
        #         "scenario": self.df_scenarios["scenario_id"],
        #     }
        # )

        # concat waterlijnen en glijcirkels
        df_damlive_lines = pd.concat(
            [df_damlive_lines_waterlijnen, df_damlive_lines_glijcirkels],
            ignore_index=True,
        )

        return df_damlive_lines

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer data_damlive_lines_enveloping vanuit de DAMlive
    # ---------------------------------------------------------------------------

    def create_df_damlive_lines_enveloping(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_lines``.

        Gebruikt onderstaande dataFrames:
        ----------

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_lines_enveloping te schrijven, met kolommen:
            measuringstationid, modelid, parameterid, itemid, layername, color, datetime, index, x, y, z, scenario
        """
        # TODO voorlopig hardcoded
        measuringstation_id: int = 322
        # model_id: 1 -> Bischop
        # model_id: 2 -> Uplift Van
        # TODO voorlopig hardcoded
        model_id: int = 2
        # parameter_id: 201 -> Phreatic line
        parameter_id: int = 201
        # itemid: uit dataFrame df_waternets, kolom line_id
        # color: uit dataFrame df_waternets, kolom line_label, gemapt naar df_colors
        # scenario: uit dataFrame df_scenarios, kolom scenario_id

        # Huidige tijd in epoch milliseconden (since 1970-01-01 UTC)
        epoch_ms = int(time.time() * 1000)

        color_map = self.df_colors[self.df_colors["type"] == "water"].set_index("name")[
            "color"
        ]

        # filter op line_type=32
        self.df_waternets_filtered = self.df_waternets[
            self.df_waternets["line_id"] == "32"
        ]

        # TODO: voorlopig zijn min en max gelijk aan Freatische lijn (line_id=32), maar in de toekomst kunnen dit andere lijnen zijn
        df_damlive_lines_enveloping_max = pd.DataFrame(
            {
                "measuringstationid": measuringstation_id,
                "modelid": model_id,
                "parameterid": parameter_id,
                "itemid": self.df_waternets_filtered["line_id"],
                "layername": "Freatische lijn (maximum)",
                "color": self.df_waternets_filtered["line_label"].map(color_map),
                "datetime": epoch_ms,
                "index": self.df_waternets_filtered.groupby("line_id").cumcount() + 1,
                "x": self.df_waternets_filtered["x"],
                "y": self.df_waternets_filtered["z"],
                "z": 0,
                "scenario": self.df_scenarios["scenario_id"],
            }
        )

        df_damlive_lines_enveloping_min = pd.DataFrame(
            {
                "measuringstationid": measuringstation_id,
                "modelid": model_id,
                "parameterid": parameter_id,
                "itemid": self.df_waternets_filtered["line_id"],
                "layername": "Freatische lijn (minimum)",
                "color": self.df_waternets_filtered["line_label"].map(color_map),
                "datetime": epoch_ms,
                "index": self.df_waternets_filtered.groupby("line_id").cumcount() + 1,
                "x": self.df_waternets_filtered["x"],
                "y": self.df_waternets_filtered["z"],
                "z": 0,
                "scenario": self.df_scenarios["scenario_id"],
            }
        )

        # concat max en min
        df_damlive_lines_enveloping = pd.concat(
            [df_damlive_lines_enveloping_max, df_damlive_lines_enveloping_min]
        )

        return df_damlive_lines_enveloping

    # ---------------------------------------------------------------------------
    # Hulpfunctie: creeer data_damlive_values vanuit de DAMlive
    # ---------------------------------------------------------------------------

    def create_df_damlive_values(self) -> pd.DataFrame:
        """
        Bouw het DataFrame op dat overeenkomt met de tabel ``data_damlive_values``.

        Gebruikt onderstaande dataFrames:
        ----------

        Returns
        -------
        pd.DataFrame
            DataFrame klaar om naar data_damlive_values te schrijven, met kolommen:
            measuringstationid, modelid, datetime, stability_factor, number_of_slices, xcentrepoint, ycentrepoint, radius, xcoordinate_left_surface, xcoordinate_right_surface, scenario, stateid
        """
        # TODO voorlopig hardcoded
        measuringstation_id: int = 322
        # model_id: 1 -> Bischop
        # model_id: 2 -> Uplift Van
        # TODO voorlopig hardcoded
        model_id: int = 2
        # stability_factor
        # number_of_slices
        # xcentrepoint
        # ycentrepoint
        # radius
        # xcoordinate_left_surface
        # xcoordinate_right_surface
        # scenario
        # stateid

        # Huidige tijd in epoch milliseconden (since 1970-01-01 UTC)
        epoch_ms = int(time.time() * 1000)

        # TODO stability_factor, number_of_slices, xcoordinate_left_surface, xcoordinate_right_surface voorlopig hardcoded en check of ycentrepoint circle_center_z moet zijn
        # TODO self.df_stages["state_id"] is wel de state_id die hier gebruikt moet worden?
        df_damlive_values = pd.DataFrame(
            {
                "measuringstationid": measuringstation_id,
                "modelid": model_id,
                "datetime": epoch_ms,
                "stability_factor": 1,
                "number_of_slices": 0,
                "xcentrepoint": self.df_merged_calculations["circle_center_x"],
                "ycentrepoint": self.df_merged_calculations["circle_center_z"],
                "radius": self.df_merged_calculations["circle_radius"],
                "xcoordinate_left_surface": 0,
                "xcoordinate_right_surface": 0,
                "scenario": self.df_merged_calculations["scenario_id"],
                "stateid": 1,
            }
        )

        return df_damlive_values


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
