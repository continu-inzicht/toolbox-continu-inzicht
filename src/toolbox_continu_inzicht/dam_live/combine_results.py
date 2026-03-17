from pydantic.dataclasses import dataclass
import pandas as pd
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from typing import Optional
from toolbox_continu_inzicht.base.base_module import ToolboxBase
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon
import numpy as np
from shapely.geometry import Polygon as ShapelyPolygon, LineString
from shapely.ops import unary_union


@dataclass(config={"arbitrary_types_allowed": True})
class CombineDamLiveResults(ToolboxBase):
    data_adapter: DataAdapter

    df_in: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: list[str]) -> None:
        """
        Haalt stage-gerelateerde data op via de DataAdapter,
        voert de benodigde merges uit en schrijft de
        gecombineerde resultaten terug naar de opgegeven outputs.

        Parameters
        ----------
        input: list[str]
            Lijst met namen van DataAdapter-inputs in de volgorde: [stages, geometries, soils, soillayers, waternets, calculationsettings].
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

        # ValueError: check of alle benodigde DataFrames aanwezig zijn en data bevatten ---
        dataframes = {
            "stages": self.df_stages,
            "geometries": self.df_geometries,
            "soils": self.df_soils,
            "soillayers": self.df_soillayers,
            "waternets": self.df_waternets,
            "calculationsettings": self.df_calculationsettings,
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
            "soils": ["soil_id", "name", "color"],
            "waternets": [
                "waternet_id",
                "line_id",
                "x",
                "z",
                "line_label",
                "line_type",
            ],
            "calculationsettings": ["calculationsettings_id"],
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

        self.data_adapter.output(output[0], self.df_merged_soils)
        self.data_adapter.output(output[1], self.df_merged_waternet)
        self.data_adapter.output(output[2], self.df_merged_calculations)

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
            "color",
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

        # Voeg kleuren toe per line_label
        line_color_map = {
            "Phreatic line (PL 1)": "#ff1493",  # deep pink
            "Head line 3 (PL 3)": "#ff8c00",  # oranje
            "Waternet line phreatic line": "#00bfff",  # helder lichtblauw
            "Waternet line lower aquifer": "#00ff7f",  # fel groen
        }

        df_merged["color"] = (
            df_merged["line_label"].map(line_color_map).fillna("#cccccc")
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
            color = row["color"]

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
            color = df_line["color"].iloc[0]

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

                slip = arc.intersection(soil_union)

                if not slip.is_empty:
                    if slip.geom_type == "MultiLineString":
                        for g in slip:
                            xs, zs = g.xy
                            ax.plot(xs, zs, color="red", linewidth=2)

                    elif slip.geom_type == "LineString":
                        xs, zs = slip.xy
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

                for geom in [arc1, tangent, arc2]:
                    slip = geom.intersection(soil_union)

                    if slip.is_empty:
                        continue

                    if slip.geom_type == "MultiLineString":
                        for g in slip:
                            xs, zs = g.xy
                            ax.plot(xs, zs, color="red", linewidth=2)

                    elif slip.geom_type == "LineString":
                        xs, zs = slip.xy
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
