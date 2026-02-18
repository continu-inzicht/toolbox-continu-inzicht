import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon, Circle

def plot_stage(
    df_merged_soil: pd.DataFrame,
    df_merged_water: pd.DataFrame,
    df_merged_calculations: pd.DataFrame,
    stage_id,
    xlim,
    ylim,
):
    """
    Plot de geometrie van een stage als ingekleurde polygons per layer,
    kleuren gebaseerd op soil type, met waterlijnen en glijvlak circles.
    """
    stage_id = str(stage_id)
    df_stage = df_merged_soil[df_merged_soil["stage_id"] == stage_id]
    df_stage_water = df_merged_water[df_merged_water["stage_id"] == stage_id]
    df_stage_calc = df_merged_calculations[
        df_merged_calculations["stage_id"] == stage_id
    ]

    if df_stage.empty:
        print(f"Geen soil data gevonden voor stage {stage_id}")
        return
    if df_stage_water.empty:
        print(f"Geen water data gevonden voor stage {stage_id}")

    fig, ax = plt.subplots(figsize=(15, 15))

    # --- Plot soils ---
    plotted_soils = {}
    df_stage = df_stage.sort_values("soil_id")
    for idx, row in df_stage.iterrows():
        points = row["points"]
        if not points:
            continue
        polygon_coords = [(p["X"], p["Z"]) for p in points]
        if polygon_coords[0] != polygon_coords[-1]:
            polygon_coords.append(polygon_coords[0])
        color = row["color"]
        poly = Polygon(
            polygon_coords, closed=True, facecolor=color, edgecolor="k", alpha=1
        )
        ax.add_patch(poly)
        soil_name = row["name"]
        if soil_name not in plotted_soils:
            plotted_soils[soil_name] = color

    # --- Plot waterlijnen ---
    plotted_lines = {}
    for line_id, df_line in df_stage_water.groupby("line_id"):
        xs = df_line["x"].tolist()
        zs = df_line["z"].tolist()
        line_label = df_line["line_label"].iloc[0]
        color = df_line["color"].iloc[0]
        ax.plot(xs, zs, color=color, linewidth=2)
        if line_label not in plotted_lines:
            plotted_lines[line_label] = color

    # --- Plot glijvlakken (circles) ---
    plotted_circles = {}
    for idx, row in df_stage_calc.iterrows():
        if pd.notna(row.get("circle_center_x")) and pd.notna(row.get("circle_radius")):
            circle = Circle(
                (row["circle_center_x"], row["circle_center_z"]),
                row["circle_radius"],
                edgecolor="red",
                facecolor="none",
                linewidth=2,
                linestyle="--",
            )

        ax.add_patch(circle)
        analysis_type = row["analysis_type"]
        radius = row["circle_radius"]
        # Bewaar radius per analysis_type voor legenda
        if analysis_type not in plotted_circles:
            plotted_circles[analysis_type] = radius
        # clip_path toevoegen als gewenst
        ax.add_patch(circle)

        # Voeg toe aan dict voor legenda
        analysis_type = row["analysis_type"]
        if analysis_type not in plotted_circles:
            plotted_circles[analysis_type] = "red"

    # --- Legenda soils ---
    soil_handles = [
        plt.Line2D([0], [0], color=color, lw=10) for color in plotted_soils.values()
    ]
    soil_labels = list(plotted_soils.keys())
    soil_legend = ax.legend(
        soil_handles,
        soil_labels,
        title="Soil type",
        loc="upper right",
        bbox_to_anchor=(1.0, 1.0),
    )

    # --- Legenda waterlijnen ---
    line_handles = [
        plt.Line2D([0], [0], color=color, lw=2) for color in plotted_lines.values()
    ]
    line_labels = list(plotted_lines.keys())
    line_legend = ax.legend(
        line_handles,
        line_labels,
        title="Water lines",
        loc="upper right",
        bbox_to_anchor=(1.0, 0.6),
    )

    # --- Legenda glijvlakken ---
    circle_handles = [
        plt.Line2D([0], [0], color="red", lw=2, linestyle="--")
        for _ in plotted_circles.values()
    ]
    circle_labels = [
        f"Slip Circle ({atype}) – R={radius:.2f}"
        for atype, radius in plotted_circles.items()
    ]
    circle_legend = ax.legend(
        circle_handles,
        circle_labels,
        title="Slip Circles",
        loc="upper right",
        bbox_to_anchor=(1.0, 0.3),
    )

    ax.add_artist(soil_legend)
    ax.add_artist(line_legend)
    ax.add_artist(circle_legend)

    ax.set_xlim(xlim)
    ax.set_ylim(ylim)

    ax.set_xlabel("X")
    ax.set_ylabel("Z")
    ax.set_title(f"Stage {stage_id}")
    ax.set_aspect("equal")
    plt.grid(True)
    plt.tight_layout()
    # ax.set_aspect(aspect=2.5)
    plt.show()