import warnings
import pandas as pd
from toolbox_continu_inzicht.base.adapters.input.dam_live.json_folder import (
    input_json_folder,
)


def input_calculationsettings(input_config: dict) -> pd.DataFrame:
    """
    Lees alle calculationsettings JSON bestanden in een folder
    en zet deze om naar een flat table.

    Per gevonden circle wordt één rij aangemaakt.
    """

    rows = []

    for item in input_json_folder(input_config):
        data = item["data"]

        calculationsettings_id = data.get("Id")
        analysis_type = data.get("AnalysisType")
        calculation_type = data.get("CalculationType")
        model_factor_mean = data.get("ModelFactorMean")
        model_factor_std = data.get("ModelFactorStandardDeviation")
        content_version = data.get("ContentVersion")

        method_data = data.get(analysis_type)

        # ==========================
        # BISHOP
        # ==========================
        if analysis_type == "Bishop" and isinstance(method_data, dict):
            circle = method_data.get("Circle")
            if isinstance(circle, dict):
                center = circle.get("Center", {})
                rows.append(
                    {
                        "calculationsettings_id": calculationsettings_id,
                        "analysis_type": analysis_type,
                        "calculation_type": calculation_type,
                        "model_factor_mean": model_factor_mean,
                        "model_factor_std": model_factor_std,
                        "circle_center_x": center.get("X"),
                        "circle_center_z": center.get("Z"),
                        "circle_radius": circle.get("Radius"),
                        "content_version": content_version,
                    }
                )

        # ==========================
        # UPLIFTVAN
        # ==========================
        elif analysis_type == "UpliftVan" and isinstance(method_data, dict):
            slip_plane = method_data.get("SlipPlane", {})

            # First circle
            first_center = slip_plane.get("FirstCircleCenter", {})
            first_radius = slip_plane.get("FirstCircleRadius")

            if first_center:
                rows.append(
                    {
                        "calculationsettings_id": calculationsettings_id,
                        "analysis_type": analysis_type,
                        "calculation_type": calculation_type,
                        "model_factor_mean": model_factor_mean,
                        "model_factor_std": model_factor_std,
                        "circle_center_x": first_center.get("X"),
                        "circle_center_z": first_center.get("Z"),
                        "circle_radius": first_radius,
                        "content_version": content_version,
                    }
                )

            # Second circle
            second_center = slip_plane.get("SecondCircleCenter", {})
            second_radius = slip_plane.get("SecondCircleRadius")

            if second_center:
                rows.append(
                    {
                        "calculationsettings_id": calculationsettings_id,
                        "analysis_type": analysis_type,
                        "calculation_type": calculation_type,
                        "model_factor_mean": model_factor_mean,
                        "model_factor_std": model_factor_std,
                        "circle_center_x": second_center.get("X"),
                        "circle_center_z": second_center.get("Z"),
                        "circle_radius": second_radius
                        if second_radius is not None
                        else first_radius,  # fallback naar first_radius als second_radius niet beschikbaar is
                        "content_version": content_version,
                    }
                )

        # ==========================
        # ANDER TYPE (later kunnen er meer types worden toegevoegd wanneer er voorbeelden van de json structuur beschikbaar komen)
        # ==========================
        else:
            warnings.warn(f"AnalysisType '{analysis_type}' wordt nog niet ondersteund.")

    return pd.DataFrame(rows)
