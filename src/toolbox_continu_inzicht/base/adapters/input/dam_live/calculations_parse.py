import pandas as pd
from toolbox_continu_inzicht.base.adapters.input.dam_live.json_folder import input_json_folder

def input_calculationsettings(input_config: dict) -> pd.DataFrame:
    """
    Lees alle calculationsettings JSON bestanden in een folder
    en zet deze om naar een flat table.

    Parameters
    ----------
    input_config : dict
        Configuratie met:
        - "abs_path": pad naar calculationsettings folder

    Returns
    -------
    pd.DataFrame
        Tabel met per calculationsettings één rij.

    Output columns:
    ----------------
    calculationsettings_id
    analysis_type
    calculation_type
    model_factor_mean
    model_factor_std
    circle_center_x
    circle_center_z
    circle_radius
    content_version
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

        # Dynamisch actieve methode ophalen
        method_data = data.get(analysis_type, {})

        circle_center_x = None
        circle_center_z = None
        circle_radius = None

        if isinstance(method_data, dict):
            circle = method_data.get("Circle")
            if isinstance(circle, dict):
                center = circle.get("Center", {})
                circle_center_x = center.get("X")
                circle_center_z = center.get("Z")
                circle_radius = circle.get("Radius")

        rows.append(
            {
                "calculationsettings_id": calculationsettings_id,
                "analysis_type": analysis_type,
                "calculation_type": calculation_type,
                "model_factor_mean": model_factor_mean,
                "model_factor_std": model_factor_std,
                "circle_center_x": circle_center_x,
                "circle_center_z": circle_center_z,
                "circle_radius": circle_radius,
                "content_version": content_version,
            }
        )

    df = pd.DataFrame(rows)
    return df
