import json
import pandas as pd


def input_soils(input_config: dict) -> pd.DataFrame:
    """
    Parameters:
    ----------
    input_config: dict
        Configuratie voor het inlezen van de calculationsettings JSON bestanden.

    Lees een losse soils JSON file via input_config dict.
    """
    file_path = input_config["abs_path"]

    SOIL_COLOR_MAP = {
        "ClaS-0": "darkgreen",
        "PeaU-1": "peru",
        "SanB-2": "lightgreen",
        "ClaU-3": "green",
        "klei-4": "sandybrown",
        "SanN-5": "yellow",
    }

    with open(file_path, "r") as f:
        data = json.load(f)

    soils = data.get("Soils", [])
    content_version = data.get("ContentVersion")

    rows = []
    for soil in soils:
        mc_adv = soil.get("MohrCoulombAdvancedShearStrengthModel", {})
        su_model = soil.get("SuShearStrengthModel", {})

        code = soil.get("Code")
        color = SOIL_COLOR_MAP.get(code, "gray")

        rows.append(
            {
                "soil_id": soil.get("Id"),
                "name": soil.get("Name"),
                "code": code,
                "color": color,
                "volumetric_weight_above_phreatic_level": soil.get(
                    "VolumetricWeightAbovePhreaticLevel"
                ),
                "volumetric_weight_below_phreatic_level": soil.get(
                    "VolumetricWeightBelowPhreaticLevel"
                ),
                "shear_strength_model_type_above_phreatic_level": soil.get(
                    "ShearStrengthModelTypeAbovePhreaticLevel"
                ),
                "shear_strength_model_type_below_phreatic_level": soil.get(
                    "ShearStrengthModelTypeBelowPhreaticLevel"
                ),
                "mohr_coulomb_advanced_cohesion": mc_adv.get("Cohesion"),
                "mohr_coulomb_advanced_friction_angle": mc_adv.get("FrictionAngle"),
                "su_shear_strength_ratio": su_model.get("ShearStrengthRatio"),
                "su_strength_increase_exponent": su_model.get(
                    "StrengthIncreaseExponent"
                ),
                "content_version": content_version,
            }
        )

    return pd.DataFrame(rows)
