import pandas as pd

from toolbox_continu_inzicht.base.adapters.input.dam_live.json_folder import input_json_folder


def input_stages(input_config: dict) -> pd.DataFrame:
    """
    Lees alle scenario JSON bestanden uit een folder en zet de stages
    om naar een platte tabel met gekoppelde calculation-id's.

    Parameters
    ----------
    input_config : dict
        Configuratie met:
        - "abs_path": pad naar scenario folder

    Returns
    -------
    pd.DataFrame
        Tabel met per stage (en per calculation) één rij.
    """

    rows = []

    for item in input_json_folder(input_config):
        data = item["data"]

        scenario_id = data.get("Id")
        scenario_label = data.get("Label")
        content_version = data.get("ContentVersion")

        calculations = data.get("Calculations", [])
        stages = data.get("Stages", [])

        for stage in stages:
            stage_id = stage.get("Id")

            # Als er geen calculations zijn, forceren we één None entry
            if not calculations:
                calculations_iter = [None]
            else:
                calculations_iter = calculations

            for calc in calculations_iter:
                rows.append(
                    {
                        "stage_id": stage_id,
                        "stage_label": stage.get("Label"),
                        "scenario_id": scenario_id,
                        "scenario_label": scenario_label,
                        "geometry_id": stage.get("GeometryId"),
                        "decorations_id": stage.get("DecorationsId"),
                        "soillayers_id": stage.get("SoilLayersId"),
                        "waternet_id": stage.get("WaternetId"),
                        "waternet_creator_settings_id": stage.get(
                            "WaternetCreatorSettingsId"
                        ),
                        "state_id": stage.get("StateId"),
                        "state_correlations_id": stage.get("StateCorrelationsId"),
                        "loads_id": stage.get("LoadsId"),
                        "reinforcements_id": stage.get("ReinforcementsId"),
                        "calculationsettings_id": (
                            calc.get("CalculationSettingsId") if calc else None
                        ),
                        "calculation_id": (
                            calc.get("Id") if calc else None
                        ),
                        "content_version": content_version,
                    }
                )

    df = pd.DataFrame(rows)

    return df
