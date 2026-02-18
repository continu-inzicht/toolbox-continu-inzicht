import pandas as pd
from toolbox_continu_inzicht.base.adapters.input.dam_live.json_folder import input_json_folder

def input_soillayers(input_config: dict) -> pd.DataFrame:
    """
    Lees alle soillayers JSON bestanden in een folder
    en zet layers om naar een platte tabel.

    Parameters
    ----------
    input_config : dict
        Configuratie met:
        - "abs_path": pad naar soillayers folder

    Returns
    -------
    pd.DataFrame
        Tabel met per layer één rij.

    Output columns
    ----------------
    soillayers_id (PRIMARY KEY)
    layer_id
    soil_id
    content_version
    """
    
    rows = []

    for item in input_json_folder(input_config):
        data = item["data"]

        soillayers_id = data.get("Id")
        content_version = data.get("ContentVersion")

        soil_layers = data.get("SoilLayers", [])

        for layer in soil_layers:
            rows.append(
                {
                    "soillayers_id": soillayers_id,
                    "layer_id": layer.get("LayerId"),
                    "soil_id": layer.get("SoilId"),
                    "content_version": content_version,
                }
            )

    df = pd.DataFrame(rows)
    return df
