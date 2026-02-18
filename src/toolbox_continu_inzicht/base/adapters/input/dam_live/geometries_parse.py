import pandas as pd

from toolbox_continu_inzicht.base.adapters.input.dam_live.json_folder import input_json_folder


def input_geometries(input_config: dict) -> pd.DataFrame:
    """
    Lees alle geometry JSON bestanden in een folder
    en zet layers om naar een platte tabel.

    Parameters
    ----------
    input_config : dict
        Configuratie met:
        - "abs_path": pad naar geometry folder

    Returns
    -------
    pd.DataFrame
        Tabel met per layer één rij.

    Output columns:
    ----------------
    geometry_id (PRIMARY KEY)
    layer_id
    layer_label
    points  # lijst van XZ coördinaten
    content_version
    """

    rows = []

    for item in input_json_folder(input_config):
        data = item["data"]

        geometry_id = data.get("Id")
        content_version = data.get("ContentVersion")

        layers = data.get("Layers", [])

        for layer in layers:
            rows.append(
                {
                    "geometry_id": geometry_id,
                    "layer_id": layer.get("Id"),
                    "layer_label": layer.get("Label"),
                    "points": layer.get("Points"),
                    "content_version": content_version,
                }
            )

    df = pd.DataFrame(rows)

    return df
