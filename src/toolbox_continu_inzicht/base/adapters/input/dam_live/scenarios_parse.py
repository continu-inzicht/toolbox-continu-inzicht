import pandas as pd

from toolbox_continu_inzicht.base.adapters.input.dam_live.json_folder import (
    input_json_folder,
)


def input_scenarios(input_config: dict) -> pd.DataFrame:
    """
    Lees alle scenario JSON bestanden.

    Parameters
    ----------
    input_config : dict
        Configuratie met:
        - "abs_path": pad naar scenario folder

    Returns
    -------
    pd.DataFrame
        Tabel met per scenario één rij.
    """

    rows = []

    for item in input_json_folder(input_config):
        data = item["data"]

        rows.append(
            {
                "scenario_id": int(data.get("Id")),
                "scenario_label": str(data.get("Label")),
            }
        )

    df = pd.DataFrame(rows)

    return df
