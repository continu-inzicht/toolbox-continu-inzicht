import json
from pathlib import Path
from typing import Iterator, Dict, Any

from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_json_folder(input_config: dict) -> Iterator[Dict[str, Any]]:
    """Lees alle JSON bestanden in een folder.

    Parameters
    ----------
    input_config : dict
        Configuratie met minimaal:
        - "abs_path": pad naar folder met JSON bestanden

    Returns
    -------
    Iterator[Dict[str, Any]]
        Generator die per JSON bestand een dictionary teruggeeft.
        Bevat:
        - "file_name": naam van het bestand
        - "data": ingelezen JSON inhoud
    """

    folder_path = Path(input_config["abs_path"])

    if not folder_path.exists():
        raise FileNotFoundError(f"Folder niet gevonden: {folder_path}")

    if not folder_path.is_dir():
        raise NotADirectoryError(f"Pad is geen directory: {folder_path}")

    kwargs = get_kwargs(json.load, input_config)

    json_files = sorted(folder_path.glob("*.json"))

    for file_path in json_files:
        with file_path.open("r", encoding="utf-8") as f:
            data = json.load(f, **kwargs)

        yield {
            "file_name": file_path.name,
            "data": data,
        }
