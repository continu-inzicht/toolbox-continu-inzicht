from pathlib import Path
from typing import Tuple
import numpy as np
import rasterio

from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_flood_risk_local_file(
    input_config: dict,
) -> Tuple[np.array, rasterio.Affine]:
    """Laadt een rasterbestand (bijv. tiff) in, gegeven een lokaal pad

    Returns:
    --------
    Tuple[np.array, rasterio.Affine]
    """

    kwargs = get_kwargs(rasterio.open, input_config)

    # aanpassingen van paden
    # 4 opties om het pad naar de scenario grids te bepalen:
    # absoluut
    # relatief onder de data dir
    # relatief tov werkdirectory
    # relatief in de data dir (niet aanbevolen omdat je data dir dan groot kan worden)
    # abs_path komt uit de data adapter -> dit is t.o.v. de data dir
    scenario_path_abs = Path(input_config.get("abs_path_user", ""))
    scenario_path_rel_str = input_config.get(
        "scenario_path", ""
    )  # releative to data dir
    scenario_path_rel = Path(scenario_path_rel_str)
    scenario_path_cwd_rel = Path.cwd() / Path(scenario_path_rel_str)
    scenario_path_abs_data_dir = Path(input_config.get("abs_path", ""))

    scenario_path_rel_data_dir = scenario_path_abs_data_dir / scenario_path_rel
    if scenario_path_abs.is_absolute() and scenario_path_abs.exists():
        scenario_path = scenario_path_abs
    elif scenario_path_rel_data_dir.exists():
        scenario_path = scenario_path_rel_data_dir
    elif scenario_path_cwd_rel.exists():
        scenario_path = scenario_path_cwd_rel
    elif scenario_path_abs_data_dir.exists():
        scenario_path = scenario_path_abs_data_dir

    # let op: grid_file is het bestandspad & wordt door de functie gezet in de calculate_flood_risk.py
    grid_path: Path = scenario_path / input_config["grid_file"]

    if not grid_path.exists():
        raise UserWarning(f"Grid file {grid_path} bestaat niet.")

    # inlezen raster
    with rasterio.open(grid_path, **kwargs) as src:
        array_msk = src.read(1, masked=True)
        affine = src.transform

    return affine, array_msk
