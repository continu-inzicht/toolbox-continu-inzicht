from pathlib import Path
from toolbox_continu_inzicht import Config
from toolbox_continu_inzicht import DataAdapter
from toolbox_continu_inzicht.loads import LoadsMatroosNetCDF
import pytest


def test_loads_matroos_NetCDF():
    """Tests LoadsMatroosNetCDF with known working dataset & known not working"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_loads_matroos_noos_config_netcdf.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    matroos = LoadsMatroosNetCDF(data_adapter=data_adapter)
    matroos.run(input="BelastingLocaties_fews_rmm_km", output="Waterstanden")
    assert len(matroos.df_out) > 10

    # herhaal, niet zo netjes maar hoeven we geen twee configs aan te maken
    data_adapter.config.global_variables["LoadsMatroosNetCDF"]["model"] = "observed"
    matroos = LoadsMatroosNetCDF(data_adapter=data_adapter)
    with pytest.raises(UserWarning):
        matroos.run(input="BelastingLocaties_observed", output="Waterstanden")
