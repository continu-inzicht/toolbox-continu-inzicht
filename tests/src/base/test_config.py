from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
import pytest


def test_Config():
    """Tests loading of csv configuration"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    print(test_data_sets_path)
    c = Config(config_path=test_data_sets_path / "test_config.yaml")
    c.lees_config()

    assert "rootdir" in c.global_variables
    keys = ["MyCSV_in", "MyCSV_out", "MyPostgresql", "MyNetCDF_in", "MyNetCDF_out"]
    assert all([key in c.data_adapters for key in keys])
    assert c.data_adapters["MyCSV_in"]["type"] == "csv"
    assert c.data_adapters["MyPostgresql"]["type"] == "postgresql_database"
    assert c.data_adapters["MyNetCDF_in"]["type"] == "netcdf"

    keys = ["schema", "database"]
    assert all([key in c.data_adapters["MyPostgresql"] for key in keys])


def test_Config_invalid():
    """Tests loading of invalid_yaml"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    print(test_data_sets_path)
    c = Config(config_path=test_data_sets_path / "test_config_invalid_tabs.yaml")
    with pytest.raises(UserWarning):
        c.lees_config()
