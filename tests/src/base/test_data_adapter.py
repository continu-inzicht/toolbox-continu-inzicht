from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht import voorbeeld_module
import pytest


def test_DataAdapter_csv_keer():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_config.yaml")
    c.lees_config()

    data = DataAdapter(config=c)

    keer_twee = voorbeeld_module.WaardesKeerTwee(
        data_adapter=data, input="MyCSV_in", output="MyCSV_out"
    )
    keer_twee.run()

    assert all((keer_twee.df_in["value"] * 2 == keer_twee.df_out["value"]).values)


def test_DataAdapter_csv_delen():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_config.yaml")
    c.lees_config()

    data = DataAdapter(config=c)

    keer_twee = voorbeeld_module.WaardesDelenTwee(
        data_adapter=data, input="MyCSV_in", output="MyCSV_out"
    )
    keer_twee.run()

    assert all((keer_twee.df_in["value"] / 2 == keer_twee.df_out["value"]).values)


def test_DataAdapter_netCDF_keer():
    test_data_sets_path = Path(__file__).parent / "data_sets"    
    c = Config(config_path=test_data_sets_path / "test_config.yaml")
    c.lees_config()

    data = DataAdapter(config=c)

    keer_twee = voorbeeld_module.WaardesKeerTwee(
        data_adapter=data, input="MyNetCDF_in", output="MyNetCDF_out"
    )
    keer_twee.run()

    assert all((keer_twee.df_in["value"] * 2 == keer_twee.df_out["value"]).values)


def test_DataAdapter_netCDF_delen():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    print(test_data_sets_path)
    c = Config(config_path=test_data_sets_path / "test_config.yaml")
    c.lees_config()

    data = DataAdapter(config=c)

    delen_twee = voorbeeld_module.WaardesDelenTwee(
        data_adapter=data, input="MyNetCDF_in", output="MyNetCDF_out"
    )
    delen_twee.run()

    assert all((delen_twee.df_in["value"] / 2 == delen_twee.df_out["value"]).values)


def test_DataAdapter_invalid_folder():
    """ "Checkt error afhandeling als een map niet bestaat"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_config_invalid_folder.yaml")
    c.lees_config()

    data = DataAdapter(config=c)
    delen_twee = voorbeeld_module.WaardesDelenTwee(
        data_adapter=data, input="MyCSV_in", output="MyCSV_out"
    )
    with pytest.raises(UserWarning):
        delen_twee.run()
