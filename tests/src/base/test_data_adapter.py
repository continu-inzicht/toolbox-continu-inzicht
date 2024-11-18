from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.proof_of_concept import ValuesTimesTwo, ValuesDivideTwo
import pytest


def test_DataAdapter_csv_keer():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    keer_twee = ValuesTimesTwo(data_adapter=data_adapter)
    keer_twee.run(input="MyCSV_in", output="MyCSV_out")

    assert all((keer_twee.df_in["value"] * 2 == keer_twee.df_out["value"]).values)


def test_DataAdapter_csv_delen():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    keer_twee = ValuesDivideTwo(data_adapter=data_adapter)
    keer_twee.run(input="MyCSV_in", output="MyCSV_out")

    assert all((keer_twee.df_in["value"] / 2 == keer_twee.df_out["value"]).values)


def test_DataAdapter_netCDF_keer():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    keer_twee = ValuesTimesTwo(data_adapter=data_adapter)
    keer_twee.run(input="MyNetCDF_in", output="MyNetCDF_out")

    assert all((keer_twee.df_in["value"] * 2 == keer_twee.df_out["value"]).values)


def test_DataAdapter_netCDF_delen():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    print(test_data_sets_path)
    config = Config(config_path=test_data_sets_path / "test_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    delen_twee = ValuesDivideTwo(data_adapter=data_adapter)
    delen_twee.run(input="MyNetCDF_in", output="MyNetCDF_out")

    assert all((delen_twee.df_in["value"] / 2 == delen_twee.df_out["value"]).values)


def test_DataAdapter_invalid_folder():
    """ "Checkt error afhandeling als een map niet bestaat"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config_invalid_folder.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)
    delen_twee = ValuesDivideTwo(data_adapter=data_adapter)
    with pytest.raises(UserWarning):
        delen_twee.run(input="MyCSV_in", output="MyCSV_out")


def test_DataAdapter_predefined_dot_env_file():
    """Checkt of we een specifieke .env kunnen opgeven"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_config_predefined_dot_env.yaml"
    )
    config.lees_config()

    data_adapter = DataAdapter(config=config)
    delen_twee = ValuesDivideTwo(data_adapter=data_adapter)
    delen_twee.run(input="my_csv_in", output="my_csv_out")
    environmental_variables = [
        "postgresql_host",
        "postgresql_port",
        "postgresql_password",
        "postgresql_user",
        "vitaal_password",
        "vitaal_user",
    ]
    print(data_adapter.config.global_variables)
    assert all(
        [var in data_adapter.config.global_variables for var in environmental_variables]
    )


def test_DataAdapter_csv_keer_python_df():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    keer_twee = ValuesTimesTwo(data_adapter=data_adapter)
    keer_twee.run(input="MyCSV_in", output="MyCSV_out")
    input_df = keer_twee.df_in
    data_adapter.supply_dataframe(key="my_df_python", df=input_df)
    keer_twee = ValuesTimesTwo(data_adapter=data_adapter)
    keer_twee.run(input="my_df_python", output="my_df_python")
    assert all((input_df["value"] * 2 == keer_twee.df_out["value"]).values)


def test_DataAdapter_invalid_name():
    """Checkt error afhandeling verkeerde key meegegeven wordt"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)
    delen_twee = ValuesDivideTwo(data_adapter=data_adapter)
    input_df = delen_twee.df_in
    with pytest.raises(UserWarning):
        data_adapter.supply_dataframe(key="wrong_name_data_adapter", df=input_df)
