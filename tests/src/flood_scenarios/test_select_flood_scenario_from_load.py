from toolbox_continu_inzicht.flood_scenarios import SelectFloodScenarioFromLoad

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def helper_create_data_adapter(name):
    """Create a DataAdapter object with the given config file name, reducing code duplication."""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / name)
    config.lees_config()
    return DataAdapter(config=config)


def test_load_from_flood_scenario():
    """test of de minimal werkt met standaard instellingen"""
    data_adapter = helper_create_data_adapter(
        "test_select_flood_scenario_from_load.yaml"
    )
    select_flood_scenario_from_load = SelectFloodScenarioFromLoad(
        data_adapter=data_adapter
    )
    select_flood_scenario_from_load.run(
        input=[
            "flood_scenario_loads",
            "breach_location_metadata",
        ],
        output="flood_scenario_grids",
    )

    df_in = data_adapter.input("flood_scenario_loads")
    df_out = select_flood_scenario_from_load.df_out
    assert not df_out.empty
    assert "hydaulicload_upperboundary" in df_out.columns
    assert len(df_out) == len(df_in)
    assert df_out["hydaulicload_upperboundary"].to_list() == [2.948, 2.93]


def test_load_from_flood_scenario_two_large():
    """test of de minimal werkt met twee scenarios, maar hoge belastingen"""
    data_adapter = helper_create_data_adapter(
        "test_select_flood_scenario_from_load.yaml"
    )
    # add to the data
    data_adapter.config.global_variables["SelectFloodScenarioFromLoad"] = {
        "return_two_scenarios": True
    }
    select_flood_scenario_from_load = SelectFloodScenarioFromLoad(
        data_adapter=data_adapter
    )
    # when the loads are too high, the upper secenario is always selected
    select_flood_scenario_from_load.run(
        input=[
            "flood_scenario_loads",
            "breach_location_metadata",
        ],
        output="flood_scenario_grids",
    )

    df_in = data_adapter.input("flood_scenario_loads")
    df_out = select_flood_scenario_from_load.df_out
    assert not df_out.empty
    assert "hydaulicload_upperboundary" in df_out.columns
    assert len(df_out) == len(df_in)
    assert df_out["hydaulicload_upperboundary"].to_list() == [2.948, 2.93]


def test_load_from_flood_scenario_two_small():
    """test of de minimal werkt"""
    data_adapter = helper_create_data_adapter(
        "test_select_flood_scenario_from_load.yaml"
    )
    # add to the data
    data_adapter.config.global_variables["SelectFloodScenarioFromLoad"] = {
        "return_two_scenarios": True
    }
    select_flood_scenario_from_load = SelectFloodScenarioFromLoad(
        data_adapter=data_adapter
    )
    # when the loads are smaller, two scenarios per locations are returned
    select_flood_scenario_from_load.run(
        input=[
            "flood_scenario_loads_two",  # only this is changed
            "breach_location_metadata",
        ],
        output="flood_scenario_grids",
    )

    df_in = data_adapter.input("flood_scenario_loads")
    df_out = select_flood_scenario_from_load.df_out
    assert not df_out.empty
    assert "hydaulicload_upperboundary" in df_out.columns
    assert len(df_out) == 2 * len(df_in)
    assert df_out["hydaulicload_upperboundary"].to_list() == [
        2.506,
        2.726,
        2.497,
        2.723,
    ]
