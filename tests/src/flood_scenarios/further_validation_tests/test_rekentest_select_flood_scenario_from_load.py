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
        "rekentest_select_flood_scenario_from_load.yaml"
    )
    select_flood_scenario_from_load = SelectFloodScenarioFromLoad(
        data_adapter=data_adapter
    )
    select_flood_scenario_from_load.run(
        input=[
            "scenarios_loads",
            "consequences_loads",
        ],
        output="scenario_consequences_grids",
    )

    df_in = data_adapter.input("scenarios_loads")
    df_out = select_flood_scenario_from_load.df_out_scenario_consequences_grids
    assert not df_out.empty
    assert "hydraulicload_upperboundary" in df_out.columns
    assert len(df_out) == len(df_in)
    assert df_out["hydraulicload_upperboundary"].to_list() == [4.723, 4.603]
    assert len(df_out.columns) == 8  # 4 grids + 3 other columns
