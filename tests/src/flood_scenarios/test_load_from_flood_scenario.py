import numpy as np
from toolbox_continu_inzicht.flood_scenarios import LoadFromFloodScenarioProbability

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
    """test of de minimal werkt"""
    data_adapter = helper_create_data_adapter("test_load_from_flood_scenario.yaml")
    load_from_flood_scenario_probability = LoadFromFloodScenarioProbability(
        data_adapter=data_adapter
    )
    load_from_flood_scenario_probability.run(
        input=[
            "segment_failure_probability",
            "section_id_to_segment_id",
            "section_failure_probability_data",
        ],
        output="flood_scenario_load_resultaten",
    )
    # todo: segment to dikesystem
    df_out = load_from_flood_scenario_probability.df_out_scenario_loads
    assert not df_out.empty
    assert "hydraulicload" in df_out.columns
    assert np.isclose(df_out.loc[34003, "hydraulicload"].max(), 14.949605)
    assert np.isclose(df_out.loc[34002, "hydraulicload"].min(), 3.970147)
