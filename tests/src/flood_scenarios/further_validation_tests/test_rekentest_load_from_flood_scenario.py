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
    data_adapter = helper_create_data_adapter(
        "rekentest_load_from_flood_scenario_probability.yaml"
    )
    load_from_flood_scenario_probability = LoadFromFloodScenarioProbability(
        data_adapter=data_adapter
    )
    load_from_flood_scenario_probability.run(
        input=[
            "scenario_failure_prob_segments",
            "section_to_segment",
            "section_fragility_curves",
        ],
        output="scenario_loads",
    )
    # todo: segment to dikesystem
    df_out = load_from_flood_scenario_probability.df_out_scenario_loads
    assert not df_out.empty
    assert "hydraulicload" in df_out.columns
    assert np.isclose(df_out.loc[7, "hydraulicload"], 4.592589)
    assert np.isclose(df_out.loc[2, "hydraulicload"], 4.063309)
