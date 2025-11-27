import numpy as np
from toolbox_continu_inzicht.flood_scenarios import CalculateFloodScenarioProbability

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


def helper_create_data_adapter(name):
    """Create a DataAdapter object with the given config file name, reducing code duplication."""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / name)
    config.lees_config()
    return DataAdapter(config=config)


def test_calculate_flood_scenario_probability():
    """test vergelijking handmatig met code"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_scenario.yaml")
    calculate_flood_scenario_probability = CalculateFloodScenarioProbability(
        data_adapter=data_adapter
    )
    calculate_flood_scenario_probability.run(
        input=["combined_failure_probability_data", "section_id_to_segment_id"],
        output="flood_scenario_probability_resultaten",
    )
    df_out = calculate_flood_scenario_probability.df_out
    df_out.set_index("segment_id", inplace=True)
    # na rekenen van segment 34003
    failure = 1
    for prob, length in zip([5.500000e-01, 1.000000e-10, 1.000000e-10], [2, 3, 1]):
        failure *= 1 - prob * length / 6
    hand_calc = 1 - failure
    assert np.isclose(
        df_out.loc[34003, "failure_probability"],
        hand_calc,
    )
