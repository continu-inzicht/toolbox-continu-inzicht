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
    data_adapter = helper_create_data_adapter(
        "rekentest_calculate_flood_scenario_probability.yaml"
    )
    calculate_flood_scenario_probability = CalculateFloodScenarioProbability(
        data_adapter=data_adapter
    )
    calculate_flood_scenario_probability.run(
        input=[
            "failuremechanism",
            "probabilities_failuremechanisme_sections",
            "sections_in_segment",
        ],
        output=[
            "scenario_failure_prob_segments",
            "combined_failure_prob_all_sections",
        ],
    )
    df_out = calculate_flood_scenario_probability.df_out_scenario_failure_prob_segments
    df_out.set_index("segment_id", inplace=True)
    stored_value_2 = (
        0.20391035  # as calculated in sprint 6 - 16-1-26 - matches sheet @bthonus
    )
    assert np.isclose(
        df_out.loc[2, "scenario_failure_probability"],
        stored_value_2,
    )
    stored_value_7 = (
        0.421769  # as calculated in sprint 6 - 16-1-26 - matches sheet @bthonus
    )
    assert np.isclose(
        df_out.loc[7, "scenario_failure_probability"],
        stored_value_7,
    )


def test_calculate_flood_scenario_probability_check_combined():
    """test vergelijking handmatig met code"""
    data_adapter = helper_create_data_adapter(
        "rekentest_calculate_flood_scenario_probability.yaml"
    )
    calculate_flood_scenario_probability = CalculateFloodScenarioProbability(
        data_adapter=data_adapter
    )
    calculate_flood_scenario_probability.run(
        input=[
            "failuremechanism",
            "probabilities_failuremechanisme_sections",
            "sections_in_segment",
        ],
        output=[
            "scenario_failure_prob_segments",
            "combined_failure_prob_all_sections",
        ],
    )
    df_out_combined_failure = (
        calculate_flood_scenario_probability.df_out_combined_failure_prob_all_sections
    )
    assert np.isclose(
        df_out_combined_failure.loc[0, "combined_failure_probability"],
        0.625679,
    )  # as calculated in sprint 6 - 16-1-26 - matches sheet @bthonus
