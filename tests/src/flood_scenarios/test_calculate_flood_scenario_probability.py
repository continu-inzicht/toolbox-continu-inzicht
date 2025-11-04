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


def test_test_calculate_flood_scenario_probability():
    """test of de minimal werkt"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_scenario.yaml")
    calculate_flood_scenario_probability = CalculateFloodScenarioProbability(
        data_adapter=data_adapter
    )
    result = calculate_flood_scenario_probability.run(
        input="grouped_sections_failure_probability", output="sections_to_segment"
    )
    result
    # assert result
