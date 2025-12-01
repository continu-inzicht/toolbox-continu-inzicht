from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.flood_scenarios.calculate_flood_risk import (
    CalculateFloodRisk,
)


def helper_create_data_adapter(name):
    """Create a DataAdapter object with the given config file name, reducing code duplication."""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / name)
    config.lees_config()
    return DataAdapter(config=config)


def test_calculate_flood_risk():
    """test vergelijking handmatig met code"""
    data_adapter = helper_create_data_adapter("test_calculate_flood_risk.yaml")
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "segment_failure_probability",
            "flood_scenario_grids",
            "areas_to_average",
        ],
        output="flood_risk_results",
    )
    df_out = calculate_flood_risk.df_out
    df_out.set_index("segment_id", inplace=True)
