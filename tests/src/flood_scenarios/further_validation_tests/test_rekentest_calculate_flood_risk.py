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
    """test minimale werkting van functie"""
    data_adapter = helper_create_data_adapter("rekentest_calculate_flood_risk.yaml")
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "scenario_failure_prob_segments",
            "scenario_consequences_grids",
            "areas_to_aggregate",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    df_out = calculate_flood_risk.df_out
    expected_cols = [
        "area_id",
        "casualties",
        "damage",
        "flooding",
        "affected_people",
        "name",
        "code",
        "zip",
        "people",
        "geometry",
    ]
    assert all([col in df_out.columns for col in expected_cols])


def test_calculate_flood_risk_one_grid():
    """test of functie ook 1 van de default grid namen aan kan"""
    data_adapter = helper_create_data_adapter("rekentest_calculate_flood_risk.yaml")
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "scenario_failure_prob_segments",
            "scenario_consequences_grids",
            "areas_to_aggregate",
            "flood_risk_local_file",
        ],
        output="flood_risk_results",
    )
    df_out = calculate_flood_risk.df_out
    expected_cols = [
        "area_id",
        "casualties",
        "name",
        "code",
        "zip",
        "people",
        "geometry",
    ]
    assert all([col in df_out.columns for col in expected_cols])


# TODO: check output values?
