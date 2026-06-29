from pathlib import Path

import pytest
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.flood_scenarios.postprocess_flood_risk import (
    PostProcessFloodRisk,
)


def helper_create_data_adapter(name):
    """Create a DataAdapter object with the given config file name, reducing code duplication."""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / name)
    config.lees_config()
    return DataAdapter(config=config)


def test_calculate_flood_risk():
    """test minimale werkting van functie"""
    data_adapter = helper_create_data_adapter("test_postprocess_flood_risk.yaml")
    postprocess_flood_risk = PostProcessFloodRisk(data_adapter=data_adapter)
    postprocess_flood_risk.run(
        input=[
            "combined_failure_probability_data",
            "section_id_to_segment_id",
            "flood_risk_results_per_segment",
        ],
        output="areas_to_determining_sections",
    )
    df_out = postprocess_flood_risk.gdf_out_areas_to_determining_sections
    # allemaal de zelfde kans, dus ook allemaal de zelfde sectie/segment combinatie
    assert not df_out.empty
    expected_columns = [
        "segment_id_casualties",
        "section_id_casualties",
        "segment_id_damage",
        "section_id_damage",
        "segment_id_flooding",
        "section_id_flooding",
        "segment_id_affected_people",
        "section_id_affected_people",
        "segment_id_waterdepth",
        "section_id_waterdepth",
    ]
    assert all(col in df_out.columns for col in expected_columns)
    expected_segment_id_casualties = {
        0: 34003,
        1: 34003,
        2: 34003,
        3: 34003,
        4: 34003,
        5: 34003,
        6: 34003,
        7: 34003,
        8: 34003,
        9: 34003,
        10: 34002,
        11: 34003,
        12: 34003,
        13: 34002,
        14: 34002,
        15: 34002,
        16: 34002,
        17: 34002,
    }
    assert df_out["segment_id_casualties"].to_dict() == expected_segment_id_casualties


def test_calculate_flood_risk_map():
    """test minimale werkting van functie"""
    data_adapter = helper_create_data_adapter("test_postprocess_flood_risk.yaml")
    postprocess_flood_risk = PostProcessFloodRisk(data_adapter=data_adapter)
    postprocess_flood_risk.run(
        input=[
            "combined_failure_probability_data",
            "section_id_to_segment_id",
            "flood_risk_results_per_segment",
        ],
        output="areas_to_determining_sections",
    )
    m = postprocess_flood_risk.make_map(risk_metric="casualties")
    assert m is not None


def test_calculate_no_valid_risk_metric():
    """test minimale werkting van functie"""
    data_adapter = helper_create_data_adapter("test_postprocess_flood_risk.yaml")
    data_adapter.set_global_variable(
        "PostProcessFloodRisk", {"risk_metric_columns": ["non-existent-metric"]}
    )  # only casualties, no damage, flooding, etc.
    postprocess_flood_risk = PostProcessFloodRisk(data_adapter=data_adapter)
    with pytest.raises(UserWarning):
        postprocess_flood_risk.run(
            input=[
                "combined_failure_probability_data",
                "section_id_to_segment_id",
                "flood_risk_results_per_segment",
            ],
            output="areas_to_determining_sections",
        )
