from pathlib import Path
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


def test_rekentest_calculate_flood_risk_minimal():
    """test minimale werkting van functie"""
    data_adapter = helper_create_data_adapter(
        "rekentest_postprocess_flood_risk_minimal.yaml"
    )
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
    assert not df_out.empty
    expected_result = {
        186: 34002010.0,
        192: 34002010.0,
        254: 34002010.0,
        265: 34003024.0,  # differs
        280: 34002010.0,
    }
    assert (
        dict(zip(df_out["area_id"], df_out["section_id_waterdepth"])) == expected_result
    )


def test_rekentest_calculate_flood_risk_een_ander_vak():
    """test minimale werkting van functie"""
    data_adapter = helper_create_data_adapter(
        "rekentest_postprocess_flood_risk_split.yaml"
    )
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
    assert not df_out.empty
    assert not all(df_out["section_id_waterdepth"] == 34002010)
    expected_result = {
        186: 34003024.0,
        192: 34002010.0,  # this one differs
        254: 34003024.0,
    }
    assert (
        dict(zip(df_out["area_id"], df_out["section_id_waterdepth"])) == expected_result
    )
