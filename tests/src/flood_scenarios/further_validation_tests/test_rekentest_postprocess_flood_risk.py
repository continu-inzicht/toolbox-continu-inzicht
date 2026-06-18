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


def test_calculate_flood_risk():
    """test minimale werkting van functie"""
    data_adapter = helper_create_data_adapter("rekentest_postprocess_flood_risk.yaml")
    postprocess_flood_risk = PostProcessFloodRisk(data_adapter=data_adapter)
    postprocess_flood_risk.run(
        input=[
            "section_id_to_segment_id",
            "combined_failure_probability_data",
            "scenario_failure_prob_segments",
            "flood_risk_results_per_segment",
        ],
        output="areas_to_determining_sections",
    )
    df_out = postprocess_flood_risk.gdf_out_areas_to_determining_sections
    assert not df_out.empty