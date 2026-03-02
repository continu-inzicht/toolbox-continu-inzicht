# initialiseer de (toolbox continu inzicht) modules
from pathlib import Path

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.dam_live.merge_stage import StageMerger

# ## Inladen van .json bestanden{#sec-Inladenjson}
#
# Dit voorbeeld laat zien hoe de verschillende mappen binnen het .stix bestand kunnen worden ingelezen doormiddel van data adapters. Er worden


def setup_data_adapter():
    test_data_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_path / "test_dam_live_parse_config.yaml")
    config.lees_config()

    return DataAdapter(config)


def test_dam_live_parse():
    data_adapter = setup_data_adapter()
    merge_df = StageMerger(data_adapter=data_adapter)
    merge_df.run(
        input=[
            "scenario",
            "geometries",
            "soils",
            "soillayers",
            "waternets",
            "calculationsettings",
        ],
        output=["merge_soil", "merge_waternet", "merge_calculations"],
    )
    assert len(merge_df.df_merged_soils) > 0
    assert len(merge_df.df_merged_waternet) > 0
    assert len(merge_df.df_merged_calculations) > 0
