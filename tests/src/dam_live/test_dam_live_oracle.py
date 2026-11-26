"""The test oracle reproduces the original `CombineDamLiveResults` output.

Runs the original JSON-folder implementation on an extracted copy of each DamLive
result `.stix` and compares it with `damlive_oracle`. Only meaningful while the
original implementation still exists; remove together with it.
"""

import zipfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from damlive_oracle import expected_soil_color, expected_tables
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.dam_live.combine_results import CombineDamLiveResults

RUN_DAMLIVE = Path(__file__).parent / "data_sets" / "run_damlive"
COLORS = RUN_DAMLIVE / "colors.csv"
RESULT_STIX = sorted(
    (RUN_DAMLIVE / "WV2030_Purmer.Calc_tests_sets" / "Stability").glob(
        "*/*_result.stix"
    )
)

# order matters: the original run() reads its inputs by position
FOLDER_INPUTS = {
    "scenario": {"type": "stages", "path": "scenarios"},
    "geometries": {"type": "geometries", "path": "geometries"},
    "soils": {"type": "soils", "path": "soils.json"},
    "soillayers": {"type": "soillayers", "path": "soillayers"},
    "waternets": {"type": "waternets", "path": "waternets"},
    "calculationsettings": {
        "type": "calculationsettings",
        "path": "calculationsettings",
    },
}
OUTPUTS = ["merge_soil", "merge_waternet", "merge_calculations"]


def _run_original(stix: Path, workdir: Path) -> CombineDamLiveResults:
    extracted = workdir / "extracted"
    with zipfile.ZipFile(stix) as zf:
        zf.extractall(extracted)
    adapters = dict(FOLDER_INPUTS)
    adapters["colors"] = {"type": "csv", "abs_path": str(COLORS), "path": str(COLORS)}
    adapters.update({out: {"type": "csv", "path": f"{out}.csv"} for out in OUTPUTS})
    config_path = workdir / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"GlobalVariables": {"rootdir": str(extracted)}, "DataAdapter": adapters}
        )
    )
    config = Config(config_path=config_path)
    config.lees_config()
    combine = CombineDamLiveResults(data_adapter=DataAdapter(config))
    combine.run(input=[*FOLDER_INPUTS, "colors"], output=OUTPUTS)
    return combine


@pytest.mark.parametrize("stix", RESULT_STIX, ids=lambda p: p.name[19:60])
def test_oracle_matches_original_implementation(stix, tmp_path):
    assert len(RESULT_STIX) == 4
    original = _run_original(stix, tmp_path)
    expected = expected_tables(stix)

    pd.testing.assert_frame_equal(original.df_merged_soils, expected["merged_soil"])
    pd.testing.assert_frame_equal(
        original.df_merged_waternet, expected["merged_waternet"]
    )
    pd.testing.assert_frame_equal(
        original.df_merged_calculations, expected["merged_calculations"]
    )
    pd.testing.assert_frame_equal(
        original.create_df_damlive_soil().drop(columns=["datetime"]),
        expected["damlive_soil"],
    )
    pd.testing.assert_frame_equal(
        original.create_df_damlive_soil_color(),
        expected_soil_color(pd.read_csv(COLORS)),
    )
