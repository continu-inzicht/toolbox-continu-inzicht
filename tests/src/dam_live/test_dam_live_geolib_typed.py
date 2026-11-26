"""Go/no-go check: GEOLib's typed classes can read DamLive's `.stix` files.

`DStabilityModel.parse()` rejects DamLive's reduced result files, so the `.stix`
content is validated per file into GEOLib's typed classes instead.
"""

import json
import zipfile
from pathlib import Path

import pytest

from damlive_oracle import expected_tables

pytest.importorskip("geolib")
from geolib.models.dstability import internal  # noqa: E402

DATA_SETS = Path(__file__).parent / "data_sets"
RUN_DAMLIVE = DATA_SETS / "run_damlive"
RESULT_STIX = sorted(
    (RUN_DAMLIVE / "WV2030_Purmer.Calc_tests_sets" / "Stability").glob(
        "*/*_result.stix"
    )
)
INPUT_FIXTURES = [
    RUN_DAMLIVE / "WV2030_Purmer.geometries2D" / "PU0013_87074-1_DWP001.stix",
    RUN_DAMLIVE / "WV2030_Purmer.geometries2D" / "PU0021_87074-1_DWP002.stix",
]


def _drop_nulls(value):
    """DAM Engine writes `null` for sub-objects GEOLib expects; dropping lets defaults apply."""
    if isinstance(value, dict):
        return {k: _drop_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_drop_nulls(v) for v in value]
    return value


def _read_typed(stix: Path) -> dict:
    with zipfile.ZipFile(stix) as zf:

        def many(folder: str, cls):
            names = sorted(
                n
                for n in zf.namelist()
                if n.startswith(f"{folder}/") and n.endswith(".json")
            )
            return [cls.model_validate_json(zf.read(n)) for n in names]

        return {
            "scenarios": many("scenarios", internal.Scenario),
            "geometries": many("geometries", internal.Geometry),
            "soillayers": many("soillayers", internal.SoilLayerCollection),
            "waternets": many("waternets", internal.Waternet),
            "calculationsettings": many(
                "calculationsettings", internal.CalculationSettings
            ),
            "soils": internal.SoilCollection.model_validate(
                _drop_nulls(json.loads(zf.read("soils.json")))
            ),
        }


def _ids(df, column: str) -> set[str]:
    return {str(v) for v in df[column].dropna().unique()}


@pytest.mark.parametrize("stix", RESULT_STIX, ids=lambda p: p.name[19:60])
def test_damlive_result_reads_into_typed_classes(stix):
    assert len(RESULT_STIX) == 4
    typed = _read_typed(stix)
    expected = expected_tables(stix)
    soil, waternet, calcs = (
        expected["merged_soil"],
        expected["merged_waternet"],
        expected["merged_calculations"],
    )
    stages = [stage for scenario in typed["scenarios"] for stage in scenario.Stages]
    calculations = [c for scenario in typed["scenarios"] for c in scenario.Calculations]

    assert {s.Id for s in typed["scenarios"]} == _ids(soil, "scenario_id")
    assert {s.Id for s in stages} == _ids(soil, "stage_id")
    assert {s.GeometryId for s in stages} == _ids(soil, "geometry_id")
    assert {s.SoilLayersId for s in stages} == _ids(soil, "soillayers_id")
    assert {s.WaternetId for s in stages} == _ids(waternet, "waternet_id")
    assert {c.CalculationSettingsId for c in calculations} == _ids(
        calcs, "calculationsettings_id"
    )
    assert {cs.Id for cs in typed["calculationsettings"]} == _ids(
        calcs, "calculationsettings_id"
    )
    # DamLive result files hold no calculated results
    assert all(c.ResultId is None for c in calculations)
    assert len(typed["soils"].Soils) > 0


@pytest.mark.parametrize("stix", INPUT_FIXTURES, ids=lambda p: p.stem)
def test_damlive_input_profile_reads_into_typed_classes(stix):
    typed = _read_typed(stix)
    stages = [stage for scenario in typed["scenarios"] for stage in scenario.Stages]

    assert stages
    assert {g.Id for g in typed["geometries"]} >= {s.GeometryId for s in stages}
    assert {w.Id for w in typed["waternets"]} >= {s.WaternetId for s in stages}
    assert len(typed["soils"].Soils) > 0
