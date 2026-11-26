"""Loading `.stix` files through the `damlive_stix` DataAdapter input type."""

import shutil
import warnings
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from damlive_oracle import expected_tables
from toolbox_continu_inzicht.base.adapters.input.dam_live.stix import (
    StixCollection,
    StixFile,
)
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter

CALC = Path("WV2030_Purmer.Calc_tests_sets")
INPUT_PROFILE = Path("WV2030_Purmer.geometries2D") / "PU0013_87074-1_DWP001.stix"


def load(rootdir: Path, path: Path | str) -> StixCollection:
    """Load `path` through a DataAdapter entry of type `damlive_stix`."""
    config_path = rootdir / "hidden_stix_config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "GlobalVariables": {"rootdir": str(rootdir)},
                "DataAdapter": {"stix": {"type": "damlive_stix", "path": str(path)}},
            }
        )
    )
    config = Config(config_path=config_path)
    config.lees_config()
    return DataAdapter(config).input("stix")


def test_calc_folder_loads_every_damlive_result(run_damlive_dir):
    before = sorted(p.relative_to(run_damlive_dir) for p in run_damlive_dir.rglob("*"))

    collection = load(run_damlive_dir, CALC)

    assert len(collection) == 4
    assert sorted(
        (f.location, f.step, f.calculation_model, f.timestamp)
        for f in (collection[name] for name in collection)
    ) == [
        ("PU0013_87074-1_DWP001", 0, "Bishop", datetime(2024, 11, 6, 7)),
        ("PU0013_87074-1_DWP001", 1, "Bishop", datetime(2024, 11, 6, 8)),
        ("PU0021_87074-1_DWP002", 0, "UpliftVan", datetime(2024, 11, 6, 7)),
        ("PU0021_87074-1_DWP002", 1, "UpliftVan", datetime(2024, 11, 6, 8)),
    ]
    # nothing is extracted: only the config this test wrote is new
    after = sorted(p.relative_to(run_damlive_dir) for p in run_damlive_dir.rglob("*"))
    assert sorted(set(after) - set(before)) == [Path("hidden_stix_config.yaml")]


def test_damlive_result_matches_original_tables(run_damlive_dir):
    """IDs and soil assignments agree with the original implementation (test oracle)."""
    collection = load(run_damlive_dir, CALC)
    for name in collection:
        stix = collection[name]
        expected = expected_tables(stix.path)
        soil = expected["merged_soil"]

        stages = [stage for _, stage in stix.stages()]
        assert {s.Id for s in stages} == set(soil["stage_id"])
        assert {s.GeometryId for s in stages} == set(soil["geometry_id"])
        assert {s.WaternetId for s in stages} == set(
            expected["merged_waternet"]["waternet_id"]
        )
        calculations = [c for scenario in stix.scenarios for c in scenario.Calculations]
        assert {stix.settings_of(c).Id for c in calculations} == set(
            expected["merged_calculations"]["calculationsettings_id"]
        )
        # DamLive result files hold no calculated results
        assert all(c.ResultId is None for c in calculations)

        for _, stage in stix.stages():
            assigned = {
                layer.Id: stix.soil_of_layer(stage, layer.Id).Id
                for layer in stix.geometry_of(stage).Layers
            }
            expected_assigned = dict(
                soil[soil["stage_id"] == stage.Id][["layer_id", "soil_id"]].values
            )
            assert assigned == expected_assigned


def test_single_input_profile_loads_without_damlive_metadata(run_damlive_dir):
    collection = load(run_damlive_dir, INPUT_PROFILE)

    assert len(collection) == 1
    stix = collection["PU0013_87074-1_DWP001"]
    assert (stix.location, stix.step, stix.calculation_model, stix.timestamp) == (
        None,
        None,
        None,
        None,
    )
    for _, stage in stix.stages():
        assert stix.geometry_of(stage).Layers
        assert stix.waternet_of(stage).Id == stage.WaternetId


def test_select_picks_one_file_by_location_and_time(run_damlive_dir):
    collection = load(run_damlive_dir, CALC)

    stix = collection.select("PU0013_87074-1_DWP001", "2024-11-06T08:00")
    assert (stix.step, stix.calculation_model) == (1, "Bishop")
    assert (
        collection.select(
            "PU0021_87074-1_DWP002", datetime(2024, 11, 6, 7), "UpliftVan"
        ).step
        == 0
    )

    with pytest.raises(KeyError, match="Geen stix-bestand"):
        collection.select("PU0013_87074-1_DWP001", "2024-11-06T09:00")

    duplicated = StixCollection({"a": stix, "b": stix})
    with pytest.raises(KeyError, match="Meerdere stix-bestanden"):
        duplicated.select("PU0013_87074-1_DWP001", "2024-11-06T08:00")


def test_unreadable_input_names_the_path(run_damlive_dir, tmp_path):
    with pytest.raises(FileNotFoundError, match="missing.stix"):
        StixFile.from_path(tmp_path / "missing.stix")

    not_a_zip = tmp_path / "not_a_zip_result.stix"
    not_a_zip.write_text("not a zip")
    with pytest.raises(ValueError, match="not_a_zip_result.stix"):
        StixFile.from_path(not_a_zip)
    # through the DataAdapter the same problem is a UserWarning, as for other inputs
    with pytest.raises(UserWarning, match="not_a_zip_result.stix"):
        load(run_damlive_dir, not_a_zip)

    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(UserWarning, match="Geen `\\*_result.stix`"):
        load(run_damlive_dir, empty)

    with pytest.raises(UserWarning, match="bestaat niet"):
        load(run_damlive_dir, tmp_path / "nowhere")


def test_folder_warns_for_names_outside_damlive_naming(run_damlive_dir, tmp_path):
    folder = tmp_path / "calc"
    folder.mkdir()
    damlive = next((run_damlive_dir / CALC).rglob("*_result.stix"))
    shutil.copy(damlive, folder / damlive.name)
    shutil.copy(damlive, folder / "renamed_result.stix")

    with pytest.warns(UserWarning, match="renamed_result.stix"):
        collection = load(run_damlive_dir, folder)

    assert collection["renamed_result"].location is None
    assert collection[damlive.stem].location == "PU0013_87074-1_DWP001"


def test_single_file_outside_damlive_naming_does_not_warn(run_damlive_dir):
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        load(run_damlive_dir, INPUT_PROFILE)
