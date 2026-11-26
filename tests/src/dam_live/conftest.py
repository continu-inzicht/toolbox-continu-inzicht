"""Shared fixtures for the DamLive tests.

Tests run on a copy of the DamLive test data in a temporary folder, so they never
write into the repository's data_sets folder. Files the tests generate themselves
(`*.Calc`, `hidden_*`, `*.png`) are never part of the copy.
"""

import shutil
from pathlib import Path

import pytest


@pytest.fixture
def run_damlive_dir(tmp_path) -> Path:
    """Writable copy of `data_sets/run_damlive`."""
    return Path(
        shutil.copytree(
            Path(__file__).parent / "data_sets" / "run_damlive",
            tmp_path / "run_damlive",
            ignore=shutil.ignore_patterns("*.Calc", "hidden_*", "*.png"),
        )
    )


@pytest.fixture
def fake_damlive():
    """Stand-in for `UpdateDamLive._execute`: behaves like a DamLive run on the fixtures.

    Reads the command like DamLive does and writes what DamLive would: the results
    folder `<project>.Calc` next to the `.damx` (`-d`) and the output time series (`-o`),
    both taken from `run_damlive/WV2030_Purmer.Calc_tests_sets`.
    """

    def run(cmd: list[str]) -> None:
        damx = Path(cmd[cmd.index("-d") + 1])
        output_xml = Path(cmd[cmd.index("-o") + 1])
        results = (
            Path(__file__).parent
            / "data_sets"
            / "run_damlive"
            / "WV2030_Purmer.Calc_tests_sets"
        )
        shutil.copytree(results, damx.with_suffix(".Calc"), dirs_exist_ok=True)
        shutil.copy(results / "live.OutputTimeSeries.xml", output_xml)

    return run


@pytest.fixture
def parse_dir(tmp_path) -> Path:
    """Writable copy of the extracted `.stix` folder used by the parse test."""
    return Path(
        shutil.copytree(
            Path(__file__).parent / "data_sets" / "WV2030_PU0013_87074-1",
            tmp_path / "WV2030_PU0013_87074-1",
            ignore=shutil.ignore_patterns("*.Calc", "hidden_*", "*.png"),
        )
    )
