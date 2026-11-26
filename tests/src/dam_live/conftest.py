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
def parse_dir(tmp_path) -> Path:
    """Writable copy of the extracted `.stix` folder used by the parse test."""
    return Path(
        shutil.copytree(
            Path(__file__).parent / "data_sets" / "WV2030_PU0013_87074-1",
            tmp_path / "WV2030_PU0013_87074-1",
            ignore=shutil.ignore_patterns("*.Calc", "hidden_*", "*.png"),
        )
    )
