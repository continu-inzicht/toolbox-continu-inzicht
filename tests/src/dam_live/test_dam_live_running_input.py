import subprocess
import sys
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsToMoments
from toolbox_continu_inzicht.dam_live import UpdateDamLive


data_sets_path = Path(__file__).parent / "data_sets" / "run_damlive"
# DAMLIVE_EXE comes from the .env file; the template provides a placeholder value
env_template = Path(__file__).parents[3] / ".env.template"


def setup_update_dam_live(rootdir: Path) -> UpdateDamLive:
    config = Config(config_path=data_sets_path / "test_run_damlive.yaml")
    config.lees_config()
    config.global_variables["rootdir"] = rootdir
    config.global_variables["dotenv_path"] = env_template
    return UpdateDamLive(data_adapter=DataAdapter(config))


def test_integration_damlive_input(run_damlive_dir, fake_damlive):
    """DamLive requires custom binaries which we can't provide in CI.
    Only the call to the DamLive executable is replaced; the rest of run() is tested.
    """
    config = Config(config_path=data_sets_path / "test_loads_fews_grondwater.yaml")
    config.lees_config()
    config.global_variables["rootdir"] = run_damlive_dir

    data_adapter = DataAdapter(config)
    data_adapter.set_global_variable("moments", [i for i in range(0, 2, 1)])

    loads_to_moments = LoadsToMoments(data_adapter=data_adapter)
    loads_to_moments.run(input="waterstanden_xml", output="waterstanden_xml_hourly")

    update_dam_live = setup_update_dam_live(run_damlive_dir)

    with mock.patch.object(
        update_dam_live, "_execute", side_effect=fake_damlive
    ) as execute:
        update_dam_live.run(
            input=["waterstanden_xml_uur", "parameters_bishop_csv"],
            output="output_file",
        )

    # input files for DamLive are written next to the project
    assert (run_damlive_dir / "live.InputTimeSeries.xml").exists()
    assert (run_damlive_dir / "live.ParametersFile.xml").exists()

    # DamLive is started once, with the project, input, output and parameter files
    execute.assert_called_once()
    cmd = execute.call_args.args[0]
    assert cmd[0] == "xx"
    root = run_damlive_dir.resolve()
    assert cmd[1:] == [
        "-d",
        (root / "WV2030_Purmer.damx").as_posix(),
        "-i",
        (root / "live.InputTimeSeries.xml").as_posix(),
        "-o",
        (root / "live.OutputTimeSeries.xml").as_posix(),
        "-p",
        (root / "live.ParametersFile.xml").as_posix(),
    ]

    df_output_damlive = update_dam_live.data_adapter.input("output_file")
    assert not df_output_damlive.empty
    assert set(
        ["date_time", "measurement_location_code", "parameter_code", "value"]
    ).issubset(df_output_damlive.columns)

    with mock.patch.object(update_dam_live, "_execute", side_effect=fake_damlive):
        update_dam_live.run(
            input=["waterstanden_xml_uur", "parameters_uplift_csv"],
            output="output_file",
        )

    df_output_uplift = update_dam_live.data_adapter.input("output_file")
    assert not df_output_uplift.empty


def test_damlive_without_output_file(run_damlive_dir):
    """When DamLive writes no output file, run() reports it and writes no output."""
    update_dam_live = setup_update_dam_live(run_damlive_dir)

    with (
        mock.patch.object(update_dam_live, "_execute"),
        mock.patch.object(update_dam_live.data_adapter.logger, "error") as log_error,
    ):
        update_dam_live.run(
            input=["waterstanden_xml_uur", "parameters_bishop_csv"],
            output="output_file",
        )

    log_error.assert_called_once()
    assert "live.OutputTimeSeries.xml" in log_error.call_args.args[0]
    assert update_dam_live.df_out is None
    assert not (run_damlive_dir / "hidden_test.csv").exists()


def test_execute_streams_log_and_raises_on_failure(run_damlive_dir):
    """The execution boundary itself, with Python standing in for the DamLive executable."""
    update_dam_live = setup_update_dam_live(run_damlive_dir)

    with mock.patch.object(update_dam_live.data_adapter.logger, "info") as log_info:
        update_dam_live._execute([sys.executable, "-c", "print('DamLive says hi')"])
    log_info.assert_any_call("DamLive says hi")

    with pytest.raises(subprocess.CalledProcessError):
        update_dam_live._execute([sys.executable, "-c", "raise SystemExit(1)"])


def test_duplicate_dates_per_location_are_dropped(run_damlive_dir, fake_damlive):
    """DamLive can't handle two values for the same location and time."""
    update_dam_live = setup_update_dam_live(run_damlive_dir)
    data_adapter = update_dam_live.data_adapter

    # loads with one duplicated value for one location
    df_loads = data_adapter.input("waterstanden_xml_uur")
    location = df_loads["measurement_location_code"].iloc[0]
    duplicate = df_loads[df_loads["measurement_location_code"] == location].iloc[[0]]
    data_adapter.config.data_adapters["loads_with_duplicate"] = {
        "type": "xml_timeseries",
        "path": "hidden_loads_with_duplicate.xml",
    }
    data_adapter.output("loads_with_duplicate", pd.concat([df_loads, duplicate]))

    with mock.patch.object(update_dam_live, "_execute", side_effect=fake_damlive):
        update_dam_live.run(
            input=["loads_with_duplicate", "parameters_bishop_csv"],
            output="output_file",
        )

    df_written = data_adapter.input("live.InputTimeSeries")
    key = ["measurement_location_code", "date_time"]
    assert not df_written.duplicated(subset=key).any()
    # only the duplicate is gone: every location and time is still there
    assert len(df_written) == len(df_loads.drop_duplicates(subset=key))
