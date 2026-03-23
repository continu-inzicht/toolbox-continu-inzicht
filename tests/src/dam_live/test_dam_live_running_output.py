import datetime
from pathlib import Path
import shutil
from unittest import mock

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.dam_live import UpdateDamLive

from toolbox_continu_inzicht.dam_live.combine_results import CombineDamLiveResults


def test_integration_damlive():
    """DamLive requires custom binaries & lisence which we can't provide in CI
    Therefore we mock the run function, but we can still test the rest of the code and the flow.
    """
    data_sets_path = Path(__file__).parent / "data_sets" / "run_damlive"

    config = Config(config_path=data_sets_path / "test_run_damlive.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config)

    update_dam_live = UpdateDamLive(data_adapter=data_adapter)

    # with mock.patch("toolbox_continu_inzicht.dam_live.UpdateDamLive.run") as mock_run:
    #     mock_run.return_value = None  # or set appropriate return value
    with mock.patch.object(update_dam_live, "run", mock_run):
        update_dam_live.run(
            self=update_dam_live,
            input=["waterstanden_xml_uur", "parameters_bishop_csv"],
            output="output_file",
        )

    df_output_damlive = update_dam_live.data_adapter.input("output_file")
    update_dam_live.unzip_damlive_results()

    config = Config(config_path=data_sets_path / "test_dam_live_parse_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config)

    # na het uitpakken slaan we de resulterende bestandsnamen op, deze kunnen we hier weer inlezen
    # dit is een voorbeeld specifiek voor deze configuratie en je paden, daarom is het niet in de functie verwerkt.
    unziped_dirs = [
        str(path).split("TBCI\\toolbox-continu-inzicht\\")[1]
        for path in update_dam_live.lst_unzipped_damlive_results
    ]
    # uit de bestands namen halen we de locatie en tijdstap, hier koppelen we vervolgens weer met de output dataframe
    loc_time = [
        (fname.split("\\")[-1].split(".")[0], fname.split("\\")[-1].split(".")[-1])
        for fname in unziped_dirs
    ]
    print(f"unzipped {len(unziped_dirs)} dirs")
    unziped_dirs[:5]

    # We kunnen nu een van de resultaten bekijken:
    run_index = 1
    config.global_variables["rootdir"] = unziped_dirs[run_index]
    combine_damlive_results = CombineDamLiveResults(data_adapter)

    combine_damlive_results.run(
        input=[
            "scenario",
            "geometries",
            "soils",
            "soillayers",
            "waternets",
            "calculationsettings",
            "colors",
        ],
        output=["merge_soil", "merge_waternet", "merge_calculations"],
    )
    stage_id = list(combine_damlive_results.df_merged_soils.stage_id.unique())

    tz = datetime.timezone(datetime.timedelta(hours=1))
    loc, time = loc_time[run_index]
    date_time = df_output_damlive["date_time"].apply(
        lambda x: datetime.datetime.fromisoformat(x)
        .astimezone(tz)
        .strftime("%Y-%m-%dT%H")
    )
    df_corresponding_output = df_output_damlive[
        (df_output_damlive["measurement_location_code"] == loc) & (date_time == time)
    ]
    df_corresponding_output

    # En vervolgens plotten -> test of het werkt is al een check

    fig, ax = combine_damlive_results.plot_stage(
        stage_id=int(stage_id[0]),
        xlim=(0, 65),
        ylim=(-15, 6),
    )
    sf = df_corresponding_output.set_index("parameter_code").loc[
        "StabilityInsideFactor", "value"
    ]
    ax.set_title(
        f"Stage {stage_id[0]} - Location: {loc} - Time: {time}, StabilityInsideFactor: {sf:.3f}"
    )
    with mock.patch.object(update_dam_live, "run", mock_run):
        update_dam_live.run(
            self=update_dam_live,
            input=["waterstanden_xml_uur", "parameters_uplift_csv"],
            output="output_file",
        )

    update_dam_live.data_adapter.input("output_file").head(5)

    update_dam_live.unzip_damlive_results()
    unziped_dirs = [
        str(path).split("TBCI\\toolbox-continu-inzicht\\")[1]
        for path in update_dam_live.lst_unzipped_damlive_results
    ]
    loc_time = [
        (fname.split("\\")[-1].split(".")[0], fname.split("\\")[-1].split(".")[-1])
        for fname in unziped_dirs
    ]
    print(f"unzipped {len(unziped_dirs)} dirs")
    fig.savefig(data_sets_path / "test_run_damlive_plot.png")


def mock_run(self, input, output):
    """Mock run that focuses on the output files generated without running the binary."""
    # input is unused but loading it ensure the data adapter has the right paths
    _ = self.data_adapter.input(
        input=input[0],
        schema=self.schema_loads,
    )
    # copy files from test data to output location to simulate the unzipping of damlive results
    root_dir = self.data_adapter.get_global_variable("used_root_dir")
    options = self.data_adapter.config.global_variables.get("UpdateDamLive", {})
    damlive_name = ".".join(options["DAMLIVE_FILE"].split(".")[:-1]) + ".Calc"
    results_dir = root_dir / damlive_name
    results_from_dir = root_dir / "WV2030_Purmer.Calc_tests_sets"

    shutil.copytree(results_from_dir, results_dir, dirs_exist_ok=True)

    shutil.copy(
        results_dir / "live.OutputTimeSeries.xml",
        root_dir / "live.OutputTimeSeries.xml",
    )
    damlive_data_adapters = {
        "live.OutputTimeSeries": {
            "type": "xml_timeseries",
            "path": "live.OutputTimeSeries.xml",
        },
    }
    self.data_adapter.config.data_adapters.update(damlive_data_adapters)
    self.df_out = self.data_adapter.input(
        input="live.OutputTimeSeries",
    )
    self.data_adapter.output(output=output, df=self.df_out)
