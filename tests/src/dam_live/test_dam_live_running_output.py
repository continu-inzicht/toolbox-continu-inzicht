from pathlib import Path

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsToMoments
from toolbox_continu_inzicht.dam_live import UpdateDamLive
from toolbox_continu_inzicht.dam_live import CombineDamLiveResults
import datetime


def test_integration_damlive():
    """DamLive requires custom binaries & lisence which we can't provide in CI
    Therefore we mock the run function, but we can still test the rest of the code and the flow.
    """
    config = Config(
        config_path=Path().cwd()
        / "data_sets"
        / "run_damlive"
        / "test_loads_fews_grondwater.yaml"
    )
    config.lees_config()

    data_adapter = DataAdapter(config)

    data_adapter.set_global_variable("moments", [i for i in range(0, 2, 1)])

    loads_to_moments = LoadsToMoments(data_adapter=data_adapter)
    loads_to_moments.run(input="waterstanden_xml", output="waterstanden_xml_hourly")

    config = Config(
        config_path=Path(__file__).parent
        / "data_sets"
        / "run_damlive"
        / "run_damlive.yaml"
    )
    config.lees_config()

    data_adapter = DataAdapter(config)

    update_dam_live = UpdateDamLive(data_adapter=data_adapter)

    # with mock.patch("toolbox_continu_inzicht.dam_live.UpdateDamLive.run") as mock_run:
    #     mock_run.return_value = None  # or set appropriate return value
    update_dam_live.run(
        input=["waterstanden_xml_uur", "parameters_bishop_csv"],
        output="output_file",
    )

    df_output_damlive = update_dam_live.data_adapter.input("output_file")
    df_output_damlive.head(5)

    update_dam_live.unzip_damlive_results()

    config = Config(
        config_path=Path().cwd()
        / "data_sets"
        / "run_damlive"
        / "dam_live_parse_config.yaml"
    )
    config.lees_config()

    data_adapter = DataAdapter(config)

    # na het uitpakken slaan we de resulterende bestandsnamen op, deze kunnen we hier weer inlezen
    # dit is een voorbeeld specifiek voor deze configuratie en je paden, daarom is het niet in de functie verwerkt.
    unziped_dirs = [
        str(path).split("development_notebooks\\dam_live\\")[1]
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

    # TODO: fix
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

    # En vervolgens plotten

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

    update_dam_live.run(
        input=["waterstanden_xml_uur", "parameters_uplift_csv"], output="output_file"
    )

    update_dam_live.data_adapter.input("output_file").head(5)

    update_dam_live.unzip_damlive_results()
    unziped_dirs = [
        str(path).split("development_notebooks\\dam_live\\")[1]
        for path in update_dam_live.lst_unzipped_damlive_results
    ]
    loc_time = [
        (fname.split("\\")[-1].split(".")[0], fname.split("\\")[-1].split(".")[-1])
        for fname in unziped_dirs
    ]
    print(f"unzipped {len(unziped_dirs)} dirs")
