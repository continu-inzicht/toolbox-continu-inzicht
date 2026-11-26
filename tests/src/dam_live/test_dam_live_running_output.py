import datetime
from pathlib import Path
from unittest import mock


from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.dam_live import UpdateDamLive

from toolbox_continu_inzicht.dam_live.combine_results import CombineDamLiveResults


def test_integration_damlive(run_damlive_dir, fake_damlive, tmp_path):
    """DamLive requires custom binaries which we can't provide in CI.
    Only the call to the DamLive executable is replaced; the rest of the flow is tested.
    """
    data_sets_path = Path(__file__).parent / "data_sets" / "run_damlive"

    config = Config(config_path=data_sets_path / "test_run_damlive.yaml")
    config.lees_config()
    config.global_variables["rootdir"] = run_damlive_dir
    # DAMLIVE_EXE comes from the .env file; the template provides a placeholder value
    config.global_variables["dotenv_path"] = Path(__file__).parents[3] / ".env.template"

    data_adapter = DataAdapter(config)

    update_dam_live = UpdateDamLive(data_adapter=data_adapter)

    with mock.patch.object(update_dam_live, "_execute", side_effect=fake_damlive):
        update_dam_live.run(
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
    unziped_dirs = [str(path) for path in update_dam_live.lst_unzipped_damlive_results]
    # uit de bestands namen halen we de locatie en tijdstap, hier koppelen we vervolgens weer met de output dataframe
    # folder names are <location>.<step>.<time>
    loc_time = [
        (loc, time)
        for loc, _step, time in (Path(fname).name.split(".") for fname in unziped_dirs)
    ]
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
        lambda x: (
            datetime.datetime.fromisoformat(x).astimezone(tz).strftime("%Y-%m-%dT%H")
        )
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
    with mock.patch.object(update_dam_live, "_execute", side_effect=fake_damlive):
        update_dam_live.run(
            input=["waterstanden_xml_uur", "parameters_uplift_csv"],
            output="output_file",
        )

    update_dam_live.data_adapter.input("output_file")

    update_dam_live.unzip_damlive_results()
    unziped_dirs = [str(path) for path in update_dam_live.lst_unzipped_damlive_results]
    # folder names are <location>.<step>.<time>
    loc_time = [
        (loc, time)
        for loc, _step, time in (Path(fname).name.split(".") for fname in unziped_dirs)
    ]
    fig.savefig(tmp_path / "test_run_damlive_plot.png")
