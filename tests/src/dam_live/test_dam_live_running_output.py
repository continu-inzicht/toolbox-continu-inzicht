from datetime import datetime
from pathlib import Path
from unittest import mock

import pandas as pd

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.dam_live import UpdateDamLive
from toolbox_continu_inzicht.dam_live.combine_results import (
    DAMLIVE_TIMEZONE,
    CombineDamLiveResults,
)


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

    df_output_damlive = data_adapter.input("output_file")

    # We kunnen nu een van de resultaten bekijken, gekozen op locatie en tijd in de config
    combine_config = Config(config_path=data_sets_path / "test_combine_damlive.yaml")
    combine_config.lees_config()
    combine_config.global_variables["rootdir"] = run_damlive_dir
    options = combine_config.global_variables["CombineDamLiveResults"]
    loc = options["location"]
    time = datetime.fromisoformat(options["timestamp"])

    combine_damlive_results = CombineDamLiveResults(
        data_adapter=DataAdapter(combine_config)
    )
    combine_damlive_results.run(
        input=["damlive_results", "colors"],
        output=["merge_soil", "merge_waternet", "merge_calculations"],
    )
    assert combine_damlive_results.stix.step == 1

    # en koppelen aan de uitvoer van DamLive, die in dezelfde tijdzone staat
    date_time = pd.to_datetime(df_output_damlive["date_time"]).dt.tz_convert(
        DAMLIVE_TIMEZONE
    )
    df_corresponding_output = df_output_damlive[
        (df_output_damlive["measurement_location_code"] == loc)
        & (date_time == pd.Timestamp(time, tz=DAMLIVE_TIMEZONE))
    ]
    sf = df_corresponding_output.set_index("parameter_code").loc[
        "StabilityInsideFactor", "value"
    ]

    # En vervolgens plotten -> test of het werkt is al een check
    stage_id = combine_damlive_results.df_merged_soils["stage_id"].iloc[0]
    fig, ax = combine_damlive_results.plot_stage(
        stage_id=stage_id,
        xlim=(0, 65),
        ylim=(-15, 6),
    )
    ax.set_title(
        f"Stage {stage_id} - Location: {loc} - Time: {time}, StabilityInsideFactor: {sf:.3f}"
    )
    fig.savefig(tmp_path / "test_run_damlive_plot.png")

    with mock.patch.object(update_dam_live, "_execute", side_effect=fake_damlive):
        update_dam_live.run(
            input=["waterstanden_xml_uur", "parameters_uplift_csv"],
            output="output_file",
        )

    assert len(data_adapter.input("output_file")) > 0
