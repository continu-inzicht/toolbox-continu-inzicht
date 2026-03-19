from pathlib import Path
from unittest import mock

import pandas as pd

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsToMoments
from toolbox_continu_inzicht.dam_live import UpdateDamLive


DATA_SETS_PATH = Path(__file__).parent / "data_sets" / "run_damlive"


def test_integration_damlive_input():
    """DamLive requires custom binaries & license which we can't provide in CI.
    Therefore we mock the run function, but we can still test the rest of the code and the flow.
    Checks if the input goes well
    """
    config = Config(config_path=DATA_SETS_PATH / "test_loads_fews_grondwater.yaml")
    config.lees_config()
    config.global_variables["rootdir"] = DATA_SETS_PATH

    data_adapter = DataAdapter(config)
    data_adapter.set_global_variable("moments", [i for i in range(0, 2, 1)])

    loads_to_moments = LoadsToMoments(data_adapter=data_adapter)
    loads_to_moments.run(input="waterstanden_xml", output="waterstanden_xml_hourly")

    config = Config(config_path=DATA_SETS_PATH / "run_damlive.yaml")
    config.lees_config()
    config.global_variables["rootdir"] = DATA_SETS_PATH

    data_adapter = DataAdapter(config)
    update_dam_live = UpdateDamLive(data_adapter=data_adapter)

    def mock_run(input, output):
        """Mock run that loads inputs and places output file without running the binary."""
        df_in = update_dam_live.data_adapter.input(
            input=input[0],
            schema=update_dam_live.schema_loads,
        )
        lst_df_locations = []
        for loc in df_in["measurement_location_code"].unique():
            subset = df_in[df_in["measurement_location_code"] == loc].copy()
            subset.drop_duplicates(subset=["date_time"])
            lst_df_locations.append(subset)
        update_dam_live.df_in_loads = pd.concat(lst_df_locations)

        update_dam_live.df_in_calculation_settings = update_dam_live.data_adapter.input(
            input=input[1],
            schema=update_dam_live.schema_calculation_settings,
        )

        # Create mock output: one StabilityInsideFactor row per location per moment
        locations = update_dam_live.df_in_loads["measurement_location_code"].unique()
        times = update_dam_live.df_in_loads["date_time"].unique()
        rows = [
            {
                "date_time": t,
                "measurement_location_code": loc,
                "parameter_code": "StabilityInsideFactor",
                "unit": "-",
                "value": 1.5,
            }
            for loc in locations
            for t in times
        ]
        df_out = pd.DataFrame(rows)
        update_dam_live.df_out = df_out
        update_dam_live.data_adapter.output(output=output, df=df_out)

    with mock.patch.object(update_dam_live, "run", mock_run):
        update_dam_live.run(
            input=["waterstanden_xml_uur", "parameters_bishop_csv"],
            output="output_file",
        )

    df_output_damlive = update_dam_live.data_adapter.input("output_file")
    assert not df_output_damlive.empty
    assert set(
        ["date_time", "measurement_location_code", "parameter_code", "value"]
    ).issubset(df_output_damlive.columns)

    with mock.patch.object(update_dam_live, "run", mock_run):
        update_dam_live.run(
            input=["waterstanden_xml_uur", "parameters_uplift_csv"],
            output="output_file",
        )

    df_output_uplift = update_dam_live.data_adapter.input("output_file")
    assert not df_output_uplift.empty
