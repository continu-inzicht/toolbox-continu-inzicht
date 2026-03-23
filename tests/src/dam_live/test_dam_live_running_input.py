from pathlib import Path
from unittest import mock

import pandas as pd

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsToMoments
from toolbox_continu_inzicht.dam_live import UpdateDamLive


data_sets_path = Path(__file__).parent / "data_sets" / "run_damlive"


def test_integration_damlive_input():
    """DamLive requires custom binaries & license which we can't provide in CI.
    Therefore we mock the run function, but we can still test the rest of the code and the flow.
    Checks if the input goes well
    """
    config = Config(config_path=data_sets_path / "test_loads_fews_grondwater.yaml")
    config.lees_config()
    config.global_variables["rootdir"] = data_sets_path

    data_adapter = DataAdapter(config)
    data_adapter.set_global_variable("moments", [i for i in range(0, 2, 1)])

    loads_to_moments = LoadsToMoments(data_adapter=data_adapter)
    loads_to_moments.run(input="waterstanden_xml", output="waterstanden_xml_hourly")

    config = Config(config_path=data_sets_path / "test_run_damlive.yaml")
    config.lees_config()
    # DAMLIVE_EXE is set in the .env file, but we also need to set it here for the test to work,
    # use template
    config.global_variables["dotenv_path"] = Path.cwd() / ".env.template"

    data_adapter = DataAdapter(config)
    update_dam_live = UpdateDamLive(data_adapter=data_adapter)

    with mock.patch.object(update_dam_live, "run", mock_run):
        update_dam_live.run(
            self=update_dam_live,
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
            self=update_dam_live,
            input=["waterstanden_xml_uur", "parameters_uplift_csv"],
            output="output_file",
        )

    df_output_uplift = update_dam_live.data_adapter.input("output_file")
    assert not df_output_uplift.empty


def mock_run(self, input, output):
    """Mock run that loads focussed on the input data and the data adapter handling of the run function."""
    # inladen van scenariokansen per deeltraject (segment)
    df_in = self.data_adapter.input(
        input=input[0],
        schema=self.schema_loads,
    )
    # DAMLIVE is gevoelig voor dubbele datums per locatie,
    # Bij de loads to moments functie kan dit echter wel gebeuren
    # daarom worden deze hier verwijderd. Er wordt per locatie gefilterd op unieke datums.
    lst_df_locations: list[pd.DataFrame] = []
    for loc in df_in["measurement_location_code"].unique():
        subset = df_in[df_in["measurement_location_code"] == loc].copy()
        subset.drop_duplicates(subset=["date_time"])
        lst_df_locations.append(subset)

    self.df_in_loads = pd.concat(lst_df_locations)

    self.df_in_calculation_settings = self.data_adapter.input(
        input=input[1],
        schema=self.schema_calculation_settings,
    )
    global_variables = self.data_adapter.config.global_variables
    options = global_variables.get("UpdateDamLive", {})

    assert "DAMLIVE_FILE" in options, (
        "DAMLIVE_FILE {'.damx'}is not set in global variables"
    )

    # DAM LIVE verwacht een vast formaat dus daarom worden de data adapters voor deze module hier hard gedefinieerd.
    damlive_data_adapters = {
        "live.InputTimeSeries": {
            "type": "xml_timeseries",
            "path": "live.InputTimeSeries.xml",
        },
        "live.ParametersFile": {
            "type": "xml_calculation_parameters",
            "path": "live.ParametersFile.xml",
        },
        "live.OutputTimeSeries": {
            "type": "xml_timeseries",
            "path": "hidden_live.OutputTimeSeries.xml",
        },
    }
    self.data_adapter.config.data_adapters.update(damlive_data_adapters)

    # write output to the folder
    self.data_adapter.output(output="live.InputTimeSeries", df=self.df_in_loads)
    self.data_adapter.output(
        output="live.ParametersFile", df=self.df_in_calculation_settings
    )
    damlive_exe = self.data_adapter.config.data_adapters["live.InputTimeSeries"].get(
        "DAMLIVE_EXE", None
    )
    assert damlive_exe is not None, (
        "DAMLIVE_EXE is not set, ensure that it is set in the .env file"
    )

    # up till here was the normal function, below is the mock output creation,
    # this should be replaced with the actual run of the binary and reading the output file.

    # Create mock output: one StabilityInsideFactor row per location per moment
    locations = self.df_in_loads["measurement_location_code"].unique()
    times = self.df_in_loads["date_time"].unique()
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
    self.df_out = df_out
    self.data_adapter.output(output=output, df=df_out)
