import subprocess
from typing import ClassVar, Optional
from pydantic.dataclasses import dataclass

import pandas as pd

from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class UpdateDamLive(ToolboxBase):
    """
    Run de dam live module.

    Attributes
    ----------
    data_adapter: DataAdapter
        Data adapter voor in- en output van dataframes
    df_in_loads: Optional[pd.DataFrame] | None
        Dataframe met scenariokansen per deeltraject (segment)
    df_in_calculation_settings: Optional[pd.DataFrame] | None

    Notes
    -----

    """

    data_adapter: DataAdapter

    df_in_loads: Optional[pd.DataFrame] | None = None
    df_in_calculation_settings: Optional[pd.DataFrame] | None = None

    schema_loads: ClassVar[dict[str, str]] = {
        "date_time": "<M8[ns]",
        "measurement_location_code": "object",
        "parameter_code": "object",
        "unit": "object",
        "value": "float64",
    }
    schema_calculation_settings: ClassVar[dict[str, str]] = {
        "parameter_values": "object",
        "parameter_names": "object",
    }
    df_in_calculation_settings: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        De runner van de Update Dam Live module.

        parameters
        ----------
        input: list[str]
            Lijst met namen van de data adapter
        output: str
            Data adapter voor output van overstromingsrisico resultaten
        """

        # inladen van scenariokansen per deeltraject (segment)
        self.df_in_loads = self.data_adapter.input(
            input=input[0],
            schema=self.schema_loads,
        )

        self.df_in_calculation_settings = self.data_adapter.input(
            input=input[1],
            schema=self.schema_calculation_settings,
        )
        global_variables = self.data_adapter.config.global_variables
        options = global_variables.get("UpdateDamLive", {})

        assert "DAMLIVE_FILE" in options, (
            "DAMLIVE_FILE {'.damx'}is not set in global variables"
        )
        self.data_adapter.config.data_adapters

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
                "path": "live.OutputTimeSeries.xml",
            },
        }
        self.data_adapter.config.data_adapters.update(damlive_data_adapters)

        # write output to the folder
        self.data_adapter.output(output="live.InputTimeSeries", df=self.df_in_loads)
        self.data_adapter.output(
            output="live.ParametersFile", df=self.df_in_calculation_settings
        )
        root_dir = self.data_adapter.get_global_variable("used_root_dir")
        damlive_exe = self.data_adapter.config.data_adapters[
            "live.InputTimeSeries"
        ].get("DAMLIVE_EXE", None)
        assert damlive_exe is not None, (
            "DAMLIVE_EXE is not set, ensure that it is set in the .env file"
        )
        cmd = [
            damlive_exe,
            "-d",
            (root_dir / options["DAMLIVE_FILE"]).as_posix(),
            "-i",
            (root_dir / "live.InputTimeSeries.xml").as_posix(),
            "-o",
            (root_dir / "live.OutputTimeSeries.xml").as_posix(),
            "-p",
            (root_dir / "live.ParametersFile.xml").as_posix(),
        ]
        # start dam live
        subprocess.run(cmd)

        self.df_out = self.data_adapter.input(
            input="live.OutputTimeSeries",
        )
        self.data_adapter.output(output=output, df=self.df_out)
