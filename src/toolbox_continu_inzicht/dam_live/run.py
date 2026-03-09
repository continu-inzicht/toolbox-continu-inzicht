import os
import subprocess
import sys
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
        Dataframe met instellingen voor Dam Live berekeningen
    schema_loads: ClassVar[dict[str, str]]
        Schema voor inladen belastingen
    schema_calculation_settings: ClassVar[dict[str, str]]
        Schema voor instellingen voor Dam Live berekeningen

    Notes
    -----

    """

    data_adapter: DataAdapter

    df_in_loads: Optional[pd.DataFrame] | None = None
    df_in_calculation_settings: Optional[pd.DataFrame] | None = None

    schema_loads: ClassVar[dict[str, str]] = {
        "date_time": ["datetime64[ns, UTC]", "datetime64[ns]"],
        "measurement_location_code": "object",
        "parameter_code": "object",
        "unit": "object",
        "value": "float64",
    }
    schema_calculation_settings: ClassVar[dict[str, str]] = {
        "parameter_values": "object",
        "parameter_names": "object",
    }

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
        delete_output_folder = options.get("delete_output_folder", False)

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

        damlive_name = ".".join(options["DAMLIVE_FILE"].split(".")[:-1]) + ".Calc"

        if (root_dir / damlive_name).exists():
            if delete_output_folder:
                remove_dir(root_dir / damlive_name)
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
        self.data_adapter.logger.debug(f"Running command:\n{' '.join(cmd)}")
        # start dam live
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
        )
        for line in proc.stdout:
            self.data_adapter.logger.info(line.rstrip())
        returncode = proc.wait()
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, cmd)

        self.df_out = self.data_adapter.input(
            input="live.OutputTimeSeries",
        )
        self.data_adapter.output(output=output, df=self.df_out)


def remove_dir(folder):
    folder = str(folder)
    if sys.platform == "win32" and not folder.startswith("\\\\?\\"):
        folder = "\\\\?\\" + folder
    for root, dirs, files in os.walk(folder, topdown=False):
        for name in files:
            filepath = os.path.join(root, name)
            os.chmod(filepath, 0o666)
            os.remove(filepath)
        for name in dirs:
            os.rmdir(os.path.join(root, name))
    os.rmdir(folder)
