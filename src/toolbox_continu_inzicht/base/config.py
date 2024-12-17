import yaml
from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel
from yaml.scanner import ScannerError
from datetime import datetime, timezone


class Config(PydanticBaseModel):
    """Basis functie om de configuratie in te laden.

    Parameters
    ----------
    config_path: Path
                 Pad naar een  `.yaml` bestand waarin per functie staat beschreven wat de in/ouput bestanden zijn

    """

    config_path: Path
    global_variables: dict = {}
    data_adapters: dict = {}
    # available_types: list[str] = [
    #     "csv",
    #     "python",
    #     "csv_source",
    #     "postgresql_database",
    #     "netcdf",
    #     "ci_postgresql_from_waterlevels",
    #     "ci_postgresql_from_conditions",
    #     "ci_postgresql_from_measuringstations",
    #     "ci_postgresql_to_data",
    #     "ci_postgresql_to_states",
    # ]

    def lees_config(self):
        """Laad het gegeven pad in, zet de configuraties klaar in de Config class"""

        with self.config_path.open() as fin:
            try:
                data = yaml.safe_load(fin)
            except ScannerError:
                raise UserWarning(
                    f"Het yaml configuratie bestand '{self.config_path}' kan niet worden gelezen."
                    + "Controleer dat spatie worden gebruikt inplaats van tabs."
                )

        for header, configuration in data.items():
            match header:
                case "DataAdapter":
                    self.data_adapters = configuration
                case "GlobalVariables":
                    # add a central calculating time in case not specified
                    if "calc_time" not in configuration:
                        dt_now = datetime.now(timezone.utc)
                        t_now = datetime(
                            dt_now.year,
                            dt_now.month,
                            dt_now.day,
                            dt_now.hour,
                            0,
                            0,
                        ).replace(tzinfo=timezone.utc)
                        configuration["calc_time"] = t_now
                    else:
                        try:
                            dt = datetime.fromisoformat(configuration["calc_time"])
                        except Exception as e:
                            raise UserWarning(
                                f"Issue parsing calc_time: {configuration['calc_time']}.\ncheck: {e}"
                            )
                        formated_dt = datetime(
                            dt.year,
                            dt.month,
                            dt.day,
                            dt.hour,
                            0,
                            0,
                        ).replace(tzinfo=timezone.utc)
                        configuration["calc_time"] = formated_dt

                    self.global_variables = configuration
        
        self.init_data_adapters()
            

    def init_data_adapters(self):
        # opties die in de DataAdapter worden mee gegeven
        # worden toegevoegd aan de adapters, mits de adapter zelf niet die waarde heeft
        if "default_options" in self.data_adapters:
            default_options = self.data_adapters["default_options"]
            adapters = set(self.data_adapters.keys())
            adapters.discard("default_options")
            for adapter in adapters:
                input_type = self.data_adapters[adapter]["type"]
                if input_type in default_options:
                    for key in default_options[input_type]:
                        # alleen nieuwe opties toeveogen als die er niet al zijn
                        if key not in self.data_adapters[adapter]:
                            self.data_adapters[adapter][key] = default_options[
                                input_type
                            ][key]
