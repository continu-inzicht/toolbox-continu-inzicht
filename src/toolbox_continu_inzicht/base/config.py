import yaml
from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel
from yaml.scanner import ScannerError
from datetime import datetime, timezone


class Config(PydanticBaseModel):
    """Basisfunctie om de configuratie in te laden.

    Attributes
    ----------
    config_path: Path
        Pad naar een `.yaml`-bestand waarin per functie staat beschreven wat de input- en outputbestanden zijn.
    global_variables: dict
        Globale variabelen die in de configuratie kunnen worden opgegeven
    data_adapters: dict
        Data adapters die in de configuratie kunnen worden op

    """

    config_path: Path
    global_variables: dict = {}
    data_adapters: dict = {}

    def lees_config(self):
        """Laadt het gegeven pad in, zet de configuraties klaar in de Config class."""

        with self.config_path.open(encoding="utf-8") as fin:
            try:
                data = yaml.safe_load(fin)
            except ScannerError:
                raise UserWarning(
                    f"Het YAML-configuratiebestand '{self.config_path}' kan niet worden gelezen."
                    + "Controleer of spaties worden gebruikt in plaats van tabs."
                )

        for header, configuration in data.items():
            match header:
                case "DataAdapter":
                    self.data_adapters = configuration
                case "GlobalVariables":
                    # Voeg een centrale rekentijd toe als deze niet is gespecificeerd
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
        # Opties die in de DataAdapter worden meegegeven
        # worden toegevoegd aan de adapters, mits de adapter zelf niet die waarde heeft
        if "default_options" in self.data_adapters:
            default_options = self.data_adapters["default_options"]
            adapters = set(self.data_adapters.keys())
            adapters.discard("default_options")
            for adapter in adapters:
                input_type = self.data_adapters[adapter]["type"]
                if input_type in default_options:
                    for key in default_options[input_type]:
                        # alleen nieuwe opties toevoegen als die er niet al zijn
                        if key not in self.data_adapters[adapter]:
                            self.data_adapters[adapter][key] = default_options[
                                input_type
                            ][key]
