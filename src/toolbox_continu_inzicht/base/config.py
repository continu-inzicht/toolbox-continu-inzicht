import yaml
from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel
from yaml.scanner import ScannerError


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
    available_types: list[str] = ["csv", "postgresql_database", "netcdf"]

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
                    self.global_variables = configuration

        # opties die in de DataAdapter worden mee gegeven
        # worden toegevoegd aan de adapters, mits de adapter zelf niet die waarde heeft
        if "default_options" in self.data_adapters:
            for name, adapter in self.data_adapters.items():
                if "type" in adapter and adapter["type"] in self.available_types:
                    if adapter["type"] in self.data_adapters["default_options"]:
                        options = self.data_adapters["default_options"][adapter["type"]]
                        for option in options:
                            if option not in self.data_adapters[name]:
                                self.data_adapters[name][option] = options[option]
