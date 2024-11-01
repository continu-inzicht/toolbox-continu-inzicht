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
    available_types: list[str] = ["csv", "postgresql_database", "netcdf", "ci_postgresql_waterlevels"]

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
        default_options = self.data_adapters["default_options"]
        adapters = set(self.data_adapters.keys())
        adapters.discard("default_options")
        for adapter in adapters:
            input_type = self.data_adapters[adapter]["type"]
            if input_type in default_options:
                for key in default_options[input_type]:
                    if key not in self.data_adapters[adapter]:
                        self.data_adapters[adapter][key] = default_options[input_type][
                            key
                        ]
