import yaml
from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel


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

    def lees_config(self):
        """Laad het gegeven pad in, zet de configuraties klaar in de Config class"""

        with self.config_path.open() as fin:
            data = yaml.safe_load(fin)

        for header, configuration in data.items():
            match header:
                case "DataAdapter":
                    self.data_adapters = configuration
                case "GlobalVariables":
                    self.global_variables = configuration

        # add any applicable global variables to the
        # not that fast but config shouldn't get too big
        for val in self.global_variables:
            for name, adapter in self.data_adapters.items():
                if adapter["type"] == val:
                    self.data_adapters[name].update(self.global_variables[val])
