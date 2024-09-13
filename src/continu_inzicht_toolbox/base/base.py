import yaml

from pathlib import Path

from pydantic import BaseModel as PydanticBaseModel


class Config(PydanticBaseModel):
    config_path: Path

    # elke functie heeft een dictionary met daar in de configuratie
    toegelaten_functies: set = {"waardes_keer_twee", "waardes_delen_twee"}
    waardes_keer_twee: dict = {}
    waardes_delen_twee: dict = {}

    #
    dump: dict = {}

    def read_config(self):
        """Laad het gegeven pad in, zet de configuraties klaar in de Config class"""

        with self.config_path.open() as fin:
            data = yaml.safe_load(fin)

        for functie, configuratie in data.items():
            if functie in self.toegelaten_functies:
                setattr(self, functie, configuratie)
            else:  # als de functie niet is geconfigureerd: vang dat hier af
                self.dump.update({functie: configuratie})
