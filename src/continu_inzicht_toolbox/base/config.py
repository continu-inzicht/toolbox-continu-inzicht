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

    # elke functie heeft een dictionary met daar in de configuratie
    toegelaten_functies: set = {
        "WaardesKeerTwee",
        "WaardesDelenTwee",
        "global_variables",
    }
    WaardesKeerTwee: dict = {}
    WaardesDelenTwee: dict = {}
    global_variables: dict = {}

    #
    dump: dict = {}

    def lees_config(self):
        """Laad het gegeven pad in, zet de configuraties klaar in de Config class"""

        with self.config_path.open() as fin:
            data = yaml.safe_load(fin)

        for functie, configuratie in data.items():
            if functie in self.toegelaten_functies:
                setattr(self, functie, configuratie)
            else:  # als de functie niet is geconfigureerd: vang dat hier af
                self.dump.update({functie: configuratie})
                # TODO: self.logger(f"{functie} niet gevonden, kijk of deze functie bestaat")
