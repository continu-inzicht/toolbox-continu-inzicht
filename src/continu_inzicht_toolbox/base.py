import yaml
from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel
import pandas as pd
import sqlalchemy


class Config(PydanticBaseModel):
    """Basis functie om de configuratie in te laden.

    Parameters
    ----------
    config_path: Path
                 Pad naar een  `.yaml` bestand waarin per functie staat beschreven wat de in/ouput bestanden zijn

    """

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
                # TODO: self.logger(f"{functie} niet gevonden, kijk of deze functie bestaat")


class DataAdapter(PydanticBaseModel):
    """Basis DataAdapter"""

    config: Config
    input_types: dict = {}

    def initialize_input_types(self):
        self.input_types["csv"] = self.input_csv
        self.input_types["postgresql"] = self.input_postgresql

    def input(self, functie):
        """Gegeven het config, stuurt de juiste input waarde aan"""
        # TODO: pas dit aan
        self.initialize_input_types()  # maak een dictionary van type: functie
        # haal de input configuratie op van de functie
        functie_input_config = getattr(self.config, functie)["input"]
        # leid het data type af
        data_type = functie_input_config["type"]
        file_path = Path(
            functie_input_config["path"]
        )  # TO fix path to be relative unless specified
        # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
        return self.input_types[data_type](file_path)

    @staticmethod
    def input_csv(path):
        """Laat een csv bestand in gegeven een pad

        returns:
        --------
        pd.Dataframe
        """
        # Data checks worden gedaan in de functies zelf, hier alleen geladen
        df = pd.read_csv(path)
        return df

    @staticmethod
    def input_postgresql(path):
        """Laat data in van een postgresql database gegeven het pad naar een credential bestand.
        In het credential bestand moet staan:
        ------------------------------------
        user: str
        password: str
        host: str
        port: str
        database: str
        schema: str
        table: str

        returns:
        --------
        pd.Dataframe

        """
        # TODO: aanpassen naar juiste versie
        with open(path) as fin:
            credentials = yaml.safe_load(fin)

        engine = sqlalchemy.create_engine(
            f"postgresql://{credentials.user}:{credentials.password}@{credentials.host}:{credentials.port}/{credentials.database}"
        )

        query = f"SELECT objectid, objecttype, parameterid, datetime, value FROM {credentials.schema}.{credentials.table};"

        # qurey uitvoeren op de database
        with engine.connect() as connection:
            df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        # verbinding opruimen
        engine.dispose()

        return df
