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
    toegelaten_functies: set = {"WaardesKeerTwee", "WaardesDelenTwee"}
    WaardesKeerTwee: dict = {}
    WaardesDelenTwee: dict = {}

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


class DataAdapter(PydanticBaseModel):
    """Basis DataAdapter"""

    config: Config
    input_types: dict = {}
    output_types: dict = {}

    def initialize_input_types(self):
        self.input_types["csv"] = self.input_csv
        self.input_types["postgresql_database"] = self.input_postgresql

    def initialize_output_types(self):
        self.output_types["csv"] = self.output_csv
        self.output_types["postgresql_database"] = self.output_postgresql

    def input(self, functie, **opties):
        """Gegeven het config, stuurt de juiste input waarde aan

        Parameters:
        -----------
        functie: str
                 naam van de functie die bij het bestands type hoort

        opties: dict
                  extra informatie die ook naar de functie moet om het bestand te lezen

        """
        # TODO: kan dit eleganters?
        self.initialize_input_types()  # maak een dictionary van type: functie
        # haal de input configuratie op van de functie
        functie_input_config = getattr(self.config, functie)["input"]
        # leid het data type af
        data_type = functie_input_config["type"]
        bestand_pad = Path(
            functie_input_config["path"]
        )  # TO fix path to be relative unless specified

        # alle andere opties uit de config willen we voorrang geven op de standaard waardes uit de functies
        # in de functie is een standaard waarde gespecificeerd, de gebruiker kan deze in de config overschrijven
        for key in functie_input_config.keys():
            if key in opties:
                opties["type"][key] = functie_input_config[key]

        # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
        bijbehorende_functie = self.input_types[data_type]
        df = bijbehorende_functie(bestand_pad, **opties[data_type])
        return df

    @staticmethod
    def input_csv(path, **opties):
        """Laat een csv bestand in gegeven een pad

        Returns:
        --------
        pd.Dataframe
        """
        # Data checks worden gedaan in de functies zelf, hier alleen geladen
        df = pd.read_csv(path)
        return df

    @staticmethod
    def input_postgresql(path, **opties):
        """Schrijft data naar een postgresql database gegeven het pad naar een credential bestand.

        Parametes:
        ----------
        path: Path
              pad naar het credentials bestand
        schema: str
                naam van het schema in de postgresql database
        table: str
               naam van de tabel in de postgresql database


        Notes:
        ------
        In het credential bestand moet staan:
        user: str
        password: str
        host: str
        port: str
        database: str


        Returns:
        --------
        pd.Dataframe

        """
        # TODO: doen we dit zo?
        table = opties["table"]
        schema = opties["schema"]
        with open(path) as fin:
            credentials = yaml.safe_load(fin)

        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"
        )

        query = f"SELECT objectid, objecttype, parameterid, datetime, value FROM {schema}.{table};"

        # qurey uitvoeren op de database
        with engine.connect() as connection:
            df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        # verbinding opruimen
        engine.dispose()

        return df

    def output(self, functie, df, **opties):
        """Gegeven het config, stuurt de juiste input waarde aan

        Parameters:
        -----------
        functie: str
                 naam van de functie die bij het bestands type hoort
        df: pd.Dataframe
            pandas dataframe om weg te schrijvne

        opties: dict
                extra informatie die ook naar de functie moet om het bestand te schrijven

        """
        # TODO: kan dit eleganters?
        self.initialize_output_types()  # maak een dictionary van type: functie
        # haal de input configuratie op van de functie
        functie_output_config = getattr(self.config, functie)["output"]
        # leid het data type af
        data_type = functie_output_config["type"]
        bestand_pad = Path(
            functie_output_config["path"]
        )  # TO fix path to be relative unless specified

        # alle andere opties uit de config willen we voorrang geven op de standaard waardes uit de functies
        # in de functie is een standaard waarde gespecificeerd, de gebruiker kan deze in de config overschrijven
        for key in functie_output_config.keys():
            if key in opties:
                opties[key].update({key: functie_output_config[key]})

        # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
        bijbehorende_functie = self.output_types[data_type]
        df = bijbehorende_functie(bestand_pad, df, **opties[data_type])
        return df

    @staticmethod
    def output_csv(path, df, **opties):
        """schrijft een csv bestand in gegeven een pad

        Notes:
        ------
        Gebruikt hiervoor de pandas.DataFrame.to_csv
        Opties om dit aan te passen kunnen worden mee gegeven in het configuratie bestand.

        Returns:
        --------
        None
        """
        # Data checks worden gedaan in de functies zelf, hier alleen geladen

        # TODO: opties voor csv mogen alleen zijn wat er mee gegeven mag wroden aan .to_csv
        df.to_csv(path, **opties)

    @staticmethod
    def output_postgresql(path, df, **opties):
        """Schrijft data naar een postgresql database gegeven het pad naar een credential bestand.

        Parametes:
        ----------
        df: pd.Dataframe
            dataframe met data om weg te schrijven
        path: Path
              pad naar het credentials bestand
        opties: dict
                dictionary met extra opties waar onder:
                    schema: str
                            naam van het schema in de postgresql database
                    table: str
                        naam van de tabel in de postgresql database


        Notes:
        ------
        In het credential bestand moet staan:
        user: str
        password: str
        host: str
        port: str
        database: str


        Returns:
        --------
        None

        """
        table = opties["table"]
        schema = opties["schema"]

        # TODO: doen we dit zo?
        with open(path) as fin:
            credentials = yaml.safe_load(fin)

        engine = sqlalchemy.create_engine(
            f"postgresql://{credentials['user']}:{credentials['password']}@{credentials['host']}:{credentials['port']}/{credentials['database']}"
        )

        df.to_sql(
            table,
            con=engine,
            schema=schema,
            if_exists="replace",  # append
            index=False,
        )

        # verbinding opruimen
        engine.dispose()
