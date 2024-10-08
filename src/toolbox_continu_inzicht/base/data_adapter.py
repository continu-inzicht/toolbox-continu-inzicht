from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel
import pandas as pd
import sqlalchemy
import xarray as xr

from dotenv import load_dotenv, dotenv_values
from toolbox_continu_inzicht.base.config import Config


class DataAdapter(PydanticBaseModel):
    """Basis DataAdapter"""

    config: Config
    input_types: dict = {}
    output_types: dict = {}

    def initialize_input_types(self):
        self.input_types["csv"] = self.input_csv
        self.input_types["postgresql_database"] = self.input_postgresql
        self.input_types["netcdf"] = self.input_netcdf

    def initialize_output_types(self):
        self.output_types["csv"] = self.output_csv
        self.output_types["postgresql_database"] = self.output_postgresql
        self.output_types["netcdf"] = self.output_netcdf

    def input(self, input: str):
        """Gegeven het config, stuurt de juiste input waarde aan

        Parameters:
        -----------
        input: str
               Naam van de data adapter die gebruikt wordt.

        opties: dict
                  extra informatie die ook naar de functie moet om het bestand te lezen

        """
        self.initialize_input_types()  # maak een dictionary van type: functie
        # haal de input configuratie op van de functie
        functie_input_config = self.config.data_adapters[input]
        # leid het data type af
        data_type = functie_input_config["type"]

        # path relative to rootdir specified in config
        if "path" in functie_input_config:
            # if user provides a complete path:
            if Path(self.config.global_variables["rootdir"]).is_absolute():
                functie_input_config["path"] = (
                    Path(self.config.global_variables["rootdir"])
                    / functie_input_config["path"]
                )
            # else we assume its relative to the toolbox_continu_inzicht for testing
            else:
                functie_input_config["path"] = (
                    Path.cwd()
                    / self.config.global_variables["rootdir"]
                    / functie_input_config["path"]
                )

        # uit het .env bestand halen we de extra waardes en laden deze in de config
        if load_dotenv():
            environmental_variables = dict(dotenv_values())
        else:
            raise UserWarning("Ensure a `.env` file is present in the root directory")

        functie_input_config.update(environmental_variables)

        # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
        bijbehorende_functie = self.input_types[data_type]
        df = bijbehorende_functie(functie_input_config)
        return df

    @staticmethod
    def input_csv(input_config):
        """Laat een csv bestand in gegeven een pad

        Returns:
        --------
        pd.Dataframe
        """
        # Data checks worden gedaan in de functies zelf, hier alleen geladen
        path = input_config["path"]
        df = pd.read_csv(path)
        return df

    @staticmethod
    def input_postgresql(input_config: dict):
        """Schrijft data naar een postgresql database gegeven het pad naar een credential bestand.

        Parametes:
        ----------
        input_config: dict
                     in


        Notes:
        ------
        In de `.env` environment bestand moet staan:
        user: str
        password: str
        host: str
        port: str
        database: str
        schema: str



        Returns:
        --------
        pd.Dataframe

        """
        # TODO: doen we dit zo?
        table = input_config["table"]

        keys = ["user", "password", "host", "port", "database", "schema"]
        assert all(key in input_config for key in keys)

        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{input_config['user']}:{input_config['password']}@{input_config['host']}:{int(input_config['port'])}/{input_config['database']}"
        )

        query = f"SELECT objectid, objecttype, parameterid, datetime, value FROM {input_config['schema']}.{table};"

        # qurey uitvoeren op de database
        with engine.connect() as connection:
            df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        # verbinding opruimen
        engine.dispose()

        return df

    def output(self, output: str, df: pd.DataFrame):
        """Gegeven het config, stuurt de juiste input waarde aan

        Parameters:
        -----------
        output: name of the data adapter to use
        df: pd.Dataframe
            pandas dataframe om weg te schrijven

        opties: dict
                extra informatie die ook naar de functie moet om het bestand te schrijven

        """

        # TODO: kan dit eleganters?
        self.initialize_output_types()  # maak een dictionary van type: functie
        # haal de input configuratie op van de functie
        functie_output_config = self.config.data_adapters[output]

        # leid het data type af
        data_type = functie_output_config["type"]

        # path relative to rootdir specified in config
        if "path" in functie_output_config:
            # if user provides a complete path:
            if Path(self.config.global_variables["rootdir"]).is_absolute():
                functie_output_config["path"] = (
                    Path(self.config.global_variables["rootdir"])
                    / functie_output_config["path"]
                )
            # else we assume its relative to the toolbox_continu_inzicht for testing
            else:
                functie_output_config["path"] = (
                    Path.cwd()
                    / self.config.global_variables["rootdir"]
                    / functie_output_config["path"]
                )
        # uit het .env bestand halen we de extra waardes en laden deze in de config
        if load_dotenv():
            environmental_variables = dict(dotenv_values())
        else:
            raise UserWarning("Ensure a `.env` file is present in the root directory")

        functie_output_config.update(environmental_variables)

        # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
        bijbehorende_functie = self.output_types[data_type]
        df = bijbehorende_functie(functie_output_config, df)
        return df

    @staticmethod
    def input_netcdf(input_config):
        """Laat een netcdf bestand in gegeven een pad

        Notes:
        --------
        Lees het netCDF bestand met xarray in en converteer de dataset naar
        een pandas dataframe.

        Returns:
        --------
        pd.Dataframe
        """
        # Data checks worden gedaan in de functies zelf, hier alleen geladen
        path = input_config["path"]
        ds = xr.open_dataset(path)

        # netcdf dataset to pandas dataframe
        df = xr.Dataset.to_dataframe(ds)
        return df

    @staticmethod
    def output_csv(output_config, df):
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
        path = output_config["path"]
        df.to_csv(path)

    @staticmethod
    def output_postgresql(output_config, df):
        """Schrijft data naar een postgresql database gegeven het pad naar een credential bestand.

        Parametes:
        ----------
        df: pd.Dataframe
            dataframe met data om weg te schrijven
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
        table = output_config["table"]
        schema = output_config["schema"]

        # check all required variables are availible from the .env file
        keys = ["user", "password", "host", "port", "database", "schema"]
        assert all(key in output_config for key in keys)

        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['user']}:{output_config['password']}@{output_config['host']}:{int(output_config['port'])}/{output_config['database']}"
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

    @staticmethod
    def output_netcdf(output_config, df):
        """schrijft een netCDF bestand in gegeven een pad

        Notes:
        ------
        Gebruikt hiervoor eerst de xarray.from_dataframe om een xarray dataset te maken
        vervolgens xarray to_netcdf om het bestand te genereren.
        Opties om dit aan te passen kunnen worden mee gegeven in het configuratie bestand.

        Returns:
        --------
        None
        """
        # Data checks worden gedaan in de functies zelf, hier alleen geladen

        # TODO: opties voor csv mogen alleen zijn wat er mee gegeven mag worden aan .to_netcdf
        path = output_config["path"]

        # TODO: netcdf kent geen int64 dus converteren naar float
        for column in df.columns:
            if df[column].dtype == "int64":
                df[column] = df[column].astype("float64")

        ds = xr.Dataset.from_dataframe(df)
        ds.to_netcdf(path, mode="w", engine="scipy")
