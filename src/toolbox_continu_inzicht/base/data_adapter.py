from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel
import pandas as pd
import sqlalchemy
import inspect
import warnings
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

    @staticmethod
    def validate_dataframe(df: pd.DataFrame, schema: dict):
        
        expected_columns = list(schema.keys())

        actual_columns = df.columns.tolist()
        actual_dtypes = df.dtypes.to_dict()
        
        columns_match = set(expected_columns).issubset(set(actual_columns))        
        dtypes_match = all(schema[key] == actual_dtypes[key] for key in schema if key in actual_dtypes)
        
        if columns_match and dtypes_match:
            return 0, "DataFrame is geldig."
        else:
            if not columns_match:
                return 1, f"Kolommen komen niet overeen. \nVerwachte kolommen: {schema}.\nHuidige kolommen: {actual_dtypes}."
            if not dtypes_match:
                return 2, f"Datatypes komen niet overeen.\nVerwachte kolommen: {schema}.\nHuidige kolommen: {actual_dtypes}."

    def input(self, input: str, schema: dict = None):
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

        check_rootdir(self.config.global_variables)

        # pad relatief tot rootdir mee gegeven in config
        if "file" in functie_input_config:
            # als de gebruiker een compleet pad mee geeft:
            if Path(self.config.global_variables["rootdir"]).is_absolute():
                functie_input_config["abs_path"] = (
                    Path(self.config.global_variables["rootdir"])
                    / functie_input_config["file"]
                )
            # als rootdir geen absoluut pad is, nemen we relatief aan
            else:
                functie_input_config["abs_path"] = (
                    Path.cwd()
                    / self.config.global_variables["rootdir"]
                    / functie_input_config["file"]
                )

            if not functie_input_config["abs_path"].is_absolute():
                raise UserWarning(
                    f"Check if root dir ({self.config.global_variables['rootdir']}) and file ({functie_input_config['file']}) exist"
                )
        # als een pad wordt mee gegeven
        elif "path" in functie_input_config:
            # eerst checken of het absoluut is
            if Path(functie_input_config["path"]).is_absolute():
                functie_input_config["abs_path"] = Path(functie_input_config["path"])
            # anders alsnog toevoegen
            else:
                functie_input_config["abs_path"] = (
                    Path(self.config.global_variables["rootdir"])
                    / functie_input_config["path"]
                )

        # uit het .env bestand halen we de extra waardes en laden deze in de config
        environmental_variables = {}
        if load_dotenv():
            environmental_variables = dict(dotenv_values())
        else:
            warnings.warn(
                "A `.env` file is not present in the root directory, continuing without",
                UserWarning,
            )

        # in eerste instantie alleen beschikbaar voor de data adapters
        functie_input_config.update(environmental_variables)
        # maar je wilt er  vanuit de functies ook bij kunnen
        self.config.global_variables.update(environmental_variables)

        # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
        bijbehorende_functie = self.input_types[data_type]
        df = bijbehorende_functie(functie_input_config)

        # Als schema is meegegeven, controleer of de data aan het schema voldoet.
        if schema is not None:
            status, message = self.validate_dataframe(df=df, schema=schema)
            if status > 0:
                raise UserWarning(message)

        return df

    @staticmethod
    def input_csv(input_config):
        """Laat een csv bestand in gegeven een pad

        Returns:
        --------
        pd.Dataframe
        """
        path = input_config["abs_path"]

        kwargs = get_kwargs(pd.read_csv, input_config)

        df = pd.read_csv(path, **kwargs)
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

        # check of de rootdir bestaat
        check_rootdir(self.config.global_variables)

        if "file" in functie_output_config:
            # als de gebruiker een compleet pad mee geeft:
            if Path(self.config.global_variables["rootdir"]).is_absolute():
                functie_output_config["abs_path"] = (
                    Path(self.config.global_variables["rootdir"])
                    / functie_output_config["file"]
                )
            # als rootdir geen absoluut pad is, nemen we relatief aan
            else:
                functie_output_config["abs_path"] = (
                    Path.cwd()
                    / self.config.global_variables["rootdir"]
                    / functie_output_config["file"]
                )

            if not functie_output_config["abs_path"].is_absolute():
                raise UserWarning(
                    f"Check if root dir ({self.config.global_variables['rootdir']}) and file ({functie_output_config['file']}) exist"
                )
        # als een pad wordt mee gegeven
        elif "path" in functie_output_config:
            # eerst checken of het absoluut is
            if Path(functie_output_config["path"]).is_absolute():
                functie_output_config["abs_path"] = Path(functie_output_config["path"])
            # anders alsnog toevoegen
            else:
                functie_output_config["abs_path"] = (
                    Path(self.config.global_variables["rootdir"])
                    / functie_output_config["path"]
                )

        # uit het .env bestand halen we de extra waardes en laden deze in de config
        environmental_variables = {}
        if load_dotenv():
            environmental_variables = dict(dotenv_values())
        else:
            warnings.warn(
                "A `.env` file is not present in the root directory, continuing without",
                UserWarning,
            )

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
        abs_path = input_config["abs_path"]
        kwargs = get_kwargs(xr.open_dataset, input_config)
        ds = xr.open_dataset(abs_path, **kwargs)

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
        path = output_config["abs_path"]
        kwargs = get_kwargs(pd.DataFrame.to_csv, output_config)
        df.to_csv(path, **kwargs)

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
        # TODO: netcdf kent geen int64 dus converteren naar float
        for column in df.columns:
            if df[column].dtype == "int64":
                df[column] = df[column].astype("float64")

        ds = xr.Dataset.from_dataframe(df)
        kwargs = get_kwargs(xr.Dataset.to_netcdf, output_config)
        if "mode" not in kwargs:
            kwargs["mode"] = "w"
        if "engine" not in kwargs:
            kwargs["engine"] = "scipy"

        kwargs["path"] = output_config["abs_path"]
        # path is al een kwarg
        ds.to_netcdf(**kwargs)


def get_kwargs(function, input_config):
    """
    Gegeven een input/output functie, stuurt de relevanten kwargs uit de input config naar de functie.
    """
    # We kijken welke argumenten we aan de functie kunnen geven
    possible_parameters_function = set(inspect.signature(function).parameters.keys())
    # Vervolgens nemen we alleen de namen van de parameters over die ook opgegeven zijn
    wanted_keys = list(possible_parameters_function.intersection(input_config.keys()))
    # en geven we een kwargs dictionary door aan de inlees functie
    return {key: input_config[key] for key in wanted_keys}


def check_rootdir(global_variables):
    """
    Checkt of de rootdir bestaat
    """
    if "rootdir" in global_variables:
        condition1 = not Path(global_variables["rootdir"]).exists()
        condition2 = not (Path.cwd() / global_variables["rootdir"]).exists()
        if condition1 or condition2:
            raise UserWarning(
                f"De rootdir map '{global_variables["rootdir"]}' bestaat niet"
            )
