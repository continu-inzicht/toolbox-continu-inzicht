from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel
import numpy as np
import pandas as pd
import sqlalchemy
import inspect
import warnings
import xarray as xr
from typing import Tuple, Any

from dotenv import load_dotenv, dotenv_values
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.utils.datetime_functions import epoch_from_datetime


class DataAdapter(PydanticBaseModel):
    """Basis DataAdapter"""

    config: Config
    input_types: dict = {}
    output_types: dict = {}

    def initialize_input_types(self) -> None | AssertionError:
        """
        Initializes input mapping and checks to see if type in the configured types

        Future editor: ensure that changes made here are also reflected in Config.available_types
        """
        self.input_types["csv"] = self.input_csv
        assert "csv" in self.config.available_types

        self.input_types["python"] = self.input_python
        assert "python" in self.config.available_types

        self.input_types["netcdf"] = self.input_netcdf
        assert "netcdf" in self.config.available_types

        self.input_types["postgresql_database"] = self.input_postgresql
        assert "postgresql_database" in self.config.available_types

        self.input_types["ci_postgresql_from_waterlevels"] = (
            self.input_ci_postgresql_from_waterlevels
        )
        assert "ci_postgresql_from_waterlevels" in self.config.available_types

        self.input_types["ci_postgresql_from_conditions"] = (
            self.input_ci_postgresql_from_conditions
        )
        assert "ci_postgresql_from_conditions" in self.config.available_types

        self.input_types["ci_postgresql_from_measuringstations"] = (
            self.input_ci_postgresql_from_measuringstations
        )
        assert "ci_postgresql_from_measuringstations" in self.config.available_types

        self.input_types["csv_source"] = self.input_csv_source
        assert "csv_source" in self.config.available_types

    def initialize_output_types(self) -> None | AssertionError:
        """Initializes ouput mapping and checks to see if type in the configured types"""
        self.output_types["csv"] = self.output_csv
        assert "csv" in self.config.available_types

        self.output_types["python"] = self.output_python
        assert "python" in self.config.available_types

        self.output_types["postgresql_database"] = self.output_postgresql
        assert "postgresql_database" in self.config.available_types

        self.output_types["netcdf"] = self.output_netcdf
        assert "netcdf" in self.config.available_types

        self.output_types["ci_postgresql_to_data"] = self.output_ci_postgresql_to_data
        assert "ci_postgresql_to_data" in self.config.available_types

        self.output_types["ci_postgresql_to_states"] = (
            self.output_ci_postgresql_to_states
        )
        assert "ci_postgresql_to_states" in self.config.available_types

        self.input_types["csv_source"] = self.input_csv_source
        assert "csv_source" in self.config.available_types

    @staticmethod
    def validate_dataframe(df: pd.DataFrame, schema: dict) -> Tuple[int, str]:
        expected_columns = list(schema.keys())

        actual_columns = df.columns.tolist()
        actual_dtypes = df.dtypes.to_dict()

        columns_match = set(expected_columns).issubset(set(actual_columns))
        dtypes_match = all(
            schema[key] == actual_dtypes[key] for key in schema if key in actual_dtypes
        )

        if columns_match and dtypes_match:
            return 0, "DataFrame is geldig."
        else:
            if not columns_match:
                return (
                    1,
                    f"Kolommen komen niet overeen. \nVerwachte kolommen: {schema}.\nHuidige kolommen: {actual_dtypes}.",
                )
            if not dtypes_match:
                return (
                    2,
                    f"Datatypes komen niet overeen.\nVerwachte kolommen: {schema}.\nHuidige kolommen: {actual_dtypes}.",
                )

    def input(self, input: str, schema: dict = None) -> pd.DataFrame:
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
        function_input_config = self.config.data_adapters[input]
        # leid het data type af
        data_type = function_input_config["type"]

        check_rootdir(self.config.global_variables)
        check_file_and_path(function_input_config, self.config.global_variables)

        # uit het .env bestand halen we de extra waardes en laden deze in de config
        # .env is een lokaal bestand waar wachtwoorden in kunnen worden opgeslagen, zie .evn.template
        environmental_variables = {}
        dotenv_path = None
        if "dotenv_path" in self.config.global_variables:
            dotenv_path = self.config.global_variables["dotenv_path"]

        if load_dotenv(dotenv_path=dotenv_path):
            environmental_variables = dict(dotenv_values(dotenv_path=dotenv_path))
        else:
            warnings.warn(
                "A `.env` file is not present in the root directory, continuing without",
                UserWarning,
            )

        # in eerste instantie alleen beschikbaar voor de data adapters
        function_input_config.update(environmental_variables)
        # maar je wilt er  vanuit de functies ook bij kunnen
        self.config.global_variables.update(environmental_variables)

        # roep de bijbehorende functie bij het data type aan en geef het input pad mee.
        correspinding_function = self.input_types[data_type]
        df = correspinding_function(function_input_config)

        # Controleer of er data is opgehaald.
        if len(df) == 0:
            raise UserWarning("Geen data")

        # Als schema is meegegeven, controleer of de data aan het schema voldoet.
        if schema is not None:
            status, message = self.validate_dataframe(df=df, schema=schema)
            if status > 0:
                raise UserWarning(message)

        return df

    @staticmethod
    def input_python(input_config: dict) -> pd.DataFrame:
        """Wrapper voor de functionalitiet om dataframes vanuit python te ondersteunen

        Returns:
        --------
        pd.Dataframe
        """
        return input_config["dataframe_from_python"]

    @staticmethod
    def input_csv(input_config: dict) -> pd.DataFrame:
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
    def input_csv_source(input_config: dict) -> pd.DataFrame:
        """Laat een csv bestand in gegeven een pad en filter op een waarde

        Returns:
        --------
        pd.Dataframe
        """
        path = input_config["abs_path"]

        kwargs = get_kwargs(pd.read_csv, input_config)

        df = pd.read_csv(path, **kwargs)

        if "source" in df.columns:
            filter_parameter = input_config["filter"]
            filter = f"source.str.contains('{filter_parameter}', case=False)"
            filtered_df = df.query(filter)
            df = filtered_df
        else:
            raise UserWarning("Kolom 'source' is niet aanwezig in het CSV bestand.")

        return df

    @staticmethod
    def input_postgresql(input_config: dict) -> pd.DataFrame:
        """Schrijft data naar een postgresql database gegeven het pad naar een credential bestand.

        Parametes:
        ----------
        input_config: dict
                     in


        Notes:
        ------
        In de `.env` environment bestand moet staan:
        postgresql_user: str
        postgresql_password: str
        postgresql_host: str
        postgresql_port: str
        database: str
        schema: str

        Returns:
        --------
        pd.Dataframe

        """
        keys = [
            "postgresql_user",
            "postgresql_password",
            "postgresql_host",
            "postgresql_port",
            "database",
            "schema",
        ]
        assert all(key in input_config for key in keys)

        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{input_config['postgresql_user']}:{input_config['postgresql_password']}@{input_config['postgresql_host']}:{int(input_config['postgresql_port'])}/{input_config['database']}"
        )

        schema = ""
        table = ""
        query = ""

        if "query" in input_config:
            # bepaal eventueel de tabelnaam voor het vervangen in de query string
            if "table" in input_config:
                table = input_config["table"]

            if "schema" in input_config:
                schema = input_config["schema"]

            query = input_config["query"]
            query = query.replace("{{schema}}", schema).replace("{{table}}", table)

        elif "table" in input_config:
            query = f"SELECT * FROM {input_config['schema']}.{input_config["table"]};"
        else:
            raise UserWarning(
                "De parameter 'table' en/ of 'query' zijn niet in de DataAdapter gedefinieerd."
            )

        # qurey uitvoeren op de database
        with engine.connect() as connection:
            df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        # verbinding opruimen
        engine.dispose()

        return df

    @staticmethod
    def input_netcdf(input_config: dict) -> pd.DataFrame:
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
    def input_ci_postgresql_from_waterlevels(input_config: dict) -> pd.DataFrame:
        """
        Ophalen belasting uit een continu database voor het whatis scenario.

        Args:
        ----------
        input_config (dict):

        Opmerking:
        ------
        In de `.env` environment bestand moeten de volgende parameters staan:
        postgresql_user (str):
        postgresql_password (str):
        postgresql_host (str):
        postgresql_port (str):

        In de 'yaml' config moeten de volgende parameters staan:
        database (str):
        schema (str):

        Returns:
        --------
        pd.Dataframe

        """
        keys = [
            "postgresql_user",
            "postgresql_password",
            "postgresql_host",
            "postgresql_port",
            "database",
            "schema",
        ]

        assert all(key in input_config for key in keys)

        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{input_config['postgresql_user']}:{input_config['postgresql_password']}@{input_config['postgresql_host']}:{int(input_config['postgresql_port'])}/{input_config['database']}"
        )

        schema = input_config["schema"]
        query = f"""
            SELECT                
                        waterlevel.measuringstationid AS measurement_location_id, 
                        measuringstation.code AS measurement_location_code,
                        measuringstation.name AS measurement_location_description,	
                        parameter.id AS parameter_id,
                        parameter.code AS parameter_code,
                        parameter.name AS parameter_description,
                        parameter.unit AS unit,			
                        TO_TIMESTAMP(waterlevel.datetime/1000) AS date_time, 
                        value*100 AS value,
                        (
                            CASE 
                                WHEN waterlevel.datetime > simulation.datetime THEN 'verwacht'
                                ELSE 'gemeten'
                            END
                        ) AS value_type
                        
            FROM {schema}.waterlevels AS waterlevel
            INNER JOIN 
                (
                    SELECT MIN(moments.id)*(60*60*1000) AS start_diff, MAX(moments.id)*(60*60*1000) AS end_diff
                    FROM {schema}.moments
                ) AS moment ON 1=1
            INNER JOIN {schema}.simulation ON simulation.scenarioid=waterlevel.scenarioid	
            INNER JOIN {schema}.measuringstations AS measuringstation ON waterlevel.measuringstationid=measuringstation.id
            INNER JOIN {schema}.parameters AS parameter ON waterlevel.parameter=parameter.id
            WHERE 	
                waterlevel.datetime >= simulation.datetime + moment.start_diff AND
                waterlevel.datetime <= simulation.datetime + moment.end_diff;
        """

        # qurey uitvoeren op de database
        with engine.connect() as connection:
            df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        # verbinding opruimen
        engine.dispose()

        # Datum kolom moet een object zijn en niet een 'datetime64[ns, UTC]'
        df["date_time"] = df["date_time"].astype(object)

        return df

    @staticmethod
    def input_ci_postgresql_from_conditions(input_config: dict) -> pd.DataFrame:
        """
        Ophalen dremple waarden uit een continu database.

        Args:
        ----------
        input_config (dict):

        Opmerking:
        ------
        In de `.env` environment bestand moeten de volgende parameters staan:
        postgresql_user (str):
        postgresql_password (str):
        postgresql_host (str):
        postgresql_port (str):

        In de 'yaml' config moeten de volgende parameters staan:
        database (str):
        schema (str):

        Returns:
        --------
        pd.Dataframe

        """
        keys = [
            "postgresql_user",
            "postgresql_password",
            "postgresql_host",
            "postgresql_port",
            "database",
            "schema",
        ]

        assert all(key in input_config for key in keys)

        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{input_config['postgresql_user']}:{input_config['postgresql_password']}@{input_config['postgresql_host']}:{int(input_config['postgresql_port'])}/{input_config['database']}"
        )

        schema = input_config["schema"]
        query = f"""
            SELECT 
                measuringstation.code AS measurement_location_code, 
                LAG(condition.upperboundary, 1) OVER (PARTITION BY condition.objectid ORDER BY condition.stateid) AS van, 
                condition.upperboundary AS tot, 
                condition.color AS kleur, 
                condition.description AS label, 
                parameter.unit AS unit
            FROM {schema}.conditions AS condition
            INNER JOIN {schema}.measuringstations AS measuringstation ON measuringstation.id=condition.objectid
            INNER JOIN {schema}.parameters AS parameter ON parameter.id=1
            WHERE condition.objecttype='measuringstation'
            ORDER BY condition.objectid,condition.stateid;;
        """

        # qurey uitvoeren op de database
        with engine.connect() as connection:
            df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        # verbinding opruimen
        engine.dispose()

        return df

    @staticmethod
    def input_ci_postgresql_from_measuringstations(input_config: dict) -> pd.DataFrame:
        """
        Ophalen meetstations uit een continu database.

        Args:
        ----------
        input_config (dict):

        Opmerking:
        ------
        In de `.env` environment bestand moeten de volgende parameters staan:
        postgresql_user (str):
        postgresql_password (str):
        postgresql_host (str):
        postgresql_port (str):

        In de 'yaml' config moeten de volgende parameters staan:
        database (str):
        schema (str):

        Returns:
        --------
        pd.Dataframe

        """
        keys = [
            "postgresql_user",
            "postgresql_password",
            "postgresql_host",
            "postgresql_port",
            "database",
            "schema",
            "source",
        ]

        assert all(key in input_config for key in keys)

        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{input_config['postgresql_user']}:{input_config['postgresql_password']}@{input_config['postgresql_host']}:{int(input_config['postgresql_port'])}/{input_config['database']}"
        )

        schema = input_config["schema"]

        # WaterInfo, NOOS Matroos/ ...
        source = input_config["source"]
        query = f"""
            SELECT 
                id AS measurement_location_id, 
                code AS measurement_location_code,
                name AS measurement_location_description 
            FROM {schema}.measuringstations
            WHERE source='{source}';
        """

        # qurey uitvoeren op de database
        with engine.connect() as connection:
            df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        # verbinding opruimen
        engine.dispose()

        return df

    def output(self, output: str, df: pd.DataFrame) -> None:
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
        check_file_and_path(functie_output_config, self.config.global_variables)

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
        bijbehorende_functie(functie_output_config, df)

    @staticmethod
    def output_python(output_config: dict, df: pd.DataFrame) -> None:
        """Wrapper voor de functionalitiet om dataframes vanuit python te ondersteunen

        Returns:
        --------
        None
        """
        pass

    @staticmethod
    def output_csv(output_config: dict, df: pd.DataFrame):
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
        path = output_config["abs_path"]
        kwargs = get_kwargs(pd.DataFrame.to_csv, output_config)
        df.to_csv(path, **kwargs)

    @staticmethod
    def output_postgresql(output_config: dict, df: pd.DataFrame):
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
        postgresql_user: str
        postgresql_password: str
        postgresql_host: str
        postgresql_port: str
        database: str


        Returns:
        --------
        None

        """
        table = output_config["table"]
        schema = output_config["schema"]

        # check all required variables are availible from the .env file
        keys = [
            "postgresql_user",
            "postgresql_password",
            "postgresql_host",
            "postgresql_port",
            "database",
            "schema",
        ]
        assert all(key in output_config for key in keys)

        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
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

    @staticmethod
    def output_ci_postgresql_to_data(output_config: dict, df: pd.DataFrame):
        """
        Schrijft data naar Continu Inzicht database

        Args:
        ----------
        output_config (dict):

        Opmerking:
        ------
        In de `.env` environment bestand moeten de volgende parameters staan:
        postgresql_user (str):
        postgresql_password (str):
        postgresql_host (str):
        postgresql_port (str):

        In de 'yaml' config moeten de volgende parameters staan:
        database (str):
        schema (str):

        Returns:
        --------
        pd.Dataframe

        """
        keys = [
            "postgresql_user",
            "postgresql_password",
            "postgresql_host",
            "postgresql_port",
            "database",
            "schema",
            "objecttype",
        ]

        def bepaal_parameter_id(value_type: str):
            if value_type == "meting":
                return 1
            else:
                return 2

        assert all(key in output_config for key in keys)

        table = "data"
        schema = output_config["schema"]
        objecttype = output_config["objecttype"]

        if len(df) > 0:
            # objectid, objecttype, parameterid, datetime, value, calculating
            if objecttype == "measuringstation":
                # maak verbinding object
                engine = sqlalchemy.create_engine(
                    f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
                )

                df["objecttype"] = objecttype
                df["calculating"] = False
                df["datetime"] = df["date_time"].apply(epoch_from_datetime)
                df["parameterid"] = df["value_type"].apply(bepaal_parameter_id)
                # TODO fix units:
                df["value"] = df["value"].div(100)
                df_data = df.loc[
                    :,
                    [
                        "measurement_location_id",
                        "objecttype",
                        "parameterid",
                        "datetime",
                        "value",
                        "calculating",
                    ],
                ]
                df_data = df_data.rename(
                    columns={"measurement_location_id": "objectid"}
                )

                # Eerst bestaande gegevens van meetstations verwijderen
                location_ids = df_data.objectid.unique()
                location_ids_str = ",".join(map(str, location_ids))

                # with engine.connect() as connection:
                #     connection.execute(
                #         sqlalchemy.text(f"""
                #                         DELETE FROM {schema}.{table}
                #                         WHERE objectid IN ({location_ids_str}) AND
                #                               calculating=true AND
                #                               objecttype='measuringstation';
                #                         """)
                #     )
                #     connection.commit()  # commit the transaction

                with engine.connect() as connection:
                    connection.execute(
                        sqlalchemy.text(f"""
                                        DELETE FROM {schema}.{table} 
                                        WHERE objectid IN ({location_ids_str}) AND                                               
                                              objecttype='measuringstation';
                                        """)
                    )
                    connection.commit()  # commit the transaction

                # schrijf data naar de database
                df_data.to_sql(
                    table,
                    con=engine,
                    schema=schema,
                    if_exists="append",
                    index=False,
                )

                # verbinding opruimen
                engine.dispose()

            else:
                raise UserWarning("Geen gegevens om op te slaan.")

        return df

    @staticmethod
    def output_ci_postgresql_to_states(output_config: dict, df: pd.DataFrame):
        """
        Schrijft data naar Continu Inzicht database tabel states

        Args:
        ----------
        output_config (dict):

        Opmerking:
        ------
        In de `.env` environment bestand moeten de volgende parameters staan:
        postgresql_user (str):
        postgresql_password (str):
        postgresql_host (str):
        postgresql_port (str):

        In de 'yaml' config moeten de volgende parameters staan:
        database (str):
        schema (str):

        Returns:
        --------
        pd.Dataframe

        """
        keys = [
            "postgresql_user",
            "postgresql_password",
            "postgresql_host",
            "postgresql_port",
            "database",
            "schema",
            "objecttype",
        ]

        assert all(key in output_config for key in keys)

        table = "states"
        schema = output_config["schema"]
        objecttype = output_config["objecttype"]

        # ,measurement_location_code,date_time,value,van,tot,kleur,label
        # 2,DENH,1991-01-13 22:40:00+00:00,173.4,170.0,190.0,#4bacc6,verhoogd

        # objectid, objecttype, parameterid, momentid, stateid, calculating, changedate
        if objecttype == "measuringstation":
            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            # eerst lookup data ophalen om de juiste id's te bepalen
            query = f""" 
                SELECT 
                    id AS objectid, 
                    code AS measurement_location_code
                FROM {schema}.measuringstations;
            """

            # query uitvoeren op de database
            with engine.connect() as connection:
                df_stations = pd.read_sql_query(
                    sql=sqlalchemy.text(query), con=connection
                )

            # eerst lookup data ophalen om de juiste id's te bepalen
            query = f""" 
                SELECT 
                    moment.id AS momentid, 
                    TO_TIMESTAMP(moment.calctime/1000) AS date_time
                FROM {schema}.moments AS moment;
            """

            # query uitvoeren op de database
            with engine.connect() as connection:
                df_moments = pd.read_sql_query(
                    sql=sqlalchemy.text(query), con=connection
                )

            query = f""" 
                SELECT 
                    stateid AS stateid, 
                    objectid AS objectid, 
                    upperboundary AS tot
                FROM {schema}.conditions
                WHERE objecttype='measuringstation'
                ORDER BY objectid, stateid;
            """

            # query uitvoeren op de database
            with engine.connect() as connection:
                df_conditions = pd.read_sql_query(
                    sql=sqlalchemy.text(query), con=connection
                )

            df.set_index("measurement_location_code")
            df_merge = df.merge(
                df_stations, on="measurement_location_code", how="outer"
            )

            df_moments["date_time"] = df_moments["date_time"].astype(object)
            df_moments.set_index("date_time")

            df_merge["date_time"] = df_merge["date_time"].astype(object)
            df_merge.set_index("date_time")
            df_merge = df_merge.merge(
                df_moments, on="date_time", how="inner", validate="many_to_one"
            )

            df_merge.set_index(["objectid", "tot"])
            df_merge = df_merge.merge(
                df_conditions,
                on=["objectid", "tot"],
                how="left",
                validate="many_to_one",
            )

            # haal [objectid, objecttype, parameterid, momentid, stateid, calculating, changedate] op

            # Eerst bestaande gegevens van meetstations verwijderen
            location_ids = df_merge.objectid.unique()
            location_ids_str = ",".join(map(str, location_ids))

            with engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text(f"""
                                        DELETE FROM {schema}.{table} 
                                        WHERE objectid IN ({location_ids_str}) AND 
                                              calculating=true AND
                                              objecttype='measuringstation';
                                        """)
                )
                connection.commit()  # commit the transaction

            df_merge["objecttype"] = objecttype
            df_merge["parameterid"] = np.where(df_merge["momentid"] <= 0, 1, 2)
            df_merge["calculating"] = True
            df_merge["changedate"] = 0

            df_states = df_merge.loc[
                :,
                [
                    "objectid",
                    "objecttype",
                    "parameterid",
                    "momentid",
                    "stateid",
                    "calculating",
                    "changedate",
                ],
            ]

            # schrijf data naar de database
            df_states.to_sql(
                table,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
            )

            # verbinding opruimen
            engine.dispose()

    def set_global_variable(self, key: str, value: Any):
        """
        Functie voor het dynamisch overschrijven van global variablen.

        Parameters:
        -----------
        key: str
            naam van de waarde om te overschrijven

        value: Any
            Object om mee te geven
        """
        self.config.global_variables[key] = value

    # TODO: support voor geodataframe in de toekomst?
    def set_dataframe_adapter(
        self, key: str, df: pd.DataFrame, if_not_exist: str = "raise"
    ) -> None:
        """
        Functie om een dataframe mee te geven aan een data adapter met `type: python`.
        Let er zelf op dat de kollom namen en datatypes overeen komen met de beoogde functie.

        Parameters:
        -----------
        key: str
            naam van de data adapter zoals opgegeven in de configuratie yaml

        df: pd.Dataframe
            Object om mee te geven

        if_not_exist: str[raise, create]
            Geeft aan wat te doen als de data adapter niet bestaat,
            bij raise krijg je een error, bij create wordt er een nieuwe data adapter aangemaakt.
        """
        if key in self.config.data_adapters:
            data_adapter_config = self.config.data_adapters[key]
            if data_adapter_config["type"] == "python":
                data_adapter_config["dataframe_from_python"] = df
            else:
                raise UserWarning(
                    "Deze functionaliteit is voor data adapters van type `python`, "
                )
        elif if_not_exist == "raise":
            raise UserWarning(
                f"Data adapter `{key}` niet gevonden, zorg dat deze goed in het config bestand staat met type `python`"
            )
        elif if_not_exist == "create":
            self.config.data_adapters[key] = {
                "type": "python",
                "dataframe_from_python": df,
            }

        else:
            raise UserWarning(
                f"Data adapter `{key=}` niet gevonden, en {if_not_exist=} is ongeldig, moet `raise` of `create` zijn"
            )


def get_kwargs(function, input_config: dict) -> dict:
    """
    Gegeven een input/output functie, stuurt de relevanten kwargs uit de input config naar de functie.
    """
    # We kijken welke argumenten we aan de functie kunnen geven
    possible_parameters_function = set(inspect.signature(function).parameters.keys())
    # Vervolgens nemen we alleen de namen van de parameters over die ook opgegeven zijn
    wanted_keys = list(possible_parameters_function.intersection(input_config.keys()))
    # en geven we een kwargs dictionary door aan de inlees functie
    return {key: input_config[key] for key in wanted_keys}


def check_rootdir(global_variables: dict) -> None | UserWarning:
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


def check_file_and_path(function_config: dict, global_vars: dict) -> None:
    # pad relatief tot rootdir mee gegeven in config
    if "file" in function_config:
        # als de gebruiker een compleet pad mee geeft:
        if Path(global_vars["rootdir"]).is_absolute():
            function_config["abs_path"] = (
                Path(global_vars["rootdir"]) / function_config["file"]
            )
        # als rootdir geen absoluut pad is, nemen we relatief aan
        else:
            function_config["abs_path"] = (
                Path.cwd() / global_vars["rootdir"] / function_config["file"]
            )

        if not function_config["abs_path"].is_absolute():
            raise UserWarning(
                f"Check if root dir ({global_vars['rootdir']}) and file ({function_config['file']}) exist"
            )
    # als een pad wordt mee gegeven
    elif "path" in function_config:
        # eerst checken of het absoluut is
        if Path(function_config["path"]).is_absolute():
            function_config["abs_path"] = Path(function_config["path"])
        # anders alsnog toevoegen
        else:
            function_config["abs_path"] = (
                Path(global_vars["rootdir"]) / function_config["path"]
            )
