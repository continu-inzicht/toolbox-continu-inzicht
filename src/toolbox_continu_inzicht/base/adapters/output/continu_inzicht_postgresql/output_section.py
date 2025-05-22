"""
Data adapters voor het schrijven naar de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy

from datetime import datetime
from typing import Dict
from sqlalchemy.engine import Engine
from toolbox_continu_inzicht.utils.datetime_functions import epoch_from_datetime


def output_ci_postgresql_section_load_to_data(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft data voor dijkvakken de belasting naar Continu Inzicht database (tabel: data)

    Yaml example:\n
        type: ci_postgresql_section_load_to_data
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"
        unit_conversion_factor: 0.01

    Args:\n
    output_config (dict): configuratie opties\n
    df (DataFrame):\n
    - measurement_location_id: int64
    - date_time: datetime64[ns, UTC]
    - value: float64
    - value_type: str

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    """

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]

    def bepaal_parameter_id(value_type: str):
        if value_type == "meting":
            return 1
        else:
            return 2

    assert all(key in output_config for key in keys)

    table = "data"
    schema = output_config["schema"]
    objecttype = "section"
    calculating = True

    if len(df) > 0:
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        df_data = df.dropna(how="any")

        df_data = df_data.assign(objecttype=str(objecttype))
        df_data = df_data.assign(calculating=True)

        # df_data["datetime"] = df_data["date_time"].apply(datetime.fromisoformat)
        df_data["datetime"] = df_data["date_time"].apply(epoch_from_datetime)
        df_data["parameterid"] = df_data["value_type"].apply(bepaal_parameter_id)

        df_data = df_data.loc[
            :,
            [
                "id",
                "objecttype",
                "parameterid",
                "datetime",
                "value",
                "calculating",
            ],
        ]

        df_data = df_data.rename(columns={"id": "objectid"})
        df_data = df_data.reset_index(drop=True)

        section_ids = df_data.objectid.unique()
        section_ids_str = ",".join(map(str, section_ids))

        with engine.connect() as connection:
            connection.execute(
                sqlalchemy.text(f"""
                                DELETE FROM {schema}.{table}
                                WHERE objectid IN ({section_ids_str}) AND
                                        objecttype='{objecttype}' AND
                                        calculating={calculating};
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


def output_ci_postgresql_section_to_data(output_config: dict, df: pd.DataFrame) -> None:
    """
    Schrijft data naar Continu Inzicht database

    Args:
    ----------
    output_config (dict):

    Opmerking:
    ------
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str):
    - postgresql_password (str):
    - postgresql_host (str):
    - postgresql_port (str):

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str):
    - schema (str):

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

    assert all(key in output_config for key in keys)

    def get_parameter_id(value_type: str) -> int:
        if value_type == "meting":
            return 1
        elif value_type == "verwacht":
            return 2
        else:
            raise UserWarning(f"Onbekende value_type: {value_type}.")

    def get_failuremechanism_id(name: str, failuremechanism: Dict[str, int]) -> int:
        if name in failuremechanism:
            return failuremechanism[name]
        else:
            raise UserWarning(f"Onbekende faalmechanisme: {name}.")

    schema = output_config["schema"]
    calculating = True
    objecttype = "section"

    parameter_id = output_config["parameter_id"]

    #   5: Faalkans
    # 100: Faalkans technical
    # 101: Faalkans measure
    # 102: Faalkans expert judgement
    if parameter_id in [5, 100, 101, 102]:
        objecttype = "failureprobability"

    # 1: Gemeten waterstand
    # 2: Voorspelde waterstand
    # 3: Voorspelde waterstand ensemble hoog
    # 4: Voorspelde waterstand ensemble laag
    elif parameter_id in [1, 2, 3, 4]:
        objecttype = "section"

    if len(df) > 0:
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        # ophalen faalmechanismes
        failuremechanisms = get_failuremechanisms(engine=engine, schema=schema)

        df_data = df.dropna(how="any")
        df_data = df_data.assign(objecttype=str(objecttype))
        df_data = df_data.assign(calculating=calculating)

        df_data["datetime"] = df_data["date_time"].apply(epoch_from_datetime)
        df_data["parameterid"] = parameter_id

        if "value_parameter_id" not in df_data:
            df_data["value_parameter_id"] = df_data["value_type"].apply(
                get_parameter_id
            )

        if "failuremechanism_id" not in df_data and "failuremechanism" not in df_data:
            raise UserWarning("Faalmechanisme kan niet bepaald worden.")

        if "failuremechanism_id" not in df_data:
            df_data["failuremechanismid"] = df_data["failuremechanism"].apply(
                get_failuremechanism_id, args=(failuremechanisms,)
            )
        else:
            df_data["failuremechanismid"] = df_data["failuremechanism_id"]

        if "measure_id" not in df_data:
            df_data["measureid"] = 0
        else:
            df_data["measureid"] = df_data["measure_id"]

        if objecttype == "section":
            pass

        # eventueel toevoegen aan de failureprobability tabel
        elif objecttype == "failureprobability":
            # failureprobability table
            df_combintions = (
                df_data.groupby(
                    [
                        "section_id",
                        "value_parameter_id",
                        "failuremechanismid",
                        "measureid",
                    ]
                )
                .size()
                .reset_index()
                .iloc[:, :-1]
                .copy()
            )
            df_combintions = df_combintions.assign(objecttype="section")
            df_combintions = df_combintions.rename(columns={"section_id": "objectid"})

            if len(df_combintions) > 0:
                # Genereer de insert query
                insert_query = f"INSERT INTO {schema}.failureprobability (objectid, objecttype, parameterid, measureid, failuremechanismid) VALUES "
                values = ", ".join(
                    [
                        f"({repr(row.objectid)},{repr(row.objecttype)}, {repr(row.value_parameter_id)}, {repr(row.measureid)}, {repr(row.failuremechanismid)})"
                        for row in df_combintions.itertuples(index=False)
                    ]
                )
                insert_query += (
                    values
                    + "ON CONFLICT (objectid, objecttype, parameterid, measureid, failuremechanismid) DO NOTHING;"
                )

                # Genereer de select query
                select_query = f"""
                    SELECT id, objectid, objecttype, parameterid, measureid, failuremechanismid
                    FROM {schema}.failureprobability
                    WHERE (objectid, objecttype, parameterid, measureid, failuremechanismid) IN ({values});
                """

                with engine.connect() as connection:
                    connection.execute(sqlalchemy.text(insert_query))
                    connection.commit()

                    df_failureprobability = pd.read_sql_query(
                        sql=sqlalchemy.text(select_query), con=connection
                    )

                    if len(df_failureprobability) > 0:
                        df_data_merged = pd.merge(
                            df_data,
                            df_failureprobability,
                            left_on=[
                                "section_id",
                                "value_parameter_id",
                                "measureid",
                                "failuremechanismid",
                            ],
                            right_on=[
                                "objectid",
                                "parameterid",
                                "measureid",
                                "failuremechanismid",
                            ],
                            how="left",
                            suffixes=("", "_drop"),
                        )

                        # nu de data verzamelen
                        df_data_merged = df_data_merged.loc[
                            :,
                            [
                                "id",
                                "objecttype",
                                "parameterid",
                                "datetime",
                                "failureprobability",
                                "calculating",
                            ],
                        ]

                        df_data_merged = df_data_merged.rename(
                            columns={"id": "objectid", "failureprobability": "value"}
                        )
                        df_data_merged = df_data_merged.reset_index(drop=True)

                        # Eerst oude data verwijderen
                        df_combintions = (
                            df_data_merged.groupby(
                                ["objectid", "objecttype", "parameterid", "calculating"]
                            )
                            .size()
                            .reset_index()
                            .iloc[:, :-1]
                            .copy()
                        )
                        values = ", ".join(
                            [
                                f"({repr(row.objectid)},{repr(row.objecttype)},{repr(row.parameterid)},{repr(row.calculating)})"
                                for row in df_combintions.itertuples(index=False)
                            ]
                        )

                        with engine.connect() as connection:
                            connection.execute(
                                sqlalchemy.text(
                                    f"""
                                    DELETE FROM {schema}.data
                                    WHERE (objectid,objecttype,parameterid,calculating) IN ({values});
                                    """
                                )
                            )
                            connection.commit()  # commit the transaction

                        # schrijf data naar de database
                        df_data_merged.to_sql(
                            "data",
                            con=engine,
                            schema=schema,
                            if_exists="append",
                            index=False,
                        )

        # verbinding opruimen
        engine.dispose()

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_section_to_states(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft state naar Continu Inzicht database

    Args:
    ----------
    output_config (dict):

    Opmerking:
    ------
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str):
    - postgresql_password (str):
    - postgresql_host (str):
    - postgresql_port (str):

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str):
    - schema (str):

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

    assert all(key in output_config for key in keys)

    schema = output_config["schema"]
    calculating = True
    objecttype = "failureprobability"

    if len(df) > 0:
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        df_data = df.dropna(how="any")
        df_data = df_data.assign(objecttype=str(objecttype))
        df_data = df_data.assign(calculating=calculating)
        df_data = df_data.assign(changedate=int(datetime.utcnow().timestamp()) * 1000)

        df_data["epoch"] = df_data["date_time"].apply(epoch_from_datetime)

        select_moments_query = f"""
            SELECT
                id AS moment_id,
                calctime AS calc_time
            FROM {schema}.moments;
        """

        with engine.connect() as connection:
            df_moments = pd.read_sql_query(
                sql=sqlalchemy.text(select_moments_query), con=connection
            )
            if len(df_moments) > 0:
                df_data_merge = pd.merge(
                    df_data,
                    df_moments,
                    left_on="epoch",
                    right_on="calc_time",
                    how="left",
                )

                # als datetime niet overeenkomt met de moment tabel dan zal het veld moment_id 'NaN' zijn.
                df_data_merge = df_data_merge.dropna(how="any")

                if len(df_data_merge) > 0:
                    # nu de data verzamelen
                    df_states = df_data_merge.loc[
                        :,
                        [
                            "failureprobability_id",
                            "objecttype",
                            "parameter_id",
                            "moment_id",
                            "state_id",
                            "calculating",
                            "changedate",
                        ],
                    ]

                    df_states = df_states.rename(
                        columns={
                            "failureprobability_id": "objectid",
                            "parameter_id": "parameterid",
                            "state_id": "stateid",
                            "moment_id": "momentid",
                        }
                    )

                    # Eerst oude data verwijderen
                    df_combintions = (
                        df_states.groupby(
                            ["objectid", "objecttype", "parameterid", "calculating"]
                        )
                        .size()
                        .reset_index()
                        .iloc[:, :-1]
                        .copy()
                    )
                    values = ", ".join(
                        [
                            f"({repr(row.objectid)},{repr(row.objecttype)},{repr(row.parameterid)},{repr(row.calculating)})"
                            for row in df_combintions.itertuples(index=False)
                        ]
                    )

                    with engine.connect() as connection:
                        connection.execute(
                            sqlalchemy.text(
                                f"""
                                DELETE FROM {schema}.states
                                WHERE (objectid,objecttype,parameterid,calculating) IN ({values});
                                """
                            )
                        )
                        connection.commit()  # commit the transaction

                    # schrijf data naar de database
                    df_states.to_sql(
                        "states",
                        con=engine,
                        schema=schema,
                        if_exists="append",
                        index=False,
                    )

                else:
                    raise UserWarning(
                        "Momenten komen niet overeen met de momenten tabel."
                    )

        # verbinding opruimen
        engine.dispose()

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_section(output_config: dict, df: pd.DataFrame) -> None:
    """
    Schrijft secties naar de Continu Inzicht database (tabel: sections).

    Yaml example:\n
        type: ci_postgresql_section
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
    output_config (dict): configuratie opties
    df (DataFrame):\n
    - id: int64: id van de sectie
    - segmentid: int64: id van het segment waartoe de sectie behoort
    - name: str: naam van de sectie
    - geometry: geom: geometrie (ligging) van de sectie (let op projectie altijd EPSG4326!)

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    """

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]

    assert all(key in output_config for key in keys)

    schema = output_config["schema"]

    if not df.empty:
        if "id" in df and "segmentid" in df and "name" in df and "geometry" in df:
            query = []

            query.append(f"TRUNCATE {schema}.sections;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.sections(id, segmentid, name, geometry) VALUES ({str(row['id'])}, {str(row['segmentid'])}, '{str(row['name'])}', '{str(row['geometry'])}');"
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()  # commit the transaction

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_sectionfractions(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft (interpolatie)fracties van meetlocatie per sectie naar de Continu Inzicht database (tabel: sectionfractions).

    Yaml example:\n
        type: ci_postgresql_sectionfractions
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
    output_config (dict): configuratie opties\n
    df (DataFrame):
    - sectionid: int64          : id van de sectie
    - idup: int64               : id van het meetlocatie (measuringlocation), stroomopwaarst gelegen (indien van toepassing)
    - iddown: int64             : id van het meetlocatie (measuringlocation), stroomafwaarst gelegen (indien van toepassing)
    - fractionup: float64       : fractie van de belasting gebruikt voor interpolatie belasting bij sectie stroomopwaarst gelegen (indien van toepassing)
    - fractiondown: float64     : fractie van de belasting gebruikt voor interpolatie belasting bij sectie stroomafwaarst gelegen (indien van toepassing)
    - parameters: XXXXXX          : OPTIONEEL lijst van (belasting)parameters (ROLF VRAGEN)

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    """

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]

    assert all(key in output_config for key in keys)

    schema = output_config["schema"]

    if not df.empty:
        if (
            "sectionid" in df
            and "idup" in df
            and "iddown" in df
            and "fractionup" in df
            and "fractiondown" in df
            and "parameters" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.sectionfractions;")

            parameters_rows = df[df["parameters"].isna()]
            for _, row in parameters_rows.iterrows():
                query.append(
                    f"INSERT INTO {schema}.sectionfractions(sectionid, idup, iddown, fractionup, fractiondown) VALUES ({str(row['sectionid'])}, {str(row['idup'])}, {str(row['iddown'])}, {str(row['fractionup'])}, {str(row['fractiondown'])});"
                )

            non_parameters_rows = df[df["parameters"].notna()]
            for _, row in non_parameters_rows.iterrows():
                query.append(
                    f"INSERT INTO {schema}.sectionfractions(sectionid, idup, iddown, fractionup, fractiondown, parameters) VALUES ({str(row['sectionid'])}, {str(row['idup'])}, {str(row['iddown'])}, {str(row['fractionup'])}, {str(row['fractiondown'])}, '{str(row['parameters']).replace(';', ',')}');"
                )
            # ROLF VRAGEN : naar , of DELIMTER OMZETTEN

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()  # commit the transaction

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def get_parameters(engine: Engine, schema: str) -> dict:
    """
    Ophalen lijst met parameters uit de Continu Inzicht database

    Faalmechanismes:\n
    - code: naam
    - H10: Gemeten waterstand
    - H10V:	Voorspelde waterstand
    - H10EH: Voorspelde waterstand ensemble hoog
    - H10EL: Voorspelde waterstand ensemble laag
    - PF: Faalkans
    - BO: Beheerdersoordeel
    - MR: Maatregel
    - RSCH: Risico verwachte schade
    - RSL: Risico verwachte slachtoffers
    - RGETR: Risico getroffenen
    - RPLPF: Risico plaatsgebonden overstromingskans
    - PFT: Faalkans technical
    - PFM: Faalkans measure
    - PFEJ: Faalkans expert judgement
    """
    # ophalen parameters
    parameters = []

    with engine.connect() as connection:
        select_query = f"""
            SELECT id, code AS name FROM {schema}.parameters;
        """
        df_parameters = pd.read_sql_query(
            sql=sqlalchemy.text(select_query), con=connection
        )

        if len(df_parameters) > 0:
            parameters = df_parameters.set_index("name").to_dict()["id"]

    return parameters


def get_failuremechanisms(engine: Engine, schema: str) -> dict:
    """
    Ophalen lijst met faalmechanismes uit de Continu Inzicht database

    Faalmechanismes:\n
    - code: naam
    - COMB: Combinatie faalmechanismen
    - GEKB: Overloop en overslag dijken
    - STPH: Opbarsten en piping dijken
    - STBI: Stabiliteit binnenwaarts dijken
    - HTKW: Overloop en overslag langsconstructies
    - STKWl: Stabiliteit langsconstructies
    - PKW: Piping langsconstructies
    """
    # ophalen faalmechanismes
    failuremechanisms = []

    with engine.connect() as connection:
        select_query = f"""
            SELECT id, name FROM {schema}.failuremechanism;
        """
        df_failuremechanisms = pd.read_sql_query(
            sql=sqlalchemy.text(select_query), con=connection
        )

        if len(df_failuremechanisms) > 0:
            failuremechanisms = df_failuremechanisms.set_index("name").to_dict()["id"]

    return failuremechanisms
