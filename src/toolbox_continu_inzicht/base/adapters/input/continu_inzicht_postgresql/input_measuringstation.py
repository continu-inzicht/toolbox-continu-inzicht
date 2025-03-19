"""
DataAdapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def input_ci_postgresql_measuringstation_data_table(input_config: dict) -> pd.DataFrame:
    """
    Haalt tijdreeks van belasting per meetstation op uit een Continu Inzicht database.

    YAML voorbeeld:\n
        type: ci_postgresql_measuringstation_data_table
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In het `.env`-bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml'-config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:\n
    df (DataFrame):\n
    - measurement_location_id: int64        : id van het meetstation
    - measurement_location_code: str        : code van het meetstation
    - measurement_location_description: str : naam van het meetstation
    - parameter_id: int64                   : id van de parameter
    - parameter_code: str                   : code van de parameter
    - parameter_description: str            : omschrijving van de parameter
    - unit: str                             : unit van de parameter
    - date_time: datetime64                 : datum/ tijd van het tijdreeksitem
    - value: float64                        : waarde van het tijdreeksitem
    - value_type: str                       : type waarde van het tijdreeksitem (meting of verwacht)
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
            data.objectid AS measurement_location_id,
            measuringstation.code AS measurement_location_code,
            measuringstation.name AS measurement_location_description,
            parameter.id AS parameter_id,
            parameter.code AS parameter_code,
            parameter.name AS parameter_description,
            parameter.unit AS unit,
            TO_TIMESTAMP(data.datetime/1000) AS date_time,
            value AS value,
            (
                CASE
                    WHEN parameter.id > 1 THEN 'verwacht'
                    ELSE 'meting'
                END
            ) AS value_type
        FROM {schema}.data
        INNER JOIN {schema}.measuringstations AS measuringstation ON data.objectid=measuringstation.id
        INNER JOIN {schema}.parameters AS parameter ON data.parameterid=parameter.id
        WHERE data.objecttype='measuringstation' AND data.calculating=True;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_from_waterlevels(input_config: dict) -> pd.DataFrame:
    """
    Haalt belasting op uit de Continu Inzicht database voor het WhatIf scenario (tabel: waterstanden).

    YAML voorbeeld:\n
        type: ci_postgresql_from_waterlevels
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In het `.env`-bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:\n
    df (DataFrame):\n
    - measurement_location_id: int64        : id van het meetstation
    - measurement_location_code: str        : code van het meetstation
    - measurement_location_description: str : naam van het meetstation
    - parameter_id: int64                   : id van de parameter
    - parameter_code: str                   : code van de parameter
    - parameter_description: str            : omschrijving van de parameter
    - unit: str                             : unit van de parameter
    - date_time: datetime64                 : datum/ tijd van de tijdreeksitem
    - value: float64                        : waarde van de tijdreeksitem
    - value_type: str                       : type waarde van de tijdreeksitem (meting of verwacht)
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
                    value AS value,
                    (
                        CASE
                            WHEN waterlevel.datetime > simulation.datetime THEN 'verwacht'
                            ELSE 'meting'
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

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_from_conditions(input_config: dict) -> pd.DataFrame:
    """
    Haalt klassegrenzen op uit een Continu Inzicht database.

    YAML voorbeeld:\n
        type: ci_postgresql_from_conditions
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie-opties

    **Opmerking:**\n
    In het `.env`-bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml'-config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:\n
    df (DataFrame):\n
    - measurement_location_id: int64    : id van het meetstation
    - measurement_location_code: str    : code van het meetstation
    - lower_boundary: float64           : ondergrens van de klassegrens
    - upper_boundary: float64           : bovengrens van de klassegrens
    - color: str                        : kleur van de klassegrens
    - label: str                        : legendanaam van de klassegrens
    - unit: str                         : unit van de klassegrens
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
            measuringstation.id AS measurement_location_id,
            measuringstation.code AS measurement_location_code,
            LAG(condition.upperboundary/100, 1) OVER (PARTITION BY condition.objectid ORDER BY condition.stateid) AS lower_boundary,
            condition.upperboundary/100 AS upper_boundary,
            condition.color AS color,
            condition.description AS label,
            parameter.unit AS unit
        FROM {schema}.conditions AS condition
        INNER JOIN {schema}.measuringstations AS measuringstation ON measuringstation.id=condition.objectid
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=1
        WHERE condition.objecttype='measuringstation'
        ORDER BY condition.objectid,condition.stateid;;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_from_measuringstations(input_config: dict) -> pd.DataFrame:
    """
    Haalt meetstations op uit een continu database.

    Yaml example:\n
        type: ci_postgresql_from_measuringstations
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"
        source: "waterinfo"

    Args:\n
    input_config (dict): configuratie-opties

    **Opmerking:**\n
    In het `.env`-bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    - source (str): source (veld) waar de meetstations aan gekoppeld zijn.

    Returns:\n
    df (DataFrame):\n
    - measurement_location_id: int64        : id van het meetstation
    - measurement_location_code: str        : code van het meetstation
    - measurement_location_description: str : naam van het meetstation
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

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
