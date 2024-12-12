"""
Data adapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def input_ci_postgresql_from_sections(input_config: dict) -> pd.DataFrame:
    """
    Ophalen section data uit de Continu Inzicht database.

    Yaml example:\n
        type: ci_postgresql_from_sections
        database: "geoserver"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:\n
    df (DataFrame):\n
    - id: int64             : id van de dijkvak
    - name: str             : naam van de dijkvak
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
            id AS id,
            name AS name
        FROM {schema}.sections;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_from_sectionfractions(input_config: dict) -> pd.DataFrame:
    """
    Ophalen sections fractions uit een continu database.

    Yaml example:\n
        type: ci_postgresql_from_sectionfractions
        database: "geoserver"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:\n
    df (DataFrame):\n
    - id: int64             : id van de dijkvak
    - idup: int64           : id van bovenstrooms meetstation
    - iddown: int64         : id van benedestrooms meetstation
    - fractionup: float64   : fractie van bovenstrooms meetstation
    - fractiondown: float64 : fractie van benedestrooms meetstation
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
            sectionid AS id,
            idup,
            iddown,
            fractionup,
            fractiondown
        FROM {schema}.sectionfractions;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_load_from_data_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen tijdreeks van belasting per dijkvak

    Yaml example:\n
        type: ci_postgresql_section_load_from_data_table
        database: "geoserver"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:\n
    df (DataFrame):\n
    - section_id: int64     : id van de dijkvak
    - parameter_id: int64   : id van de parameter
    - unit: str             : unit van de parameter
    - date_time: datetime64 : datum/ tijd van de tijdreeksitem
    - value: float64        : waarde van de tijdreeksitem
    - value_type: str       : type waarde van de tijdreeksitem (meting of verwacht)
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

    calculating = True
    if "calculating" in input_config:
        calculating = input_config["calculating"]

    schema = input_config["schema"]
    query = f"""
        SELECT
            data.objectid AS section_id,
            parameter.id AS parameter_id,
            parameter.unit As unit,
            TO_TIMESTAMP(data.datetime/1000) AS date_time,
            data.value AS value,
            CASE WHEN parameter.measured THEN 'meting' ELSE 'verwacht' END AS value_type
        FROM {schema}.data
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=data.parameterid
        WHERE data.objecttype='section' AND data.calculating={calculating};
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_failure_probability_from_data_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen faalkansen per dijkvak per moment

    Yaml example:\n
        type: ci_postgresql_section_failure_probability_from_data_table
        database: "geoserver"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:\n
    df (DataFrame):\n
    - failureprobability_id: in64   : id van de dijkvak/faalmechanisme/maatregel combinatie
    - section_id: int64             : id van de dijkvak
    - value_parameter_id: int64     : id van de belasting parameter (1/2/3/4)
    - failuremechanism_id: int64    : id van het faalmechanisme
    - failuremechanism: str         : naam van het faalmechanisme
    - measures_id: int64            : id van de maatregel
    - measure: str                  : naam van de maatregel
    - parameter_id: int64           : id van de faalkans parameter (5/100/101/102)
    - unit: int64                   : unit van de belasting
    - date_time: datetime64         : datum/ tijd van de tijdreeksitem
    - value: float                  : waarde van de tijdreeksitem
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
            failureprobability.id AS failureprobability_id,
            failureprobability.objectid AS section_id,
            failureprobability.parameterid AS value_parameter_id,
            failuremechanism.id AS failuremechanism_id,
            failuremechanism.name AS failuremechanism,
            failureprobability.measureid AS measures_id,
            measures.name AS measure,
            parameter.id AS parameter_id,
            parameter.unit As unit,
            TO_TIMESTAMP(data.datetime/1000) AS date_time,
            data.value AS value
        FROM {schema}.data
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=data.parameterid
        INNER JOIN {schema}.failureprobability ON failureprobability.id=data.objectid
        LEFT JOIN {schema}.failuremechanism ON failuremechanism.id=failureprobability.failuremechanismid
        LEFT JOIN {schema}.measures ON measures.id=failureprobability.measureid
        WHERE data.objecttype='failureprobability' AND data.calculating=true;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_thresholds_from_conditions_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen klassegrenzen (faalkans) van een dijkvak uit een continu database.

    Yaml example:\n
        type: ci_postgresql_section_thresholds_from_conditions_table
        database: "geoserver"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:\n
    df (DataFrame):\n
    - state_id: float64             : id van de klassegrens
    - lower_boundary: float64       : ondergrens van de klassegrens
    - upper_boundary: float64       : bovengrens van de klassegrens
    - color: str                    : kleur van de klassegrens
    - label: str                    : legendanaam van de klassegrens
    - unit: str                     : unit van de klassegrens
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
            condition.stateid AS state_id,
            LAG(condition.upperboundary, 1) OVER (PARTITION BY condition.objectid ORDER BY condition.stateid) AS lower_boundary,
            condition.upperboundary AS upper_boundary,
            condition.color AS color,
            condition.description AS label,
            parameter.unit AS unit
        FROM {schema}.conditions AS condition
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=5
        WHERE condition.objecttype='section'
        ORDER BY condition.objectid,condition.stateid;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
