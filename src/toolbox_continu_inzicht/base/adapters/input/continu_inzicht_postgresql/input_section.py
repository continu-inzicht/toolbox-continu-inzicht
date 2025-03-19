"""
DataAdapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def input_ci_postgresql_from_sections(input_config: dict) -> pd.DataFrame:
    """
    Haalt sectie data op uit de Continu Inzicht database.

    YAML voorbeeld:\n
        type: ci_postgresql_from_sections
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
    - id: int64             : id van het dijkvak
    - name: str             : naam van het dijkvak
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

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_from_sectionfractions(input_config: dict) -> pd.DataFrame:
    """
    Haalt sectiefracties uit een continu database.

    YAML voorbeeld:\n
        type: ci_postgresql_from_sectionfractions
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
    - id: int64             : id van het dijkvak
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

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_load_from_data_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Haalt tijdreeks van belasting per dijkvak op

    YAML voorbeeld:\n
        type: ci_postgresql_section_load_from_data_table
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
    - section_id: int64     : id van het dijkvak
    - parameter_id: int64   : id van de parameter
    - unit: str             : unit van de parameter
    - date_time: datetime64 : datum/ tijd van het tijdreeksitem
    - value: float64        : waarde van het tijdreeksitem
    - value_type: str       : type waarde van het tijdreeksitem (meting of verwacht)
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

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_failure_probability_from_data_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Haalt faalkansen per dijkvak per moment op

    YAML voorbeeld:\n
        type: ci_postgresql_section_failure_probability_from_data_table
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
    - failureprobability_id: in64   : id van de dijkvak/faalmechanisme/maatregel-combinatie
    - section_id: int64             : id van het dijkvak
    - value_parameter_id: int64     : id van de belastingparameter (1/2/3/4)
    - failuremechanism_id: int64    : id van het faalmechanisme
    - failuremechanism: str         : naam van het faalmechanisme
    - measures_id: int64            : id van de maatregel
    - measure: str                  : naam van de maatregel
    - parameter_id: int64           : id van de faalkansparameter (5/100/101/102)
    - unit: int64                   : unit van de belasting
    - date_time: datetime64         : datum/ tijd van het tijdreeksitem
    - value: float                  : waarde van het tijdreeksitem
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

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_thresholds_from_conditions_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Haalt klassegrenzen (faalkans) van een dijkvak op uit de continu database.

    YAML voorbeeld:\n
        type: ci_postgresql_section_thresholds_from_conditions_table
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
    - state_id: int64               : id van de klassegrens
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

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_expert_judgement_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Haalt klassegrenzen (faalkans) van een dijkvak op uit de continu database.

    YAML voorbeeld:\n
        type: ci_postgresql_section_thresholds_from_conditions_table
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
    - state_id: int64               : id van de klassegrens
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
            sectionid AS section_id,
            parameters.id AS parameter_id,
            parameters.unit AS unit,
            TO_TIMESTAMP(moments.calctime/1000) AS date_time,
            expertjudgement AS state_id,
            (
                CASE
                WHEN moments.id > 0 THEN 'verwacht'
                ELSE 'meting'
                END
            ) AS value_type,
            expertjudgementrate AS failureprobability,
            failuremechanism.id AS failuremechanism_id,
            failuremechanism.name AS failuremechanism
        FROM {schema}.expertjudgement
        INNER JOIN {schema}.moments ON 1=1
        INNER JOIN {schema}.failuremechanism ON failuremechanism.name='COMB'
        INNER JOIN {schema}.parameters ON parameters.id=102
        WHERE NOT expertjudgement IS NULL AND expertjudgement > 0;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
