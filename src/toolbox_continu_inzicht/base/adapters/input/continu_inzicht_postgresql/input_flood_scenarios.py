"""
DataAdapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def input_ci_postgresql_failuremechanisms_with_description(
    input_config: dict,
) -> pd.DataFrame:
    """
    Haalt lijst met faalmechanismes op uit de Continu Inzicht database

    Faalmechanismes:
        code: naam
        COMB: Combinatie faalmechanismen
        GEKB: Overloop en overslag dijken
        STPH: Opbarsten en piping dijken
        STBI: Stabiliteit binnenwaarts dijken
        HTKW: Overloop en overslag langsconstructies
        STKWl: Stabiliteit langsconstructies
        PKW: Piping langsconstructies
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

    # ophalen faalmechanismes
    with engine.connect() as connection:
        select_query = f"""
            SELECT
              id as failuremechanism_id,
              name,
              description
            FROM {schema}.failuremechanism;
        """
        df = pd.read_sql_query(sql=sqlalchemy.text(select_query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_sections_and_segments(input_config: dict) -> pd.DataFrame:
    """
    Haalt sectie data met segment op uit de Continu Inzicht database.

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
            id AS section_id,
            segmentid AS segment_id
        FROM {schema}.sections;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_failureprobability_per_section(
    input_config: dict,
) -> pd.DataFrame:
    """
    Haalt voor de dijkvakken de gekozen maatregel op (tabel: expertjudgement).

    YAML voorbeeld:

        type: ci_postgresql_section_id_to_measure_id
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"

    Args:

    input_config (dict): configuratie-opties

    **Opmerking:**

    In het `.env`-bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml'-config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht

    Returns:

    df (DataFrame):

    - section_id: int64                 : id van de maatregel
    - failuremechanism_id: int64        : id van het faalmechanisme
    - measure_id: int64                 : id van de gekozen maatregel
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
    custom_filter = input_config.get("custom_filter", "")
    if len(custom_filter) > 0:
        filters = ""
        for value in custom_filter:
            filters += f" AND {value}"
        custom_filter = filters
    query = f"""
        SELECT
            failureprobability.objectid AS section_id,
            failuremechanism.id AS failuremechanism_id,
            TO_TIMESTAMP(data.datetime/1000) AS date_time,
            data.value AS failure_probability
        FROM {schema}.data
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=data.parameterid
        INNER JOIN {schema}.failureprobability ON failureprobability.id=data.objectid
        LEFT JOIN {schema}.failuremechanism ON failuremechanism.id=failureprobability.failuremechanismid
        LEFT JOIN {schema}.measures ON measures.id=failureprobability.measureid
        WHERE data.objecttype='failureprobability'{custom_filter};
    """
    # later wel als het in 'de loop zit'
    # --AND data.calculating=true;

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    # voor nu de laaste waarde pakken
    df["date_time"] = pd.to_datetime(df["date_time"])
    date_time = sorted(df["date_time"].unique())[-1]
    df = df[df["date_time"] == date_time]

    return df
