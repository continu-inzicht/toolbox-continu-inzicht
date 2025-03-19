"""
DataAdapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def input_ci_postgresql_whatif_from_scenario(input_config: dict) -> pd.DataFrame:
    """
    Haalt what-if scenario's op uit de continu database (tabel: scenarios).

    YAML voorbeeld:\n
        type: ci_postgresql_whatif_from_scenario
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

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
    - scenario_id: int64                : id van het scenario
    - scenario_name: str                : naam van het scenario
    - scenario_min_date_time: float64   : eerste datum/tijd van het scenario
    - scenario_max_date_time: float64   : laatste datum/tijd van het scenario
    - scenario_time_step: float64       : stapgrootte tijd in het scenario
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
            id AS scenario_id,
            name AS scenario_name,
            min AS scenario_min_date_time,
            max AS scenario_max_date_time,
            step AS scenario_time_step
        FROM {schema}.scenarios;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_whatif_load(input_config: dict) -> pd.DataFrame:
    """
    Haalt belastingen op voor een what-if scenario uit de continu database (tabel: waterlevels).

    YAML voorbeeld:\n
        type: ci_postgresql_whatif_load
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

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
    - scenario_id: int64                : id van het scenario
    - datetime: float64                 : eerste datum/tijd van het scenario
    - value: float64                    : waarde van de belasting (bijvoorbeeld een waterstand)
    - parameter: int64                  : id van de parameter
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
            waterlevels.measuringstationid AS measurement_location_id,
            waterlevels.scenarioid AS scenario_id,
            TO_TIMESTAMP(waterlevels.datetime/1000) AS date_time,
            waterlevels.value AS value,
            (
                CASE
                    WHEN waterlevels.parameter > 1 THEN 'verwacht'
                    ELSE 'meting'
                END
            ) AS value_type,
            waterlevels.parameter
        FROM {schema}.simulation AS simulation
        INNER JOIN {schema}.waterlevels AS waterlevels ON waterlevels.scenarioid=simulation.scenarioid;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
