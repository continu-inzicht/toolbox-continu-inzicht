"""
DataAdapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def input_ci_postgresql_measures_to_effect(input_config: dict) -> pd.DataFrame:
    """
    Haalt de effecten per maatregelen op (tabel: measure_effect).

    YAML voorbeeld:

        type: ci_postgresql_measures_to_effect
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

    - measure_id: int64                 : id van de maatregel
    - failuremechanism_id: int64        : id van het faalmechanisme
    - effect: float64                   : effect van de maatregel op het faalmechanisme
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
            measureid AS measure_id, 
            failuremechanismid AS failuremechanism_id,
            effect AS effect
        FROM {schema}.measure_effect
        ORDER BY measureid, failuremechanismid;        
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
