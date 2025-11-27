"""
DataAdapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def input_ci_postgresql_section_id_to_measure_id(input_config: dict) -> pd.DataFrame:
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
    query = f"""
        SELECT 
            section.id AS section_id,
            failuremechanism.id AS failuremechanism_id, 
            COALESCE(expertjudgement.measureid,0) AS measure_id
        FROM {schema}.sections AS section
        LEFT JOIN {schema}.expertjudgement AS expertjudgement ON section.id=expertjudgement.sectionid
        LEFT JOIN {schema}.failuremechanism AS failuremechanism ON failuremechanism.id=failuremechanism.id
        ORDER BY section.id;     
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
