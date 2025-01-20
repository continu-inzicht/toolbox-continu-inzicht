import pandas as pd
import sqlalchemy


def input_ci_postgresql_failuremechanisms(input_config: dict) -> pd.DataFrame:
    """
    Ophalen lijst met faalmechanismes uit de Continu Inzicht database

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
              id, 
              name 
            FROM {schema}.failuremechanism;
        """
        df = pd.read_sql_query(sql=sqlalchemy.text(select_query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_measures(input_config: dict) -> pd.DataFrame:
    """
    Ophalen lijst met maatregelen uit de Continu Inzicht database

    maatregelen:
        id, name,         description
        0,  NONE,         geen maatregel
        1,  Maatregel 1,  Maatregel 1
        2,  Maatregel 2,  Maatregel 2
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

    # ophalen measures
    with engine.connect() as connection:
        select_query = f"""
            SELECT 
              id, 
              name, 
              description 
            FROM {schema}.measures;
        """
        df = pd.read_sql_query(sql=sqlalchemy.text(select_query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
