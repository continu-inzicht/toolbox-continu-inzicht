import pandas as pd
import sqlalchemy


def input_ci_postgresql_failuremechanisms(input_config: dict) -> pd.DataFrame:
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
    Haalt lijst met maatregelen op uit de Continu Inzicht database

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

    # ophalen maatregelen
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


def input_ci_postgresql_calc_status(input_config: dict) -> pd.DataFrame:
    """
    Ophalen status continu inzicht

    is_calculating bool: is er op dit moment een berekening bezig
    wind_changed bool: is in het userinterface na de laatste berekening de wind parameter aangepast
    scenario_changed bool: is in het userinterface na de laatste berekening het scenario aangepast
    degradation_changed bool: is in het userinterface na de laatste berekening een degradatie aangepast
    measure_changed bool: is in het userinterface na de laatste berekening een maatregel aangepast
    expertjudgement_changed  bool: is in het userinterface na de laatste berekening een beheerdersoordeel aangepast
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
                (SELECT COUNT(calculating)>0 FROM {schema}.data WHERE calculating=true) AS is_calculating,
                (SELECT value  FROM {schema}.attributes WHERE name='wind') IS NOT NULL AS wind_changed,
                (SELECT value  FROM {schema}.attributes WHERE name='scenario') IS NOT NULL AS scenario_changed,
                (SELECT value  FROM {schema}.attributes WHERE name='degradation') IS NOT NULL AS degradation_changed,
                (SELECT value  FROM {schema}.attributes WHERE name='measure') IS NOT NULL AS measure_changed,
                (SELECT value  FROM {schema}.attributes WHERE name='expertjudgement') IS NOT NULL AS expertjudgement_changed
        """
        df = pd.read_sql_query(sql=sqlalchemy.text(select_query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
