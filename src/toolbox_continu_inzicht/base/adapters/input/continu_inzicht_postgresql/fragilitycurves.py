import pandas as pd
import sqlalchemy


def input_ci_postgresql_fragilitycurves_table(input_config: dict) -> pd.DataFrame:
    """
    Ophalen fragilitycurves voor alle dijkvakken en opgegeven faalmechanismes

    Args:
    ----------
    input_config (dict):

    Opmerking:
    ------
    In de `.env` environment bestand moeten de volgende parameters staan:
    postgresql_user (str):
    postgresql_password (str):
    postgresql_host (str):
    postgresql_port (str):

    In de 'yaml' config moeten de volgende parameters staan:
    database (str):
    schema (str):

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

    assert all(key in input_config for key in keys)

    # maak verbinding object
    engine = sqlalchemy.create_engine(
        f"postgresql://{input_config['postgresql_user']}:{input_config['postgresql_password']}@{input_config['postgresql_host']}:{int(input_config['postgresql_port'])}/{input_config['database']}"
    )

    schema = input_config["schema"]

    measureid = 0
    if "measureid" in input_config:
        measureid = input_config["measureid"]

    timedep = 0
    if "timedep" in input_config:
        timedep = input_config["timedep"]

    degradatieid = 0
    if "degradatieid" in input_config:
        degradatieid = input_config["degradatieid"]

    query = f"""
        SELECT
            sectionid AS section_id,
            failuremechanism.name AS failuremechanism,
            hydraulicload AS hydraulicload,
            failureprobability AS failureprobability
        FROM {schema}.fragilitycurves
        INNER JOIN {schema}.failuremechanism ON failuremechanism.id=fragilitycurves.failuremechanismid
        WHERE measureid={measureid} AND timedep={timedep} AND degradatieid={degradatieid}
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    # Datum kolom moet een object zijn en niet een 'datetime64[ns, UTC]'
    # df["date_time"] = df["date_time"].astype(object)

    return df
