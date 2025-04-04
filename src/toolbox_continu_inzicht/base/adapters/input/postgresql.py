import pandas as pd
import sqlalchemy
import geopandas as gpd


def input_postgresql_database(input_config: dict) -> pd.DataFrame:
    """Schrijft data naar een PostgreSQL-database gegeven het pad naar een credential-bestand.

    Parameters
    ----------
    input_config: dict
        input config met extra opties waaronder tabel en schema naam.


    Notes
    -----
    In het `.env` environmentbestand moet staan:

        - postgresql_user: str
        - postgresql_password: str
        - postgresql_host: str
        - postgresql_port: str
        - database: str
        - schema: str

    Raises
    ------
    AssertionError
        Als de vereiste parameters niet aanwezig zijn in de input_config.
    UserWarning
        De parameter(s) 'table' en/of 'query' zijn niet in de DataAdapter gedefinieerd.

    Returns
    -------
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

    schema = ""
    table = ""
    query = ""

    if "query" in input_config:
        # bepaal eventueel de tabelnaam voor het vervangen in de query-string
        if "table" in input_config:
            table = input_config["table"]

        if "schema" in input_config:
            schema = input_config["schema"]

        query = input_config["query"]
        query = query.replace("{{schema}}", schema).replace("{{table}}", table)

    elif "table" in input_config:
        query = f"SELECT * FROM {input_config['schema']}.{input_config['table']};"
    else:
        raise UserWarning(
            "De parameter(s) 'table' en/of 'query' zijn niet in de DataAdapter gedefinieerd."
        )

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_postgis(input_config: dict) -> gpd.GeoDataFrame:
    """Leest data uit een PostgreSQL-database gegeven het pad naar een credential-bestand.

    Parameters
    ----------
    input_config: dict
        dictionary met schema en tabel naam.


    Notes
    -----
    In het `.env` environmentbestand moet staan:
    postgresql_user: str
    postgresql_password: str
    postgresql_host: str
    postgresql_port: str
    database: str
    schema: str

    Returns
    -------
    gpd.GeoDataFrame


    Raises
    ------
    UserWarning
        De parameter(s) 'table' en/of 'query' zijn niet in de DataAdapter gedefinieerd.

    AssertionError
        Als de vereiste parameters niet aanwezig zijn in de input_config.
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

    schema = ""
    table = ""
    query = ""
    geom_col = input_config.get("geom_col", "geometry")
    if "query" in input_config:
        # bepaal eventueel de tabelnaam voor het vervangen in de query-string
        if "table" in input_config:
            table = input_config["table"]

        if "schema" in input_config:
            schema = input_config["schema"]

        query = input_config["query"]
        query = query.replace("{{schema}}", schema).replace("{{table}}", table)

    elif "table" in input_config:
        query = f"""
                SELECT
                    *
                FROM
                    {input_config["schema"]}.{input_config["table"]};
                """
    else:
        raise UserWarning(
            "De parameter(s) 'table' en/of 'query' zijn niet in de DataAdapter gedefinieerd."
        )

    # query uitvoeren op de database
    with engine.connect() as connection:
        gdf = gpd.read_postgis(
            sql=sqlalchemy.text(query), con=connection, geom_col=geom_col
        )

    # verbinding opruimen
    engine.dispose()

    return gdf
