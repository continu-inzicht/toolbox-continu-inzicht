import pandas as pd
import sqlalchemy
import geopandas as gpd


def output_postgresql_database(output_config: dict, df: pd.DataFrame):
    """Schrijft data naar een PostgreSQL-database gegeven het pad naar een credential bestand.

    Parameters
    ----------
    output_config: dict
        dictionary met extra opties waaronder
    df: pd.DataFrame
        DataFrame met data om weg te schrijven


    Notes
    -----
    In het credentialbestand moet staan:
        - postgresql_user: str
        - postgresql_password: str
        - postgresql_host: str
        - postgresql_port: str
        - database: str

    In de output_config moet staan:

        - schema: str, naam van het schema in de PostgreSQL-database
        - table: str, naam van de tabel in de PostgreSQL-database


    Raises
    ------
    AssertionError
        Als niet alle benodigde variabelen beschikbaar zijn in het .env-bestand.

    """
    table = output_config["table"]
    schema = output_config["schema"]
    if_exists = output_config.get("if_exists", "replace")

    # controleer of alle benodigde variabelen beschikbaar zijn in het .env-bestand
    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]
    assert all(key in output_config for key in keys)

    engine = sqlalchemy.create_engine(
        f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
    )
    df = df.fillna("").copy()
    df.to_sql(
        table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
    )

    # verbinding opruimen
    engine.dispose()


def output_postgis(output_config: dict, gdf: gpd.GeoDataFrame):
    """Schrijft Geografische data naar een PostgreSQL-database gegeven het pad naar een credentialbestand.

    Parameters
    ----------
    output_config: dict
        dictionary met schema en tabel naam.
    gdf: gpd.GeoDataFrame
        GeoDataFrame met data om weg te schrijven


    Notes
    -----
    In het credentialbestand moet staan:

        - postgresql_user: str
        - postgresql_password: str
        - postgresql_host: str
        - postgresql_port: str
        - database: str

    In de output_config moet staan:

        - schema: str, naam van het schema in de PostgreSQL-database
        - table: str, naam van de tabel in de PostgreSQL-database



    Raises
    ------
    AssertionError
        Als niet alle benodigde variabelen beschikbaar zijn in het .env-bestand.

    """
    table = output_config["table"]
    schema = output_config["schema"]
    if_exists = output_config.get("if_exists", "replace")
    # geom_col = output_config.get("geom_col", "geometry")

    # controleer of alle benodigde variabelen beschikbaar zijn in het .env-bestand
    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]
    assert all(key in output_config for key in keys)

    engine = sqlalchemy.create_engine(
        f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
    )
    gdf.fillna("", inplace=True)
    gdf.to_postgis(
        name=table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
    )

    # verbinding opruimen
    engine.dispose()
