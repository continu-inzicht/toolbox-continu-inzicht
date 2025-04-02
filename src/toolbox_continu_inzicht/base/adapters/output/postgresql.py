import pandas as pd
import sqlalchemy


def output_postgresql_database(output_config: dict, df: pd.DataFrame):
    """Schrijft data naar een PostgreSQL-database gegeven het pad naar een credentialbestand.

    Parametes:
    ----------
    df: pd.Dataframe
        DataFrame met data om weg te schrijven
    opties: dict
            dictionary met extra opties waaronder:
                schema: str
                        naam van het schema in de PostgreSQL-database
                table: str
                    naam van de tabel in de PostgreSQL-database


    Notes:
    ------
    In het credentialbestand moet staan:
    postgresql_user: str
    postgresql_password: str
    postgresql_host: str
    postgresql_port: str
    database: str


    Returns:
    --------
    None

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
    df.fillna("", inplace=True)
    df.to_sql(
        table,
        con=engine,
        schema=schema,
        if_exists=if_exists,
        index=False,
    )

    # verbinding opruimen
    engine.dispose()
