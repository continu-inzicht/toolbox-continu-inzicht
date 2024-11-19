import pandas as pd
import sqlalchemy


def output_postgresql_database(output_config: dict, df: pd.DataFrame):
    """Schrijft data naar een postgresql database gegeven het pad naar een credential bestand.

    Parametes:
    ----------
    df: pd.Dataframe
        dataframe met data om weg te schrijven
    opties: dict
            dictionary met extra opties waar onder:
                schema: str
                        naam van het schema in de postgresql database
                table: str
                    naam van de tabel in de postgresql database


    Notes:
    ------
    In het credential bestand moet staan:
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

    # check all required variables are availible from the .env file
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

    df.to_sql(
        table,
        con=engine,
        schema=schema,
        if_exists="replace",  # append
        index=False,
    )

    # verbinding opruimen
    engine.dispose()
