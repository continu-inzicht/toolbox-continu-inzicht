"""
Data adapters voor het schrijven naar de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy

from datetime import datetime, timezone
from toolbox_continu_inzicht.utils.datetime_functions import epoch_from_datetime


def output_ci_postgresql_to_calculation_start(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Update de calculatietijd in de Continu Inzicht database tabel moments

    Yaml example:\n
        type: ci_postgresql_to_calculation_start
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - moment_id: int64
        - calc_time: datetime64[ns, UTC]

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    """

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]

    assert all(key in output_config for key in keys)

    schema = output_config["schema"]

    if not df.empty:
        if "calc_time" in df and "moment_id" in df:
            df["calctime"] = df["calc_time"].apply(epoch_from_datetime)
            query = []

            for _, row in df.iterrows():
                query.append(
                    f"UPDATE {schema}.moments SET calctime={str(row['calctime'])} WHERE id={str(row['moment_id'])};"
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()  # commit the transaction

            # Verwijder eventueel berekening data die bij een afgebroken berekening if blijven staan
            query = []
            query.append(f"DELETE FROM {schema}.data   WHERE calculating=true;")
            query.append(f"DELETE FROM {schema}.states WHERE calculating=true;")
            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()


def output_ci_postgresql_to_calculation_end(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
     - Update de datetime in de Continu Inzicht database tabel moments
     - Verwijderd alle data waar calculating op False staat
     - Zet alle data waar calculating op True staat naar False

    Yaml example:\n
         type: ci_postgresql_to_calculation_end
         database: "continuinzicht"
         schema: "continuinzicht_demo_realtime"

     Args:\n
         * output_config (dict): configuratie opties
         * df (DataFrame): geen data nodig

     **Opmerking:**\n
     In de `.env` environment bestand moeten de volgende parameters staan:\n
     - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
     - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
     - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
     - postgresql_port (str): poort van de Continu Inzicht databaseserver

     In de 'yaml' config moeten de volgende parameters staan:\n
     - database (str): database van de Continu Inzicht
     - schema (str): schema van de Continu Inzicht
    """

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]

    assert all(key in output_config for key in keys)

    schema = output_config["schema"]

    # maak verbinding object
    engine = sqlalchemy.create_engine(
        f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
    )

    query = f"UPDATE {schema}.moments SET datetime=calctime;"
    with engine.connect() as connection:
        connection.execute(sqlalchemy.text(str(query)))
        connection.commit()

    # Verwijder productie data
    query = []
    query.append(f"DELETE FROM {schema}.data   WHERE calculating=false;")
    query.append(f"DELETE FROM {schema}.states WHERE calculating=false;")
    query = " ".join(query)

    with engine.connect() as connection:
        connection.execute(sqlalchemy.text(str(query)))
        connection.commit()

    # Update berekening data naar productie data
    query = []
    query.append(
        f"UPDATE {schema}.data   SET calculating=false WHERE calculating=true;"
    )
    query.append(
        f"UPDATE {schema}.states SET calculating=false WHERE calculating=true;"
    )
    query = " ".join(query)

    with engine.connect() as connection:
        connection.execute(sqlalchemy.text(str(query)))
        connection.commit()

    # Update modified date van de attribute tabel en verwijder de aangegeven wijzigingen
    # vanuit de user interface
    date_time_now = datetime.now(timezone.utc)
    modified_date = datetime(
        date_time_now.year,
        date_time_now.month,
        date_time_now.day,
        date_time_now.hour,
        0,
        0,
    ).replace(tzinfo=date_time_now.tzinfo)

    modified_date_epoch = epoch_from_datetime(modified_date)

    query = []
    query.append(
        f"INSERT INTO {schema}.attributes(name, value) VALUES('modified_date',{modified_date_epoch}) ON CONFLICT(name) DO UPDATE SET value={modified_date_epoch};"
    )
    query.append(f"DELETE FROM {schema}.attributes WHERE NOT name='modified_date';")
    query = " ".join(query)

    with engine.connect() as connection:
        connection.execute(sqlalchemy.text(str(query)))
        connection.commit()

    # verbinding opruimen
    engine.dispose()
