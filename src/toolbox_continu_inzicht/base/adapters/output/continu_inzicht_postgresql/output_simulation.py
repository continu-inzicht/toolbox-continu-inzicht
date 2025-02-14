"""
Data adapters voor het schrijven naar de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy
from toolbox_continu_inzicht.utils.datetime_functions import epoch_from_datetime


def output_ci_postgresql_to_simulation(output_config: dict, df: pd.DataFrame) -> None:
    """
    Schrijft simulatie data (gekozen scenario en tijdstap) naar Continu Inzicht database (tabel: simulation).

    Yaml example:\n
        type: ci_postgresql_to_simulation
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - id: int64                 : id van de what-if simulatie
        - scenarioid: int64         : id van het scenario
        - datetime: float64         : datum/tijd van berekening
        - starttime: float64        : eerste datum/tijd van het scenario
        - endtime: float64          : laatste datum/tijd van het scenario
        - timestep: float64         : stapgrootte tijd in het scenario
        - active: bool              : berekening is actief of niet


    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    - unit_conversion_factor (optioneel, float): conversiefactor om waarde om te zetten naar meters
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
        if (
            "id" in df
            and "scenarioid" in df
            and "datetime" in df
            and "starttime" in df
            and "endtime" in df
            and "timestep" in df
            and "active" in df
        ):
            df["datetime"] = df["datetime"].apply(epoch_from_datetime)
            df["starttime"] = df["starttime"].apply(epoch_from_datetime)
            df["endtime"] = df["endtime"].apply(epoch_from_datetime)
            query = []

            for _, row in df.iterrows():
                query.append(
                    f"UPDATE {schema}.simulation SET id={str(row['id'])}, scenarioid={str(row['scenarioid'])}, datetime={str(row['datetime'])}, starttime={str(row['starttime'])}, endtime={str(row['endtime'])}, timestep={str(row['timestep'])}, active={str(row['active'])} WHERE id={str(row['id'])};"
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()  # commit the transaction

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")
