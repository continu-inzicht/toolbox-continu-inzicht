"""
Data adapters voor het schrijven naar de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def output_ci_postgresql_to_scenarios(output_config: dict, df: pd.DataFrame) -> None:
    """
    Schrijft what-if scenarios naar Continu Inzicht database (tabel: scenarios).

    Yaml example:\n
        type: ci_postgresql_to_scenarios
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - id: int64                 : id van het scenario
        - name: str                 : naam van het scenario
        - min: float64              : eerste datum/tijd van het scenario
        - max: float64              : laatste datum/tijd van het scenario
        - step: float64             : stapgrootte tijd in het scenario

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
        if "id" in df and "name" in df and "min" in df and "max" in df and "step" in df:
            query = []

            query.append(f"TRUNCATE {schema}.scenarios;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.scenarios(id, name, min, max, step) VALUES ({str(row['id'])}, '{row['name']}', {str(row['min'])}, {str(row['max'])}, {str(row['step'])});"
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


def output_ci_postgresql_to_load(output_config: dict, df: pd.DataFrame):
    """
    Schrijft what-if belastingen naar Continu Inzicht database (tabel: waterlevels).

    Yaml example:\n
        type: ci_postgresql_to_load
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - measuringstation_id: int64        : id van het meetstation
        - scenario_id: int64                : id van het scenario
        - datetime: float64                 : eerste datum/tijd van het scenario
        - value: float64                    : waarde van de belasting (bijvoorbeeld een waterstand)
        - parameter: int64                  : id van de parameter

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
            "measuringstationid" in df
            and "scenarioid" in df
            and "datetime" in df
            and "value" in df
            and "parameter" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.waterlevels;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.waterlevels(measuringstationid, scenarioid, datetime, value, parameter) VALUES ({str(row['measuringstationid'])}, {str(row['scenarioid'])}, {str(row['datetime'])}, {str(row['value'])}, {str(row['parameter'])});"
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
