"""
Data adapters voor het schrijven naar de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def output_ci_postgresql_dikesystem(output_config: dict, df: pd.DataFrame) -> None:
    """
    Schrijft waterkeringsystemen naar de Continu Inzicht database (tabel: dikesystems).

    Yaml example:\n
        type: ci_postgresql_dikesystem
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - id: int64                 : id van het waterkeringsysteem
        - nr: str                   : nummer van het waterkeringsystemen
        - name: str                 : naam van het waterkeringsystemen
        - geometry: geom            : geometrie (ligging) van het waterkeringsystemen (let op projectie altijd EPSG4326!)

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
        if "id" in df and "nr" in df and "name" in df and "geometry":
            query = []

            query.append(f"TRUNCATE {schema}.dikesystems;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.dikesystems(id, nr, name, geometry) VALUES ({str(row['id'])}, '{str(row['nr'])}', '{str(row['name'])}', '{str(row['geometry'])}');"
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
