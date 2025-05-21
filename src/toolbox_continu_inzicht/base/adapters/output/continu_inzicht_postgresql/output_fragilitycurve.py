import pandas as pd
import sqlalchemy

from toolbox_continu_inzicht.base.adapters.output.postgresql import (
    output_postgresql_database,
)


def output_ci_postgresql_to_fragilitycurves(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voorgedefinieerde fragilitycurves naar Continu Inzicht database (tabel: fragilitycurves).

    Yaml example:\n
        type: ci_postgresql_to_fragilitycurves
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - sectionid: int64              : id van een sectie
        - failuremechanismid: int64     : id van een scenario
        - measureid: int64              : id van een maatregel
        - hydraulicload: float64        : waarde van een hydraulische belasting (bijv. waterstand)
        - failureprobability: float64   : conditionele faalkans
        - timedep int64                 : tijdsafhankelijk?
        - degradatieid int64            : rekening houden met degratie

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
            "sectionid" in df
            and "failuremechanismid" in df
            and "measureid" in df
            and "hydraulicload" in df
            and "failureprobability" in df
            and "timedep" in df
            and "degradatieid" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.fragilitycurves;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.fragilitycurves(sectionid, failuremechanismid, measureid, hydraulicload, failureprobability, timedep, degradatieid) VALUES ({str(row['sectionid'])}, {str(row['failuremechanismid'])}, {str(row['measureid'])}, {str(row['hydraulicload'])}, {str(row['failureprobability'])}, {str(row['timedep'])}, {str(row['degradatieid'])});"
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


def output_ci_postgresql_fragilitycurves_table(
    output_config: dict, df: pd.DataFrame
) -> None:
    df = df.copy()
    """Schrijft fragility curve data naar een postgresql database in de profile tabel & zet namen goed"""
    table = "fragilitycurves"
    if "table" in output_config:
        table = output_config["table"]

    schema = output_config["schema"]

    # hernoemen van de kolom section_id naar profile_id
    df.rename(
        columns={
            "section_id": "sectionid",
            "failure_probability": "failureprobability",
            "hydraulicload": "hydraulicload",
        },
        inplace=True,
    )
    columns = {"failuremechanismid": 0, "measureid": 0, "timedep": 0, "degradatieid": 0}
    for col in columns:
        if col not in df.columns:
            df[col] = columns[col]

    # check all required variables are available from the .env file
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

    # eerst bestaande curves verwijderen

    # Haal de unieke combinaties van kolommen
    unique_df = df[["sectionid", "failuremechanismid", "measureid"]].drop_duplicates()

    if len(unique_df) > 0:
        # Zet de unieke waarden om naar een lijst van tuples
        unique_values = [tuple(x) for x in unique_df.to_numpy()]

        # Maak een string van de unieke waarden voor de SQL-query
        values_str = ", ".join([f"({v[0]}, {v[1]}, {v[2]})" for v in unique_values])

        with engine.connect() as connection:
            connection.execute(
                sqlalchemy.text(f"""
                    DELETE FROM {schema}.{table}
                    WHERE (sectionid, failuremechanismid, measureid) IN ({values_str});
                    """)
            )
            connection.commit()  # commit the transaction

        # nu de nieuwe fragility curves toevoegen in de database
        df.to_sql(
            table,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
        )

    else:
        raise UserWarning("Geen gegevens om op te slaan.")

    # verbinding opruimen
    engine.dispose()


def output_ci_postgresql_profiles(output_config: dict, df: pd.DataFrame) -> None:
    """Schrijft profile data naar een postgresql database in de profile tabel & zet namen goed."""
    output_config["table"] = "profiles"
    # hernoemen van de kolom section_id naar profile_id
    df = df.copy()
    df.rename(columns={"section_id": "profileid"}, inplace=True)
    output_postgresql_database(output_config, df)


def output_ci_postgresql_slopes(output_config: dict, df: pd.DataFrame) -> None:
    """Schrijft slopes data naar een postgresql database in de slopes tabel  & zet namen goed."""
    output_config["table"] = "slopes"
    # hernoemen van de kolom section_id naar sectionid
    df = df.copy()
    df.rename(columns={"section_id": "sectionid"}, inplace=True)
    output_postgresql_database(output_config, df)


def output_ci_postgresql_bedlevelfetch(output_config: dict, df: pd.DataFrame) -> None:
    """Schrijft slopes data naar een postgresql database in de slopes tabel  & zet namen goed."""
    output_config["table"] = "bedlevelfetch"
    # hernoemen van de kolom section_id naar sectionid
    df = df.copy()
    df.rename(columns={"section_id": "sectionid"}, inplace=True)
    output_postgresql_database(output_config, df)


def output_ci_postgresql_probablistic_piping(
    output_config: dict, df: pd.DataFrame
) -> None:
    """Schrijft probablistic data naar van postgresql database in de probablistic piping tabel & hernoemt de kollomen."""
    continu_inzicht_to_db = {
        "section_id": "sectionid",
        "scenario_id": "scenarioid",
        "mechanism": "mechanism",
        "Naam": "naam",
        "Waarde": "waarde",
        "Kansverdeling": "kansverdeling",
        "Verschuiving": "verschuiving",
        "Mean": "mean",
        "Spreiding": "spreiding",
        "Spreidingstype": "spreidingstype",
        "Afknot_links": "afknotlinks",
        "Afknot_rechts": "afknotrechts",
        "Min": "min",
        "Step": "step",
        "Max": "max",
        "StDev": "stdev",
    }
    df = df.copy()
    output_config["table"] = "probabilisticpiping"
    df.rename(columns=continu_inzicht_to_db, inplace=True)
    output_postgresql_database(output_config, df)
