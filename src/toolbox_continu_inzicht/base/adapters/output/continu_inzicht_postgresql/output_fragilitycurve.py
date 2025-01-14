import pandas as pd
import sqlalchemy

from toolbox_continu_inzicht.base.adapters.output.postgresql import (
    output_postgresql_database,
)


def output_ci_postgresql_fragilitycurves_table(
    output_config: dict, df: pd.DataFrame
) -> None:
    df = df.copy()
    """Schrijft fragility curve data naar een postgresql database in de profile tabel & zet namen goed"""
    table = "fragilitycurves"
    schema = output_config["schema"]
    # hernoemen van de kolom section_id naar profile_id
    df.rename(
        columns={
            "section_id": "sectionid",
            "failure_probability": "failureprobability",
            "waterlevels": "hydraulicload",
        },
        inplace=True,
    )
    columns = {"failuremechanismid": 0, "measureid": 0, "timedep": 0, "degradatieid": 0}
    for col in columns:
        if col not in df.columns:
            df[col] = columns[col]

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
        if_exists="append",
        index=False,
    )

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


def output_ci_postgresql_probablistic_pipping(
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
    output_config["table"] = "probabilisticpipping"
    df.rename(columns=continu_inzicht_to_db, inplace=True)
    output_postgresql_database(output_config, df)
