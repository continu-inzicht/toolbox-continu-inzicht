"""
Data adapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy
from toolbox_continu_inzicht.base.adapters.input.postgresql import (
    input_postgresql_database,
)


def input_ci_postgresql_measure_fragilitycurves_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen fragilitycurves voor alle dijkvakken, faalmechanismes en maatregelen

    Yaml example:\n
        type: ci_postgresql_measure_fragilitycurves_table
        database: "geoserver"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    - timedep (int64, optioneel): tijdsafhankelijk 0=nee, 1=ja
    - degradatieid (int64, optioneel): rekening houden met degradatie (nog net geimplementeerd)

    Returns:\n
    df (DataFrame):\n
    - section_id: int64             : id van de dijkvak
    - measure_id: int64             : id van de maatregel
    - measure: str                  : naam van de maatregel
    - failuremechanismid: int64     : id van het faalmechanisme
    - failuremechanism: str         : naam van het faalmechanisme
    - hydraulicload: float64        : belasting van de tijdreeksitem
    - failureprobability: float64   : faalkans van de tijdreeksitem
    - successrate: float64          : kans op succes van de maatregel
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

    schema = input_config["schema"]

    timedep = 0
    if "timedep" in input_config:
        timedep = input_config["timedep"]

    degradatieid = 0
    if "degradatieid" in input_config:
        degradatieid = input_config["degradatieid"]

    query = f"""
        SELECT
            fragilitycurves.sectionid AS section_id,
			measures.id AS measure_id,
			measures.name AS measure,
			failuremechanism.id AS failuremechanismid,
            failuremechanism.name AS failuremechanism,
            fragilitycurves.hydraulicload AS hydraulicload,
            fragilitycurves.failureprobability AS failureprobability,
			expertjudgement.successrate AS successrate
        FROM {schema}.fragilitycurves
        INNER JOIN {schema}.failuremechanism ON
			failuremechanism.id=fragilitycurves.failuremechanismid
		INNER JOIN {schema}.expertjudgement ON
			expertjudgement.sectionid=fragilitycurves.sectionid AND
			expertjudgement.measureid=fragilitycurves.measureid
        INNER JOIN {schema}.measures ON
            measures.id=fragilitycurves.measureid
        WHERE fragilitycurves.timedep={timedep} AND fragilitycurves.degradatieid={degradatieid}
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_fragilitycurves_table(input_config: dict) -> pd.DataFrame:
    """
    Ophalen fragilitycurves voor alle dijkvakken, faalmechanismes en opgegeven maatregel

    Yaml example:\n
        type: ci_postgresql_fragilitycurves_table
        database: "geoserver"
        schema: "continuinzicht_demo_realtime"
        measureid: 0

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    - measureid (int64, optioneel): maatregel id, bij geen waarde wordt geen maatregel
      (measureid=0) gebruikt
    - timedep (int64, optioneel): tijdsafhankelijk 0=nee, 1=ja
    - degradatieid (int64, optioneel): rekening houden met degradatie (nog net geimplementeerd)

    Returns:\n
    df (DataFrame):\n
    - section_id: int64             : id van de dijkvak
    - measure_id: int64             : id van de maatregel
    - measure: str                  : naam van de maatregel
    - failuremechanismid: int64     : id van het faalmechanisme
    - failuremechanism: str         : naam van het faalmechanisme
    - hydraulicload: float64        : belasting van de tijdreeksitem
    - failureprobability: float64   : faalkans van de tijdreeksitem
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

    schema = input_config["schema"]

    measureid = 0
    if "measureid" in input_config:
        measureid = input_config["measureid"]

    timedep = 0
    if "timedep" in input_config:
        timedep = input_config["timedep"]

    degradatieid = 0
    if "degradatieid" in input_config:
        degradatieid = input_config["degradatieid"]

    query = f"""
        SELECT
            sectionid AS section_id,
            measures.id AS measure_id,
			measures.name AS measure,
			failuremechanism.id AS failuremechanismid,
            failuremechanism.name AS failuremechanism,
            hydraulicload AS hydraulicload,
            failureprobability AS failureprobability
        FROM {schema}.fragilitycurves
        INNER JOIN {schema}.failuremechanism ON
            failuremechanism.id=fragilitycurves.failuremechanismid
        INNER JOIN {schema}.measures ON
            measures.id=fragilitycurves.measureid
        WHERE measureid={measureid} AND timedep={timedep} AND degradatieid={degradatieid}
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


# overtopping:
def input_ci_postgresql_profiles(input_config: dict) -> pd.DataFrame:
    """leest profile data van postgresql database in de profile tabel & zet namen goed."""
    input_config["table"] = "profiles"
    # hernoemen van de kolom section_id naar profile_id

    df = input_postgresql_database(input_config)
    df.rename(columns={"profileid": "section_id"}, inplace=True)
    return df


def input_ci_postgresql_slopes(input_config: dict) -> pd.DataFrame:
    """leest slopes data van postgresql database in de slopes tabel  & zet namen goed."""
    input_config["table"] = "slopes"
    # hernoemen van de kolom section_id naar sectionid

    df = input_postgresql_database(input_config)
    df.rename(columns={"sectionid": "section_id"}, inplace=True)
    return df


def input_ci_postgresql_bedlevelfetch(input_config: dict) -> pd.DataFrame:
    """leest bedlevelfetch data van postgresql database in de bedlevelfetch tabel  & zet namen goed."""
    input_config["table"] = "bedlevelfetch"
    # hernoemen van de kolom section_id naar sectionid
    df = input_postgresql_database(input_config)
    df.rename(columns={"sectionid": "section_id"}, inplace=True)
    return df


def input_ci_postgresql_fragilitycurves_overtopping(input_config: dict) -> pd.DataFrame:
    """leest fragility curves data van postgresql database in , zet namen goed en filterd op pipping."""
    overtopping_id = 2
    if overtopping_id in input_config:
        overtopping_id = input_config["overtopping_id"]
    input_config["query"] = (
        f"SELECT * FROM {input_config['schema']}.fragilitycurves WHERE failuremechanismid={overtopping_id}"
    )
    # hernoemen van de kolom section_id naar sectionid
    df = input_postgresql_database(input_config)
    df.rename(
        columns={
            "sectionid": "section_id",
            "failureprobability": "failure_probability",
            "hydraulicload": "waterlevels",
        },
        inplace=True,
    )
    return df


def input_ci_postgresql_fragilitycurves_pipping(input_config: dict) -> pd.DataFrame:
    """leest fragility curves data van postgresql database in , zet namen goed en filterd op pipping."""
    input_config["table"] = "fragilitycurves"
    piping_id = 3
    if piping_id in input_config:
        piping_id = input_config["piping_id"]
    input_config["query"] = (
        f"SELECT * FROM {input_config['schema']}.fragilitycurves WHERE failuremechanismid={piping_id}"
    )
    # hernoemen van de kolom section_id naar sectionid
    df = input_postgresql_database(input_config)
    df.rename(
        columns={
            "sectionid": "section_id",
            "failureprobability": "failure_probability",
            "hydraulicload": "waterlevels",
        },
        inplace=True,
    )
    return df


def input_ci_postgresql_fragilitycurves_stability(input_config: dict) -> pd.DataFrame:
    """leest fragility curves data van postgresql database in , zet namen goed en filterd op pipping."""
    input_config["table"] = "fragilitycurves"
    stability_id = 4
    if stability_id in input_config:
        stability_id = input_config["stability_id"]
    input_config["query"] = (
        f"SELECT * FROM {input_config['schema']}.fragilitycurves WHERE failuremechanismid={stability_id}"
    )
    # hernoemen van de kolom section_id naar sectionid
    df = input_postgresql_database(input_config)
    df.rename(
        columns={
            "sectionid": "section_id",
            "failureprobability": "failure_probability",
            "hydraulicload": "waterlevels",
        },
        inplace=True,
    )
    return df


def input_ci_postgresql_probablistic_pipping(input_config: dict) -> pd.DataFrame:
    """leest probablistic data van postgresql database in de probablistic piping tabel en hernoemt de kollomen."""
    db_to_continu_inzicht = {
        "sectionid": "section_id",
        "scenarioid": "scenario_id",
        "mechanism": "mechanism",
        "naam": "Naam",
        "waarde": "Waarde",
        "kansverdeling": "Kansverdeling",
        "verschuiving": "Verschuiving",
        "mean": "Mean",
        "spreiding": "Spreiding",
        "spreidingstype": "Spreidingstype",
        "afknotlinks": "Afknot_links",
        "afknotrechts": "Afknot_rechts",
        "min": "Min",
        "step": "Step",
        "max": "Max",
        "stdev": "StDev",
    }
    input_config["table"] = "probabilisticpipping"
    df = input_postgresql_database(input_config)
    df.rename(columns=db_to_continu_inzicht, inplace=True)
    return df
