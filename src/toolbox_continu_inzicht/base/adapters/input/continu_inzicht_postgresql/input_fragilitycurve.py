"""
Data adapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


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
