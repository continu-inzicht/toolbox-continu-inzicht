"""
DataAdapters voor het lezen van data uit de Continu Inzicht database
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
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"

    Args:\n
    input_config (dict): configuratie opties

    **Opmerking:**\n
    In het `.env`-bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml'-config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht database
    - schema (str): schema van de Continu Inzicht database
    - timedep (int64, optioneel): tijdsafhankelijk 0=nee, 1=ja
    - degradatieid (int64, optioneel): rekening houden met degradatie (nog net geïmplementeerd)

    Returns:\n
    df (DataFrame):\n
    - section_id: int64             : id van het dijkvak
    - measure_id: int64             : id van de maatregel
    - measure: str                  : naam van de maatregel
    - failuremechanismid: int64     : id van het faalmechanisme
    - failuremechanism: str         : naam van het faalmechanisme
    - hydraulicload: float64        : belasting van het tijdreeksitem
    - failureprobability: float64   : faalkans van het tijdreeksitem
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

    YAML voorbeeld:\n
        type: ci_postgresql_fragilitycurves_table
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"
        measureid: 0

    Args:\n
    input_config (dict): configuratie-opties

    **Opmerking:**\n
    In het `.env`-bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht database
    - schema (str): schema van de Continu Inzicht database
    - measureid (int64, optioneel): maatregel id, bij geen waarde wordt geen maatregel
      (measureid=0) gebruikt
    - timedep (int64, optioneel): tijdsafhankelijk 0=nee, 1=ja
    - degradatieid (int64, optioneel): rekening houden met degradatie (nog net geïmplementeerd)

    Returns:\n
    df (DataFrame):\n
    - section_id: int64             : id van het dijkvak
    - measure_id: int64             : id van de maatregel
    - measure: str                  : naam van de maatregel
    - failuremechanismid: int64     : id van het faalmechanisme
    - failuremechanism: str         : naam van het faalmechanisme
    - hydraulicload: float64        : belasting van het tijdreeksitem
    - failureprobability: float64   : faalkans van het tijdreeksitem
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


def input_ci_postgresql_fragilitycurves_methode2_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen fragilitycurves voor alle dijkvakken, faalmechanismes en opgegeven maatregel

    YAML voorbeeld:\n
        type: ci_postgresql_fragilitycurves_table
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"
        measureid: 0

    Args:\n
    input_config (dict): configuratie-opties

    **Opmerking:**\n
    In het `.env`-bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht database
    - schema (str): schema van de Continu Inzicht database
    - measureid (int64, optioneel): maatregel id, bij geen waarde wordt geen maatregel
      (measureid=0) gebruikt
    - timedep (int64, optioneel): tijdsafhankelijk 0=nee, 1=ja
    - degradatieid (int64, optioneel): rekening houden met degradatie (nog net geïmplementeerd)

    Returns:\n
    df (DataFrame):\n
    - section_id: int64             : id van het dijkvak
    - measure_id: int64             : id van de maatregel
    - measure: str                  : naam van de maatregel
    - failuremechanismid: int64     : id van het faalmechanisme
    - failuremechanism: str         : naam van het faalmechanisme
    - hydraulicload: float64        : belasting van het tijdreeksitem
    - failureprobability: float64   : faalkans van het tijdreeksitem
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

    sectionid = 0
    if "sectionid" in input_config:
        sectionid = input_config["sectionid"]

    measureid = 0
    if "measureid" in input_config:
        measureid = input_config["measureid"]

    failuremechanismid = 1  # gecombineerd
    if "failuremechanismid" in input_config:
        failuremechanismid = input_config["failuremechanismid"]

    # 1 "STBI, STPH en GEKB"
    # 2 "GEBU"
    # 3 "Kunstwerken"
    fmgroup = 1
    if "fmgroup" in input_config:
        fmgroup = input_config["fmgroup"]

    if failuremechanismid == 1:
        # gecombineerde faalmechanisme
        query = f"""
            SELECT
                a.hydraulicload,
                a.failureprobability as failureprobability,
                a.failureprobability as failure_probability,
                b.measureid AS measureid,
                b.failuremechanismid as failuremechanismid,
                d.name AS failuremechanism,
                b.sectionid AS section_id
            FROM
                continuinzicht_realtime.fragilitycurves_comb AS a
            INNER JOIN
                continuinzicht_realtime.fragilitycurves_combinations AS b
                ON a.combinationid=b.id
            INNER JOIN
                continuinzicht_realtime.wind_combinations AS c
                ON b.windcombinationid=c.id
            INNER JOIN
                continuinzicht_realtime.failuremechanism AS d
                ON b.failuremechanismid=d.id
                AND c.winddep=d.winddep
            WHERE
                b.fragcurvenumber=1
                --AND b.sectionid={sectionid}
                AND b.measureid={measureid}
                AND a.fmgroup={fmgroup}
            ORDER BY
                a.hydraulicload, a.failureprobability ASC
        """
    elif failuremechanismid == 0 and sectionid == 0:
        # alle faalmechanismes behalve gecombineerd en alle dijkvakken
        query = f"""
            SELECT
                a.hydraulicload,
                a.failureprobability as failureprobability,
                a.failureprobability as failure_probability,
                b.measureid AS measureid,
                b.failuremechanismid as failuremechanismid,
                d.name AS failuremechanism,
                b.sectionid AS section_id
            FROM
                {schema}.fragilitycurves_all AS a
            INNER JOIN
                {schema}.fragilitycurves_combinations AS b
                ON a.combinationid=b.id
            INNER JOIN
                {schema}.wind_combinations AS c
                ON b.windcombinationid=c.id
            INNER JOIN
                {schema}.failuremechanism AS d
                ON b.failuremechanismid=d.id
                AND c.winddep=d.winddep
            INNER JOIN
                {schema}.wind AS w
                ON w.windspeed=c.windspeed AND w.sectormin=c.sectormin AND w.sectorsize=c.sectorsize
            WHERE
                b.fragcurvenumber=1 AND
                b.measureid={measureid}
            ORDER BY
                a.hydraulicload, a.failureprobability ASC
        """
    else:
        # per faalmechanisme
        query = f"""
            SELECT
                a.hydraulicload,
                a.failureprobability as failureprobability,
                a.failureprobability as failure_probability,
                b.measureid AS measureid,
                b.failuremechanismid as failuremechanismid,
                d.name AS failuremechanism,
                b.sectionid AS section_id
            FROM
                {schema}.fragilitycurves_all AS a
            INNER JOIN
                {schema}.fragilitycurves_combinations AS b
                ON a.combinationid=b.id
            INNER JOIN
                {schema}.wind_combinations AS c
                ON b.windcombinationid=c.id
            INNER JOIN
                {schema}.failuremechanism AS d
                ON b.failuremechanismid=d.id
                AND c.winddep=d.winddep
            INNER JOIN
                {schema}.wind AS w
                ON w.windspeed=c.windspeed AND w.sectormin=c.sectormin AND w.sectorsize=c.sectorsize
            WHERE
                b.fragcurvenumber=1
                AND b.sectionid={sectionid}
                AND b.measureid={measureid}
                AND b.failuremechanismid={failuremechanismid}
            ORDER BY
                a.hydraulicload, a.failureprobability ASC
        """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


# overtopping:
def input_ci_postgresql_profiles(input_config: dict) -> pd.DataFrame:
    """Leest profieldata van PostgreSQL-database in de profieltabel en zet namen goed."""

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

    query = f"""
        SELECT
            profiles.sectionid AS section_id,
            profiles.name,
            profiles.crestlevel,
            profiles.orientation,
            profiles.dam,
            profiles.damheight,
            (
                CASE
                    WHEN profiles.qcr_dist='gesloten' OR profiles.qcr_dist='close' OR profiles.qcr_dist='closed' THEN 'closed'
                    WHEN profiles.qcr_dist='open' THEN 'open'
                    WHEN profiles.qcr_dist='fragmentarisch' THEN 'open'
                    ELSE 'closed'
                END
            ) AS qcr,
            wind.windspeed,
            wind.sectormin,
            wind.sectorsize,
            0 AS closing_situation
        FROM {schema}.profiles
        CROSS JOIN {schema}.wind;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_slopes(input_config: dict) -> pd.DataFrame:
    """leest hellingsdata van PostgreSQL-database in de hellingstabel en zet namen goed."""

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

    query = f"""
        SELECT
            profiles.sectionid AS section_id,
            slopes.slopetypeid,
            slopes.x,
            slopes.y,
            slopes.r,
            slopes.damheight
        FROM {schema}.slopes
        INNER JOIN {schema}.profiles ON profiles.id=slopes.profileid;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_slopes_limburg(input_config: dict) -> pd.DataFrame:
    """leest hellingsdata van PostgreSQL-database in de hellingstabel en zet namen goed."""

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

    query = f"""
        SELECT
            profiles.sectionid AS section_id,
            slopes.slopetypeid,
            slopes.x,
            slopes.y,
            slopes.r,
            profiles.damheight
        FROM {schema}.slopes
        INNER JOIN {schema}.profiles ON profiles.id=slopes.profileid;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_bedlevelfetch(input_config: dict) -> pd.DataFrame:
    """Leest bedlevelfetch data van PostgreSQL-database in de bedlevelfetch tabel en zet namen goed."""

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

    query = f"""
        SELECT
            sectionid AS section_id,
            direction,
            bedlevel,
            "fetch"
        FROM {schema}.bedlevelfetch;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_fragilitycurves(input_config: dict) -> pd.DataFrame:
    """Leest fragility curves data van PostgreSQL-database in."""

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

    measureid = 0
    if "measureid" in input_config:
        measureid = input_config["measureid"]

    query = f"""
        SELECT
            sectionid AS section_id,
            failuremechanismid,
            measureid,
            hydraulicload,
            failureprobability AS failure_probability,
            timedep,
            degradatieid
        FROM {schema}.fragilitycurves
        INNER JOIN {schema}.failuremechanism ON failuremechanism.id=fragilitycurves.failuremechanismid
        WHERE measureid={measureid} AND timedep={timedep} AND degradatieid={degradatieid} AND NOT failuremechanism.name = 'COMB'
    """

    if "failuremechanism" in input_config:
        failuremechanism = input_config["failuremechanism"]
        query = f"{query} AND failuremechanism.name='{failuremechanism}'"

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_fragilitycurves_overtopping(input_config: dict) -> pd.DataFrame:
    """Leest fragility curves data van PostgreSQL-database in, zet namen goed en filtert op overtopping."""

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

    failuremechanism = "GEKB"
    if failuremechanism in input_config:
        failuremechanism = input_config["failuremechanism"]

    timedep = 0
    if "timedep" in input_config:
        timedep = input_config["timedep"]

    degradatieid = 0
    if "degradatieid" in input_config:
        degradatieid = input_config["degradatieid"]

    measureid = 0
    if "measureid" in input_config:
        measureid = input_config["measureid"]

    query = f"""
        SELECT
            sectionid AS section_id,
            failuremechanismid,
            measureid,
            hydraulicload,
            failureprobability AS failure_probability,
            timedep,
            degradatieid
        FROM {schema}.fragilitycurves
        INNER JOIN {schema}.failuremechanism ON failuremechanism.id=fragilitycurves.failuremechanismid
        WHERE failuremechanism.name='{failuremechanism}' AND measureid={measureid} AND timedep={timedep} AND degradatieid={degradatieid};
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_fragilitycurves_piping(input_config: dict) -> pd.DataFrame:
    """Leest fragility curves data van PostgreSQL-database in, zet namen goed en filtert op piping."""

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

    failuremechanism = "STPH"
    if failuremechanism in input_config:
        failuremechanism = input_config["failuremechanism"]

    timedep = 0
    if "timedep" in input_config:
        timedep = input_config["timedep"]

    degradatieid = 0
    if "degradatieid" in input_config:
        degradatieid = input_config["degradatieid"]

    measureid = 0
    if "measureid" in input_config:
        measureid = input_config["measureid"]

    query = f"""
        SELECT
            sectionid AS section_id,
            failuremechanismid,
            measureid,
            hydraulicload,
            failureprobability AS failure_probability,
            timedep,
            degradatieid
        FROM {schema}.fragilitycurves
        INNER JOIN {schema}.failuremechanism ON failuremechanism.id=fragilitycurves.failuremechanismid
        WHERE failuremechanism.name='{failuremechanism}' AND measureid={measureid} AND timedep={timedep} AND degradatieid={degradatieid};
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_fragilitycurves_stability(input_config: dict) -> pd.DataFrame:
    """Leest fragility curves data van PostgreSQL-database in, zet namen goed en filtert op piping."""
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
            "hydraulicload": "hydraulicload",
        },
        inplace=True,
    )
    return df


def input_ci_postgresql_probablistic_piping(input_config: dict) -> pd.DataFrame:
    """Leest probablistic data van PostgreSQL-database in de probablistic piping tabel en hernoemt de kolomen."""
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
    input_config["table"] = "probabilisticpiping"
    df = input_postgresql_database(input_config)
    df.rename(columns=db_to_continu_inzicht, inplace=True)
    return df
