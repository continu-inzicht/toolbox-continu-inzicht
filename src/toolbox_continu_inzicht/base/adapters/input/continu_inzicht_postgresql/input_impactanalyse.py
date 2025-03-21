"""
Data adapters voor het lezen van data uit de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def input_ci_postgresql_impactanalyse_variants(input_config: dict) -> pd.DataFrame:
    """
    Ophalen varianten uit de Continu Inzicht database.

    Yaml example:\n
        type: ci_postgresql_zorgplicht_variants
        database: "continuinzicht"
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

    Returns:\n
    df (DataFrame):\n
    - id: int64             : id van de dijkvak
    - name: str             : naam van de dijkvak
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

    query = f"""
        SELECT
            id AS id,
            name AS name,
            hydraulicload_location_id AS hydraulicload_location_id,
            overleefde_belasting AS overleefde_belasting
        FROM {schema}.sections;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_impactanalyse_sections(input_config: dict) -> pd.DataFrame:
    """
    Ophalen section data uit de Continu Inzicht database.

    Yaml example:\n
        type: ci_postgresql_impactanalyse_sections
        database: "continuinzicht"
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

    Returns:\n
    df (DataFrame):\n
    - id: int64             : id van de dijkvak
    - name: str             : naam van de dijkvak
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

    query = f"""
        SELECT
            id AS id,
            name AS name,
            hydraulicload_location_id AS hydraulicload_location_id,
            overleefde_belasting AS overleefde_belasting
        FROM {schema}.sections;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_impactanalyse_calculate_list(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen varianten uit de Continu Inzicht database.

    Yaml example:\n
        type: ci_postgresql_impactanalyse_variants
        database: "continuinzicht"
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

    Returns:\n
    df (DataFrame):\n
    - id: int64             : id van de dijkvak
    - name: str             : naam van de dijkvak
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

    query = f"""
        SELECT
            variant.id AS variant_id,
            statistic.id AS statistic_id,
            fragilitycurves_data IS NOT NULL AS has_fragilitycurves,
            fragilitycurves_data_base IS NOT NULL AS has_fragilitycurves_base,
            fragilitycurves_data_base.failuremechanism_id AS failuremechanism_id,
            expertjudgement.id AS expertjudgement_id,
            expertjudgement.fragilitycurves_id AS fragilitycurves_id,
            expertjudgement.fragilitycurves_base_id AS fragilitycurves_base_id,
            section.id AS section_id,
            section.hydraulicload_location_id AS hydraulicload_location_id,
            intergrate IS NOT NULL AS has_intergrate,
            intergrate_data IS NOT NULL AS has_intergrate_data,
            intergrate.id AS intergrate_id,
            intergrate.load_limit AS load_limit,
            intergrate.probability_contribution_reductionfactor AS probability_contribution_reductionfactor
        FROM {schema}.expertjudgements AS expertjudgement
        INNER JOIN {schema}.sections AS section ON 1=1
        INNER JOIN {schema}.variants AS variant ON variant.id=expertjudgement.variant_id
        INNER JOIN {schema}.statistics AS statistic ON statistic.id=variant.statistics_id
        LEFT JOIN (
                SELECT DISTINCT
                    fragilitycurves_data_base.fragilitycurves_id,
                    fragilitycurves_data_base.section_id,
                    fragilitycurves_data_base.failuremechanism_id
                FROM {schema}.fragilitycurves_data AS fragilitycurves_data_base
        ) AS fragilitycurves_data_base ON
            fragilitycurves_data_base.section_id=section.id AND
            fragilitycurves_data_base.fragilitycurves_id=expertjudgement.fragilitycurves_base_id
        LEFT JOIN (
                SELECT DISTINCT
                    fragilitycurves_data.fragilitycurves_id,
                    fragilitycurves_data.section_id,
                    fragilitycurves_data.failuremechanism_id
                FROM {schema}.fragilitycurves_data AS fragilitycurves_data
        ) AS fragilitycurves_data ON
            fragilitycurves_data.section_id=section.id AND
            fragilitycurves_data.fragilitycurves_id=expertjudgement.fragilitycurves_id
        LEFT JOIN {schema}.fragilitycurves_intergrate AS intergrate ON
            intergrate.expertjudgement_id=expertjudgement.id AND
            intergrate.failuremechanism_id=fragilitycurves_data_base.failuremechanism_id AND
            intergrate.section_id=section.id
        LEFT JOIN (
                SELECT DISTINCT
                    intergrate_data.intergrate_id
                FROM continuinzicht_demo_zorgplicht.fragilitycurves_intergrate_data AS intergrate_data
        ) AS intergrate_data ON
            intergrate_data.intergrate_id=intergrate.id
        ORDER BY variant.id,failuremechanism_id, expertjudgement.id, section.id;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_impactanalyse_fragilitycurves_data(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen fragilitycurves uit de Continu Inzicht database.

    Yaml example:\n
        type: ci_postgresql_impactanalyse_fragilitycurves_data
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"
        section_id: 1
        failuremechanism_id: 1
        expertjudgement_id: 1

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

    Returns:\n
    df (DataFrame):\n
    - id: int64             : id van de dijkvak
    - name: str             : naam van de dijkvak
    """

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
        "section_id",
        "failuremechanism_id",
        "fragilitycurves_id",
    ]

    assert all(key in input_config for key in keys)

    # maak verbinding object
    engine = sqlalchemy.create_engine(
        f"postgresql://{input_config['postgresql_user']}:{input_config['postgresql_password']}@{input_config['postgresql_host']}:{int(input_config['postgresql_port'])}/{input_config['database']}"
    )

    schema = input_config["schema"]
    section_id = input_config["section_id"]
    failuremechanism_id = input_config["failuremechanism_id"]
    fragilitycurves_id = input_config["fragilitycurves_id"]

    query = f"""
        SELECT
            hydraulicload,
            failure_probability
        FROM {schema}.fragilitycurves_data
        WHERE
            fragilitycurves_id={fragilitycurves_id} AND
            section_id={section_id} AND
            failuremechanism_id={failuremechanism_id}
        ORDER BY hydraulicload;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_impactanalyse_statistics_data(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen fragilitycurves uit de Continu Inzicht database.

    Yaml example:\n
        type: ci_postgresql_impactanalyse_statistics_data
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"
        statistics_id: 1
        hydraulicload_location_id: 1

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

    Returns:\n
    df (DataFrame):\n
    - id: int64             : id van de dijkvak
    - name: str             : naam van de dijkvak
    """

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
        "statistics_id",
        "hydraulicload_location_id",
    ]

    assert all(key in input_config for key in keys)

    # maak verbinding object
    engine = sqlalchemy.create_engine(
        f"postgresql://{input_config['postgresql_user']}:{input_config['postgresql_password']}@{input_config['postgresql_host']}:{int(input_config['postgresql_port'])}/{input_config['database']}"
    )

    schema = input_config["schema"]
    statistics_id = input_config["statistics_id"]
    hydraulicload_location_id = input_config["hydraulicload_location_id"]

    query = f"""
        SELECT
            probability_exceedance,
            hydraulicload
        FROM {schema}.statistics_data
        WHERE
            statistics_id={statistics_id} AND
            hydraulicload_location_id={hydraulicload_location_id}
        ORDER BY hydraulicload;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
