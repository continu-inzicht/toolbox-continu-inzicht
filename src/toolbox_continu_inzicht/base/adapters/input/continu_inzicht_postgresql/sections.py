import pandas as pd
import sqlalchemy


def input_ci_postgresql_from_sections(input_config: dict) -> pd.DataFrame:
    """
    Ophalen sections uit een continu database.

    Args:
    ----------
    input_config (dict):

    Opmerking:
    ------
    In de `.env` environment bestand moeten de volgende parameters staan:
    postgresql_user (str):
    postgresql_password (str):
    postgresql_host (str):
    postgresql_port (str):

    In de 'yaml' config moeten de volgende parameters staan:
    database (str):
    schema (str):

    Returns:
    --------
    pd.Dataframe

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
            name AS name
        FROM {schema}.sections;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_from_sectionfractions(input_config: dict) -> pd.DataFrame:
    """
    Ophalen sections fractions uit een continu database.

    Args:
    ----------
    input_config (dict):

    Opmerking:
    ------
    In de `.env` environment bestand moeten de volgende parameters staan:
    postgresql_user (str):
    postgresql_password (str):
    postgresql_host (str):
    postgresql_port (str):

    In de 'yaml' config moeten de volgende parameters staan:
    database (str):
    schema (str):

    Returns:
    --------
    pd.Dataframe

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
            sectionid AS id, 
            idup, 
            iddown, 
            fractionup, 
            fractiondown
        FROM {schema}.sectionfractions;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_load_from_data_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen belasting per dijkvak per moment

    Args:
    ----------
    input_config (dict):

    Opmerking:
    ------
    In de `.env` environment bestand moeten de volgende parameters staan:
    postgresql_user (str):
    postgresql_password (str):
    postgresql_host (str):
    postgresql_port (str):

    In de 'yaml' config moeten de volgende parameters staan:
    database (str):
    schema (str):

    Returns:
    --------
    pd.Dataframe

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
            data.objectid AS section_id, 
            parameter.id AS parameter_id,
            parameter.unit As unit,
            TO_TIMESTAMP(data.datetime/1000) AS date_time, 	
            data.value AS value,
            CASE WHEN parameter.measured THEN 'meting' ELSE 'verwacht' END AS value_type
        FROM {schema}.data
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=data.parameterid
        WHERE data.objecttype='section' AND data.calculating=true;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    # Datum kolom moet een object zijn en niet een 'datetime64[ns, UTC]'
    # df["date_time"] = df["date_time"].astype(object)

    return df


def input_ci_postgresql_section_failure_probability_from_data_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen faalkansen per dijkvak per moment

    Args:
    ----------
    input_config (dict):

    Opmerking:
    ------
    In de `.env` environment bestand moeten de volgende parameters staan:
    postgresql_user (str):
    postgresql_password (str):
    postgresql_host (str):
    postgresql_port (str):

    In de 'yaml' config moeten de volgende parameters staan:
    database (str):
    schema (str):

    Returns:
    --------
    pd.Dataframe

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
            failureprobability.id AS failureprobability_id,
            failureprobability.objectid AS section_id,         
            failuremechanism.id AS failuremechanism_id,
            failuremechanism.name AS failuremechanism,
            failureprobability.measureid AS measures_id,
            measures.name AS measures,	
            parameter.id AS parameter_id,            
            parameter.unit As unit,            
            TO_TIMESTAMP(data.datetime/1000) AS date_time,
            data.value AS value	
        FROM {schema}.data        
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=data.parameterid    
        INNER JOIN {schema}.failureprobability ON failureprobability.id=data.objectid
        LEFT JOIN {schema}.failuremechanism ON failuremechanism.id=failureprobability.failuremechanismid
        LEFT JOIN {schema}.measures ON measures.id=failureprobability.measureid
        WHERE data.objecttype='failureprobability' AND data.calculating=true;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_section_thresholds_from_conditions_table(
    input_config: dict,
) -> pd.DataFrame:
    """
    Ophalen klassegrenzen uit een continu database.

    Args:
    ----------
    input_config (dict):

    Opmerking:
    ------
    In de `.env` environment bestand moeten de volgende parameters staan:
    postgresql_user (str):
    postgresql_password (str):
    postgresql_host (str):
    postgresql_port (str):

    In de 'yaml' config moeten de volgende parameters staan:
    database (str):
    schema (str):

    Returns:
    --------
    pd.Dataframe

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
            condition.stateid AS state_id,
            LAG(condition.upperboundary, 1) OVER (PARTITION BY condition.objectid ORDER BY condition.stateid) AS lower_boundary, 
            condition.upperboundary AS upper_boundary, 
            condition.color AS color, 
            condition.description AS label, 
            parameter.unit AS unit
        FROM {schema}.conditions AS condition
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=5
        WHERE condition.objecttype='section'
        ORDER BY condition.objectid,condition.stateid;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
