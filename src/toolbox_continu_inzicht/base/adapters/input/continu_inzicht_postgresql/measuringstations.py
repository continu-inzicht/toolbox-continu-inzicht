import pandas as pd
import sqlalchemy


def input_ci_postgresql_measuringstation_data_table(input_config: dict) -> pd.DataFrame:
    """
    Ophalen belasting uit een continu database voor het whatis scenario.

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
            data.objectid AS measurement_location_id,
            measuringstation.code AS measurement_location_code,
            measuringstation.name AS measurement_location_description,
            parameter.id AS parameter_id,
            parameter.code AS parameter_code,
            parameter.name AS parameter_description,
            parameter.unit AS unit,
            TO_TIMESTAMP(data.datetime/1000) AS date_time,
            value AS value,
            (
                CASE
                    WHEN parameter.id > 1 THEN 'verwacht'
                    ELSE 'meting'
                END
            ) AS value_type
        FROM {schema}.data
        INNER JOIN {schema}.measuringstations AS measuringstation ON data.objectid=measuringstation.id
        INNER JOIN {schema}.parameters AS parameter ON data.parameterid=parameter.id
        WHERE data.objecttype='measuringstation' AND data.calculating=True;    
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    # Datum kolom moet een object zijn en niet een 'datetime64[ns, UTC]'
    # df["date_time"] = df["date_time"].astype(object)

    return df


def input_ci_postgresql_from_waterlevels(input_config: dict) -> pd.DataFrame:
    """
    Ophalen belasting uit een continu database voor het whatis scenario.

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
                    waterlevel.measuringstationid AS measurement_location_id,
                    measuringstation.code AS measurement_location_code,
                    measuringstation.name AS measurement_location_description,
                    parameter.id AS parameter_id,
                    parameter.code AS parameter_code,
                    parameter.name AS parameter_description,
                    parameter.unit AS unit,
                    TO_TIMESTAMP(waterlevel.datetime/1000) AS date_time,
                    value AS value,
                    (
                        CASE
                            WHEN waterlevel.datetime > simulation.datetime THEN 'verwacht'
                            ELSE 'meting'
                        END
                    ) AS value_type

        FROM {schema}.waterlevels AS waterlevel
        INNER JOIN
            (
                SELECT MIN(moments.id)*(60*60*1000) AS start_diff, MAX(moments.id)*(60*60*1000) AS end_diff
                FROM {schema}.moments
            ) AS moment ON 1=1
        INNER JOIN {schema}.simulation ON simulation.scenarioid=waterlevel.scenarioid
        INNER JOIN {schema}.measuringstations AS measuringstation ON waterlevel.measuringstationid=measuringstation.id
        INNER JOIN {schema}.parameters AS parameter ON waterlevel.parameter=parameter.id
        WHERE
            waterlevel.datetime >= simulation.datetime + moment.start_diff AND
            waterlevel.datetime <= simulation.datetime + moment.end_diff;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    # Datum kolom moet een object zijn en niet een 'datetime64[ns, UTC]'
    df["date_time"] = df["date_time"].astype(object)

    return df


def input_ci_postgresql_from_conditions(input_config: dict) -> pd.DataFrame:
    """
    Ophalen dremple waarden uit een continu database.

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
            measuringstation.id AS measurement_location_id,
            measuringstation.code AS measurement_location_code,
            LAG(condition.upperboundary, 1) OVER (PARTITION BY condition.objectid ORDER BY condition.stateid) AS lower_boundary,
            condition.upperboundary AS upper_boundary,
            condition.color AS color,
            condition.description AS label,
            parameter.unit AS unit
        FROM {schema}.conditions AS condition
        INNER JOIN {schema}.measuringstations AS measuringstation ON measuringstation.id=condition.objectid
        INNER JOIN {schema}.parameters AS parameter ON parameter.id=1
        WHERE condition.objecttype='measuringstation'
        ORDER BY condition.objectid,condition.stateid;;
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df


def input_ci_postgresql_from_measuringstations(input_config: dict) -> pd.DataFrame:
    """
    Ophalen meetstations uit een continu database.

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
        "source",
    ]

    assert all(key in input_config for key in keys)

    # maak verbinding object
    engine = sqlalchemy.create_engine(
        f"postgresql://{input_config['postgresql_user']}:{input_config['postgresql_password']}@{input_config['postgresql_host']}:{int(input_config['postgresql_port'])}/{input_config['database']}"
    )

    schema = input_config["schema"]

    # WaterInfo, NOOS Matroos/ ...
    source = input_config["source"]
    query = f"""
        SELECT
            id AS measurement_location_id,
            code AS measurement_location_code,
            name AS measurement_location_description
        FROM {schema}.measuringstations
        WHERE source='{source}';
    """

    # qurey uitvoeren op de database
    with engine.connect() as connection:
        df = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

    # verbinding opruimen
    engine.dispose()

    return df
