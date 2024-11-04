import os
import pandas as pd
import pytest

from dotenv import load_dotenv
from datetime import datetime, timezone
from pathlib import Path
from sqlalchemy import create_engine, text
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter

load_dotenv()


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Kan alleen lokaal getest worden"
)
def test_dataadapter_ci_postgresql_from_waterlevels():
    """
    Deze test haalt waterstanden op uit een Continu Inzicht database
    Het formaat moet voldoen aan het uitvoerformaat van functies die data ophalen
    van bijvoorbeeld Fews, matroos e.d.
    """
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config_ci.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    input_string = "ci_waterlevels"
    schema = {
        "measurement_location_id": "int64",
        "measurement_location_code": "object",
        "measurement_location_description": "object",
        "parameter_id": "int64",
        "parameter_code": "object",
        "parameter_description": "object",
        "unit": "object",
        "date_time": "object",
        "value": "float64",
        "value_type": "object",
    }

    df_in = data_adapter.input(input=input_string, schema=schema)
    assert df_in is not None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Kan alleen lokaal getest worden"
)
def test_dataadapter_ci_postgresql_from_measuringstations():
    """
    Deze test haalt meetstations op uit een Continu Inzicht database
    Het formaat moet voldoen aan het invoerformaat van functies die data ophalen
    van bijvoorbeeld Fews, matroos e.d.
    """
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config_ci.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    input_string = "ci_measuringstations"
    schema = {"id": "int64", "name": "object", "code": "object"}

    df_in = data_adapter.input(input=input_string, schema=schema)
    assert df_in is not None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Kan alleen lokaal getest worden"
)
def test_dataadapter_ci_postgresql_from_conditions():
    """
    Deze test haalt drempelwaarden van meetstation op uit een Continu Inzicht database
    Het formaat moet voldoen aan het invoerformaat van functies die belasting
    classificeren
    """
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config_ci.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    input_string = "ci_conditions"
    schema = {
        "measurement_location_code": "object",
        "van": "float64",
        "tot": "float64",
        "kleur": "object",
        "label": "object",
    }

    df_in = data_adapter.input(input=input_string, schema=schema)
    assert df_in is not None


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Kan alleen lokaal getest worden"
)
def test_dataadapter_ci_postgresql_to_data():
    """
    Deze test schrijft records naar de data-tabel van een Continu Inzicht database
    """
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config_ci.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    output_string = "ci_data"

    postgresql_user = os.getenv("postgresql_user")
    postgresql_password = os.getenv("postgresql_password")
    postgresql_host = os.getenv("postgresql_host")
    postgresql_port = os.getenv("postgresql_port")

    output_info = config.data_adapters
    database = output_info[output_string]["database"]
    schema = output_info[output_string]["schema"]

    # verwijder alle data met objectid gelijk aan 999999
    dummy_object_id = 999999
    engine = create_engine(
        f"postgresql://{postgresql_user}:{postgresql_password}@{postgresql_host}:{int(postgresql_port)}/{database}"
    )

    with engine.connect() as connection:
        connection.execute(
            text(f"DELETE FROM {schema}.data WHERE objectid={dummy_object_id};")
        )
        connection.commit()  # commit the transaction

        # maak een dummy record aan
        dataframe = pd.DataFrame()
        records = []
        record = {
            "measurement_location_id": dummy_object_id,
            "measurement_location_code": "measurement_location_code",
            "measurement_location_description": "measurement_location_description",
            "parameter_id": 1,
            "parameter_code": "parameter_code",
            "parameter_description": "parameter_description",
            "unit": "unit",
            "date_time": datetime(2024, 10, 16, 15, 00).replace(tzinfo=timezone.utc),
            "value": 12.34,
            "value_type": "gemeten",
        }

        records.append(record)
        dataframe = pd.DataFrame.from_records(records)

        # schijf record naar database
        df_out = data_adapter.output(output=output_string, df=dataframe)

        assert df_out is not None

        # controleer of record in database staat
        df_from_db = pd.read_sql_query(
            sql=text(f"SELECT * FROM {schema}.data WHERE objectid={dummy_object_id};"),
            con=connection,
        )

        assert df_from_db is not None

        # verwijder tijdelijke data
        connection.execute(
            text(f"DELETE FROM {schema}.data WHERE objectid={dummy_object_id};")
        )
        connection.commit()  # commit the transaction

    # dispose connection
    engine.dispose()


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true", reason="Kan alleen lokaal getest worden"
)
def test_dataadapter_ci_postgresql_to_states():
    """
    Deze test schrijft records naar de states-tabel van een Continu Inzicht database
    """
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config_ci.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    output_string = "ci_states"

    postgresql_user = os.getenv("postgresql_user")
    postgresql_password = os.getenv("postgresql_password")
    postgresql_host = os.getenv("postgresql_host")
    postgresql_port = os.getenv("postgresql_port")

    output_info = config.data_adapters
    database = output_info[output_string]["database"]
    schema = output_info[output_string]["schema"]

    # verwijder alle data met objectid gelijk aan 999999
    dummy_object_id = 7  # SPY
    engine = create_engine(
        f"postgresql://{postgresql_user}:{postgresql_password}@{postgresql_host}:{int(postgresql_port)}/{database}"
    )

    with engine.connect() as connection:
        delete_query = f"""
            DELETE 
            FROM {schema}.states 
            WHERE objecttype='measuringstation' AND objectid={dummy_object_id};
        """
        connection.execute(text(delete_query))
        connection.commit()  # commit the transaction

        # maak een dummy record aan
        dataframe = pd.DataFrame()
        records = []

        # controleer of record in database staat
        select_query = f"""
            SELECT 
                moment.id AS momentid, 
                TO_TIMESTAMP(moment.calctime/1000) AS date_time
            FROM {schema}.moments AS moment;
        """
        df_moments_from_db = pd.read_sql_query(sql=text(select_query), con=connection)

        for index, row in df_moments_from_db.iterrows():
            records.append(
                {
                    "measurement_location_code": "SPY",
                    "date_time": row["date_time"],
                    "value": 81.0,
                    "van": -92.0,
                    "tot": 200.0,
                    "kleur": "#39870C",
                    "label": "Normaal (-92 tot 200cm)",
                }
            )

        dataframe = pd.DataFrame.from_records(records)

        # schijf record naar database
        df_out = data_adapter.output(output=output_string, df=dataframe)

        assert df_out is not None

        # controleer of record in database staat
        select_query = f"""
            SELECT * 
            FROM {schema}.states 
            WHERE objecttype='measuringstation' AND objectid={dummy_object_id};
        """
        df_from_db = pd.read_sql_query(sql=text(select_query), con=connection)

        assert df_from_db is not None
        assert len(df_from_db) == 4

        # verwijder tijdelijke data
        connection.execute(text(delete_query))
        connection.commit()  # commit the transaction

    # dispose connection
    engine.dispose()
