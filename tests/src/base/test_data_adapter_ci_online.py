import unittest
import pandas as pd
from toolbox_continu_inzicht.base.adapters.output.continu_inzicht_postgresql.output_measuringstation import (
    output_ci_postgresql_conditions,
    output_ci_postgresql_measuringstation_to_data,
)
import unittest.mock as mock


class TestOutputCiPostgresql(unittest.TestCase):
    @mock.patch("sqlalchemy.create_engine")
    def test_output_ci_postgresql_conditions(self, mock_create_engine):
        # Mock the engine and connection
        mock_engine = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        # Mock the DataFrame
        data = {
            "objecttype": ["measuringstation", "measuringstation"],
            "objectid": [38383, 9121],
            "id": [1, 2],
            "upperboundary": [0.0, 1.0],
            "stateid": ["type1", "type2"],
            "statevalue": ["condition1", "condition2"],
            "name": ["test_wit", "test_zwart"],
            "color": ["#000000", "#ffffff"],
            "description": ["test1", "test2"],
        }
        df = pd.DataFrame(data)

        # Mock the output_config
        output_config = {
            "postgresql_user": "user",
            "postgresql_password": "password",
            "postgresql_host": "localhost",
            "postgresql_port": "5432",
            "database": "continuinzicht",
            "schema": "continuinzicht_demo_realtime",
        }

        # Call the function
        output_ci_postgresql_conditions(output_config, df)

        # Assertions
        mock_create_engine.assert_called_once_with(
            "postgresql://user:password@localhost:5432/continuinzicht"
        )

        mock_engine.connect.assert_called_once()
        mock_connection.execute.assert_called()
        mock_connection.commit.assert_called_once()
        mock_engine.dispose.assert_called_once()

        # Check the SQL command executed
        args, kwargs = mock_connection.execute.call_args
        sql_command = args[0].text
        expected_query = """TRUNCATE continuinzicht_demo_realtime.conditions;
        INSERT INTO continuinzicht_demo_realtime.conditions(id, stateid, objectid, objecttype, upperboundary, name, description, color, statevalue) VALUES (1, type1, 38383, 'measuringstation', 0.0, 'test_wit', 'test1', '#000000', condition1);
        INSERT INTO continuinzicht_demo_realtime.conditions(id, stateid, objectid, objecttype, upperboundary, name, description, color, statevalue) VALUES (2, type2, 9121, 'measuringstation', 1.0, 'test_zwart', 'test2', '#ffffff', condition2);"""
        self.assertEqual(
            sql_command.strip(), expected_query.strip().replace("\n       ", "")
        )

    @mock.patch("sqlalchemy.create_engine")
    def test_output_ci_postgresql_measuringstation_to_data(self, mock_create_engine):
        # Mock the engine and connection
        mock_engine = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        # Mock the DataFrame
        data = {
            "measurement_location_id": [1, 2],
            "date_time": pd.to_datetime(["2023-01-01", "2023-01-02"], utc=True),
            "value": [10.0, 20.0],
            "value_type": ["meting", "prognose"],
        }
        df = pd.DataFrame(data)

        # Mock the output_config
        output_config = {
            "postgresql_user": "user",
            "postgresql_password": "password",
            "postgresql_host": "localhost",
            "postgresql_port": "5432",
            "database": "continuinzicht",
            "schema": "continuinzicht_demo_realtime",
            "unit_conversion_factor": 0.01,
        }

        # Call the function
        output_ci_postgresql_measuringstation_to_data(output_config, df)

        # Assertions
        mock_create_engine.assert_called_once_with(
            "postgresql://user:password@localhost:5432/continuinzicht"
        )

        mock_engine.connect.assert_called_once()
        mock_connection.execute.assert_called()
        mock_connection.commit.assert_called_once()
        mock_engine.dispose.assert_called_once()

        # Check the SQL command executed
        args, kwargs = mock_connection.execute.call_args
        sql_command = args[0].text
        expected_query = """DELETE FROM continuinzicht_demo_realtime.data
                                WHERE objectid IN (1,2) AND
                                        objecttype='measuringstation' AND
                                        calculating=True;"""
        self.assertEqual(sql_command.strip(), expected_query.strip())
