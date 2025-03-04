import unittest
import pandas as pd

from toolbox_continu_inzicht.base.adapters.output.continu_inzicht_postgresql.output_measuringstation import (
    output_ci_postgresql_measuringstation_to_data,
    output_ci_postgresql_to_states,
    output_ci_postgresql_measuringstation,
    output_ci_postgresql_to_moments,
    output_ci_postgresql_conditions,
)
import unittest.mock as mock

# nog toe te voegen:
(output_ci_postgresql_measuringstation,)
(output_ci_postgresql_to_moments,)


class TestOutputCiPostgresql(unittest.TestCase):
    @mock.patch("sqlalchemy.create_engine")
    def test_output_ci_postgresql_measuringstation_to_data(self, mock_create_engine):
        """Test de geproduceerde query voor output_ci_postgresql_measuringstation_to_data met behulp van mocking."""
        mock_engine = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        data = {
            "measurement_location_id": [1, 2],
            "date_time": pd.to_datetime(["2023-01-01", "2023-01-02"], utc=True),
            "value": [10.0, 20.0],
            "value_type": ["meting", "prognose"],
        }
        df = pd.DataFrame(data)

        output_config = {
            "postgresql_user": "user",
            "postgresql_password": "password",
            "postgresql_host": "localhost",
            "postgresql_port": "5432",
            "database": "continuinzicht",
            "schema": "continuinzicht_demo_realtime",
            "unit_conversion_factor": 0.01,
        }

        output_ci_postgresql_measuringstation_to_data(output_config, df)
        mock_create_engine.assert_called_once_with(
            "postgresql://user:password@localhost:5432/continuinzicht"
        )

        mock_engine.connect.assert_called_once()
        mock_connection.execute.assert_called()
        mock_connection.commit.assert_called_once()
        mock_engine.dispose.assert_called_once()

        # Controleer de uitgevoerde SQL-opdracht
        args, kwargs = mock_connection.execute.call_args
        sql_command = args[0].text
        expected_query = """DELETE FROM continuinzicht_demo_realtime.data
                                WHERE objectid IN (1,2) AND
                                        objecttype='measuringstation' AND
                                        calculating=True;"""
        self.assertEqual(sql_command.strip(), expected_query.strip())

    def output_ci_postgresql_to_states_mocking(self, sql, con):
        sql, con  # voor nu niets doen
        return pd.DataFrame(
            {
                "stateid": ["state1", "state2"],
                "objectid": [1, 2],
                "upper_boundary": [0.0, 1.0],
            }
        )

    @mock.patch("sqlalchemy.create_engine")
    def test_output_ci_postgresql_to_states(self, mock_create_engine):
        """Test de geproduceerde query voor output_ci_postgresql_to_states met behulp van mocking."""
        mock_engine = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        data = {
            "measurement_location_id": [1, 2],
            "date_time": pd.to_datetime(["2023-01-01", "2023-01-02"], utc=True),
            "value": [10.0, 20.0],
            "value_type": ["meting", "prognose"],
            "hours": [1, 2],
            "upper_boundary": [0.0, 1.0],
        }
        df = pd.DataFrame(data)
        output_config = {
            "postgresql_user": "user",
            "postgresql_password": "password",
            "postgresql_host": "localhost",
            "postgresql_port": "5432",
            "database": "continuinzicht",
            "schema": "continuinzicht_demo_realtime",
            "unit_conversion_factor": 0.01,
        }

        # dit heeft een extra database interactie, mock die ook
        with mock.patch(
            "pandas.read_sql_query", self.output_ci_postgresql_to_states_mocking
        ):
            output_ci_postgresql_to_states(output_config, df)

        mock_create_engine.assert_called_once_with(
            "postgresql://user:password@localhost:5432/continuinzicht"
        )

        mock_engine.connect.assert_called()
        mock_connection.execute.assert_called()
        mock_connection.commit.assert_called_once()
        mock_engine.dispose.assert_called_once()

        # Controleer de uitgevoerde SQL-opdracht
        args, kwargs = mock_connection.execute.call_args
        sql_command = args[0].text
        expected_query = """DELETE FROM continuinzicht_demo_realtime.states
                                WHERE objectid IN (1,2) AND
                                        calculating=true AND
                                        objecttype='measuringstation';"""
        self.assertEqual(sql_command.strip(), expected_query.strip())

    @mock.patch("sqlalchemy.create_engine")
    def test_output_ci_postgresql_conditions(self, mock_create_engine):
        mock_engine = mock.MagicMock()
        mock_connection = mock.MagicMock()
        mock_create_engine.return_value = mock_engine
        mock_engine.connect.return_value.__enter__.return_value = mock_connection

        # Mock de DataFrame
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

        # Mock de output_config
        output_config = {
            "postgresql_user": "user",
            "postgresql_password": "password",
            "postgresql_host": "localhost",
            "postgresql_port": "5432",
            "database": "continuinzicht",
            "schema": "continuinzicht_demo_realtime",
        }

        output_ci_postgresql_conditions(output_config, df)

        mock_create_engine.assert_called_once_with(
            "postgresql://user:password@localhost:5432/continuinzicht"
        )

        mock_engine.connect.assert_called_once()
        mock_connection.execute.assert_called()
        mock_connection.commit.assert_called_once()
        mock_engine.dispose.assert_called_once()

        # Controleer de uitgevoerde SQL-opdracht
        args, kwargs = mock_connection.execute.call_args
        sql_command = args[0].text
        expected_query = """TRUNCATE continuinzicht_demo_realtime.conditions;
        INSERT INTO continuinzicht_demo_realtime.conditions(id, stateid, objectid, objecttype, upperboundary, name, description, color, statevalue) VALUES (1, type1, 38383, 'measuringstation', 0.0, 'test_wit', 'test1', '#000000', condition1);
        INSERT INTO continuinzicht_demo_realtime.conditions(id, stateid, objectid, objecttype, upperboundary, name, description, color, statevalue) VALUES (2, type2, 9121, 'measuringstation', 1.0, 'test_zwart', 'test2', '#ffffff', condition2);"""
        self.assertEqual(
            sql_command.strip(), expected_query.strip().replace("\n       ", "")
        )
