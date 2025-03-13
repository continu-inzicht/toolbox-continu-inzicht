"""
End script voor Continu Inzicht Viewer
"""

from toolbox_continu_inzicht import DataAdapter
import sqlalchemy
from dotenv import dotenv_values


def reset_database(data_adapter: DataAdapter, tables: dict, output: str) -> None:
    """ """

    output_config = data_adapter.config.data_adapters[output]
    environmental_variables = dict(dotenv_values())

    # voeg alle environmental variables toe aan de functie output config
    output_config.update(environmental_variables)

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]

    assert all(key in output_config for key in keys)

    schema = output_config["schema"]

    if len(tables) > 0:
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        with engine.connect() as connection:
            query = []
            for table in tables:
                if "reset_index" in table and table["reset_index"]:
                    query.append(
                        f"TRUNCATE TABLE {schema}.{table['name']} RESTART IDENTITY;"
                    )
                else:
                    query.append(f"TRUNCATE TABLE {schema}.{table['name']};")

            query = " ".join(query)

            connection.execute(sqlalchemy.text(str(query)))
            connection.commit()

            # verbinding opruimen
            engine.dispose()
