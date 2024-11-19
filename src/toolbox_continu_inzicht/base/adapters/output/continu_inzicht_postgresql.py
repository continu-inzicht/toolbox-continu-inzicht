import numpy as np
import pandas as pd
import sqlalchemy
from toolbox_continu_inzicht.utils.datetime_functions import epoch_from_datetime


def output_ci_postgresql_to_data(output_config: dict, df: pd.DataFrame):
    """
    Schrijft data naar Continu Inzicht database

    Args:
    ----------
    output_config (dict):

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
        "objecttype",
    ]

    def bepaal_parameter_id(value_type: str):
        if value_type == "meting":
            return 1
        else:
            return 2

    assert all(key in output_config for key in keys)

    table = "data"
    schema = output_config["schema"]
    objecttype = output_config["objecttype"]

    if len(df) > 0:
        # objectid, objecttype, parameterid, datetime, value, calculating
        if objecttype == "measuringstation":
            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            df["objecttype"] = objecttype
            df["calculating"] = False
            df["datetime"] = df["date_time"].apply(epoch_from_datetime)
            df["parameterid"] = df["value_type"].apply(bepaal_parameter_id)
            # TODO fix units:
            df["value"] = df["value"].div(100)
            df_data = df.loc[
                :,
                [
                    "measurement_location_id",
                    "objecttype",
                    "parameterid",
                    "datetime",
                    "value",
                    "calculating",
                ],
            ]
            df_data = df_data.rename(columns={"measurement_location_id": "objectid"})

            # Eerst bestaande gegevens van meetstations verwijderen
            location_ids = df_data.objectid.unique()
            location_ids_str = ",".join(map(str, location_ids))

            # with engine.connect() as connection:
            #     connection.execute(
            #         sqlalchemy.text(f"""
            #                         DELETE FROM {schema}.{table}
            #                         WHERE objectid IN ({location_ids_str}) AND
            #                               calculating=true AND
            #                               objecttype='measuringstation';
            #                         """)
            #     )
            #     connection.commit()  # commit the transaction

            with engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text(f"""
                                    DELETE FROM {schema}.{table} 
                                    WHERE objectid IN ({location_ids_str}) AND                                               
                                            objecttype='measuringstation';
                                    """)
                )
                connection.commit()  # commit the transaction

            # schrijf data naar de database
            df_data.to_sql(
                table,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False,
            )

            # verbinding opruimen
            engine.dispose()

        else:
            raise UserWarning("Geen gegevens om op te slaan.")

    return df


def output_ci_postgresql_to_states(output_config: dict, df: pd.DataFrame):
    """
    Schrijft data naar Continu Inzicht database tabel states

    Args:
    ----------
    output_config (dict):

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
        "objecttype",
    ]

    assert all(key in output_config for key in keys)

    table = "states"
    schema = output_config["schema"]
    objecttype = output_config["objecttype"]

    # ,measurement_location_code,date_time,value,van,tot,kleur,label
    # 2,DENH,1991-01-13 22:40:00+00:00,173.4,170.0,190.0,#4bacc6,verhoogd

    # objectid, objecttype, parameterid, momentid, stateid, calculating, changedate
    if objecttype == "measuringstation":
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        # eerst lookup data ophalen om de juiste id's te bepalen
        query = f""" 
            SELECT 
                id AS objectid, 
                code AS measurement_location_code
            FROM {schema}.measuringstations;
        """

        # query uitvoeren op de database
        with engine.connect() as connection:
            df_stations = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        # eerst lookup data ophalen om de juiste id's te bepalen
        query = f""" 
            SELECT 
                moment.id AS momentid, 
                TO_TIMESTAMP(moment.calctime/1000) AS date_time
            FROM {schema}.moments AS moment;
        """

        # query uitvoeren op de database
        with engine.connect() as connection:
            df_moments = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

        query = f""" 
            SELECT 
                stateid AS stateid, 
                objectid AS objectid, 
                upperboundary AS tot
            FROM {schema}.conditions
            WHERE objecttype='measuringstation'
            ORDER BY objectid, stateid;
        """

        # query uitvoeren op de database
        with engine.connect() as connection:
            df_conditions = pd.read_sql_query(
                sql=sqlalchemy.text(query), con=connection
            )

        df.set_index("measurement_location_code")
        df_merge = df.merge(df_stations, on="measurement_location_code", how="outer")

        df_moments["date_time"] = df_moments["date_time"].astype(object)
        df_moments.set_index("date_time")

        df_merge["date_time"] = df_merge["date_time"].astype(object)
        df_merge.set_index("date_time")
        df_merge = df_merge.merge(
            df_moments, on="date_time", how="inner", validate="many_to_one"
        )

        df_merge.set_index(["objectid", "tot"])
        df_merge = df_merge.merge(
            df_conditions,
            on=["objectid", "tot"],
            how="left",
            validate="many_to_one",
        )

        # haal [objectid, objecttype, parameterid, momentid, stateid, calculating, changedate] op

        # Eerst bestaande gegevens van meetstations verwijderen
        location_ids = df_merge.objectid.unique()
        location_ids_str = ",".join(map(str, location_ids))

        with engine.connect() as connection:
            connection.execute(
                sqlalchemy.text(f"""
                                    DELETE FROM {schema}.{table} 
                                    WHERE objectid IN ({location_ids_str}) AND 
                                            calculating=true AND
                                            objecttype='measuringstation';
                                    """)
            )
            connection.commit()  # commit the transaction

        df_merge["objecttype"] = objecttype
        df_merge["parameterid"] = np.where(df_merge["momentid"] <= 0, 1, 2)
        df_merge["calculating"] = True
        df_merge["changedate"] = 0

        df_states = df_merge.loc[
            :,
            [
                "objectid",
                "objecttype",
                "parameterid",
                "momentid",
                "stateid",
                "calculating",
                "changedate",
            ],
        ]

        # schrijf data naar de database
        df_states.to_sql(
            table,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
        )

        # verbinding opruimen
        engine.dispose()
