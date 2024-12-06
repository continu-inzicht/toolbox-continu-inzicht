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
    calculating = False

    unit_conversion_factor = 1
    if "unit_conversion_factor" in output_config:
        unit_conversion_factor = float(output_config["unit_conversion_factor"])

    if len(df) > 0:
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        # objectid, objecttype, parameterid, datetime, value, calculating
        if objecttype == "measuringstation":
            # dubbele records geven een foutmeldig bij opslaan in database
            df = df.drop_duplicates(
                subset=["measurement_location_id", "date_time", "value_type"]
            )

            # df["objecttype"] = str(objecttype)
            # df["calculating"] = False

            df = df.assign(objecttype=str(objecttype))
            df = df.assign(calculating=calculating)

            df["datetime"] = df["date_time"].apply(epoch_from_datetime)
            df["parameterid"] = df["value_type"].apply(bepaal_parameter_id)

            # TODO fix units:
            df["value"] = df["value"].mul(unit_conversion_factor)
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

            with engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text(f"""
                                    DELETE FROM {schema}.{table}
                                    WHERE objectid IN ({location_ids_str}) AND
                                            objecttype='{objecttype}' AND
                                            calculating={calculating};
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

        elif objecttype == "section":
            df_data = df.dropna(how="any")

            df_data = df_data.assign(objecttype=str(objecttype))
            df_data = df_data.assign(calculating=False)

            # df_data["datetime"] = df_data["date_time"].apply(datetime.fromisoformat)
            df_data["datetime"] = df_data["date_time"].apply(epoch_from_datetime)
            df_data["parameterid"] = df_data["value_type"].apply(bepaal_parameter_id)

            df_data = df_data.loc[
                :,
                [
                    "id",
                    "objecttype",
                    "parameterid",
                    "datetime",
                    "value",
                    "calculating",
                ],
            ]

            df_data = df_data.rename(columns={"id": "objectid"})
            df_data = df_data.reset_index(drop=True)

            section_ids = df_data.objectid.unique()
            section_ids_str = ",".join(map(str, section_ids))

            with engine.connect() as connection:
                connection.execute(
                    sqlalchemy.text(f"""
                                    DELETE FROM {schema}.{table}
                                    WHERE objectid IN ({section_ids_str}) AND
                                            objecttype='{objecttype}' AND
                                            calculating={calculating};
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

        query = f"""
            SELECT
                stateid AS stateid,
                objectid AS objectid,
                upperboundary AS upper_boundary
            FROM {schema}.conditions
            WHERE objecttype='measuringstation'
            ORDER BY objectid, stateid;
        """

        # query uitvoeren op de database
        with engine.connect() as connection:
            df_conditions = pd.read_sql_query(
                sql=sqlalchemy.text(query), con=connection
            )

        df_merge = df.copy()
        df_merge.rename(
            columns={"hours": "momentid", "measurement_location_id": "objectid"},
            inplace=True,
        )
        df_merge.set_index("objectid")

        df_merge.set_index(["objectid", "upper_boundary"])
        df_merge = df_merge.merge(
            df_conditions,
            on=["objectid", "upper_boundary"],
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
                                            calculating=false AND
                                            objecttype='measuringstation';
                                    """)
            )
            connection.commit()  # commit the transaction

        df_merge["objecttype"] = objecttype
        df_merge["parameterid"] = np.where(df_merge["momentid"] <= 0, 1, 2)
        df_merge["calculating"] = False
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


def output_ci_postgresql_to_moments(output_config: dict, df: pd.DataFrame):
    """
    Schrijft moments naar Continu Inzicht database tabel moments

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
    ]

    assert all(key in output_config for key in keys)

    schema = output_config["schema"]

    if not df.empty:
        if "date_time" in df and "calc_time" in df and "moment_id" in df:
            df["datetime"] = df["date_time"].apply(epoch_from_datetime)
            df["calctime"] = df["calc_time"].apply(epoch_from_datetime)
            query = []

            for _, row in df.iterrows():
                query.append(
                    f"UPDATE {schema}.moments SET datetime={str(row["datetime"])},calctime={str(row["calctime"])} WHERE id={str(row["moment_id"])};"
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()  # commit the transaction

            # verbinding opruimen
            engine.dispose()
