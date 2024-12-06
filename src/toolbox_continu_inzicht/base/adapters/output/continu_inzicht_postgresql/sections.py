from datetime import datetime
import pandas as pd
import sqlalchemy

from toolbox_continu_inzicht.utils.datetime_functions import epoch_from_datetime


def output_ci_postgresql_section_to_data(output_config: dict, df: pd.DataFrame):
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
    ]

    assert all(key in output_config for key in keys)

    def get_parameter_id(value_type: str) -> int:
        if value_type == "meting":
            return 1
        elif value_type == "verwacht":
            return 2
        else:
            raise UserWarning(f"Onbekende value_type: {value_type}.")

    def get_failuremechanism_id(name: str) -> int:
        if name == "COMB":
            return 1
        elif name == "GEKB":
            return 2
        elif name == "STPH":
            return 3
        elif name == "STBI":
            return 4
        elif name == "HTKW":
            return 5
        elif name == "STKWl":
            return 6
        elif name == "PKW":
            return 7
        else:
            raise UserWarning(f"Onbekende faalmeschanisme: {name}.")

    schema = output_config["schema"]
    calculating = True
    objecttype = "section"

    parameter_id = output_config["parameter_id"]

    #   5: Faalkans
    # 100: Faalkans technical
    # 101: Faalkans measure
    # 102: Faalkans expert judgement
    if parameter_id in [5, 100, 101, 102]:
        objecttype = "failureprobability"

    # 1: Gemeten waterstand
    # 2: Voorspelde waterstand
    # 3: Voorspelde waterstand ensemble hoog
    # 4: Voorspelde waterstand ensemble laag
    elif parameter_id in [1, 2, 3, 4]:
        objecttype = "section"

    if len(df) > 0:
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        df_data = df.dropna(how="any")
        df_data = df_data.assign(objecttype=str(objecttype))
        df_data = df_data.assign(calculating=calculating)
        df_data = df_data.assign(measureid=0)

        df_data["datetime"] = df_data["date_time"].apply(epoch_from_datetime)

        df_data["value_parameter_id"] = df_data["value_type"].apply(get_parameter_id)
        df_data["failuremechanismid"] = df_data["failuremechanism"].apply(
            get_failuremechanism_id
        )
        df_data["parameterid"] = parameter_id

        if objecttype == "section":
            print(df)

        # eventueel toevoegen aan de failureprobability tabel
        elif objecttype == "failureprobability":
            # failureprobability table
            df_combintions = (
                df_data.groupby(
                    [
                        "section_id",
                        "value_parameter_id",
                        "failuremechanismid",
                        "measureid",
                    ]
                )
                .size()
                .reset_index()
                .iloc[:, :-1]
                .copy()
            )
            df_combintions = df_combintions.assign(objecttype="section")
            df_combintions = df_combintions.rename(columns={"section_id": "objectid"})

            if len(df_combintions) > 0:
                # Genereer de insert query
                insert_query = f"INSERT INTO {schema}.failureprobability (objectid, objecttype, parameterid, measureid, failuremechanismid) VALUES "
                values = ", ".join(
                    [
                        f"({repr(row.objectid)},{repr(row.objecttype)}, {repr(row.value_parameter_id)}, {repr(row.measureid)}, {repr(row.failuremechanismid)})"
                        for row in df_combintions.itertuples(index=False)
                    ]
                )
                insert_query += (
                    values
                    + "ON CONFLICT (objectid, objecttype, parameterid, measureid, failuremechanismid) DO NOTHING;"
                )

                # Genereer de select query
                select_query = f"""
                    SELECT id, objectid, objecttype, parameterid, measureid, failuremechanismid
                    FROM {schema}.failureprobability
                    WHERE (objectid, objecttype, parameterid, measureid, failuremechanismid) IN ({values});
                """

                with engine.connect() as connection:
                    connection.execute(sqlalchemy.text(insert_query))
                    connection.commit()

                    df_failureprobability = pd.read_sql_query(
                        sql=sqlalchemy.text(select_query), con=connection
                    )

                    if len(df_failureprobability) > 0:
                        df_data_merged = pd.merge(
                            df_data,
                            df_failureprobability,
                            left_on=[
                                "section_id",
                                "value_parameter_id",
                                "measureid",
                                "failuremechanismid",
                            ],
                            right_on=[
                                "objectid",
                                "parameterid",
                                "measureid",
                                "failuremechanismid",
                            ],
                            how="left",
                            suffixes=("", "_drop"),
                        )

                        # nu de data verzamelen
                        df_data_merged = df_data_merged.loc[
                            :,
                            [
                                "id",
                                "objecttype",
                                "parameterid",
                                "datetime",
                                "failureprobability",
                                "calculating",
                            ],
                        ]

                        df_data_merged = df_data_merged.rename(
                            columns={"id": "objectid", "failureprobability": "value"}
                        )
                        df_data_merged = df_data_merged.reset_index(drop=True)

                        # Eerst oude data verwijderen
                        df_combintions = (
                            df_data_merged.groupby(
                                ["objectid", "objecttype", "parameterid", "calculating"]
                            )
                            .size()
                            .reset_index()
                            .iloc[:, :-1]
                            .copy()
                        )
                        values = ", ".join(
                            [
                                f"({repr(row.objectid)},{repr(row.objecttype)},{repr(row.parameterid)},{repr(row.calculating)})"
                                for row in df_combintions.itertuples(index=False)
                            ]
                        )

                        with engine.connect() as connection:
                            connection.execute(
                                sqlalchemy.text(
                                    f"""
                                    DELETE FROM {schema}.data 
                                    WHERE (objectid,objecttype,parameterid,calculating) IN ({values});
                                    """
                                )
                            )
                            connection.commit()  # commit the transaction

                        # schrijf data naar de database
                        df_data_merged.to_sql(
                            "data",
                            con=engine,
                            schema=schema,
                            if_exists="append",
                            index=False,
                        )

        # verbinding opruimen
        engine.dispose()

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_section_to_states(output_config: dict, df: pd.DataFrame):
    """
    Schrijft state naar Continu Inzicht database

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
    calculating = True
    objecttype = "failureprobability"

    if len(df) > 0:
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        df_data = df.dropna(how="any")
        df_data = df_data.assign(objecttype=str(objecttype))
        df_data = df_data.assign(calculating=calculating)
        df_data = df_data.assign(changedate=int(datetime.utcnow().timestamp()) * 1000)

        df_data["epoch"] = df_data["date_time"].apply(epoch_from_datetime)

        select_moments_query = f"""
            SELECT 
                id AS moment_id, 
                calctime AS calc_time
            FROM {schema}.moments;
        """

        with engine.connect() as connection:
            df_moments = pd.read_sql_query(
                sql=sqlalchemy.text(select_moments_query), con=connection
            )
            if len(df_moments) > 0:
                df_data_merge = pd.merge(
                    df_data,
                    df_moments,
                    left_on="epoch",
                    right_on="calc_time",
                    how="left",
                )

                # als datetime niet overeenkomt met de moment tabel dan zal het veld moment_id 'NaN' zijn.
                df_data_merge = df_data_merge.dropna(how="any")

                if len(df_data_merge) > 0:
                    # nu de data verzamelen
                    df_states = df_data_merge.loc[
                        :,
                        [
                            "failureprobability_id",
                            "objecttype",
                            "parameter_id",
                            "moment_id",
                            "state_id",
                            "calculating",
                            "changedate",
                        ],
                    ]

                    df_states = df_states.rename(
                        columns={
                            "failureprobability_id": "objectid",
                            "parameter_id": "parameterid",
                            "state_id": "stateid",
                            "moment_id": "momentid",
                        }
                    )

                    # Eerst oude data verwijderen
                    df_combintions = (
                        df_states.groupby(
                            ["objectid", "objecttype", "parameterid", "calculating"]
                        )
                        .size()
                        .reset_index()
                        .iloc[:, :-1]
                        .copy()
                    )
                    values = ", ".join(
                        [
                            f"({repr(row.objectid)},{repr(row.objecttype)},{repr(row.parameterid)},{repr(row.calculating)})"
                            for row in df_combintions.itertuples(index=False)
                        ]
                    )

                    with engine.connect() as connection:
                        connection.execute(
                            sqlalchemy.text(
                                f"""
                                DELETE FROM {schema}.states 
                                WHERE (objectid,objecttype,parameterid,calculating) IN ({values});
                                """
                            )
                        )
                        connection.commit()  # commit the transaction

                    # schrijf data naar de database
                    df_states.to_sql(
                        "states",
                        con=engine,
                        schema=schema,
                        if_exists="append",
                        index=False,
                    )

                else:
                    raise UserWarning(
                        "Momenten komen niet overeen met de momenten tabel."
                    )

        # verbinding opruimen
        engine.dispose()

    else:
        raise UserWarning("Geen gegevens om op te slaan.")
