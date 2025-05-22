"""
Data adapters voor het schrijven naar de Continu Inzicht database
"""

import numpy as np
import pandas as pd
import sqlalchemy
from toolbox_continu_inzicht.utils.datetime_functions import epoch_from_datetime


def output_ci_postgresql_measuringstation_to_data(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor meetstations de belasting data naar de Continu Inzicht database (tabel: data).

    Yaml example:\n
        type: ci_postgresql_measuringstation_to_data
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"
        unit_conversion_factor: 0.01

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - measurement_location_id: int64
        - date_time: datetime64[ns, UTC]
        - value: float64
        - value_type: str

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
    - unit_conversion_factor (optioneel, float): conversiefactor om waarde om te zetten naar meters
    """

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
        "schema",
    ]

    def bepaal_parameter_id(value_type: str):
        if value_type == "meting":
            return 1
        else:
            return 2

    assert all(key in output_config for key in keys)

    table = "data"
    schema = output_config["schema"]
    objecttype = "measuringstation"
    calculating = True

    unit_conversion_factor = 1
    if "unit_conversion_factor" in output_config:
        unit_conversion_factor = float(output_config["unit_conversion_factor"])

    if len(df) > 0:
        # maak verbinding object
        engine = sqlalchemy.create_engine(
            f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
        )

        # dubbele records geven een foutmeldig bij opslaan in database
        df = df.drop_duplicates(
            subset=["measurement_location_id", "date_time", "value_type"]
        )

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

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_to_states(output_config: dict, df: pd.DataFrame) -> None:
    """
    Schrijft voor meetstations de classificatie data naar de Continu Inzicht database (tabel: states).

    Yaml example:\n
        type: ci_postgresql_to_states
        database: "continuinzicht"
        schema: "continuinzicht_demo_realtime"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - measurement_location_id: int64
        - date_time: datetime64[ns, UTC]
        - hours: int64
        - upper_boundary: int64
        - value: float64

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
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

    table = "states"
    schema = output_config["schema"]
    objecttype = "measuringstation"

    # maak verbinding object
    engine = sqlalchemy.create_engine(
        f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
    )

    query = f"""
        SELECT
            stateid AS stateid,
            objectid AS objectid,
            upperboundary/100 AS upper_boundary
        FROM {schema}.conditions
        WHERE objecttype='{objecttype}'
        ORDER BY objectid, stateid;
    """

    # query uitvoeren op de database
    with engine.connect() as connection:
        df_conditions = pd.read_sql_query(sql=sqlalchemy.text(query), con=connection)

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

    # Eerst bestaande gegevens van meetstations verwijderen
    location_ids = df_merge.objectid.unique()
    location_ids_str = ",".join(map(str, location_ids))

    with engine.connect() as connection:
        connection.execute(
            sqlalchemy.text(f"""
                                DELETE FROM {schema}.{table}
                                WHERE objectid IN ({location_ids_str}) AND
                                        calculating=true AND
                                        objecttype='{objecttype}';
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


def output_ci_postgresql_measuringstation(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor meetstations de classificatie data naar de Continu Inzicht database (tabel: states).

    Yaml example:\n
        type: ci_postgresql_measuringstation
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - id: int64                 : id van het meetstation
        - name: str                 : naam van het meetstation
        - code: str                 : code van het meetstation
        - source: str               : bron van de data van het meetstation
        - geometry: geom            : geometrie (ligging) van het meetstation (let op projectie!)
        - tide: bool                : meetstation bevat belasting waar een getijperiode een tol speelt (default false)
        - area_geometry: geom       : OPTIONEEL geometrie van het gebied waarbinnen de belasting wordt bepaald (bijv. een gemiddelde waarde)

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
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
        if (
            "id" in df
            and "name" in df
            and "code" in df
            and "source" in df
            and "geometry" in df
            and "tide" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.measuringstations;")

            area_geometry_rows = df[df["area_geometry"].isna()]
            for _, row in area_geometry_rows.iterrows():
                query.append(
                    f"INSERT INTO {schema}.measuringstations(id, name, code, source, geometry, tide) VALUES ({str(row['id'])}, '{row['name']}', '{row['code']}', '{row['source']}', '{str(row['geometry'])}', {str(row['tide'])});"
                )

            non_area_geometry_rows = df[df["area_geometry"].notna()]
            for _, row in non_area_geometry_rows.iterrows():
                query.append(
                    f"INSERT INTO {schema}.measuringstations(id, name, code, source, geometry, tide, area_geometry)	VALUES ({str(row['id'])}, '{row['name']}', '{row['code']}', '{row['source']}', '{str(row['geometry'])}', {str(row['tide'])}, '{str(row['area_geometry'])}');"
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
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_to_moments(output_config: dict, df: pd.DataFrame) -> None:
    """
    Schrijft moments naar Continu Inzicht database (tabel: moments)

    Yaml example:\n
        type: ci_postgresql_to_moments
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
    output_config (dict): configuratie opties\n
    df (DataFrame):\n
    - date_time: int64      : datum/tijd van het huidige moment
    - calc_time: int64      : datum/tijd van eerst volgende rekenstap

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
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

    expected_columns = ["date_time", "calc_time", "moment_id"]
    if not df.empty:
        if set(expected_columns).issubset(df.columns):
            df["datetime"] = df["date_time"].apply(epoch_from_datetime)
            df["calctime"] = df["calc_time"].apply(epoch_from_datetime)
            query = []

            for _, row in df.iterrows():
                query.append(
                    f"UPDATE {schema}.moments SET datetime={str(row['datetime'])},calctime={str(row['calctime'])} WHERE id={str(row['moment_id'])};"
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
        else:
            raise UserWarning(
                f"Ontbrekende variabelen: {set(expected_columns).difference(df.columns)} in dataframe!"
            )

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_conditions(output_config: dict, df: pd.DataFrame) -> None:
    """
    Schrijft voor condities van belastingen voor meetstations naar de Continu Inzicht database (tabel: conditions).

    Yaml example:\n
        type: ci_postgresql_conditions
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
    output_config (dict): configuratie opties
    df (DataFrame):\n
    - id: int64                 : id van de conditie
    - stateid: int64            : id van de status van een conditie
    - objectid: int64           : id van het object waartoe de conditie behoort (altijd in combinatie met objecttype)
    - objecttype: str           : het type object waartoe de conditie behoort (bijv. een 'section' of 'measuringstation')
    - upperboundary: float64    : de bovengrenswaarde van de status van een conditie (overgang van de betreffende status naar de volgende status)
    - name: str                 : naam van de status van de conditie
    - description: str          : omschrijving van de status van de conditie
    - color: str                : HEX kleurcode van de kleur van de status van de conditie
    - statevalue: float64       : middenwaarde van statusovergangen (specifiek voor objecttype 'sections')

    **Opmerking:**\n
    In de `.env` environment bestand moeten de volgende parameters staan:\n
    - postgresql_user (str): inlog gebruikersnaam van de Continu Inzicht database
    - postgresql_password (str): inlog wachtwoord van de Continu Inzicht database
    - postgresql_host (str): servernaam/ ip adres van de Continu Inzicht databaseserver
    - postgresql_port (str): poort van de Continu Inzicht databaseserver

    In de 'yaml' config moeten de volgende parameters staan:\n
    - database (str): database van de Continu Inzicht
    - schema (str): schema van de Continu Inzicht
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

    expected_columns = [
        "id",
        "stateid",
        "objectid",
        "objecttype",
        "upperboundary",
        "name",
        "description",
        "color",
        "statevalue",
    ]
    if not df.empty:
        if set(expected_columns).issubset(df.columns):
            query = []
            query.append(f"TRUNCATE {schema}.conditions;")

            statevalue_rows = df[df["statevalue"].isna()]
            for _, row in statevalue_rows.iterrows():
                query.append(
                    f"INSERT INTO {schema}.conditions(id, stateid, objectid, objecttype, upperboundary, name, description, color) VALUES ({str(row['id'])}, {str(row['stateid'])}, {str(row['objectid'])}, '{row['objecttype']}', {str(row['upperboundary'])}, '{str(row['name'])}', '{row['description']}', '{row['color']}');"
                )

            non_statevalue_rows = df[df["statevalue"].notna()]
            for _, row in non_statevalue_rows.iterrows():
                query.append(
                    f"INSERT INTO {schema}.conditions(id, stateid, objectid, objecttype, upperboundary, name, description, color, statevalue) VALUES ({str(row['id'])}, {str(row['stateid'])}, {str(row['objectid'])}, '{row['objecttype']}', {str(row['upperboundary'])}, '{str(row['name'])}', '{row['description']}', '{row['color']}', {str(row['statevalue'])});"
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
        else:
            raise UserWarning(
                f"Ontbrekende variabelen: {set(expected_columns).difference(df.columns)} in dataframe!"
            )

    else:
        raise UserWarning("Geen gegevens om op te slaan.")
