"""
DataAdapter voor het wegschrijven van DAMlive grondlaag- en kleurdata
naar de Continu Inzicht PostgreSQL-database.

Doeltabellen:
    - data_damlive_models     : modelgegevens
    - data_damlive_scenarios  : scenario's
    - data_damlive_soil       : grondlagen
    - data_damlive_soil_color : kleurwaarden per grondsoort (uit colors.csv)

Brondata:
    - df_models      : model_id, name, description
    - df_scenarios   : scenario_id, name
    - df_geometries  : geometry_id, layer_id, layer_label, points (list[{X, Z}])
    - df_soillayers  : soillayers_id, layer_id, soil_id
    - df_soils       : soil_id, name
    - df_stages      : stage_id, geometry_id, soillayers_id, ...
    - df_colors      : type, color  (colours.csv gefilterd op type=='soil')

Gebruik (yaml config voorbeeld):
    type: ci_postgresql_damlive_models
    database: "continuinzicht"
    schema: "continuinzicht_demo_damlive"

    type: ci_postgresql_damlive_scenarios
    database: "continuinzicht"
    schema: "continuinzicht_demo_damlive"

    type: ci_postgresql_damlive_soil
    database: "continuinzicht"
    schema: "continuinzicht_demo_damlive"

    type: ci_postgresql_damlive_soil_color
    database: "continuinzicht"
    schema: "continuinzicht_demo_damlive"

    type: ci_postgresql_damlive_lines
    database: "continuinzicht"
    schema: "continuinzicht_hhnk_purmer_realtime"

    type: ci_postgresql_damlive_lines_enveloping
    database: "continuinzicht"
    schema: "continuinzicht_hhnk_purmer_realtime"

    type: ci_postgresql_damlive_lines_values
    database: "continuinzicht"
    schema: "continuinzicht_hhnk_purmer_realtime"

    type: ci_postgresql_damlive_lines_conditions
    database: "continuinzicht"
    schema: "continuinzicht_hhnk_purmer_realtime"
"""

from __future__ import annotations

import pandas as pd
import sqlalchemy

# ---------------------------------------------------------------------------
# DataAdapter: output_ci_postgresql_damlive_models
# ---------------------------------------------------------------------------


def output_ci_postgresql_damlive_models(
    output_config: dict,
    df: pd.DataFrame,
) -> None:
    """
    Schrijft modeldata voor DAMlive naar de Continu Inzicht database
    (tabel: data_damlive_models).

    Het verwachte DataFrame (``df``) heeft de volgende kolommen:

    - id            : int64  – unieke ID van een model
    - name          : str  – naam van het model
    - description   : str  – beschrijving van het model

    Yaml-configuratie voorbeeld::

        type: ci_postgresql_damlive_models
        database: "continuinzicht"
        schema: "continuinzicht_demo_damlive"

    Parameters
    ----------
    output_config : dict
        Configuratiedict met database-verbindingsgegevens en schema/tabel-naam.
        Verwachte sleutels in .env: postgresql_user, postgresql_password,
        postgresql_host, postgresql_port.
        Verwachte sleutels in yaml: database, schema.
    df : pd.DataFrame
        DataFrame met de modeldata (zie kolombeschrijving hierboven).
        Gebruik :func:`create_df_damlive_models` om dit DataFrame aan te maken.

    Raises
    ------
    KeyError
        Als verplichte configuratiesleutels ontbreken.
    ValueError
        Als het DataFrame leeg is.
    """
    if df.empty:
        raise ValueError(
            "Het aangeboden DataFrame is leeg. "
            "Controleer of de DAMlive bestanden correct zijn ingelezen."
        )

    schema = output_config["schema"]

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]

    assert all(key in output_config for key in keys)

    if not df.empty:
        if "id" in df and "name" in df and "description" in df:
            query = []

            query.append(f"TRUNCATE {schema}.data_damlive_models;")

            for _, row in df.iterrows():
                query.append(f"""INSERT INTO {schema}.data_damlive_models(id, name, description)
                    VALUES ({str(row["id"])}, '{str(row["name"])}', '{str(row["description"])}');""")

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


# ---------------------------------------------------------------------------
# DataAdapter: output_ci_postgresql_damlive_scenarios
# ---------------------------------------------------------------------------


def output_ci_postgresql_damlive_scenarios(
    output_config: dict,
    df: pd.DataFrame,
) -> None:
    """
    Schrijft scenario data voor DAMlive naar de Continu Inzicht database
    (tabel: data_damlive_scenarios).

    Het verwachte DataFrame (``df``) heeft de volgende kolommen:

    - id            : int64  – unieke ID van een scenario
    - name          : str  – naam van het scenario

    Yaml-configuratie voorbeeld::

        type: ci_postgresql_damlive_scenarios
        database: "continuinzicht"
        schema: "continuinzicht_demo_damlive"

    Parameters
    ----------
    output_config : dict
        Configuratiedict met database-verbindingsgegevens en schema/tabel-naam.
        Verwachte sleutels in .env: postgresql_user, postgresql_password,
        postgresql_host, postgresql_port.
        Verwachte sleutels in yaml: database, schema.
    df : pd.DataFrame
        DataFrame met de scenariodata (zie kolombeschrijving hierboven).
        Gebruik :func:`create_df_damlive_scenarios` om dit DataFrame aan te maken.

    Raises
    ------
    KeyError
        Als verplichte configuratiesleutels ontbreken.
    ValueError
        Als het DataFrame leeg is.
    """
    if df.empty:
        raise ValueError(
            "Het aangeboden DataFrame is leeg. "
            "Controleer of de DAMlive bestanden correct zijn ingelezen."
        )

    schema = output_config["schema"]

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]

    assert all(key in output_config for key in keys)

    if not df.empty:
        if "id" in df and "name" in df:
            query = []

            query.append(f"TRUNCATE {schema}.data_damlive_scenarios;")

            for _, row in df.iterrows():
                query.append(f"""INSERT INTO {schema}.data_damlive_scenarios(id, name)
                    VALUES ({str(row["id"])}, '{str(row["name"])}');""")

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


# ---------------------------------------------------------------------------
# DataAdapter: output_ci_postgresql_damlive_soil
# ---------------------------------------------------------------------------


def output_ci_postgresql_damlive_soil(
    output_config: dict,
    df: pd.DataFrame,
) -> None:
    """
    Schrijft grondlaagdata voor DAMlive naar de Continu Inzicht database
    (tabel: data_damlive_soil).

    Het verwachte DataFrame (``df``) heeft de volgende kolommen:

    - measuringstationid : int64  – altijd 1
    - parameterid        : int64  – altijd 200
    - itemid             : int64  – uniek laag-ID (= layer_id uit geometries)
    - layername          : str    – naam van de grondsoort (uit soils.name)
    - color              : str    – leeg laten (None / NaN)
    - datetime           : int64  – huidige tijd als epoch milliseconden
    - index              : int64  – volgnummer van het punt binnen de laag (start bij 0)
    - x                  : float64 – X-coördinaat van het punt
    - y                  : float64 – altijd 0.0 (2D-dwarsprofiel)
    - z                  : float64 – Z-coördinaat / hoogte van het punt

    Yaml-configuratie voorbeeld::

        type: ci_postgresql_damlive_soil
        database: "continuinzicht"
        schema: "continuinzicht_demo_damlive"

    Parameters
    ----------
    output_config : dict
        Configuratiedict met database-verbindingsgegevens en schema/tabel-naam.
        Verwachte sleutels in .env: postgresql_user, postgresql_password,
        postgresql_host, postgresql_port.
        Verwachte sleutels in yaml: database, schema.
    df : pd.DataFrame
        DataFrame met de grondlaagdata (zie kolombeschrijving hierboven).
        Gebruik :func:`create_df_damlive_soil` om dit DataFrame aan te maken.

    Raises
    ------
    KeyError
        Als verplichte configuratiesleutels ontbreken.
    ValueError
        Als het DataFrame leeg is.
    """
    if df.empty:
        raise ValueError(
            "Het aangeboden DataFrame is leeg. "
            "Controleer of de DAMlive bestanden correct zijn ingelezen."
        )

    schema = output_config["schema"]

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]

    assert all(key in output_config for key in keys)

    if not df.empty:
        if (
            "measuringstationid" in df
            and "parameterid" in df
            and "itemid" in df
            and "layername" in df
            and "color" in df
            and "datetime" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.data_damlive_soil;")

            for _, row in df.iterrows():
                query.append(f"""INSERT INTO {schema}.data_damlive_soil(measuringstationid, parameterid, itemid, layername, color, datetime, index, x, y, z)
                    VALUES ({str(row["measuringstationid"])}, {str(row["parameterid"])}, {str(row["itemid"])}, '{str(row["layername"])}', '{str(row["color"])}',{str(row["datetime"])}, {str(row["index"])}, {str(row["x"])}, {str(row["y"])}, {str(row["z"])});""")

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


# ---------------------------------------------------------------------------
# DataAdapter: output_ci_postgresql_damlive_soil_color
# ---------------------------------------------------------------------------


def output_ci_postgresql_damlive_soil_color(
    output_config: dict,
    df: pd.DataFrame,
) -> None:
    """
    Schrijft kleurdata voor grondsoorten naar de Continu Inzicht database
    (tabel: data_damlive_soil_color).

    Het verwachte DataFrame (``df``) heeft de volgende kolommen:

    - soil_name : str   – naam van de grondsoort (kolom 'color' uit colors.csv
                          waar kolom 'type' == 'soil')
    - r         : int64 – rood-component van de RGB-kleur (0-255)
    - g         : int64 – groen-component van de RGB-kleur (0-255)
    - b         : int64 – blauw-component van de RGB-kleur (0-255)
    - color     : str   – HEX kleurcode (bijv. '#A0522D') uit colors.csv
    - stb_name  : str   – zelfde waarde als soil_name

    Yaml-configuratie voorbeeld::

        type: ci_postgresql_damlive_soil_color
        database: "continuinzicht"
        schema: "continuinzicht_demo_damlive"

    Parameters
    ----------
    output_config : dict
        Configuratiedict met database-verbindingsgegevens en schema/tabel-naam.
        Verwachte sleutels in .env: postgresql_user, postgresql_password,
        postgresql_host, postgresql_port.
        Verwachte sleutels in yaml: database, schema.
    df : pd.DataFrame
        DataFrame met de kleurdata (zie kolombeschrijving hierboven).
        Gebruik :func:`bouw_df_damlive_soil_color` om dit DataFrame aan te maken.

    Raises
    ------
    KeyError
        Als verplichte configuratiesleutels ontbreken.
    ValueError
        Als het DataFrame leeg is.
    """
    if df.empty:
        raise ValueError(
            "Het aangeboden DataFrame is leeg. "
            "Controleer of colors.csv correct is ingelezen en type=='soil' rijen bevat."
        )

    schema = output_config["schema"]

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]

    assert all(key in output_config for key in keys)

    if not df.empty:
        if (
            "soil_name" in df
            and "r" in df
            and "g" in df
            and "b" in df
            and "color" in df
            and "stb_name" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.data_damlive_soil_color;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.data_damlive_soil_color(soil_name, r, g, b, color, stb_name) VALUES ('{row['soil_name']}', {row['r']}, {row['g']}, {row['b']}, '{row['color']}', '{row['stb_name']}');"
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


def output_ci_postgresql_damlive_lines(
    output_config: dict,
    df: pd.DataFrame,
) -> None:
    """
    Schrijft freatische lijnen en glijvlak  naar de Continu Inzicht database
    (tabel: data_damlive_lines).

    Het verwachte DataFrame (``df``) heeft de volgende kolommen:

    - measuringstationid : int64 – altijd 1
    - modelid : int64 – model ID
    - parameterid : int64 – parameter ID
    - itemid : int64 – item ID
    - layername : str – naam van de laag
    - color : str – kleurcode
    - datetime : float – huidige tijd als epoch milliseconden
    - index : int – volgnummer van het punt binnen de lijn (start bij 0)
    - x : float – x-coördinaat van het punt
    - y : float – y-coördinaat van het punt
    - z : float – hoogte van het punt
    - scenario : str – scenario naam

    Yaml-configuratie voorbeeld::

        type: ci_postgresql_damlive_lines
        database: "continuinzicht"
        schema: "continuinzicht_hhnk_purmer_realtime"

    Parameters
    ----------
    output_config : dict
        Configuratiedict met database-verbindingsgegevens en schema/tabel-naam.
        Verwachte sleutels in .env: postgresql_user, postgresql_password,
        postgresql_host, postgresql_port.
        Verwachte sleutels in yaml: database, schema.
    df : pd.DataFrame
        DataFrame met de freactische lijn en glijvlak (zie kolombeschrijving hierboven).
        Gebruik :func:`create_df_damlive_lines` om dit DataFrame aan te maken.

    Raises
    ------
    KeyError
        Als verplichte configuratiesleutels ontbreken.
    ValueError
        Als het DataFrame leeg is.
    """
    if df.empty:
        raise ValueError(
            "Het aangeboden DataFrame is leeg. "
            "Controleer of de DAMlive bestanden correct zijn ingelezen."
        )

    schema = output_config["schema"]

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]

    assert all(key in output_config for key in keys)

    if not df.empty:
        if (
            "measuringstationid" in df
            and "modelid" in df
            and "parameterid" in df
            and "itemid" in df
            and "layername" in df
            and "color" in df
            and "datetime" in df
            and "index" in df
            and "x" in df
            and "y" in df
            and "z" in df
            and "scenario" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.data_damlive_lines;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.data_damlive_lines(measuringstationid, modelid, parameterid, itemid, layername, color, datetime, index, x, y, z, scenario) VALUES ('{row['measuringstationid']}', '{row['modelid']}', '{row['parameterid']}', '{row['itemid']}', '{row['layername']}', '{row['color']}', {row['datetime']}, {row['index']}, {row['x']}, {row['y']}, {row['z']}, '{row['scenario']}');"
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


def output_ci_postgresql_damlive_lines_enveloping(
    output_config: dict,
    df: pd.DataFrame,
) -> None:
    """
    Schrijft minimum en maximum freatische lijnen naar de Continu Inzicht database
    (tabel: data_damlive_lines_enveloping).

    Het verwachte DataFrame (``df``) heeft de volgende kolommen:

    - measuringstationid : int64 – altijd 1
    - modelid : int64 – model ID
    - parameterid : int64 – parameter ID
    - itemid : int64 – item ID
    - layername : str – naam van de laag
    - color : str – kleurcode
    - datetime : float – huidige tijd als epoch milliseconden
    - index : int – volgnummer van het punt binnen de lijn (start bij 0)
    - x : float – x-coördinaat van het punt
    - y : float – y-coördinaat van het punt
    - z : float – hoogte van het punt
    - scenario : str – scenario naam

    Yaml-configuratie voorbeeld::

        type: ci_postgresql_damlive_lines_enveloping
        database: "continuinzicht"
        schema: "continuinzicht_hhnk_purmer_realtime"

    Parameters
    ----------
    output_config : dict
        Configuratiedict met database-verbindingsgegevens en schema/tabel-naam.
        Verwachte sleutels in .env: postgresql_user, postgresql_password,
        postgresql_host, postgresql_port.
        Verwachte sleutels in yaml: database, schema.
    df : pd.DataFrame
        DataFrame met de minimum en maximum freatische lijnen (zie kolombeschrijving hierboven).
        Gebruik :func:`create_df_damlive_lines_enveloping` om dit DataFrame aan te maken.

    Raises
    ------
    KeyError
        Als verplichte configuratiesleutels ontbreken.
    ValueError
        Als het DataFrame leeg is.
    """
    if df.empty:
        raise ValueError(
            "Het aangeboden DataFrame is leeg. "
            "Controleer of de DAMlive bestanden correct zijn ingelezen."
        )

    schema = output_config["schema"]

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]

    assert all(key in output_config for key in keys)

    if not df.empty:
        if (
            "measuringstationid" in df
            and "modelid" in df
            and "parameterid" in df
            and "itemid" in df
            and "layername" in df
            and "color" in df
            and "datetime" in df
            and "index" in df
            and "x" in df
            and "y" in df
            and "z" in df
            and "scenario" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.data_damlive_lines_enveloping;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.data_damlive_lines_enveloping(measuringstationid, modelid, parameterid, itemid, layername, color, datetime, index, x, y, z, scenario) VALUES ('{row['measuringstationid']}', '{row['modelid']}', '{row['parameterid']}', '{row['itemid']}', '{row['layername']}', '{row['color']}', {row['datetime']}, {row['index']}, {row['x']}, {row['y']}, {row['z']}, '{row['scenario']}');"
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


def output_ci_postgresql_damlive_values(
    output_config: dict,
    df: pd.DataFrame,
) -> None:
    """
    Schrijft belangrijkste resultaten DAM-Live naar de Continu Inzicht database
    (tabel: data_damlive_lines_values).

    Het verwachte DataFrame (``df``) heeft de volgende kolommen:

    - measuringstationid : int64 – altijd 1
    - modelid : int64 – model ID
    - datetime :   float – huidige tijd als epoch milliseconden
    - stability_factor : float – stabiliteitsfactor
    - number_of_slices : int – aantal slices
    - xcentrepoint : float – x-coördinaat van het middelpunt
    - ycentrepoint : float – y-coördinaat van het middelpunt
    - radius : float – straal
    - xcoordinate_left_surface : float – x-coördinaat van het linkeroppervlak
    - xcoordinate_right_surface : float – x-coördinaat van het rechteroppervlak
    - scenario : str – scenario naam
    - stateid : int – status ID

    Yaml-configuratie voorbeeld::

        type: ci_postgresql_damlive_lines_values
        database: "continuinzicht"
        schema: "continuinzicht_hhnk_purmer_realtime"

    Parameters
    ----------
    output_config : dict
        Configuratiedict met database-verbindingsgegevens en schema/tabel-naam.
        Verwachte sleutels in .env: postgresql_user, postgresql_password,
        postgresql_host, postgresql_port.
        Verwachte sleutels in yaml: database, schema.
    df : pd.DataFrame
        DataFrame met belangrijkste resultaten DAM-Live (zie kolombeschrijving hierboven).
        Gebruik :func:`create_df_damlive_values` om dit DataFrame aan te maken.

    Raises
    ------
    KeyError
        Als verplichte configuratiesleutels ontbreken.
    ValueError
        Als het DataFrame leeg is.
    """
    if df.empty:
        raise ValueError(
            "Het aangeboden DataFrame is leeg. "
            "Controleer of de DAMlive bestanden correct zijn ingelezen."
        )

    schema = output_config["schema"]

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]

    assert all(key in output_config for key in keys)

    if not df.empty:
        if (
            "measuringstationid" in df
            and "modelid" in df
            and "datetime" in df
            and "stability_factor" in df
            and "number_of_slices" in df
            and "xcentrepoint" in df
            and "ycentrepoint" in df
            and "radius" in df
            and "xcoordinate_left_surface" in df
            and "xcoordinate_right_surface" in df
            and "scenario" in df
            and "stateid" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.data_damlive_values;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.data_damlive_values(measuringstationid, modelid, datetime, stability_factor, number_of_slices, xcentrepoint, ycentrepoint, radius, xcoordinate_left_surface, xcoordinate_right_surface, scenario, stateid) VALUES ('{row['measuringstationid']}', '{row['modelid']}', {row['datetime']}, {row['stability_factor']}, {row['number_of_slices']}, {row['xcentrepoint']}, {row['ycentrepoint']}, {row['radius']}, {row['xcoordinate_left_surface']}, {row['xcoordinate_right_surface']}, '{row['scenario']}', '{row['stateid']}');"
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


def output_ci_postgresql_damlive_conditions(
    output_config: dict,
    df: pd.DataFrame,
) -> None:
    """
    Schrijft de grenswaarden van het DAM-Live meetstations naar de Continu Inzicht database
    (tabel: data_damlive_lines_conditions).

    Het verwachte DataFrame (``df``) heeft de volgende kolommen:

    - measuringstationid : int64 – altijd 1
    - stateid : int64 – status ID
    - modelid : int64 – model ID
    - color : str – kleur
    - upperboundary : float – bovenste grenswaarde
    - lowerboundary : float – onderste grenswaarde
    - name : str – naam

    Yaml-configuratie voorbeeld::

        type: ci_postgresql_damlive_lines_conditions
        database: "continuinzicht"
        schema: "continuinzicht_hhnk_purmer_realtime"

    Parameters
    ----------
    output_config : dict
        Configuratiedict met database-verbindingsgegevens en schema/tabel-naam.
        Verwachte sleutels in .env: postgresql_user, postgresql_password,
        postgresql_host, postgresql_port.
        Verwachte sleutels in yaml: database, schema.
    df : pd.DataFrame
        DataFrame met grenswaarden (zie kolombeschrijving hierboven).
        Gebruik :func:`create_df_damlive_conditions` om dit DataFrame aan te maken.

    Raises
    ------
    KeyError
        Als verplichte configuratiesleutels ontbreken.
    ValueError
        Als het DataFrame leeg is.
    """
    if df.empty:
        raise ValueError(
            "Het aangeboden DataFrame is leeg. "
            "Controleer of de DAMlive bestanden correct zijn ingelezen."
        )

    schema = output_config["schema"]

    keys = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]

    assert all(key in output_config for key in keys)

    if not df.empty:
        if (
            "measuringstationid" in df
            and "stateid" in df
            and "modelid" in df
            and "color" in df
            and "upperboundary" in df
            and "lowerboundary" in df
            and "name" in df
        ):
            query = []

            query.append(f"TRUNCATE {schema}.data_damlive_lines_conditions;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.data_damlive_lines_conditions(measuringstationid, stateid, modelid, color, upperboundary, lowerboundary, name) VALUES ('{row['measuringstationid']}', '{row['stateid']}', '{row['modelid']}', '{row['color']}', {row['upperboundary']}, {row['lowerboundary']}, '{row['name']}');"
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
