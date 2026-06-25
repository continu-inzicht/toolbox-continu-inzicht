"""
DataAdapter voor het wegschrijven van DAMlive grondlaag- en kleurdata
naar de Continu Inzicht PostgreSQL-database.

Doeltabellen:
    - data_damlive_soil       : grondlagen
    - data_damlive_soil_color : kleurwaarden per grondsoort (uit colors.csv)

Brondata:
    - df_geometries  : geometry_id, layer_id, layer_label, points (list[{X, Z}])
    - df_soillayers  : soillayers_id, layer_id, soil_id
    - df_soils       : soil_id, name
    - df_stages      : stage_id, geometry_id, soillayers_id, ...
    - df_colors      : type, color  (colours.csv gefilterd op type=='soil')

Gebruik (yaml config voorbeeld):
    type: ci_postgresql_damlive_soil
    database: "continuinzicht"
    schema: "continuinzicht_demo_damlive"

    type: ci_postgresql_damlive_soil_color
    database: "continuinzicht"
    schema: "continuinzicht_demo_damlive"
"""

from __future__ import annotations

import pandas as pd
import sqlalchemy


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
    - soils.color        : str    – leeg laten (None / NaN)
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
        Gebruik :func:`bouw_df_damlive_soil` om dit DataFrame aan te maken.

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

    table = output_config.get("table", "data_damlive_soil")
    schema = output_config["schema"]
    if_exists = output_config.get("if_exists", "replace")

    verplichte_sleutels = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]
    ontbrekend = [k for k in verplichte_sleutels if k not in output_config]
    if ontbrekend:
        raise KeyError(
            f"De volgende verplichte sleutels ontbreken in output_config: {ontbrekend}. "
            "Controleer het .env-bestand en de yaml-configuratie."
        )

    url = (
        f"postgresql://{output_config['postgresql_user']}"
        f":{output_config['postgresql_password']}"
        f"@{output_config['postgresql_host']}"
        f":{int(output_config['postgresql_port'])}"
        f"/{output_config['database']}"
    )
    engine = sqlalchemy.create_engine(url)

    try:
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
        )
    finally:
        engine.dispose()


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

    table = output_config.get("table", "data_damlive_soil_color")
    schema = output_config["schema"]
    if_exists = output_config.get("if_exists", "replace")

    verplichte_sleutels = [
        "postgresql_user",
        "postgresql_password",
        "postgresql_host",
        "postgresql_port",
        "database",
    ]
    ontbrekend = [k for k in verplichte_sleutels if k not in output_config]
    if ontbrekend:
        raise KeyError(
            f"De volgende verplichte sleutels ontbreken in output_config: {ontbrekend}. "
            "Controleer het .env-bestand en de yaml-configuratie."
        )

    url = (
        f"postgresql://{output_config['postgresql_user']}"
        f":{output_config['postgresql_password']}"
        f"@{output_config['postgresql_host']}"
        f":{int(output_config['postgresql_port'])}"
        f"/{output_config['database']}"
    )
    engine = sqlalchemy.create_engine(url)

    try:
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
        )
    finally:
        engine.dispose()
