"""
Data adapters voor het schrijven naar de Continu Inzicht database
"""

import pandas as pd
import sqlalchemy


def output_ci_postgresql_impactanalyse_failuremechanism(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de failuremechanism data naar de Continu Inzicht database (tabel: failuremechanism).

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:
        if "id" in df and "name" in df and "description" in df:
            query = []

            if truncate:
                query.append(f"TRUNCATE TABLE {schema}.failuremechanism;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.failuremechanism(id, name, description) VALUES ({str(row['id'])}, '{row['name']}','{row['description']}');"
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.failuremechanism_id_seq', (SELECT MAX(id) FROM {schema}.failuremechanism));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_conditions(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de conditions data naar de Continu Inzicht database (tabel: conditions).

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:  # id, name, description, upper_boundary, color
        if (
            "id" in df
            and "name" in df
            and "description" in df
            and "upper_boundary" in df
            and "color" in df
        ):
            query = []

            if truncate:
                query.append(f"TRUNCATE TABLE {schema}.conditions;")

            for _, row in df.iterrows():
                query.append(
                    f"""
                    INSERT INTO {schema}.conditions(id, name, description, upper_boundary, color)
                    VALUES (
                        {str(row["id"])},
                        '{row["name"]}',
                        '{row["description"]}',
                        {str(row["upper_boundary"])},
                        '{row["color"]}'
                    );
                    """
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.conditions_id_seq', (SELECT MAX(id) FROM {schema}.conditions));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_section(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft secties naar de Continu Inzicht database (tabel: sections).

    Yaml example:\n
        type: ci_postgresql_section
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - id: int64                         : id van de sectie
        - name: str                         : naam van de sectie
        - hydraulicload_location_id: int64  : id van het meetstation waartoe de sectie behoort
        - overleefde_belasting: float64     : belasting waarvan is aangetoond dat de dijk niet bezwijkt
        - geometry: geom                    : geometrie (ligging) van de sectie (let op projectie altijd EPSG4326!)

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:
        if (
            "id" in df
            and "hydraulicload_location_id" in df
            and "name" in df
            and "overleefde_belasting" in df
            and "geometry" in df
        ):
            query = []

            if truncate:
                query.append(f"TRUNCATE TABLE {schema}.sections;")

            for _, row in df.iterrows():
                query.append(
                    f"""
                        INSERT INTO {schema}.sections(id, name, hydraulicload_location_id, overleefde_belasting, geometry)
                        VALUES (
                            {str(row["id"])},
                            '{str(row["name"])}',
                            {str(row["hydraulicload_location_id"])},
                            {str(row["overleefde_belasting"])},
                            '{str(row["geometry"])}'
                        );
                    """
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.sections_id_seq', (SELECT MAX(id) FROM {schema}.sections));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_hydraulicload_locations(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor meetstations de classificatie data naar de Continu Inzicht database (tabel: hydraulicload_locations).

    Yaml example:\n
        type: ci_postgresql_measuringstation
        database: "continuinzicht"
        schema: "continuinzicht_demo_whatif"

    Args:\n
        * output_config (dict): configuratie opties
        * df (DataFrame):\n
        - id: int64                 : id van het meetstation
        - name: str                 : naam van het meetstation
        - geometry: geom            : geometrie (ligging) van het meetstation (let op projectie!)

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:
        if "id" in df and "name" in df and "geometry" in df:
            query = []

            if truncate:
                query.append(f"TRUNCATE TABLE {schema}.hydraulicload_locations;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.hydraulicload_locations(id, name, geometry) VALUES ({str(row['id'])}, '{row['name']}','{str(row['geometry'])}');"
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.hydraulicload_locations_id_seq', (SELECT MAX(id) FROM {schema}.hydraulicload_locations));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_statistics(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de statistics data naar de Continu Inzicht database (tabel: statistics).

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:
        if (
            "id" in df
            and "name" in df
            and "hydraulicload_location_id"
            and "hydraulicload" in df
            and "probability_exceedance" in df
        ):
            query = []

            if truncate:
                query.append(f"TRUNCATE {schema}.statistics;")
                query.append(f"TRUNCATE {schema}.statistics_data;")

            df_statistics = df.drop_duplicates(subset=["id", "name"])[
                ["id", "name"]
            ].reset_index(drop=True)

            for _, row in df_statistics.iterrows():
                query.append(
                    f"INSERT INTO {schema}.statistics(id, name) VALUES ({str(row['id'])}, '{row['name']}');"
                )

            df_statistics_data = df[
                [
                    "id",
                    "hydraulicload_location_id",
                    "hydraulicload",
                    "probability_exceedance",
                ]
            ]
            df_statistics_data = df_statistics_data.rename(
                columns={"id": "statistics_id"}
            )

            for _, row in df_statistics_data.iterrows():
                query.append(
                    f"""
                    INSERT INTO {schema}.statistics_data(statistics_id, hydraulicload_location_id, hydraulicload, probability_exceedance)
                    VALUES
                    (
                        {str(row["statistics_id"])},
                        {row["hydraulicload_location_id"]},
                        {str(row["hydraulicload"])},
                        {str(row["probability_exceedance"])}
                    );
                    """
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.statistics_id_seq', (SELECT MAX(id) FROM {schema}.statistics));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_variants(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de varianten data naar de Continu Inzicht database (tabel: variants).

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:
        if "id" in df and "name" in df and "statistics_id" in df:
            query = []

            if truncate:
                query.append(f"TRUNCATE TABLE {schema}.variants;")

            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.variants(id, name, statistics_id) VALUES ({str(row['id'])}, '{row['name']}',{str(row['statistics_id'])});"
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.variants_id_seq', (SELECT MAX(id) FROM {schema}.variants));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_fragilitycurves(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de fragility curves data naar de Continu Inzicht database (tabel: "fragilitycurves" en "fragilitycurves_data").

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    insert_id: bool = False
    if "insert_id" in output_config:
        insert_id = output_config["insert_id"]

    if not df.empty:
        if (
            "id" in df
            and "name" in df
            and "section_id"
            and "failuremechanism_id" in df
            and "hydraulicload"
            and "failure_probability" in df
        ):
            query = []

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            if truncate:
                query.append(f"TRUNCATE TABLE {schema}.fragilitycurves;")
                query.append(f"TRUNCATE TABLE {schema}.fragilitycurves_data;")

            df_fragilitycurves = df.drop_duplicates(subset=["id", "name"])[
                ["id", "name"]
            ].reset_index(drop=True)

            if not truncate:
                df_fragilitycurves_data = df.drop_duplicates(
                    subset=["id", "section_id", "failuremechanism_id"]
                )[["id", "section_id", "failuremechanism_id"]].reset_index(drop=True)

                # Lijst om de tuples op te slaan
                tuples = []

                # Itereren over de rijen van de DataFrame om de tuples te maken
                for index, row in df_fragilitycurves_data.iterrows():
                    tuples.append(
                        f"({row['id']},{row['section_id']},{row['failuremechanism_id']})"
                    )

                # De tuples combineren in de gewenste string
                delete_query = f"""
                    DELETE FROM {schema}.fragilitycurves_data WHERE (fragilitycurves_id, section_id, failuremechanism_id) IN ({",".join(tuples)});
                """

                with engine.connect() as connection:
                    connection.execute(sqlalchemy.text(str(delete_query)))
                    connection.commit()  # commit the transaction

            if insert_id:
                # id mag niet bestaan
                for _, row in df_fragilitycurves.iterrows():
                    query.append(
                        f"INSERT INTO {schema}.fragilitycurves(id, name) VALUES ({str(row['id'])}, '{row['name']}');"
                    )

            df_fragilitycurves_data = df[
                [
                    "id",
                    "section_id",
                    "failuremechanism_id",
                    "hydraulicload",
                    "failure_probability",
                ]
            ]
            df_statistics_data = df_fragilitycurves_data.rename(
                columns={"id": "fragilitycurves_id"}
            )

            for _, row in df_statistics_data.iterrows():
                query.append(
                    f"""
                    INSERT INTO {schema}.fragilitycurves_data(fragilitycurves_id, section_id, failuremechanism_id, hydraulicload, failure_probability)
                    VALUES
                    (
                        {str(row["fragilitycurves_id"])},
                        {row["section_id"]},
                        {row["failuremechanism_id"]},
                        {str(row["hydraulicload"])},
                        {str(row["failure_probability"])}
                    );
                    """
                )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.fragilitycurves_id_seq', (SELECT MAX(id) FROM {schema}.fragilitycurves));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_fragilitycurves_simple(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de fragility curves data naar de Continu Inzicht database (tabel: "fragilitycurves" en "fragilitycurves_data").

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
        if "name" in df:
            query = []

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            # id mag niet bestaan
            for _, row in df.iterrows():
                query.append(
                    f"INSERT INTO {schema}.fragilitycurves(name) VALUES ('{row['name']}');"
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


def output_ci_postgresql_impactanalyse_expertjudgements(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de beheerdersoordelen naar de Continu Inzicht database (tabel: expertjudgements).

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:
        if (
            "name" in df
            and "variant_id" in df
            and "fragilitycurves_base_id" in df
            and "fragilitycurves_id" in df
        ):
            query = []

            if truncate:
                query.append(f"TRUNCATE TABLE {schema}.expertjudgements;")

            for _, row in df.iterrows():
                if "id" in df:
                    query.append(
                        f"""
                        INSERT INTO {schema}.expertjudgements(id, name, variant_id, fragilitycurves_base_id, fragilitycurves_id)
                        VALUES (
                            {str(row["id"])},
                            '{row["name"]}',
                            {str(row["variant_id"])},
                            {str(row["fragilitycurves_base_id"])},
                            {str(row["fragilitycurves_id"])}
                        );
                        """
                    )
                else:
                    query.append(
                        f"""
                        INSERT INTO {schema}.expertjudgements(name, variant_id, fragilitycurves_base_id, fragilitycurves_id)
                        VALUES (
                            '{row["name"]}',
                            {str(row["variant_id"])},
                            {str(row["fragilitycurves_base_id"])},
                            {str(row["fragilitycurves_id"])}
                        );
                        """
                    )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.expertjudgements_id_seq', (SELECT MAX(id) FROM {schema}.expertjudgements));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_fragilitycurves_intergrate(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de fragilitycurves_intergrate naar de Continu Inzicht database
    (tabel: fragilitycurves_intergrate en fragilitycurves_intergrate_data).

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:
        if (
            "expertjudgement_id" in df
            and "section_id" in df
            and "failuremechanism_id" in df
            and "load_limit" in df
            and "probability_contribution_reductionfactor" in df
        ):
            query = []

            if truncate:
                query.append(f"TRUNCATE TABLE {schema}.fragilitycurves_intergrate;")

            for _, row in df.iterrows():
                query.append(
                    f"""
                    INSERT INTO {schema}.fragilitycurves_intergrate(expertjudgement_id, section_id, failuremechanism_id, load_limit, probability_contribution_reductionfactor)
                    VALUES (
                        {str(row["expertjudgement_id"])},
                        {str(row["section_id"])},
                        {str(row["failuremechanism_id"])},
                        {str(row["load_limit"])},
                        {str(row["probability_contribution_reductionfactor"])}
                    );
                    """
                )

            # maak verbinding object
            engine = sqlalchemy.create_engine(
                f"postgresql://{output_config['postgresql_user']}:{output_config['postgresql_password']}@{output_config['postgresql_host']}:{int(output_config['postgresql_port'])}/{output_config['database']}"
            )

            query = " ".join(query)

            with engine.connect() as connection:
                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

                query = f"""
                    SELECT SETVAL('{schema}.fragilitycurves_intergrate_id_seq', (SELECT MAX(id) FROM {schema}.fragilitycurves_intergrate));
                """

                connection.execute(sqlalchemy.text(str(query)))
                connection.commit()

            # verbinding opruimen
            engine.dispose()
        else:
            raise UserWarning("Ontbrekende variabelen in dataframe!")

    else:
        raise UserWarning("Geen gegevens om op te slaan.")


def output_ci_postgresql_impactanalyse_fragilitycurves_intergrate_data(
    output_config: dict, df: pd.DataFrame
) -> None:
    """
    Schrijft voor impactanalyse de fragilitycurves_intergrate naar de Continu Inzicht database
    (tabel: fragilitycurves_intergrate en fragilitycurves_intergrate_data).

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

    truncate: bool = False
    if "truncate" in output_config:
        truncate = output_config["truncate"]

    if not df.empty:
        if (
            "intergrate_id" in df
            and "hydraulicload" in df
            and "probability_contribution" in df
        ):
            query = []

            if truncate:
                query.append(f"TRUNCATE {schema}.fragilitycurves_intergrate_data;")

            for _, row in df.iterrows():
                query.append(
                    f"""
                    INSERT INTO {schema}.fragilitycurves_intergrate_data(intergrate_id, hydraulicload, probability_contribution)
                    VALUES (
                        {str(row["intergrate_id"])},
                        {str(row["hydraulicload"])},
                        {str(row["probability_contribution"])}
                    );
                    """
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
