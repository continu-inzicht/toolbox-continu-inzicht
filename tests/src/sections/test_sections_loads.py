import os
import pandas as pd
from pandas.errors import ParserError

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.sections import SectionsLoads


def create_data_adapter(config_file: str) -> DataAdapter:
    """
    Initialiseer een data adapter

    config:
    SectionsLoads:
        MISSING_VALUE: -9999.0

    apdaters:
    in_dijkvakken:
        type: csv
        path: "test_sections_loads_dijkvakken.csv"
    in_dijkvakken_fout:
        type: csv
        path: "test_sections_loads_dijkvakken_fout.csv"
    in_dijkvakken_no_csv:
        type: csv
        path: "test_sections_loads_dijkvakken.json"
    in_dijkvakken_leeg:
        type: csv
        path: "test_sections_loads_dijkvakken_leeg.csv"
    in_dijkvakken_fout_data_type:
        type: fout_type
        path: "test_sections_loads_dijkvakken.csv"
    in_waterstanden:
        type: csv
        path: "test_sections_loads_waterstanden.csv"
    in_waterstanden_fout_nan:
        type: csv
        path: "test_sections_loads_waterstanden_fout_nan.csv"
    in_koppeling_meetstation_dijkvak:
        type: csv
        path: "test_sections_loads_koppeling_meetstation_dijkvak.csv"
    out_waterstanden_per_dijkvak:
        type: csv
        path: "hidden_waterstanden_per_dijkvak.csv"

    Args:
        config_file (str): configuratie file

    Returns:
        DataAdapter: data adapter
    """

    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / config_file)
    config.lees_config()

    return DataAdapter(config=config)


def get_output_file(data_adapter: DataAdapter, section_key: str) -> Path:
    # Bepaal uitvoerbestand
    output_info = data_adapter.config.data_adapters
    return Path(
        data_adapter.config.global_variables["rootdir"]
        / Path(output_info[section_key]["path"])
    )


def get_adapter_type(data_adapter: DataAdapter, section_key: str) -> str:
    # Bepaal datatype
    output_info = data_adapter.config.data_adapters
    return output_info[section_key]["type"]


def test_valid_run():
    """
    Test of een valide invoer een normaal uitkomst geeft.
    """

    # Aanmaken adapter
    data_adapter = create_data_adapter("test_sections_loads_config.yaml")

    # Bepaal uitvoerbestand
    output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

    # Oude gegevens verwijderen
    if os.path.exists(output_file):
        os.remove(output_file)

    # Initialiseer SectionsLoads functie
    sections_loads = SectionsLoads(data_adapter=data_adapter)

    # normale invoerbestanden
    sections_loads.run(
        input=["in_dijkvakken", "in_waterstanden", "in_koppeling_meetstation_dijkvak"],
        output="out_waterstanden_per_dijkvak",
    )

    assert os.path.exists(output_file)
    df_output = pd.read_csv(output_file)

    assert df_output is not None
    assert len(df_output) == 8250


def test_invalid_file_run():
    """
    Test of een invalide invoer een invalide uitkomst geeft.
    """
    try:
        # Aanmaken adapter
        data_adapter = create_data_adapter("test_sections_loads_config.yaml")

        # Bepaal uitvoerbestand
        output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # Initialiseer SectionsLoads functie
        sections_loads = SectionsLoads(data_adapter=data_adapter)

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # foute dijkvakken invoer
        sections_loads.run(
            input=[
                "in_dijkvakken_fout",
                "in_waterstanden",
                "in_koppeling_meetstation_dijkvak",
            ],
            output="out_waterstanden_per_dijkvak",
        )

    except UserWarning as user_warning:
        warning_message = str(user_warning)
        assert warning_message.startswith("Kolommen komen niet overeen.")

    except Exception as exception:
        warning_message = str(exception)
        assert warning_message.startswith("??")

    assert not os.path.exists(output_file)


def test_invalid_file_with_nan_run():
    """
    Test of een invalide invoer met 'not a number'  een invalide uitkomst geeft.
    """
    try:
        # Aanmaken adapter
        data_adapter = create_data_adapter("test_sections_loads_config.yaml")

        # Bepaal uitvoerbestand
        output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # Initialiseer SectionsLoads functie
        sections_loads = SectionsLoads(data_adapter=data_adapter)

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # foute dijkvakken invoer
        sections_loads.run(
            input=[
                "in_dijkvakken",
                "in_waterstanden_fout_nan",
                "in_koppeling_meetstation_dijkvak",
            ],
            output="out_waterstanden_per_dijkvak",
        )

    except UserWarning as user_warning:
        warning_message = str(user_warning)
        assert warning_message.startswith("Kolommen komen niet overeen.")

    except Exception as exception:
        warning_message = str(exception)
        assert warning_message.startswith("??")

    assert os.path.exists(output_file)
    df_output = pd.read_csv(output_file)

    assert df_output is not None
    assert len(df_output) == 6


def test_no_csv_file_run():
    """
    Test of een invalide invoer een invalide uitkomst geeft.
    """

    try:
        # Aanmaken adapter
        data_adapter = create_data_adapter("test_sections_loads_config.yaml")

        # Bepaal uitvoerbestand
        output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # Initialiseer SectionsLoads functie
        sections_loads = SectionsLoads(data_adapter=data_adapter)

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # foute dijkvakken invoer
        sections_loads.run(
            input=[
                "in_dijkvakken_no_csv",
                "in_waterstanden",
                "in_koppeling_meetstation_dijkvak",
            ],
            output="out_waterstanden_per_dijkvak",
        )

    except ParserError as parse_error:
        warning_message = str(parse_error)
        assert warning_message.startswith("Error tokenizing data.")

    except Exception as exception:
        warning_message = str(exception)
        assert warning_message.startswith("??")

    assert not os.path.exists(output_file)


def test_empty_file_run():
    """
    Test of een leeg invoer de juiste foutmelding geeft.
    """

    try:
        # Aanmaken adapter
        data_adapter = create_data_adapter("test_sections_loads_config.yaml")

        # Bepaal uitvoerbestand
        output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # Initialiseer SectionsLoads functie
        sections_loads = SectionsLoads(data_adapter=data_adapter)

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # foute dijkvakken invoer
        sections_loads.run(
            input=[
                "in_dijkvakken_leeg",
                "in_waterstanden",
                "in_koppeling_meetstation_dijkvak",
            ],
            output="out_waterstanden_per_dijkvak",
        )

    except UserWarning as user_warning:
        warning_message = str(user_warning)
        assert warning_message.startswith(
            "Ophalen van gegevens heeft niets opgeleverd."
        )

    except Exception as exception:
        warning_message = str(exception)
        assert warning_message.startswith("??")

    assert not os.path.exists(output_file)


def test_invalid_key_run():
    """
    Test of een invalide sectie sleutel de juiste foutmelding geeft.
    """

    try:
        # Aanmaken adapter
        data_adapter = create_data_adapter("test_sections_loads_config.yaml")

        # Bepaal uitvoerbestand
        output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # Initialiseer SectionsLoads functie
        sections_loads = SectionsLoads(data_adapter=data_adapter)

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        wrong_key = "in_dijkvakken_die_niet_bestaat"

        # foute dijkvakken invoer
        sections_loads.run(
            input=[wrong_key, "in_waterstanden", "in_koppeling_meetstation_dijkvak"],
            output="out_waterstanden_per_dijkvak",
        )

    except UserWarning as user_warning:
        warning_message = str(user_warning)
        assert warning_message.startswith(
            f"Adapter met de naam '{wrong_key}' niet gevonden in de configuratie (yaml)."
        )

    except Exception as exception:
        warning_message = str(exception)
        assert warning_message.startswith("??")

    assert not os.path.exists(output_file)


def test_invalid_adapter_type_run():
    """
    Test of een invalide adaptertype de juiste foutmelding geeft.
    """

    try:
        # Aanmaken adapter
        data_adapter = create_data_adapter("test_sections_loads_config.yaml")

        # Bepaal uitvoerbestand
        output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # Initialiseer SectionsLoads functie
        sections_loads = SectionsLoads(data_adapter=data_adapter)

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        data_type = get_adapter_type(data_adapter, "in_dijkvakken_fout_data_type")

        # foute dijkvakken invoer
        sections_loads.run(
            input=[
                "in_dijkvakken_fout_data_type",
                "in_waterstanden",
                "in_koppeling_meetstation_dijkvak",
            ],
            output="out_waterstanden_per_dijkvak",
        )

    except UserWarning as user_warning:
        warning_message = str(user_warning)
        assert warning_message.startswith(
            f"Adapter van het type '{data_type}' niet gevonden."
        )

    except Exception as exception:
        warning_message = str(exception)
        assert warning_message.startswith("??")

    assert not os.path.exists(output_file)


def test_less_input_run():
    """
    Test of te weinig invoer strings de juiste foutmelding geeft.
    """

    try:
        # Aanmaken adapter
        data_adapter = create_data_adapter("test_sections_loads_config.yaml")

        # Bepaal uitvoerbestand
        output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # Initialiseer SectionsLoads functie
        sections_loads = SectionsLoads(data_adapter=data_adapter)

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # foute dijkvakken invoer
        sections_loads.run(
            input=["in_waterstanden", "in_koppeling_meetstation_dijkvak"],
            output="out_waterstanden_per_dijkvak",
        )

    except UserWarning as user_warning:
        warning_message = str(user_warning)
        assert warning_message.startswith(
            "Input variabele moet 3 string waarden bevatten."
        )

    except Exception as exception:
        warning_message = str(exception)
        assert warning_message.startswith("??")

    assert not os.path.exists(output_file)


def test_wrong_input_run():
    """
    Test of verkeerde aanroep de juiste foutmelding geeft.
    """

    try:
        # Aanmaken adapter
        data_adapter = create_data_adapter("test_sections_loads_config.yaml")

        # Bepaal uitvoerbestand
        output_file = get_output_file(data_adapter, "out_waterstanden_per_dijkvak")

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # Initialiseer SectionsLoads functie
        sections_loads = SectionsLoads(data_adapter=data_adapter)

        # Oude gegevens verwijderen
        if os.path.exists(output_file):
            os.remove(output_file)

        # foute dijkvakken invoer
        sections_loads.run(
            input="in_waterstanden",
            output="out_waterstanden_per_dijkvak",  # type: ignore (dit is juist de test)
        )

    except UserWarning as user_warning:
        warning_message = str(user_warning)
        assert warning_message.startswith(
            "Input variabele moet 3 string waarden bevatten."
        )

    except Exception as exception:
        warning_message = str(exception)
        assert warning_message.startswith("??")

    assert not os.path.exists(output_file)
