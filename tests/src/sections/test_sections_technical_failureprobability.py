import os
import pandas as pd

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.sections import SectionsTechnicalFailureprobability


def create_data_adapter(config_file: str) -> DataAdapter:
    """
    Initialiseer een data adapter

    config:
    SectionsTechnicalFailureprobability:
        MISSING_VALUE: -9999.0

    apdaters:
    in_section_fragilitycurves:
        type: csv
        path: "test_sections_technical_failureprobability_fragility_curves.csv"

    in_section_fragilitycurves_nodata:
        type: csv
        path: "test_sections_technical_failureprobability_fragility_curves_nodata.csv"

    in_section_load:
        type: csv
        path: "test_sections_technical_failureprobability_load.csv"

    out_section_failureprobability:
        type: csv
        path: "hidden_sections_technical_failureprobability.csv"

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


def test_valid_run():
    """
    Test of een valide invoer een normaal uitkomst geeft.
    """

    # Aanmaken adapter
    data_adapter = create_data_adapter(
        "test_sections_technical_failureprobability_config.yaml"
    )

    # Bepaal uitvoerbestand
    output_file = get_output_file(data_adapter, "out_section_failureprobability")

    # Oude gegevens verwijderen
    if os.path.exists(output_file):
        os.remove(output_file)

    # Initialiseer SectionsTechnicalFailureprobability functie
    sections_failureprobability = SectionsTechnicalFailureprobability(
        data_adapter=data_adapter
    )

    # normale invoerbestanden
    sections_failureprobability.run(
        input=["in_section_fragilitycurves", "in_section_load"],
        output="out_section_failureprobability",
    )

    assert os.path.exists(output_file)
    df_output = pd.read_csv(output_file)

    assert df_output is not None
    assert len(df_output) == 36


def test_no_curve_run():
    """
    Test of een valide invoer een normaal uitkomst geeft.
    """

    # Aanmaken adapter
    data_adapter = create_data_adapter(
        "test_sections_technical_failureprobability_config.yaml"
    )

    # Bepaal uitvoerbestand
    output_file = get_output_file(data_adapter, "out_section_failureprobability")

    # Oude gegevens verwijderen
    if os.path.exists(output_file):
        os.remove(output_file)

    # Initialiseer SectionsTechnicalFailureprobability functie
    sections_failureprobability = SectionsTechnicalFailureprobability(
        data_adapter=data_adapter
    )

    try:
        # no curve invoerbestand
        input_lst = ["in_section_fragilitycurves_nodata", "in_section_load"]
        sections_failureprobability.run(
            input=input_lst,
            output="out_section_failureprobability",
        )
    except UserWarning as user_warning:
        warning_message = str(user_warning)
        assert warning_message.startswith(
            f"Ophalen van gegevens van {input_lst[0]} heeft niets opgeleverd."
        )

    assert not os.path.exists(output_file)
