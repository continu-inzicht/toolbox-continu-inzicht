"""
Functionaliteit om funcites van een package of directory te laden
"""

from pathlib import Path
import inspect
import sys
import os
import importlib.util
from collections.abc import ItemsView


def get_functions_from_package(package: object, remove_prefix: str) -> dict:
    """
    Haalt alle functies en routines op uit een gegeven package.

    Deze functie gebruikt de inspect-module om alle leden van het opgegeven
    package te doorlopen en voegt alle functies en routines toe aan een
    dictionary. De dictionary bevat de namen van de functies en routines als
    sleutels en de functie- of routine-objecten als waarden.

    Parameters:
    -----------
    package: object
        Het package waarvan de functies en routines moeten worden opgehaald.


    Returns:
    --------
    dict: Een dictionary met de namen van de functies en routines als sleutels
          en de functie- of routine-objecten als waarden.
    """
    functions = {}
    for name, obj in inspect.getmembers(package):
        name = name.removeprefix(remove_prefix)
        if name not in functions:
            if inspect.isfunction(obj) or inspect.isroutine(obj):
                # Haal de signatuur van de functie op
                signatuur = inspect.signature(obj)
                add_item: bool = False

                if signatuur is not None:
                    if signatuur.parameters is not None:
                        if signatuur.parameters is not None:
                            parameter_items: ItemsView = signatuur.parameters.items()
                            # controle of de input functioneert:
                            if len(parameter_items) == 1 and remove_prefix == "input_":
                                for _, parameter in parameter_items:
                                    add_item = (
                                        str(parameter) == "input_config: dict"
                                        or str(parameter) == "input_config"
                                    )
                            # controle of de output functioneert:
                            if len(parameter_items) == 2 and remove_prefix == "output_":
                                items = []
                                for _, parameter in parameter_items:
                                    add_item_1 = (
                                        str(parameter) == "output_config: dict"
                                        or str(parameter) == "output_config"
                                    )
                                    add_item_2 = (
                                        str(parameter)
                                        == "df: pandas.core.frame.DataFrame"
                                        or str(parameter) == "df"
                                        or str(parameter)
                                        == "gdf: geopandas.geodataframe.GeoDataFrame"
                                        or str(parameter) == "gdf"
                                    )
                                    items.append((add_item_1 or add_item_2))
                                add_item = all(items)

                if add_item:
                    functions[name] = obj

    return functions


def load_module_from_file(module_name, module_path) -> object | None:
    """
    Laadt een module dynamisch vanaf een opgegeven bestandspad.

    Deze functie gebruikt de importlib.util-module om een modulespecificatie
    te verkrijgen van een bestandspad en laadt vervolgens de module in het
    sys.modules woordenboek. Als de module succesvol wordt geladen, wordt
    deze geretourneerd.

    Parameters:
    -----------
    module_name: str
        De naam van de module.
    module_path: str
        Het bestandspad naar de module.

    Returns:
    --------
    module:
        De geladen module, of None als de module niet kon worden geladen.
    """
    try:
        module = None
        spec = importlib.util.spec_from_file_location(module_name, module_path)

        if spec is not None:
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module

            if spec.loader is not None:
                spec.loader.exec_module(module)

        return module

    except Exception:
        return None


def get_adapter_functions_from_plugin_path(
    plugin_path: Path, remove_prefix: str
) -> dict:
    """
    Haalt alle functies op uit Python-bestanden in een opgegeven plugin pad.

    Deze functie doorloopt alle Python-bestanden in het opgegeven pad,
    laadt de modules, en haalt alle functies op met behulp van de
    `load_module_from_file` en `get_functions_from_package` functies.
    De functies worden toegevoegd aan een dictionary met de functienamen
    als sleutels en de functie-objecten als waarden.

    Parameters:
    -----------
    path: str
        Het pad naar de directory waarin de Python-bestanden zich bevinden.

    Returns:
    --------
    dict: Een dictionary met de namen van de functies als sleutels en de
          functie-objecten als waarden.
    """
    functions = {}
    for root, _, files in os.walk(plugin_path):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)

                # Laad de module
                module = load_module_from_file(file, file_path)
                module_functions = get_functions_from_package(module, remove_prefix)
                functions.update(module_functions)

    return functions
