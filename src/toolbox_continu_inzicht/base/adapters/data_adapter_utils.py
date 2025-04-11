"""
Utility-functies die door verschillende data-adapters of de hoofd data-adapter.py worden gebruikt
"""

import inspect
from pathlib import Path


def get_kwargs(function, input_config: dict) -> dict:
    """
    Gegeven een input/output-functie, stuurt de relevante kwargs uit de input-config naar de functie.
    """
    # We kijken welke argumenten we aan de functie kunnen geven
    possible_parameters_function = set(inspect.signature(function).parameters.keys())
    # Vervolgens nemen we alleen de namen van de parameters over die ook opgegeven zijn
    wanted_keys = list(possible_parameters_function.intersection(input_config.keys()))
    # en geven we een kwargs dictionary door aan de inlees-functie
    return {key: input_config[key] for key in wanted_keys}


def check_rootdir(global_variables: dict) -> None | UserWarning:
    """
    Checkt of de rootdir bestaat
    """
    if "rootdir" in global_variables:
        condition1 = not Path(global_variables["rootdir"]).exists()
        condition2 = not (Path.cwd() / global_variables["rootdir"]).exists()
        if condition1 or condition2:
            raise UserWarning(
                f"De rootdir-map '{global_variables['rootdir']}' bestaat niet"
            )


def check_file_and_path(function_config: dict, global_vars: dict) -> None:
    # pad relatief tot rootdir meegegeven in config
    if "file" in function_config:
        # als de gebruiker een compleet pad meegeeft:
        if Path(global_vars["rootdir"]).is_absolute():
            function_config["abs_path"] = (
                Path(global_vars["rootdir"]) / function_config["file"]
            )
        # als rootdir geen absoluut pad is, nemen we relatief aan
        else:
            function_config["abs_path"] = (
                Path.cwd() / global_vars["rootdir"] / function_config["file"]
            )

        if not function_config["abs_path"].is_absolute():
            raise UserWarning(
                f"Controleer of rootdir ({global_vars['rootdir']}) en bestand ({function_config['file']}) bestaan."
            )
    # als een pad wordt meegegeven
    elif "path" in function_config:
        # eerst checken of het absoluut is
        if Path(function_config["path"]).is_absolute():
            function_config["abs_path"] = Path(function_config["path"])
        # anders alsnog toevoegen
        else:
            function_config["abs_path"] = (
                Path(global_vars["rootdir"]) / function_config["path"]
            )


def check_plugin_path(global_variables: dict, prefix: str) -> None | Path:
    """
    Controleert door gebruiker opgegeven plugin-paden

    Parameters:
    -----------

    global_variables: dict
        Dictionary die global variabelen van de config bevat

    prefix: str
        prefix namen: ['input_','output_'] waarnaar in de config moet worden gekeken

    """

    if f"{prefix}plugin_path" in global_variables:
        plugin_path = Path(global_variables[f"{prefix}plugin_path"])
        if plugin_path.is_dir():
            return plugin_path
        elif (global_variables["rootdir"] / plugin_path).is_dir():
            return (global_variables["rootdir"] / plugin_path).is_dir()
        else:
            raise UserWarning(
                f"Global Variable `plugin_path` niet gevonden: {global_variables[f'{prefix}plugin_path']}"
            )
    else:
        return None
