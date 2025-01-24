"""
Utility functions used by different data adapters or the main data adapter.py
"""

import inspect
from pathlib import Path


def get_kwargs(function, input_config: dict) -> dict:
    """
    Gegeven een input/output functie, stuurt de relevanten kwargs uit de input config naar de functie.
    """
    # We kijken welke argumenten we aan de functie kunnen geven
    possible_parameters_function = set(inspect.signature(function).parameters.keys())
    # Vervolgens nemen we alleen de namen van de parameters over die ook opgegeven zijn
    wanted_keys = list(possible_parameters_function.intersection(input_config.keys()))
    # en geven we een kwargs dictionary door aan de inlees functie
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
                f"De rootdir map '{global_variables['rootdir']}' bestaat niet"
            )


def check_file_and_path(function_config: dict, global_vars: dict) -> None:
    # pad relatief tot rootdir mee gegeven in config
    if "file" in function_config:
        # als de gebruiker een compleet pad mee geeft:
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
                f"Check if root dir ({global_vars['rootdir']}) and file ({function_config['file']}) exist"
            )
    # als een pad wordt mee gegeven
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
    Checks user defined plugin path

    Parameters:
    -----------

    global_variables: dict
        Dictionary containing global variables from the config

    prefix: str
        prefix names: ['input_','ouput_'] to look for in the config

    """

    if f"{prefix}plugin_path" in global_variables:
        plugin_path = Path(global_variables[f"{prefix}plugin_path"])
        if plugin_path.is_dir():
            return plugin_path
        else:
            raise UserWarning(
                f"Global Variable `plugin_path` niet gevonden: {global_variables[f'{prefix}plugin_path']}"
            )
    else:
        return None
