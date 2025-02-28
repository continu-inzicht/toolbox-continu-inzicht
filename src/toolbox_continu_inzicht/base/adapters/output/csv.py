import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def output_csv(output_config: dict, df: pd.DataFrame):
    """Schrijft een CSV-bestand in gegeven een pad

    Notes:
    ------
    Gebruikt hiervoor de pandas.DataFrame.to_csv
    Opties om dit aan te passen kunnen worden meegegeven in het configuratiebestand.

    Returns:
    --------
    None
    """
    # Data checks worden gedaan in de functies zelf, hier alleen geladen
    path = output_config["abs_path"]
    kwargs = get_kwargs(pd.DataFrame.to_csv, output_config)
    df.to_csv(path, **kwargs)
