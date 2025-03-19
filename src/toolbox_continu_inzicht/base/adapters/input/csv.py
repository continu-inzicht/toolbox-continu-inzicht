import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_csv(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]

    kwargs = get_kwargs(pd.read_csv, input_config)

    df = pd.read_csv(path, **kwargs)
    return df


def input_csv_source(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad en filter op een waarde

    Returns:
    --------
    pd.Dataframe
    """
    df = input_csv(input_config)

    if "source" in df.columns:
        filter_parameter = input_config["filter"]
        filter = f"source.str.contains('{filter_parameter}', case=False)"
        filtered_df = df.query(filter)
        df = filtered_df
    else:
        raise UserWarning("De kolom 'source' is niet aanwezig in het CSV-bestand.")

    return df
