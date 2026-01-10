import numpy as np
import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_csv_pre_calculated_unique_winddir(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]
    location = input_config.get("location", {})

    kwargs = get_kwargs(pd.read_csv, input_config)

    df_in = pd.read_csv(path, **kwargs)
    df_in = df_in[df_in.location == location]
    df = pd.DataFrame({"winddir": np.sort(df_in["winddir"].unique())})
    return df


def input_csv_pre_calculated_unique_windspeed(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]
    location = input_config.get("location", {})

    kwargs = get_kwargs(pd.read_csv, input_config)

    df_in = pd.read_csv(path, **kwargs)
    df_in = df_in[df_in.location == location]
    df = pd.DataFrame({"windspeed": np.sort(df_in["windspeed"].unique())})
    return df


def input_csv_pre_calculated_unique_ids(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]
    location = input_config.get("location", {})
    ws_bracket = input_config.get("windspeed_bracket", {})
    wd_bracket = input_config.get("winddir_bracket", {})

    kwargs = get_kwargs(pd.read_csv, input_config)

    df_in = pd.read_csv(path, **kwargs)
    df = df_in[
        (df_in.location == location)
        & df_in.windspeed.isin(ws_bracket)
        & df_in.winddir.isin(wd_bracket)
    ]
    return df


def input_csv_pre_calculated_filter(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]
    waveval_bracket = input_config.get("waveval_bracket", {})

    kwargs = get_kwargs(pd.read_csv, input_config)

    df_in = pd.read_csv(path, **kwargs)
    df = df_in[df_in["waveval_id"].isin(waveval_bracket)]
    return df
