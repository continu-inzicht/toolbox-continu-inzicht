import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_csv_pre_calculated_unique(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]

    kwargs = get_kwargs(pd.read_csv, input_config)

    df_in = pd.read_csv(path, **kwargs)
    df = pd.DataFrame({"direction": df_in["direction"].unique()})
    return df


def input_csv_pre_calculated_filter(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]
    direction_upper = input_config.get("direction_upper", {})
    direction_lower = input_config.get("direction_lower", {})

    kwargs = get_kwargs(pd.read_csv, input_config)

    df_in = pd.read_csv(path, **kwargs)
    df = df_in[
        (df_in["direction"] <= direction_upper)
        & (df_in["direction"] >= direction_lower)
    ]
    return df
