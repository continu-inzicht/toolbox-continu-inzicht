import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_csv_pre_calculated_unique_ids(input_config: dict) -> pd.DataFrame:
    """Laadt een CSV-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]
    hr_locid = input_config.get("hr_locid", {})

    kwargs = get_kwargs(pd.read_csv, input_config)

    df_in = pd.read_csv(path, **kwargs)
    df = df_in[(df_in["hr_locid"] == hr_locid)]
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
