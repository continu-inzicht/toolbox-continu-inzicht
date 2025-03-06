import pandas as pd

from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_json(input_config: dict) -> pd.DataFrame:
    """Lees een JSON bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]

    kwargs = get_kwargs(pd.read_json, input_config)

    df = pd.read_json(path, **kwargs)
    return df
