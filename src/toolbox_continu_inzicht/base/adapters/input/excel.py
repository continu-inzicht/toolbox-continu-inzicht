import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_excel(input_config: dict) -> pd.DataFrame:
    """Laadt een Excel-bestand in gegeven een pad

    Returns:
    --------
    pd.Dataframe
    """
    path = input_config["abs_path"]

    kwargs = get_kwargs(pd.read_excel, input_config)

    df = pd.read_excel(path, **kwargs)
    return df
