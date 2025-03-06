import pandas as pd
import xarray as xr

from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_netcdf(input_config: dict) -> pd.DataFrame:
    """Laadt een NetCDF-bestand in gegeven een pad

    Notes:
    --------
    Lees het NetCDF-bestand met xarray in en converteer de dataset naar
    een pandas dataframe.

    Returns:
    --------
    pd.Dataframe
    """
    # Data checks worden gedaan in de functies zelf, hier alleen geladen
    abs_path = input_config["abs_path"]
    kwargs = get_kwargs(xr.open_dataset, input_config)
    ds = xr.open_dataset(abs_path, **kwargs)

    # netcdf dataset to pandas dataframe
    df = xr.Dataset.to_dataframe(ds)
    return df
