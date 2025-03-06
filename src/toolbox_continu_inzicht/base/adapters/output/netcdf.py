import xarray as xr
import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def output_netcdf(output_config: dict, df: pd.DataFrame):
    """Schrijft een NetCDF-bestand in gegeven een pad

    Notes:
    ------
    Gebruikt hiervoor eerst de xarray.from_dataframe om een xarray-dataset te maken
    Gebruikt vervolgens xarray.to_netcdf om het bestand te genereren.
    Opties om dit aan te passen kunnen worden meegegeven in het configuratiebestand.

    Returns:
    --------
    None
    """
    # Data checks worden gedaan in de functies zelf, hier alleen geladen
    # netcdf kent geen int64 dus converteren naar float
    for column in df.columns:
        if df[column].dtype == "int64":
            df[column] = df[column].astype(int)

    ds = xr.Dataset.from_dataframe(df)
    kwargs = get_kwargs(xr.Dataset.to_netcdf, output_config)
    if "mode" not in kwargs:
        kwargs["mode"] = "w"
    if "engine" not in kwargs:
        kwargs["engine"] = "scipy"

    kwargs["path"] = output_config["abs_path"]
    # path is al een kwarg
    ds.to_netcdf(**kwargs)
