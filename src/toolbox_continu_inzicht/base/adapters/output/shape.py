import geopandas as gpd
import pandas as pd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def output_shape(output_config: dict, df: pd.DataFrame) -> None:
    """Output als een geo-informatiebestand (bijv. shape, geosjon) in gegeven een pad

    Returns:
    --------
    gpd.GeoDataframes
    """
    path = output_config["abs_path"]

    kwargs = get_kwargs(gpd.GeoDataFrame.to_file, output_config)
    if "schema" in kwargs:
        kwargs.pop("schema", None)
    df.to_file(path, **kwargs)
