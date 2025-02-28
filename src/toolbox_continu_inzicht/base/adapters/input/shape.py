import geopandas as gpd
from toolbox_continu_inzicht.base.adapters.data_adapter_utils import get_kwargs


def input_shape(input_config: dict) -> gpd.GeoDataFrame:
    """Laadt een geo-informatiebestand (bijv. shape, geosjon) in gegeven een pad

    Returns:
    --------
    gpd.GeoDataframe
    """
    path = input_config["abs_path"]

    kwargs = get_kwargs(gpd.read_file, input_config)

    gdf = gpd.read_file(path, **kwargs)
    return gdf
