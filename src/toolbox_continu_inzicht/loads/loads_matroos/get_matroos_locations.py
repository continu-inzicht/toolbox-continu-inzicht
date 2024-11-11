import geopandas as gpd
import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def get_matroos_locations(source=None, parameter=None) -> gpd.GeoDataFrame:
    """Haalt alle matroos locaties op, indien gewenst ook bron en parameter"""
    url = "https://noos.matroos.rws.nl/direct/get_available.php?dd_endpoint=locations&"

    params = {"format": "dd_default_2.0.0", "geojson": 1}
    if source is not None:
        params["source"] = source
    if parameter is not None:
        params["unit"] = parameter

    status, geojson_data = fetch_data_get(url=url, params=params, mime_type="json")

    if status is None and geojson_data is not None:
        if "features" in geojson_data:
            gdf = gpd.GeoDataFrame.from_features(geojson_data)
            gdf = gdf.drop(columns=["url", "node"])
            gdf = gdf.rename(
                columns={
                    "locationName": "measurement_location_code",
                    "locationId": "measurement_location_id",
                }
            )

        else:
            raise ConnectionError(f"No results in data, only:{geojson_data.keys()}")
    else:
        raise ConnectionError("Connection failed")

    return gdf


def get_matroos_models() -> pd.DataFrame:
    """Haalt alle matroos bronnen op"""
    url = "https://noos.matroos.rws.nl/timeseries/search/get_sources.php?"
    params = {"format": "dd_default_2.0.0"}
    status, json_data = fetch_data_get(url=url, params=params, mime_type="json")

    if status is None and json_data is not None:
        if "records" in json_data:
            df = pd.DataFrame.from_dict(json_data["records"])
        else:
            raise ConnectionError(f"No results in data, only:{json_data.keys()}")
    else:
        raise ConnectionError("Connection failed")

    return df
