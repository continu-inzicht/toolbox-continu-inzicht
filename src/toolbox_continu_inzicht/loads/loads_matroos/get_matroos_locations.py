import geopandas as gpd
import pandas as pd
import folium
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def get_matroos_locations(source=None, parameter=None) -> gpd.GeoDataFrame:
    """Haal alle matroos locaties op, indien gewenst ook bron en parameter."""
    url = "https://noos.matroos.rws.nl/direct/get_available.php?dd_endpoint=locations&"

    params = {"format": "dd_default_2.0.0", "geojson": 1}
    if source is not None:
        params["source"] = source
    if parameter is not None:
        params["unit"] = parameter

    status, geojson_data = fetch_data_get(
        url=url, params=params, mime_type="json", timeout=30
    )

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
        raise ConnectionError("Connection failed:", status)

    return gdf


def get_matroos_locations_map(source=None, parameter=None) -> folium.Map:
    """Haal alle matroos locaties op en maak een folium map."""
    df_location = get_matroos_locations(source=source, parameter=parameter)
    # Create a map centered at a specific location
    m = folium.Map(
        location=[df_location.geometry.y.mean(), df_location.geometry.x.mean()],
        zoom_start=10,
    )

    # Add markers to the map
    for _, row in df_location.iterrows():
        popup_text = f"ID: {row['measurement_location_id']}<br>Code: {row['measurement_location_code']}"
        folium.Marker(
            location=[row.geometry.y, row.geometry.x], popup=popup_text
        ).add_to(m)

    return m


def get_matroos_sources() -> pd.DataFrame:
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
