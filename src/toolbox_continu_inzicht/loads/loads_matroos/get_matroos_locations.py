import geopandas as gpd
import pandas as pd
import folium
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def get_matroos_locations(
    source: str | None = None,
    parameter: str | None = None,
    endpoint: str = "timeseries",
) -> gpd.GeoDataFrame:
    """Haal alle matroos locaties op, indien gewenst ook bron en parameter.

    Parameters
    ----------
    source: str | None
        Bron in de matroos, None geeft alle terug
    parameter: str | None
        Specifieke parameter waar voor je locaties zoekt, None geeft alle terug
    endpoint: str
        Naam van de endpoint, standaard 'timeseries'

    Returns
    -------
    geopandas.GeoDataFrame

    Raises
    ------
    UserWarning
        Alleen endpoint='timeseries' is nu beschikbaar
    ConnectionError
        Helaas is matroos niet altijd even stabiel, dit vangen we af met connection error

    Returns
    -------
    geopandas.GeoDataFrame
    """
    if endpoint == "timeseries":
        url = "https://noos.matroos.rws.nl/direct/get_available.php?dd_endpoint=locations&"
    else:
        raise UserWarning(
            "Op dit moment is het nog niet mogelijk om via maps1d locaties op te vragen.\
                           met get_matroos_sources(endpoint='maps1d') kan dit wel\
                          of kijk op de website: https://noos.matroos.rws.nl/maps1d/search/"
        )

    ### andere informatie afhankelijk van het format
    # params = {"format": "dd_default_2.0.0", "geojson": 1}

    ### als we format niet mee geven krijgen we wel een source
    params = {"geojson": 1}
    if source is not None:
        params["source"] = source
    if parameter is not None:
        params["unit"] = parameter

    status, geojson_data = fetch_data_get(
        url=url, params=params, mime_type="json", timeout=30
    )
    #  = json.loads(data)
    if status is None and geojson_data is not None:
        if "features" in geojson_data:
            gdf = gpd.GeoDataFrame.from_features(geojson_data)
            if "url" in gdf.columns and "node" in gdf.columns:
                gdf = gdf.drop(columns=["url", "node"])
            if "popupContent" in gdf.columns:
                gdf = gdf.drop(columns=["popupContent"])
            gdf = gdf.rename(
                columns={
                    "locationName": "measurement_location_code",
                    "locationId": "measurement_location_id",
                    "unit": "parameter",
                    "label": "measurement_location_code",
                }
            )

        else:
            raise ConnectionError(f"No results in data, only:{geojson_data.keys()}")
    else:
        raise ConnectionError("Connection failed:", status)

    return gdf


def get_matroos_locations_map(
    source: str | None = None,
    parameter: str | None = None,
    endpoint: str = "timeseries",
) -> folium.Map:
    """Haal alle matroos locaties op en maak een folium map.

    Parameters
    ----------
    source : str | None
        Bron in de matroos, None geeft alle terug
    parameter: str | None
        Specifieke parameter waar voor je locaties zoekt, None geeft alle terug
    endpoint: str
        Naam van de endpoint, standaard 'timeseries', maar voor LoadsMatroosNetCDF gebruik je 'maps1d'.

    Returns
    -------
    Folium.map
    """
    df_location = get_matroos_locations(
        source=source, parameter=parameter, endpoint=endpoint
    )
    # Create a map centered at a specific location
    m = folium.Map(
        location=[df_location.geometry.y.mean(), df_location.geometry.x.mean()],
        zoom_start=10,
    )

    # Add markers to the map
    for _, row in df_location.iterrows():
        popup_text = f"Code: {row['measurement_location_code']}<br>source: {row['source']}<br>parameter: {row['parameter']}"
        folium.Marker(
            location=[row.geometry.y, row.geometry.x], popup=popup_text
        ).add_to(m)

    return m


def get_matroos_sources(endpoint: str = "timeseries") -> pd.DataFrame:
    """Haalt alle matroos bronnen op

    Parameters
    ----------
    endpoint: str
        Naam van de endpoint, standaard 'timeseries', maar voor LoadsMatroosNetCDF gebruik je 'maps1d'.

    Raises
    ------
    ConnectionError
        If no data is returned
        If connection fails

    Returns
    -------
    pandas.DataFrame
    """
    if endpoint == "timeseries":
        url = "https://noos.matroos.rws.nl/timeseries/search/get_sources.php?"
    elif endpoint == "maps1d":
        url = "https://noos.matroos.rws.nl/maps1d/search/get_sources.php?"

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
