import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def get_waterinfo_locations(parameter_id: str = "waterhoogte") -> pd.DataFrame:
    """Haal voor Waterinfo de locaties op voor de opgegeven parameter.

    Parameters
    ----------
    parameter_id: str
        Waterinfo parameter bij geen waarde 'waterhoogte'

    Notes
    -----
    Gebruikte API: [https://waterinfo.rws.nl/api/point/latestmeasurement](https://waterinfo.rws.nl/api/point/latestmeasurement)

    Returns
    -------
    Pandas dataframe met locaties: pd.DataFrame

    Raises
    ------
    ConnectionError
        Als de verbinding met de API niet lukt of geen locaties zijn gevonden
    """

    # URL voor ophalen van waterinfo geojson
    url: str = "https://waterinfo.rws.nl/api/point/latestmeasurement"

    params: dict = {"parameterId": parameter_id}

    # Ophalen json data van de Waterinfo api
    dataframe: pd.DataFrame = pd.DataFrame()
    status, json_data = fetch_data_get(url=url, params=params, mime_type="json")
    if status is None and json_data is not None:
        if "features" in json_data:
            df_locations = pd.DataFrame(json_data["features"])

            records = []
            for index, row in df_locations.iterrows():
                geometry = row["geometry"]
                properties = row["properties"]
                name = properties["name"]
                location_code = properties["locationCode"]
                x = geometry["coordinates"][0]
                y = geometry["coordinates"][1]

                record = {
                    "id": index,
                    "name": name,
                    "location_code": location_code,
                    "x": x,
                    "y": y,
                }

                records.append(record)

            dataframe = pd.DataFrame.from_records(records)
            dataframe = dataframe.set_index("id")
        else:
            raise ConnectionError("No features found")
    else:
        raise ConnectionError("Connection failed")

    return dataframe
