import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data


async def get_waterinfo_thresholds(
    location_code: str, parameter_id: str = "waterhoogte"
) -> pd.DataFrame:
    """Haal voor Waterinfo de thresholds op voor de opgegegeven parameter

    Args:
        parameter_id (str): Waterinfo parameter bij geen waarde 'waterhoogte'

    Returns:
        Dataframe: Pandas dataframe met thressholds
    """
    url: str = "https://waterinfo.rws.nl/api/chart/get"

    params = {
        "mapType": parameter_id,
        "locationCodes": location_code,
        "values": "-48,48",
    }

    # Ophalen json data van de Waterinfo api
    status, json_data = await fetch_data(url=url, params=params, mime_type="json")

    # dataframe
    dataframe = pd.DataFrame()

    if status is None and json_data is not None:
        if "limits" in json_data:
            dataframe = pd.DataFrame(json_data["limits"])
        else:
            raise ConnectionError("No thresholds found")

    else:
        raise ConnectionError("Connection failed")

    return dataframe


if __name__ == "__main__":
    import asyncio

    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_waterinfo_thresholds("Aadorp(AADP)", "waterhoogte"))
