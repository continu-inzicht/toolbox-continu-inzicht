import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def get_waterinfo_thresholds(
    location_code: str, parameter_id: str = "waterhoogte"
) -> pd.DataFrame:
    """Haal voor Waterinfo de thresholds op voor de opgegegeven parameter.

    Parameters
    ----------
    location_code: str
        Locatiecode van de waterinfo locatie
    parameter_id: str
        Waterinfo parameter bij geen waarde 'waterhoogte'

    Notes
    -----
    Gebruikte API: [https://waterinfo.rws.nl/api/chart/get](https://waterinfo.rws.nl/api/chart/get)

    Returns
    -------
    Dataframe: Pandas dataframe met thresholds

    Raises
    ------
    ConnectionError
        Als de verbinding met de API niet lukt of geen thresholds zijn gevonden
    """
    url: str = "https://waterinfo.rws.nl/api/chart/get"

    params = {
        "mapType": parameter_id,
        "locationCodes": location_code,
        "values": "-48,48",
    }

    # Ophalen json data van de Waterinfo api
    status, json_data = fetch_data_get(url=url, params=params, mime_type="json")

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
    get_waterinfo_thresholds("Aadorp(AADP)", "waterhoogte")
