import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def get_fews_thresholds(
    host: str,
    port: int,
    region: str,
    filter_id: str,
    parameter_id: str,
    location_id: str,
) -> pd.DataFrame:
    """Haal voor FEWS de thresholds op voor de opgegeven parameter en locatie.

    Parameters
    ----------
    host: str
        FEWS server host URL
    port: int
        Port waar de REST-service draait
    region: str
        In FEWS gedefinieerde region
    filter_id: str
        Filter van de timeserie
    parameter_id: str
        Parameter van de timeserie
    location_id: str
        Locatie van de timeserie

    Raises
    ------
    ConnectionError
        Als er geen verbinding kan worden gemaakt of als de opgegeven parameter of locatie niet bestaat.

    Returns
    -------
    pd.DataFrame
        Pandas dataframe met thresholds
    """

    # Genereer de URL, geen data dus alleen header en thresholds aan.
    parameters = {
        "filterId": filter_id,
        "locationIds": location_id,
        "parameterIds": parameter_id,
        "showThresholds": True,
        "onlyHeaders": True,
        "documentFormat": "PI_JSON",
    }

    url: str = f"{host}:{port}/FewsWebServices/rest/{region}/v1/timeseries"

    status, json_data = fetch_data_get(
        url=url, params=parameters, mime_type="json", path_certificate=None
    )

    dataframe = pd.DataFrame()

    if status is None and json_data is not None:
        if "timeSeries" in json_data:
            for serie in json_data["timeSeries"]:
                if "header" in serie:
                    header = serie["header"]
                    if "thresholds" in header:
                        dataframe = pd.DataFrame(header["thresholds"])
                        print(dataframe)
                    else:
                        raise ConnectionError("No thresholds found")
                else:
                    raise ConnectionError("No header found")
        else:
            raise ConnectionError("No timeSeries found")
    else:
        raise ConnectionError("Connection failed")

    return dataframe
