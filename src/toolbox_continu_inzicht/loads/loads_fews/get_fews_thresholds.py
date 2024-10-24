import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data

async def get_fews_thresholds(host: str, port: int, region: str, filter_id: str, parameter_id: str, location_id: str)-> pd.DataFrame:
    """Haal voor Fews de thresholds op voor de opgegegeven parameter en locatie

    Args:
        host (str): fews server host url
        port (int): port waar de restservice draait
        region (str): in fews gedefinieerde region
        filter_id (str): filter van de timeserie
        parameter_id (str): parameter van de timeserie
        locatie_id (str): locatie van de timeserie        

    Returns:
        Dataframe: Pandas dataframe met thresholds
    """    
    # Genereer de url, geen data dus alleen header en thresholds aan.
    parameters = {
        "filterId": filter_id,
        "locationIds": location_id,
        "parameterIds": parameter_id,
        "showThresholds": True,
        "onlyHeaders": True,
        "documentFormat": "PI_JSON"
    }

    url: str = f"{host}:{port}/FewsWebServices/rest/{region}/v1/timeseries" 

    status, json_data = await fetch_data(
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