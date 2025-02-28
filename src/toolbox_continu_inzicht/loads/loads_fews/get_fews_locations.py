import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def get_fews_locations(
    host: str, port: int, region: str, filter_id: str
) -> pd.DataFrame:
    """Haal voor FEWS de locaties op voor de opgegegeven parameters.

    Args:
        host (str): FEWS server host URL
        port (int): port waar de REST-service draait
        region (str): in FEWS gedefinieerde region
        filter_id (str): filter van de locaties

    Returns:
        Dataframe: Pandas dataframe met locaties
    """

    dataframe: pd.DataFrame = pd.DataFrame()

    # Genereer de URL voor het ophalen van de locaties.
    parameters = {
        "filterId": filter_id,
        "showAttributes": False,
        "documentFormat": "PI_JSON",
    }

    url: str = f"{host}:{port}/FewsWebServices/rest/{region}/v1/locations"

    status, json_data = fetch_data_get(
        url=url, params=parameters, mime_type="json", path_certificate=None
    )

    if status is None and json_data is not None:
        if "locations" in json_data:
            dataframe = pd.DataFrame(json_data["locations"])
            if not parameters["showAttributes"]:
                dataframe = dataframe.drop(columns=["attributes"])

            dataframe = dataframe.set_index("locationId")

    return dataframe
