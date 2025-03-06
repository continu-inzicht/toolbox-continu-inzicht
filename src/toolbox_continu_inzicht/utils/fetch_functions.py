import requests
import httpx


def fetch_data_get(
    url: str,
    params: dict,
    mime_type: str = "text",
    timeout: float = 60.0,
    path_certificate: str = None,
):
    """
    Haal data op van gegeven URL.

    Args:
        url (str): URL-adres
        params (dict): lijst met URL-parameters.
        mime_type (str, optional): mime type. Standaardwaarde is "text".
        timeout (float, optional): tijd voordat de verbinding verbroken wordt (in seconden). Standaardwaarde is 10.0 seconden.
        path_certificate (str, optional): locatie naar een pem-bestand

    Returns:
        status: status code van de http request
        data: data als tekst of json
    """
    data = None
    result = None

    try:
        headers = {}
        if mime_type == "json":
            # Zet de 'Accept' header naar application/json
            headers = {"Accept": "application/json"}

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            if mime_type == "json":
                data = response.json()
            elif mime_type == "NETCDF":
                data = response
            else:
                data = response.text
        else:
            data = None
            result = response.text

    except Exception as error:
        result = error

    # Geef resultaat terug
    return result, data


def fetch_data_post(
    url: str, json: dict, mime_type: str = "text", timeout: float = 60.0
):
    """
    Haal data op van gegeven URL d.m.v. POST-request.

    Args:
        url (str): URL-adres
        params (dict): lijst met URL-parameters.
        mime_type (str, optional): mime type. Standaardwaarde is "text".
        timeout (float, optional): tijd voordat de verbinding verbroken wordt (in seconden). Standaardwaarde is 10.0 seconden.

    Returns:
        status: status code van de http post request
        data: data als tekst of json
    """
    data = None
    result = None

    with httpx.Client() as client:
        try:
            response = client.post(url=url, json=json, timeout=timeout)

            if response.status_code == httpx.codes.OK:
                if mime_type == "json":
                    data = response.json()
                else:
                    data = response.text
            else:
                data = None
                result = httpx.Response.text

        except httpx.RequestError as error:
            result = error

    # Geef resultaat terug
    return result, data
