import httpx


async def fetch_data(
    url: str, params: dict, mime_type: str = "text", timeout: float = 60.0
):
    """
    Haal data op van gegeven url.

    Args:
        url (str): url adres
        params (dict): lijst met url parameters.
        mime_type (str, optional): mime type. Standaard waarde is is "text".
        timeout (float, optional): tijd voordat de verbinding verbroken wordt (in seconden). Standaard waarde is 10.0 seconden.

    Returns:
        status: status code van de http request
        data: data als tekst of json
    """
    data = None
    result = None

    async with httpx.AsyncClient() as client:
        try:
            headers = {}
            if mime_type == "json":
                # Zet de 'Accept' header naar application/json
                headers = {"Accept": "application/json"}

            response = await client.get(
                url=url, params=params, timeout=timeout, headers=headers
            )

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
