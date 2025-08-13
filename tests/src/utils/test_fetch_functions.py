from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def test_fetch_data():
    """
    Test de werking van ophalen data via URL
    """
    # url = "https://moppenbot.nl/api/random/" # not working
    ## so use https://noos.matroos.rws.nl/direct/get_series.php?loc=hoekvanholland&source=observed&unit=waterlevel&format=dd instead (could also break)
    url = "https://noos.matroos.rws.nl/direct/get_series.php"
    parameters = {
        "loc": "hoekvanholland",
        "source": "observed",
        "unit": "waterlevel",
        "format": "dd",
    }
    status, json_data = fetch_data_get(url=url, params=parameters, mime_type="json")

    assert status is None
    assert len(json_data["results"]) > 0
    # check there is data
    assert len(json_data["results"][0]["events"]) > 0
