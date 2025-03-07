from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_get


def test_fetch_data():
    """
    Test de werking van ophalen data via URL
    """
    url = "https://moppenbot.nl/api/random/"
    parameters = []
    status, json_data = fetch_data_get(url=url, params=parameters, mime_type="json")

    assert status is None
    assert json_data["success"] is True
