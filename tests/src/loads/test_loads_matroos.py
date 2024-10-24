from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsMatroos
import asyncio
import pytest
from datetime import datetime, timezone


def test_LoadsMatroos_noos():
    """Tests of we belasing data van de matroos in z'n geheel kunnen ophalen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_loads_matroos_noos_config.yaml")
    c.lees_config()
    data = DataAdapter(config=c)

    matroos = LoadsMatroos(
        data_adapter=data, input="BelastingLocaties", output="Waterstanden"
    )
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    loop.run_until_complete(matroos.run())

    assert len(matroos.df_out) > 100


def test_LoadsMatroos_no_website():
    """Tests of als we geen website mee geven bij het ophalen van belasing data van de matroos, ook een error krijgen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(
        config_path=test_data_sets_path / "test_loads_matroos_no_website_config.yaml"
    )
    c.lees_config()
    data = DataAdapter(config=c)

    matroos = LoadsMatroos(
        data_adapter=data, input="BelastingLocaties", output="Waterstanden"
    )
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    match = "Specify a `website` to consult in config: Noos, Matroos or Vitaal. In case of the later two, also provide a password and username"
    with pytest.raises(UserWarning, match=match):
        loop.run_until_complete(matroos.run())

    matroos


def test_LoadsMatroos_create_url_vitaal():
    """Test het aanmaken van een url voor het ophalen van vitaal.rws.nl data"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_loads_matroos_noos_config.yaml")
    c.lees_config()
    data = DataAdapter(config=c)

    matroos = LoadsMatroos(
        data_adapter=data, input="BelastingLocaties", output="Waterstanden"
    )
    t_now = datetime(
        2024,
        10,
        22,
        16,
        0,
        0,
    ).replace(tzinfo=timezone.utc)

    options = {"website": "vitaal", "source": "observed", "parameters": ["waterlevel"]}
    global_variables = {
        "moments": [-24, 0, 24, 48],
        "vitaal_user": "test_vitaal_user",
        "vitaal_password": "test_vitaal_password",
    }
    url = matroos.generate_url(
        t_now=t_now,
        options=options,
        global_variables=global_variables,
        parameter="waterlevel",
        location_names=["hoekvanholland"],
    )
    expected_output = "https://test_vitaal_user:test_vitaal_password@vitaal.matroos.rws.nl/direct/get_series.php?loc=hoekvanholland&source=observed&unit=waterlevel&tstart=202410211600&tend=202410241600&format=dd_2.0.0&timezone=GMT&zip=0&"
    assert url == expected_output


def test_LoadsMatroos_create_url_matroos():
    """Test het aanmaken van een url voor het ophalen van matroos.rws.nl data"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_loads_matroos_noos_config.yaml")
    c.lees_config()
    data = DataAdapter(config=c)

    matroos = LoadsMatroos(
        data_adapter=data, input="BelastingLocaties", output="Waterstanden"
    )
    t_now = datetime(
        2024,
        10,
        22,
        16,
        0,
        0,
    ).replace(tzinfo=timezone.utc)

    options = {"website": "matroos", "source": "observed", "parameters": ["waterlevel"]}
    global_variables = {
        "moments": [-24, 0, 24, 48],
        "matroos_user": "test_matroos_user",
        "matroos_password": "test_matroos_password",
    }
    url = matroos.generate_url(
        t_now=t_now,
        options=options,
        global_variables=global_variables,
        parameter="waterlevel",
        location_names=["hoekvanholland"],
    )
    expected_output = "https://test_matroos_user:test_matroos_password@matroos.rws.nl/direct/get_series.php?loc=hoekvanholland&source=observed&unit=waterlevel&tstart=202410211600&tend=202410241600&format=dd_2.0.0&timezone=GMT&zip=0&"
    assert url == expected_output


def test_LoadsMatroos_create_url_noos():
    """Test het aanmaken van een url voor het ophalen van noos.matroos data"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_loads_matroos_noos_config.yaml")
    c.lees_config()
    data = DataAdapter(config=c)

    matroos = LoadsMatroos(
        data_adapter=data, input="BelastingLocaties", output="Waterstanden"
    )
    t_now = datetime(
        2024,
        10,
        22,
        16,
        0,
        0,
    ).replace(tzinfo=timezone.utc)

    options = {"website": "noos", "source": "observed", "parameters": ["waterlevel"]}
    global_variables = {"moments": [-24, 0, 24, 48]}
    url = matroos.generate_url(
        t_now=t_now,
        options=options,
        global_variables=global_variables,
        parameter="waterlevel",
        location_names=["hoekvanholland"],
    )
    expected_output = "https://noos.matroos.rws.nl/direct/get_series.php?loc=hoekvanholland&source=observed&unit=waterlevel&tstart=202410211600&tend=202410241600&format=dd_2.0.0&timezone=GMT&zip=0&"
    assert url == expected_output


def test_LoadsMatroos_data_frame():
    """Tests of we belasing data van de matroos json om kunnen zetten in een dataframe"""
    options = {
        "website": "noos",
        "source": "bma2_95",
        "parameters": ["waterlevel"],
        "MISSING_VALUE": 999,
    }

    dt_now = datetime.now(timezone.utc)
    t_now = datetime(
        dt_now.year,
        dt_now.month,
        dt_now.day,
        dt_now.hour,
        0,
        0,
    ).replace(tzinfo=timezone.utc)

    json_data = {
        "provider": {
            "supportUrl": "https://noos.matroos.rws.nl/",
            "apiVersion": "2.0.0",
            "name": "Matroos",
            "responseTimestamp": "2024-10-22T14:19:24.000000Z",
            "responseType": "TimeseriesListResponse",
        },
        "count": 1,
        "results": [
            {
                "source": {
                    "rws_url": "https://noos.matroos.rws.nl/dd/2.0/sources?sourceCode=152",
                    "name": "bma2_95",
                    "description": "BMA 95% confidence forecast for NOOS sources, delivered by HMC",
                    "institution": {"name": "HMC", "description": "RWS-HMC"},
                    "process": "forecast",
                },
                "analysisTime": "2024-10-22T00:00:00Z",
                "location": {
                    "geometry": {
                        "coordinates": [4.120131, 51.978539, 0],
                        "type": "Point",
                    },
                    "type": "Feature",
                    "properties": {
                        "locationId": "7",
                        "locationName": "Hoek van Holland",
                        "url": "https://noos.matroos.rws.nl/dd/2.0/locations/7",
                        "node": {
                            "baseUrl": "https://noos.matroos.rws.nl/",
                            "rws_revision": "3085",
                            "name": "RWS NOOS extern matroos",
                            "id": "RWS_NOOS_E",
                            "description": "Rijkswaterstaat Matroos RWS_NOOS_E",
                        },
                    },
                },
                "node": {
                    "baseUrl": "https://noos.matroos.rws.nl/",
                    "rws_revision": "3085",
                    "name": "RWS NOOS extern matroos",
                    "id": "RWS_NOOS_E",
                    "description": "Rijkswaterstaat Matroos RWS_NOOS_E",
                },
                "observationType": {
                    "unit": "m",
                    "quantityName": "waterlevel",
                    "url": "https://noos.matroos.rws.nl/dd/2.0/observationtypes/1",
                    "quantity": "waterlevel",
                    "id": "1",
                    "node": {
                        "baseUrl": "https://noos.matroos.rws.nl/",
                        "rws_revision": "3085",
                        "name": "RWS NOOS extern matroos",
                        "id": "RWS_NOOS_E",
                        "description": "Rijkswaterstaat Matroos RWS_NOOS_E",
                    },
                },
                "events": [
                    {"value": 0.4847, "timeStamp": "2024-10-21T14:00:00Z"},
                    {"value": 0.5807, "timeStamp": "2024-10-21T14:10:00Z"},
                    {"value": 0.6887, "timeStamp": "2024-10-21T14:20:00Z"},
                    {"value": 0.8267, "timeStamp": "2024-10-21T14:30:00Z"},
                    {"value": 0.9847, "timeStamp": "2024-10-21T14:40:00Z"},
                    {"value": 1.1737, "timeStamp": "2024-10-21T14:50:00Z"},
                    {"value": 1.3667, "timeStamp": "2024-10-21T15:00:00Z"},
                    {"value": 1.5517, "timeStamp": "2024-10-21T15:10:00Z"},
                    {"value": 1.7137, "timeStamp": "2024-10-21T15:20:00Z"},
                    {"value": 1.8477, "timeStamp": "2024-10-21T15:30:00Z"},
                    {"value": 1.9567, "timeStamp": "2024-10-21T15:40:00Z"},
                    {"value": 2.0457, "timeStamp": "2024-10-21T15:50:00Z"},
                    {"value": 2.1107, "timeStamp": "2024-10-21T16:00:00Z"},
                    {"value": 2.1347, "timeStamp": "2024-10-21T16:10:00Z"},
                    {"value": 2.1327, "timeStamp": "2024-10-21T16:20:00Z"},
                    {"value": 2.1007, "timeStamp": "2024-10-21T16:30:00Z"},
                    {"value": 2.0467, "timeStamp": "2024-10-21T16:40:00Z"},
                    {"value": 1.9857, "timeStamp": "2024-10-21T16:50:00Z"},
                    {"value": 1.9327, "timeStamp": "2024-10-21T17:00:00Z"},
                    {"value": 1.8727, "timeStamp": "2024-10-21T17:10:00Z"},
                    {"value": 1.8217, "timeStamp": "2024-10-21T17:20:00Z"},
                    {"value": 1.7827, "timeStamp": "2024-10-21T17:30:00Z"},
                    {"value": 1.7517, "timeStamp": "2024-10-21T17:40:00Z"},
                    {"value": 1.7187, "timeStamp": "2024-10-21T17:50:00Z"},
                    {"value": 1.6877, "timeStamp": "2024-10-21T18:00:00Z"},
                    {"value": 1.6417, "timeStamp": "2024-10-21T18:10:00Z"},
                    {"value": 1.5837, "timeStamp": "2024-10-21T18:20:00Z"},
                    {"value": 1.5127, "timeStamp": "2024-10-21T18:30:00Z"},
                    {"value": 1.4307, "timeStamp": "2024-10-21T18:40:00Z"},
                    {"value": 1.3357, "timeStamp": "2024-10-21T18:50:00Z"},
                    {"value": 1.2417, "timeStamp": "2024-10-21T19:00:00Z"},
                    {"value": 1.1407, "timeStamp": "2024-10-21T19:10:00Z"},
                    {"value": 1.0297, "timeStamp": "2024-10-21T19:20:00Z"},
                    {"value": 0.9327, "timeStamp": "2024-10-21T19:30:00Z"},
                    {"value": 0.8397, "timeStamp": "2024-10-21T19:40:00Z"},
                    {"value": 0.7437, "timeStamp": "2024-10-21T19:50:00Z"},
                    {"value": 0.6497, "timeStamp": "2024-10-21T20:00:00Z"},
                    {"value": 0.5697, "timeStamp": "2024-10-21T20:10:00Z"},
                    {"value": 0.4907, "timeStamp": "2024-10-21T20:20:00Z"},
                    {"value": 0.4357, "timeStamp": "2024-10-21T20:30:00Z"},
                    {"value": 0.3787, "timeStamp": "2024-10-21T20:40:00Z"},
                    {"value": 0.3447, "timeStamp": "2024-10-21T20:50:00Z"},
                    {"value": 0.3157, "timeStamp": "2024-10-21T21:00:00Z"},
                    {"value": 0.2907, "timeStamp": "2024-10-21T21:10:00Z"},
                    {"value": 0.2847, "timeStamp": "2024-10-21T21:20:00Z"},
                    {"value": 0.2817, "timeStamp": "2024-10-21T21:30:00Z"},
                    {"value": 0.2847, "timeStamp": "2024-10-21T21:40:00Z"},
                    {"value": 0.2857, "timeStamp": "2024-10-21T21:50:00Z"},
                    {"value": 0.2847, "timeStamp": "2024-10-21T22:00:00Z"},
                    {"value": 0.2707, "timeStamp": "2024-10-21T22:10:00Z"},
                    {"value": 0.2547, "timeStamp": "2024-10-21T22:20:00Z"},
                    {"value": 0.2327, "timeStamp": "2024-10-21T22:30:00Z"},
                    {"value": 0.2077, "timeStamp": "2024-10-21T22:40:00Z"},
                    {"value": 0.1967, "timeStamp": "2024-10-21T22:50:00Z"},
                    {"value": 0.1797, "timeStamp": "2024-10-21T23:00:00Z"},
                    {"value": 0.1477, "timeStamp": "2024-10-21T23:10:00Z"},
                    {"value": 0.1327, "timeStamp": "2024-10-21T23:20:00Z"},
                    {"value": 0.1337, "timeStamp": "2024-10-21T23:30:00Z"},
                    {"value": 0.1237, "timeStamp": "2024-10-21T23:40:00Z"},
                    {"value": 0.1107, "timeStamp": "2024-10-21T23:50:00Z"},
                    {"value": 0.1189, "timeStamp": "2024-10-22T00:00:00Z"},
                    {"value": 0.1279, "timeStamp": "2024-10-22T00:10:00Z"},
                    {"value": 0.1299, "timeStamp": "2024-10-22T00:20:00Z"},
                    {"value": 0.1193, "timeStamp": "2024-10-22T00:30:00Z"},
                    {"value": 0.126, "timeStamp": "2024-10-22T00:40:00Z"},
                    {"value": 0.1276, "timeStamp": "2024-10-22T00:50:00Z"},
                    {"value": 0.1393, "timeStamp": "2024-10-22T01:00:00Z"},
                    {"value": 0.138, "timeStamp": "2024-10-22T01:10:00Z"},
                    {"value": 0.1466, "timeStamp": "2024-10-22T01:20:00Z"},
                    {"value": 0.1653, "timeStamp": "2024-10-22T01:30:00Z"},
                    {"value": 0.184, "timeStamp": "2024-10-22T01:40:00Z"},
                    {"value": 0.2026, "timeStamp": "2024-10-22T01:50:00Z"},
                    {"value": 0.2263, "timeStamp": "2024-10-22T02:00:00Z"},
                    {"value": 0.2591, "timeStamp": "2024-10-22T02:10:00Z"},
                    {"value": 0.297, "timeStamp": "2024-10-22T02:20:00Z"},
                    {"value": 0.3598, "timeStamp": "2024-10-22T02:30:00Z"},
                    {"value": 0.4376, "timeStamp": "2024-10-22T02:40:00Z"},
                    {"value": 0.5205, "timeStamp": "2024-10-22T02:50:00Z"},
                    {"value": 0.6283, "timeStamp": "2024-10-22T03:00:00Z"},
                    {"value": 0.7583, "timeStamp": "2024-10-22T03:10:00Z"},
                    {"value": 0.8933, "timeStamp": "2024-10-22T03:20:00Z"},
                    {"value": 1.0333, "timeStamp": "2024-10-22T03:30:00Z"},
                    {"value": 1.1783, "timeStamp": "2024-10-22T03:40:00Z"},
                    {"value": 1.3033, "timeStamp": "2024-10-22T03:50:00Z"},
                    {"value": 1.4133, "timeStamp": "2024-10-22T04:00:00Z"},
                    {"value": 1.4928, "timeStamp": "2024-10-22T04:10:00Z"},
                    {"value": 1.5623, "timeStamp": "2024-10-22T04:20:00Z"},
                    {"value": 1.6018, "timeStamp": "2024-10-22T04:30:00Z"},
                    {"value": 1.6213, "timeStamp": "2024-10-22T04:40:00Z"},
                    {"value": 1.6008, "timeStamp": "2024-10-22T04:50:00Z"},
                    {"value": 1.5553, "timeStamp": "2024-10-22T05:00:00Z"},
                    {"value": 1.5158, "timeStamp": "2024-10-22T05:10:00Z"},
                    {"value": 1.4563, "timeStamp": "2024-10-22T05:20:00Z"},
                    {"value": 1.4068, "timeStamp": "2024-10-22T05:30:00Z"},
                    {"value": 1.3623, "timeStamp": "2024-10-22T05:40:00Z"},
                    {"value": 1.3278, "timeStamp": "2024-10-22T05:50:00Z"},
                    {"value": 1.2883, "timeStamp": "2024-10-22T06:00:00Z"},
                    {"value": 1.2623, "timeStamp": "2024-10-22T06:10:00Z"},
                    {"value": 1.2263, "timeStamp": "2024-10-22T06:20:00Z"},
                    {"value": 1.1903, "timeStamp": "2024-10-22T06:30:00Z"},
                    {"value": 1.1393, "timeStamp": "2024-10-22T06:40:00Z"},
                    {"value": 1.0833, "timeStamp": "2024-10-22T06:50:00Z"},
                    {"value": 1.0123, "timeStamp": "2024-10-22T07:00:00Z"},
                    {"value": 0.9411, "timeStamp": "2024-10-22T07:10:00Z"},
                    {"value": 0.845, "timeStamp": "2024-10-22T07:20:00Z"},
                    {"value": 0.7538, "timeStamp": "2024-10-22T07:30:00Z"},
                    {"value": 0.6526, "timeStamp": "2024-10-22T07:40:00Z"},
                    {"value": 0.5615, "timeStamp": "2024-10-22T07:50:00Z"},
                    {"value": 0.4653, "timeStamp": "2024-10-22T08:00:00Z"},
                    {"value": 0.3748, "timeStamp": "2024-10-22T08:10:00Z"},
                    {"value": 0.2843, "timeStamp": "2024-10-22T08:20:00Z"},
                    {"value": 0.1988, "timeStamp": "2024-10-22T08:30:00Z"},
                    {"value": 0.1233, "timeStamp": "2024-10-22T08:40:00Z"},
                    {"value": 0.0578, "timeStamp": "2024-10-22T08:50:00Z"},
                    {"value": -0.0027, "timeStamp": "2024-10-22T09:00:00Z"},
                    {"value": -0.0585, "timeStamp": "2024-10-22T09:10:00Z"},
                    {"value": -0.0944, "timeStamp": "2024-10-22T09:20:00Z"},
                    {"value": -0.1102, "timeStamp": "2024-10-22T09:30:00Z"},
                    {"value": -0.116, "timeStamp": "2024-10-22T09:40:00Z"},
                    {"value": -0.1119, "timeStamp": "2024-10-22T09:50:00Z"},
                    {"value": -0.1077, "timeStamp": "2024-10-22T10:00:00Z"},
                    {"value": -0.0825, "timeStamp": "2024-10-22T10:10:00Z"},
                    {"value": -0.0624, "timeStamp": "2024-10-22T10:20:00Z"},
                    {"value": -0.0422, "timeStamp": "2024-10-22T10:30:00Z"},
                    {"value": -0.032, "timeStamp": "2024-10-22T10:40:00Z"},
                    {"value": -0.0119, "timeStamp": "2024-10-22T10:50:00Z"},
                    {"value": -0.0017, "timeStamp": "2024-10-22T11:00:00Z"},
                    {"value": -0.0014, "timeStamp": "2024-10-22T11:10:00Z"},
                    {"value": -0.006, "timeStamp": "2024-10-22T11:20:00Z"},
                    {"value": -0.0107, "timeStamp": "2024-10-22T11:30:00Z"},
                    {"value": -0.0114, "timeStamp": "2024-10-22T11:40:00Z"},
                    {"value": -0.0117, "timeStamp": "2024-10-22T11:50:00Z"},
                    {"value": -0.0261, "timeStamp": "2024-10-22T12:00:00Z"},
                    {"value": 0.0219, "timeStamp": "2024-10-22T12:10:00Z"},
                    {"value": 0.0068, "timeStamp": "2024-10-22T12:20:00Z"},
                    {"value": -0.0042, "timeStamp": "2024-10-22T12:30:00Z"},
                    {"value": -0.0049, "timeStamp": "2024-10-22T12:40:00Z"},
                    {"value": -0.0147, "timeStamp": "2024-10-22T12:50:00Z"},
                    {"value": -0.0173, "timeStamp": "2024-10-22T13:00:00Z"},
                    {"value": -0.0204, "timeStamp": "2024-10-22T13:10:00Z"},
                    {"value": -0.012, "timeStamp": "2024-10-22T13:20:00Z"},
                    {"value": 0.0063, "timeStamp": "2024-10-22T13:30:00Z"},
                    {"value": 0.0346, "timeStamp": "2024-10-22T13:40:00Z"},
                    {"value": 0.063, "timeStamp": "2024-10-22T13:50:00Z"},
                    {"value": 0.1163, "timeStamp": "2024-10-22T14:00:00Z"},
                    {"value": 0.1638, "timeStamp": "2024-10-22T14:10:00Z"},
                    {"value": 0.2263, "timeStamp": "2024-10-22T14:20:00Z"},
                    {"value": 0.2938, "timeStamp": "2024-10-22T14:30:00Z"},
                    {"value": 0.3763, "timeStamp": "2024-10-22T14:40:00Z"},
                    {"value": 0.4588, "timeStamp": "2024-10-22T14:50:00Z"},
                    {"value": 0.5563, "timeStamp": "2024-10-22T15:00:00Z"},
                    {"value": 0.6736, "timeStamp": "2024-10-22T15:10:00Z"},
                    {"value": 0.806, "timeStamp": "2024-10-22T15:20:00Z"},
                    {"value": 0.9433, "timeStamp": "2024-10-22T15:30:00Z"},
                    {"value": 1.1056, "timeStamp": "2024-10-22T15:40:00Z"},
                    {"value": 1.273, "timeStamp": "2024-10-22T15:50:00Z"},
                    {"value": 1.4253, "timeStamp": "2024-10-22T16:00:00Z"},
                    {"value": 1.5593, "timeStamp": "2024-10-22T16:10:00Z"},
                    {"value": 1.6633, "timeStamp": "2024-10-22T16:20:00Z"},
                    {"value": 1.7373, "timeStamp": "2024-10-22T16:30:00Z"},
                    {"value": 1.7813, "timeStamp": "2024-10-22T16:40:00Z"},
                    {"value": 1.8003, "timeStamp": "2024-10-22T16:50:00Z"},
                    {"value": 1.8043, "timeStamp": "2024-10-22T17:00:00Z"},
                    {"value": 1.7826, "timeStamp": "2024-10-22T17:10:00Z"},
                    {"value": 1.741, "timeStamp": "2024-10-22T17:20:00Z"},
                    {"value": 1.6963, "timeStamp": "2024-10-22T17:30:00Z"},
                    {"value": 1.6483, "timeStamp": "2024-10-22T17:40:00Z"},
                    {"value": 1.6043, "timeStamp": "2024-10-22T17:50:00Z"},
                    {"value": 1.5573, "timeStamp": "2024-10-22T18:00:00Z"},
                    {"value": 1.5383, "timeStamp": "2024-10-22T18:10:00Z"},
                    {"value": 1.5073, "timeStamp": "2024-10-22T18:20:00Z"},
                    {"value": 1.4793, "timeStamp": "2024-10-22T18:30:00Z"},
                    {"value": 1.441, "timeStamp": "2024-10-22T18:40:00Z"},
                    {"value": 1.4001, "timeStamp": "2024-10-22T18:50:00Z"},
                    {"value": 1.3593, "timeStamp": "2024-10-22T19:00:00Z"},
                    {"value": 1.3011, "timeStamp": "2024-10-22T19:10:00Z"},
                    {"value": 1.233, "timeStamp": "2024-10-22T19:20:00Z"},
                    {"value": 1.1648, "timeStamp": "2024-10-22T19:30:00Z"},
                    {"value": 1.0866, "timeStamp": "2024-10-22T19:40:00Z"},
                    {"value": 0.9985, "timeStamp": "2024-10-22T19:50:00Z"},
                    {"value": 0.9153, "timeStamp": "2024-10-22T20:00:00Z"},
                    {"value": 0.8365, "timeStamp": "2024-10-22T20:10:00Z"},
                    {"value": 0.7476, "timeStamp": "2024-10-22T20:20:00Z"},
                    {"value": 0.6688, "timeStamp": "2024-10-22T20:30:00Z"},
                    {"value": 0.59, "timeStamp": "2024-10-22T20:40:00Z"},
                    {"value": 0.5111, "timeStamp": "2024-10-22T20:50:00Z"},
                    {"value": 0.4423, "timeStamp": "2024-10-22T21:00:00Z"},
                    {"value": 0.3631, "timeStamp": "2024-10-22T21:10:00Z"},
                    {"value": 0.304, "timeStamp": "2024-10-22T21:20:00Z"},
                    {"value": 0.2448, "timeStamp": "2024-10-22T21:30:00Z"},
                    {"value": 0.1956, "timeStamp": "2024-10-22T21:40:00Z"},
                    {"value": 0.1615, "timeStamp": "2024-10-22T21:50:00Z"},
                    {"value": 0.1323, "timeStamp": "2024-10-22T22:00:00Z"},
                    {"value": 0.1163, "timeStamp": "2024-10-22T22:10:00Z"},
                    {"value": 0.1103, "timeStamp": "2024-10-22T22:20:00Z"},
                    {"value": 0.0993, "timeStamp": "2024-10-22T22:30:00Z"},
                    {"value": 0.0983, "timeStamp": "2024-10-22T22:40:00Z"},
                    {"value": 0.0873, "timeStamp": "2024-10-22T22:50:00Z"},
                    {"value": 0.0863, "timeStamp": "2024-10-22T23:00:00Z"},
                    {"value": 0.0888, "timeStamp": "2024-10-22T23:10:00Z"},
                    {"value": 0.0763, "timeStamp": "2024-10-22T23:20:00Z"},
                    {"value": 0.0638, "timeStamp": "2024-10-22T23:30:00Z"},
                    {"value": 0.0613, "timeStamp": "2024-10-22T23:40:00Z"},
                    {"value": 0.0538, "timeStamp": "2024-10-22T23:50:00Z"},
                    {"value": 0.0513, "timeStamp": "2024-10-23T00:00:00Z"},
                    {"value": 0.0454, "timeStamp": "2024-10-23T00:10:00Z"},
                    {"value": 0.0489, "timeStamp": "2024-10-23T00:20:00Z"},
                    {"value": 0.0474, "timeStamp": "2024-10-23T00:30:00Z"},
                    {"value": 0.0559, "timeStamp": "2024-10-23T00:40:00Z"},
                    {"value": 0.0594, "timeStamp": "2024-10-23T00:50:00Z"},
                    {"value": 0.0629, "timeStamp": "2024-10-23T01:00:00Z"},
                    {"value": 0.0707, "timeStamp": "2024-10-23T01:10:00Z"},
                    {"value": 0.0636, "timeStamp": "2024-10-23T01:20:00Z"},
                    {"value": 0.0664, "timeStamp": "2024-10-23T01:30:00Z"},
                    {"value": 0.0692, "timeStamp": "2024-10-23T01:40:00Z"},
                    {"value": 0.0821, "timeStamp": "2024-10-23T01:50:00Z"},
                    {"value": 0.0999, "timeStamp": "2024-10-23T02:00:00Z"},
                    {"value": 0.1164, "timeStamp": "2024-10-23T02:10:00Z"},
                    {"value": 0.1379, "timeStamp": "2024-10-23T02:20:00Z"},
                    {"value": 0.1694, "timeStamp": "2024-10-23T02:30:00Z"},
                    {"value": 0.2109, "timeStamp": "2024-10-23T02:40:00Z"},
                    {"value": 0.2624, "timeStamp": "2024-10-23T02:50:00Z"},
                    {"value": 0.3239, "timeStamp": "2024-10-23T03:00:00Z"},
                    {"value": 0.3896, "timeStamp": "2024-10-23T03:10:00Z"},
                    {"value": 0.4702, "timeStamp": "2024-10-23T03:20:00Z"},
                    {"value": 0.5559, "timeStamp": "2024-10-23T03:30:00Z"},
                    {"value": 0.6566, "timeStamp": "2024-10-23T03:40:00Z"},
                    {"value": 0.7622, "timeStamp": "2024-10-23T03:50:00Z"},
                    {"value": 0.8629, "timeStamp": "2024-10-23T04:00:00Z"},
                    {"value": 0.9766, "timeStamp": "2024-10-23T04:10:00Z"},
                    {"value": 1.0852, "timeStamp": "2024-10-23T04:20:00Z"},
                    {"value": 1.1839, "timeStamp": "2024-10-23T04:30:00Z"},
                    {"value": 1.2726, "timeStamp": "2024-10-23T04:40:00Z"},
                    {"value": 1.3462, "timeStamp": "2024-10-23T04:50:00Z"},
                    {"value": 1.4049, "timeStamp": "2024-10-23T05:00:00Z"},
                    {"value": 1.4441, "timeStamp": "2024-10-23T05:10:00Z"},
                    {"value": 1.4632, "timeStamp": "2024-10-23T05:20:00Z"},
                    {"value": 1.4624, "timeStamp": "2024-10-23T05:30:00Z"},
                    {"value": 1.4516, "timeStamp": "2024-10-23T05:40:00Z"},
                    {"value": 1.4207, "timeStamp": "2024-10-23T05:50:00Z"},
                    {"value": 1.3899, "timeStamp": "2024-10-23T06:00:00Z"},
                    {"value": 1.3604, "timeStamp": "2024-10-23T06:10:00Z"},
                    {"value": 1.3309, "timeStamp": "2024-10-23T06:20:00Z"},
                    {"value": 1.3064, "timeStamp": "2024-10-23T06:30:00Z"},
                    {"value": 1.2769, "timeStamp": "2024-10-23T06:40:00Z"},
                    {"value": 1.2524, "timeStamp": "2024-10-23T06:50:00Z"},
                    {"value": 1.2229, "timeStamp": "2024-10-23T07:00:00Z"},
                    {"value": 1.1826, "timeStamp": "2024-10-23T07:10:00Z"},
                    {"value": 1.1372, "timeStamp": "2024-10-23T07:20:00Z"},
                    {"value": 1.0869, "timeStamp": "2024-10-23T07:30:00Z"},
                    {"value": 1.0266, "timeStamp": "2024-10-23T07:40:00Z"},
                    {"value": 0.9712, "timeStamp": "2024-10-23T07:50:00Z"},
                    {"value": 0.8859, "timeStamp": "2024-10-23T08:00:00Z"},
                    {"value": 0.8066, "timeStamp": "2024-10-23T08:10:00Z"},
                    {"value": 0.7222, "timeStamp": "2024-10-23T08:20:00Z"},
                    {"value": 0.6429, "timeStamp": "2024-10-23T08:30:00Z"},
                    {"value": 0.5486, "timeStamp": "2024-10-23T08:40:00Z"},
                    {"value": 0.4642, "timeStamp": "2024-10-23T08:50:00Z"},
                    {"value": 0.3799, "timeStamp": "2024-10-23T09:00:00Z"},
                    {"value": 0.2976, "timeStamp": "2024-10-23T09:10:00Z"},
                    {"value": 0.2202, "timeStamp": "2024-10-23T09:20:00Z"},
                    {"value": 0.1429, "timeStamp": "2024-10-23T09:30:00Z"},
                    {"value": 0.0756, "timeStamp": "2024-10-23T09:40:00Z"},
                    {"value": 0.0082, "timeStamp": "2024-10-23T09:50:00Z"},
                    {"value": -0.0541, "timeStamp": "2024-10-23T10:00:00Z"},
                    {"value": -0.0913, "timeStamp": "2024-10-23T10:10:00Z"},
                    {"value": -0.1284, "timeStamp": "2024-10-23T10:20:00Z"},
                    {"value": -0.1556, "timeStamp": "2024-10-23T10:30:00Z"},
                    {"value": -0.1728, "timeStamp": "2024-10-23T10:40:00Z"},
                    {"value": -0.1916, "timeStamp": "2024-10-23T10:50:00Z"},
                    {"value": -0.1985, "timeStamp": "2024-10-23T11:00:00Z"},
                    {"value": -0.1936, "timeStamp": "2024-10-23T11:10:00Z"},
                    {"value": -0.1951, "timeStamp": "2024-10-23T11:20:00Z"},
                    {"value": -0.1866, "timeStamp": "2024-10-23T11:30:00Z"},
                    {"value": -0.1731, "timeStamp": "2024-10-23T11:40:00Z"},
                    {"value": -0.1696, "timeStamp": "2024-10-23T11:50:00Z"},
                    {"value": -0.1561, "timeStamp": "2024-10-23T12:00:00Z"},
                    {"value": -0.1788, "timeStamp": "2024-10-23T12:10:00Z"},
                    {"value": -0.1647, "timeStamp": "2024-10-23T12:20:00Z"},
                    {"value": -0.1655, "timeStamp": "2024-10-23T12:30:00Z"},
                    {"value": -0.1513, "timeStamp": "2024-10-23T12:40:00Z"},
                    {"value": -0.1522, "timeStamp": "2024-10-23T12:50:00Z"},
                    {"value": -0.153, "timeStamp": "2024-10-23T13:00:00Z"},
                    {"value": -0.1497, "timeStamp": "2024-10-23T13:10:00Z"},
                    {"value": -0.1563, "timeStamp": "2024-10-23T13:20:00Z"},
                    {"value": -0.158, "timeStamp": "2024-10-23T13:30:00Z"},
                    {"value": -0.1597, "timeStamp": "2024-10-23T13:40:00Z"},
                    {"value": -0.1663, "timeStamp": "2024-10-23T13:50:00Z"},
                    {"value": -0.163, "timeStamp": "2024-10-23T14:00:00Z"},
                    {"value": -0.1602, "timeStamp": "2024-10-23T14:10:00Z"},
                    {"value": -0.1423, "timeStamp": "2024-10-23T14:20:00Z"},
                    {"value": -0.1195, "timeStamp": "2024-10-23T14:30:00Z"},
                    {"value": -0.0917, "timeStamp": "2024-10-23T14:40:00Z"},
                    {"value": -0.0488, "timeStamp": "2024-10-23T14:50:00Z"},
                    {"value": -0.001, "timeStamp": "2024-10-23T15:00:00Z"},
                    {"value": 0.0617, "timeStamp": "2024-10-23T15:10:00Z"},
                    {"value": 0.1293, "timeStamp": "2024-10-23T15:20:00Z"},
                    {"value": 0.207, "timeStamp": "2024-10-23T15:30:00Z"},
                    {"value": 0.2847, "timeStamp": "2024-10-23T15:40:00Z"},
                    {"value": 0.3823, "timeStamp": "2024-10-23T15:50:00Z"},
                    {"value": 0.485, "timeStamp": "2024-10-23T16:00:00Z"},
                    {"value": 0.5958, "timeStamp": "2024-10-23T16:10:00Z"},
                    {"value": 0.7117, "timeStamp": "2024-10-23T16:20:00Z"},
                    {"value": 0.8325, "timeStamp": "2024-10-23T16:30:00Z"},
                    {"value": 0.9633, "timeStamp": "2024-10-23T16:40:00Z"},
                    {"value": 1.1042, "timeStamp": "2024-10-23T16:50:00Z"},
                    {"value": 1.215, "timeStamp": "2024-10-23T17:00:00Z"},
                    {"value": 1.3027, "timeStamp": "2024-10-23T17:10:00Z"},
                    {"value": 1.3703, "timeStamp": "2024-10-23T17:20:00Z"},
                    {"value": 1.408, "timeStamp": "2024-10-23T17:30:00Z"},
                    {"value": 1.4307, "timeStamp": "2024-10-23T17:40:00Z"},
                    {"value": 1.4383, "timeStamp": "2024-10-23T17:50:00Z"},
                    {"value": 1.426, "timeStamp": "2024-10-23T18:00:00Z"},
                    {"value": 1.4033, "timeStamp": "2024-10-23T18:10:00Z"},
                    {"value": 1.3757, "timeStamp": "2024-10-23T18:20:00Z"},
                    {"value": 1.348, "timeStamp": "2024-10-23T18:30:00Z"},
                    {"value": 1.3203, "timeStamp": "2024-10-23T18:40:00Z"},
                    {"value": 1.2927, "timeStamp": "2024-10-23T18:50:00Z"},
                    {"value": 1.27, "timeStamp": "2024-10-23T19:00:00Z"},
                    {"value": 1.2355, "timeStamp": "2024-10-23T19:10:00Z"},
                    {"value": 1.206, "timeStamp": "2024-10-23T19:20:00Z"},
                    {"value": 1.1665, "timeStamp": "2024-10-23T19:30:00Z"},
                    {"value": 1.127, "timeStamp": "2024-10-23T19:40:00Z"},
                    {"value": 1.0775, "timeStamp": "2024-10-23T19:50:00Z"},
                    {"value": 1.0203, "timeStamp": "2024-10-23T20:00:00Z"},
                    {"value": 0.962, "timeStamp": "2024-10-23T20:10:00Z"},
                    {"value": 0.8936, "timeStamp": "2024-10-23T20:20:00Z"},
                    {"value": 0.8303, "timeStamp": "2024-10-23T20:30:00Z"},
                    {"value": 0.762, "timeStamp": "2024-10-23T20:40:00Z"},
                    {"value": 0.6936, "timeStamp": "2024-10-23T20:50:00Z"},
                    {"value": 0.6253, "timeStamp": "2024-10-23T21:00:00Z"},
                    {"value": 0.562, "timeStamp": "2024-10-23T21:10:00Z"},
                    {"value": 0.4936, "timeStamp": "2024-10-23T21:20:00Z"},
                    {"value": 0.4353, "timeStamp": "2024-10-23T21:30:00Z"},
                    {"value": 0.3609, "timeStamp": "2024-10-23T21:40:00Z"},
                    {"value": 0.3059, "timeStamp": "2024-10-23T21:50:00Z"},
                    {"value": 0.2453, "timeStamp": "2024-10-23T22:00:00Z"},
                    {"value": 0.187, "timeStamp": "2024-10-23T22:10:00Z"},
                    {"value": 0.1386, "timeStamp": "2024-10-23T22:20:00Z"},
                    {"value": 0.0953, "timeStamp": "2024-10-23T22:30:00Z"},
                    {"value": 0.0447, "timeStamp": "2024-10-23T22:40:00Z"},
                    {"value": 0.0097, "timeStamp": "2024-10-23T22:50:00Z"},
                    {"value": -0.0197, "timeStamp": "2024-10-23T23:00:00Z"},
                    {"value": -0.043, "timeStamp": "2024-10-23T23:10:00Z"},
                    {"value": -0.0623, "timeStamp": "2024-10-23T23:20:00Z"},
                    {"value": -0.0795, "timeStamp": "2024-10-23T23:30:00Z"},
                    {"value": -0.1017, "timeStamp": "2024-10-23T23:40:00Z"},
                    {"value": -0.1188, "timeStamp": "2024-10-23T23:50:00Z"},
                    {"value": -0.131, "timeStamp": "2024-10-24T00:00:00Z"},
                ],
                "endTime": "2024-12-10T14:00:00Z",
                "url": "https://noos.matroos.rws.nl/dd/2.0/timeseries/26220",
                "startTime": "2024-10-21T14:00:00Z",
                "id": "26220",
            }
        ],
    }

    df_out = LoadsMatroos.create_dataframe(
        options=options, t_now=t_now, json_data=json_data
    )
    assert len(df_out) == 349
    columns_names = [
        "objectid",
        "objecttype",
        "parameterid",
        "datetime",
        "value",
        "calculating",
        "measurementcode",
    ]
    assert all([col in list(df_out.columns) for col in columns_names])
