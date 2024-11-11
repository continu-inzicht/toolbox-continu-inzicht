import os
import pandas as pd
import pytest

from datetime import datetime, timedelta, timezone
from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsFews


@pytest.mark.skip(reason="Eerst een Fews rest service definieren")
def test_run():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_loads_fews_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    # Oude gegevens verwijderen
    output_info = config.data_adapters
    output_file = Path(
        config.global_variables["rootdir"] / Path(output_info["waterstanden"]["path"])
    )
    if os.path.exists(output_file):
        os.remove(output_file)

    fews = LoadsFews(data_adapter=data_adapter)
    df_output = fews.run(input="locaties", output="waterstanden")

    assert os.path.exists(output_file)

    assert df_output is not None
    assert len(df_output) > 0


def test_create_url():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_loads_fews_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)
    fews = LoadsFews(data_adapter=data_adapter)

    options = {"host": "https://**********", "port": 8443, "region": "region"}

    url = fews.create_url(options)
    assert url == "https://**********:8443/FewsWebServices/rest/region/v1/timeseries"


def test_create_params():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_loads_fews_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)
    fews = LoadsFews(data_adapter=data_adapter)

    options = {
        "host": "https://**********",
        "port": 8443,
        "region": "fewspiservice",
        "version": "1.25",
        "filter": "HKV_WV_1",
        "parameters": ["WNSHDB1"],
    }

    locations = pd.DataFrame(data={"name": ["location_a", "location_b"]})
    moments = [-24, 0, 24, 48]

    t_now = datetime(
        2024,
        10,
        17,
        12,
        0,
        0,
    ).replace(tzinfo=timezone.utc)
    params = fews.create_params(
        t_now=t_now, options=options, moments=moments, locations=locations
    )

    n_moments = len(moments) - 1
    starttime = t_now + timedelta(hours=int(moments[0]))
    endtime = t_now + timedelta(hours=int(moments[n_moments]))

    assert params["filterId"] == options["filter"]
    assert params["documentVersion"] == options["version"]
    assert params["parameterIds"] == options["parameters"]
    assert params["locationIds"] == locations["name"].tolist()
    assert params["startTime"] == starttime.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert params["endTime"] == endtime.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_create_dataframe():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_loads_fews_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)
    fews = LoadsFews(data_adapter=data_adapter)

    options = {"parameters": ["WNSHDB1"]}

    locations = pd.DataFrame(data={"name": ["VOV9345"], "id": [1]})
    t_now = datetime(
        2024,
        10,
        17,
        14,
        0,
        0,
    ).replace(tzinfo=timezone.utc)

    json_data = {
        "version": "1.25",
        "timeZone": "0.0",
        "timeSeries": [
            {
                "header": {
                    "type": "instantaneous",
                    "moduleInstanceId": "Productie",
                    "locationId": "VOV9345",
                    "parameterId": "WNSHDB1",
                    "timeStep": {"unit": "second", "multiplier": "900"},
                    "startDate": {"date": "2024-10-16", "time": "14:00:00"},
                    "endDate": {"date": "2024-10-19", "time": "14:00:00"},
                    "missVal": "-999.0",
                    "stationName": "VOV9345 - Krooshekreiniger Paviljoenpolder, Bath_Krooshekreiniger",
                    "lat": "51.4016519479478",
                    "lon": "4.24145342494035",
                    "x": "75273.34",
                    "y": "379793.38",
                    "z": "0.0",
                    "units": "mNAP",
                    "thresholds": [
                        {
                            "id": "Hardmax",
                            "name": "Hardmax",
                            "value": "5.0",
                            "type": "highLevelThreshold",
                        },
                        {
                            "id": "Hardmin",
                            "name": "Hardmin",
                            "value": "-5.0",
                            "type": "highLevelThreshold",
                        },
                    ],
                },
                "events": [
                    {
                        "date": "2024-10-16",
                        "time": "14:00:00",
                        "value": "-0.61",
                        "flag": "0",
                    }
                ],
            }
        ],
    }

    df_out = fews.create_dataframe(
        options=options, t_now=t_now, json_data=json_data, locations=locations
    )
    assert df_out is not None
    assert len(df_out) == 1
