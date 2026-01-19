import json
from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsWaterwebservicesRWS
from datetime import datetime, timezone
import pandas as pd


def test_BelastingWaterwebservicesRWS():
    """Tests of we belasing data van de waterwebservices van RWS kunnen ophalen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_loads_rws_one_config.yaml")
    c.lees_config()
    data_adapter = DataAdapter(config=c)
    # set calc time as to not be so dependent on outage
    data_adapter.config.global_variables["calc_time"] = datetime(
        2025,
        6,
        22,
        16,
        0,
        0,
    ).replace(tzinfo=timezone.utc)

    RWS_webservice = LoadsWaterwebservicesRWS(data_adapter=data_adapter)
    RWS_webservice.run(input="BelastingLocaties", output="Waterstanden")

    assert len(RWS_webservice.df_out) > 50


# old version
def test_test_BelastingWaterwebservicesRWS_create_dataframe():
    """ "tests creating of dataframe"""

    df_in = pd.DataFrame(
        data=[
            {
                "measurement_location_id": 1,
                "measurement_location_code": "denhelder.marsdiep",
                "measurement_location_code_name": "denhelder.marsdiep",
            }
        ]
    )
    options = {"MISSING_VALUE": 999999999.0}
    calc_time = datetime(
        2025,
        6,
        21,
        17,
        0,
        0,
    ).replace(tzinfo=timezone.utc)
    with open(
        Path(__file__).parent / "data_sets" / "test_rws_lst_data.txt",
        encoding="utf-8",
    ) as f:
        data_str = f.read()
    data = json.loads(data_str)
    df_out = LoadsWaterwebservicesRWS.create_dataframe(
        options=options,
        calc_time=calc_time,
        lst_data=data,
        df_in=df_in,
        global_variables={},
    )

    assert len(df_out) == 433
