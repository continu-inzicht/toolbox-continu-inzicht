from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.belastingen import BelastingWaterwebservicesRWS
import asyncio


def test_BelastingWaterwebservicesRWS():
    """Tests of we belasing data van de waterwebservices van RWS kunnen ophalen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(config_path=test_data_sets_path / "test_belasting_rws_8420_config.yaml")
    c.lees_config()
    data = DataAdapter(config=c)

    RWS_webservice = BelastingWaterwebservicesRWS(
        data_adapter=data, input="BelastingLocaties", output="Waterstanden"
    )
    loop = asyncio.get_event_loop()
    loop.run_until_complete(RWS_webservice.run())

    assert len(RWS_webservice.df_out) > 300
