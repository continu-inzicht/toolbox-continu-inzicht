from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.belastingen import BelastingMatroos
import asyncio
import pytest


def test_BelastingMatroos_noos():
    """Tests of we belasing data van de waterwebservices van RWS kunnen ophalen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(
        config_path=test_data_sets_path / "test_belasting_matroos_noos_config.yaml"
    )
    c.lees_config()
    data = DataAdapter(config=c)

    matroos = BelastingMatroos(
        data_adapter=data, input="BelastingLocaties", output="Waterstanden"
    )
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    loop.run_until_complete(matroos.run())

    assert len(matroos.df_out) > 100


def test_BelastingMatroos_no_website():
    """Tests of we belasing data van de waterwebservices van RWS kunnen ophalen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    c = Config(
        config_path=test_data_sets_path
        / "test_belasting_matroos_no_website_config.yaml"
    )
    c.lees_config()
    data = DataAdapter(config=c)

    matroos = BelastingMatroos(
        data_adapter=data, input="BelastingLocaties", output="Waterstanden"
    )
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()

    match = "Specify a `website` to consult in config: Noos, Matroos or Vitaal. In case of the later two, also provide a password and username"
    with pytest.raises(UserWarning, match=match):
        loop.run_until_complete(matroos.run())
