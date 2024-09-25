from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht import voorbeeld_module


def test_DataAdapter_csv():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    print(test_data_sets_path)
    c = Config(config_path=test_data_sets_path / "test_config_csv.yaml")
    c.lees_config()

    data = DataAdapter(config=c)

    keer_twee = voorbeeld_module.WaardesKeerTwee(data_adapter=data)
    keer_twee.run()

    assert all((keer_twee.df_in["value"] * 2 == keer_twee.df_out["value"]).values)
