from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.proof_of_concept import ValuesTimesTwo


def test_DataAdapter_load_external_config():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_load_external_dataset_config.yaml"
    )
    config.lees_config()

    data_adapter = DataAdapter(config=config)
    keer_twee = ValuesTimesTwo(data_adapter=data_adapter)
    keer_twee.run(input="MyCSV_in", output="MyCSV_out")

    assert "my_adapter_test_in" in data_adapter.input_types
    assert "fake_my_adapter_test_in2" not in data_adapter.input_types
    assert "my_adapter_test_out" in data_adapter.output_types
    assert "my_adapter_test_out2" in data_adapter.output_types
