import os

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.loads import LoadsClassify


def test_run():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_loads_classify_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    # Oude gegevens verwijderen
    output_info = config.data_adapters
    output_file = Path(
        config.global_variables["rootdir"] / Path(output_info["classificatie"]["path"])
    )
    if os.path.exists(output_file):
        os.remove(output_file)

    classify = LoadsClassify(data_adapter=data_adapter)

    df_out = classify.run(input=["thresholds", "waterstanden"], output="classificatie")

    assert os.path.exists(output_file)
    assert df_out is not None
    # assert {"code", "value", "van", "tot", "kleur", "label"}.issubset(df_out.columns)
