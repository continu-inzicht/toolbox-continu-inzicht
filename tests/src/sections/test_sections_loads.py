import os
import pytest

from pathlib import Path
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.sections import SectionsLoads


@pytest.mark.asyncio()
async def test_run():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_sections_loads_config.yaml")
    config.lees_config()

    data_adapter = DataAdapter(config=config)

    # Oude gegevens verwijderen
    output_info = config.data_adapters
    output_file = Path(
        config.global_variables["rootdir"]
        / Path(output_info["waterstanden_per_dijkvak"]["path"])
    )
    if os.path.exists(output_file):
        os.remove(output_file)

    sections_loads = SectionsLoads(
        data_adapter=data_adapter,
        input="dijkvakken",
        input2="waterstanden",
        input3="koppeling_meetstation_dijkvak",
        output="waterstanden_per_dijkvak",
    )

    df_output = await sections_loads.run()

    assert os.path.exists(output_file)

    assert df_output is not None
    assert len(df_output) > 0
