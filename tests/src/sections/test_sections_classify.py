from pathlib import Path


from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.sections import SectionsClassify


def test_sections_classify_database():
    """Test the sections classify module."""
    path = Path(__file__).parent / "data_sets"

    config = Config(config_path=path / "test_sections_classify.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    sections_classify = SectionsClassify(data_adapter=data_adapter)
    sections_classify.run(
        input=["section_conditions", "section_data_failure_probability"],
        output="section_states",
    )
    assert sections_classify.df_out is not None
