from pathlib import Path

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    CreateFragilityCurvesWaveOvertopping,
)


def test_fragility_curves_wave_overtopping():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_fragility_curves_wave_overtopping.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    wave_overtopping_fragility_curve = CreateFragilityCurvesWaveOvertopping(
        data_adapter=data_adapter
    )
    wave_overtopping_fragility_curve.run(
        input=["slopes", "profiles", "bedlevel_fetch"], output="fragility_curves"
    )
    assert len(wave_overtopping_fragility_curve.df_out) == 405
