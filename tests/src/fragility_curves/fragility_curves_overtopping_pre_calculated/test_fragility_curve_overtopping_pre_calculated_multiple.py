from pathlib import Path

import pandas as pd
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    FragilityCurveOvertoppingPreCalculatedMultiple,
)


def test_fragility_curves_pre_calculated():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path
        / "test_fragility_curves_overtopping_pre_calculated.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    fragility_curve_overtopping = FragilityCurveOvertoppingPreCalculatedMultiple(
        data_adapter=data_adapter
    )
    fragility_curve_overtopping.run(
        input=[
            "slopes",
            "profiles",
            "section_hrloc",
            "waveval_uncert",
            "waveval_id",
            "waveval",
        ],
        output="fragility_curves",
    )
    df_out = data_adapter.input("fragility_curves")

    # @TODO proper assert
    assert isinstance(df_out, pd.DataFrame)
