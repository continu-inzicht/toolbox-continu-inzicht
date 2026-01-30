from pathlib import Path

import pandas as pd
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    FragilityCurveOvertoppingWaveDataMultiple,
)


def test_fragility_curves_wavedata():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path
        / "test_fragility_curves_overtopping_wavedata.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    fragility_curve_overtopping = FragilityCurveOvertoppingWaveDataMultiple(
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

    df_expected = pd.DataFrame(
        {
            "hydraulicload": {
                45: 14.050000000000018,
                46: 14.10000000000002,
                47: 14.15000000000002,
                48: 14.20000000000002,
                49: 14.25000000000002,
                50: 14.300000000000022,
                51: 14.350000000000025,
                52: 14.400000000000023,
                53: 14.450000000000024,
                54: 14.500000000000025,
                55: 14.550000000000026,
                56: 14.600000000000026,
                57: 14.650000000000029,
                58: 14.700000000000028,
                59: 14.750000000000028,
            },
            "failure_probability": {
                45: 4.7468497091041815e-11,
                46: 3.32922380467357e-08,
                47: 3.995344459189519e-06,
                48: 0.0001112188847314,
                49: 0.0013277826902383,
                50: 0.0083690452494891,
                51: 0.0327875373361805,
                52: 0.090297101816804,
                53: 0.1913879413256281,
                54: 0.3322162832523351,
                55: 0.4941158283413638,
                56: 0.6512326465972151,
                57: 0.78189982380075,
                58: 0.8762143427164634,
                59: 0.9359579266952373,
            },
        }
    )
    df_actual = df_out.loc[df_expected.index, ["hydraulicload", "failure_probability"]]
    pd.testing.assert_frame_equal(df_actual, df_expected, rtol=1e-12)
