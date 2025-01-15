from pathlib import Path

import numpy as np


from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter

from toolbox_continu_inzicht.fragility_curves import (
    CombineFragilityCurvesIndependent,
    CombineFragilityCurvesDependent,
)

# %%
expected_result = [
    0.08309275463435695,
    0.09091203389022784,
    0.09946713094814819,
    0.10821218894485785,
    0.11772610433834596,
    0.12739152315661117,
    0.13785048153398716,
    0.14841364380113076,
    0.15978623666177194,
    0.17120774153294194,
    0.18344565447684402,
    0.19567034002348427,
    0.20870966976074523,
    0.22166816360018404,
    0.235431232343983,
    0.2490419296677251,
    0.26343948555646823,
    0.27761084014196624,
    0.29254452270712794,
    0.30744384523905366,
    0.3231019917949469,
    0.33955760936014046,
    0.3568513132148907,
    0.37502578717984236,
    0.39412588896700074,
    0.4141987608962422,
    0.43529394625062756,
    0.45746351155770304,
    0.4807621750985994,
    0.5052474419621161,
    0.5309797459771197,
    0.5580225988735766,
    0.5864427470403712,
    0.6163103362668171,
    0.6476990848744655,
    0.6806864656665463,
    0.7153538971440999,
    0.7517869444607653,
    0.7900755306122322,
    0.8303141583815818,
    0.8726021435883202,
    0.9170438602168283,
    0.9637489980292051,
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
    1.0,
]


# %%
def test_combine_fragility_curves_indep1_csv():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_combine_fragility_curve.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    combine_fragility_curve = CombineFragilityCurvesIndependent(
        data_adapter=data_adapter
    )
    combine_fragility_curve.run(
        input=[
            "fragility_curve_pipping_csv",
            "fragility_curve_overtopping_csv",
        ],
        output="fragility_curves",
    )
    result = combine_fragility_curve.df_out
    assert np.isclose(
        result.iloc[100:150]["failure_probability"], expected_result
    ).all()


def test_combine_fragility_curves_indep2_csv():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_combine_fragility_curve.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    combine_fragility_curve = CombineFragilityCurvesIndependent(
        data_adapter=data_adapter
    )
    combine_fragility_curve.run(
        input=[
            "fragility_curve_pipping_csv",
            "fragility_curve_pipping_csv",
        ],
        output="fragility_curves",
    )
    result = combine_fragility_curve.df_out
    assert np.isclose(
        result.iloc[100:150]["failure_probability"], expected_result
    ).all()


def test_combine_fragility_curves_dep_csv():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_combine_fragility_curve.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    combine_fragility_curve = CombineFragilityCurvesDependent(data_adapter=data_adapter)
    combine_fragility_curve.run(
        input=[
            "fragility_curve_pipping_csv",
            "fragility_curve_overtopping_csv",
        ],
        output="fragility_curves",
    )
    result = combine_fragility_curve.df_out
    assert np.isclose(
        result.iloc[100:150]["failure_probability"], expected_result
    ).all()


def test_combine_fragility_curves_weighted_csv():
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_combine_fragility_curve.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    combine_fragility_curve = CombineFragilityCurvesDependent(data_adapter=data_adapter)
    combine_fragility_curve.run(
        input=[
            "fragility_curve_pipping_csv",
            "fragility_curve_overtopping_csv",
        ],
        output="fragility_curves",
    )
    result = combine_fragility_curve.df_out
    assert np.isclose(
        result.iloc[100:150]["failure_probability"], expected_result
    ).all()
