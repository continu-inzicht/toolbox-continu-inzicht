from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    CombineFragilityCurvesDependent,
    CombineFragilityCurvesIndependent,
    CombineFragilityCurvesWeightedSum,
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

expected_result_indep2 = [
    0.1592811,
    0.17355907,
    0.18904055,
    0.2047145,
    0.22159277,
    0.23855445,
    0.25669821,
    0.27480068,
    0.29404083,
    0.31310339,
    0.333239,
    0.3530538,
    0.37385961,
    0.39419955,
    0.4154346,
    0.43606198,
    0.45747861,
    0.4781539,
    0.49950675,
    0.52036597,
    0.54180909,
]

expected_result_weighted = [
    0.60003404,
    0.60007504,
    0.60016124,
    0.60033766,
    0.60068876,
    0.60136737,
    0.60263824,
    0.60493711,
    0.60895024,
    0.615688,
    0.62652686,
    0.6431485,
    0.66728855,
    0.70023975,
    0.74215722,
    0.79135854,
    0.84398822,
    0.85594372,
    0.86706978,
    0.89224341,
    0.91931635,
    0.94109863,
    0.9573561,
    0.96918644,
    0.97769601,
    0.98379072,
    0.98815654,
    0.99129298,
    0.99355658,
    0.99519941,
    0.9963991,
    0.99728071,
    0.99793281,
    0.99841856,
    0.99878274,
    0.99905751,
]


# Note: Deze testen zijn handmatig gevalideerd met de sheet data_sets/manual_test_fragility_curve_combine.xlsx


def test_combine_fragility_curves_indep1_csv():
    """Test de CombineFragilityCurvesIndependent functie met 2 fragility curves: piping en overtopping"""
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
            "fragility_curve_piping_csv",
            "fragility_curve_overtopping_csv",
        ],
        output="fragility_curves",
    )
    result = combine_fragility_curve.df_out
    assert np.isclose(
        result.iloc[100:150]["failure_probability"], expected_result
    ).all()


def test_combine_fragility_curves_indep2_csv():
    """Test de CombineFragilityCurvesIndependent functie met 2 dezelfde piping fragility curves"""
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
            "fragility_curve_piping_csv",
            "fragility_curve_piping_csv",
        ],
        output="fragility_curves",
    )
    result = combine_fragility_curve.df_out
    assert np.isclose(
        result.iloc[100:150]["failure_probability"], expected_result_indep2
    ).all()


def test_combine_fragility_curves_dep_csv():
    """Test de CombineFragilityCurvesDependent functie met 2 fragility curves: piping en overtopping"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_combine_fragility_curve.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    combine_fragility_curve = CombineFragilityCurvesDependent(data_adapter=data_adapter)
    combine_fragility_curve.run(
        input=[
            "fragility_curve_piping_csv",
            "fragility_curve_overtopping_csv",
        ],
        output="fragility_curves",
    )
    result = combine_fragility_curve.df_out
    assert np.isclose(
        result.iloc[100:150]["failure_probability"], expected_result
    ).all()


def test_combine_fragility_curves_weighted_csv():
    """Test de CombineFragilityCurvesWeightedSum functie met 2 fragility curves: piping en overtopping en de gewichten 60/40"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_combine_fragility_curve.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    combine_fragility_curve = CombineFragilityCurvesWeightedSum(
        data_adapter=data_adapter
    )
    combine_fragility_curve.run(
        input=[
            "fragility_curve_piping_csv",
            "fragility_curve_overtopping_csv",
            "weighting_factor_csv",
        ],
        output="fragility_curves",
    )
    result = combine_fragility_curve.df_out
    assert np.isclose(
        result.iloc[280:316]["failure_probability"], expected_result_weighted
    ).all()


@pytest.mark.performance
def test_combine_fragility_curves_largeset_csv(benchmark):
    """Test de CombineFragilityCurvesIndependent met een grote dataset"""

    def perf_test():
        test_data_sets_path = Path(__file__).parent / "data_sets"
        df_large = pd.read_csv(test_data_sets_path / "fragilitycurves_largeset.csv")

        config = Config(
            config_path=test_data_sets_path / "test_combine_fragility_curve.yaml"
        )
        config.lees_config()
        data_adapter = DataAdapter(config=config)

        usecols = ["hydraulicload", "failure_probability"]
        results = []
        for measureid, mgroup in df_large.groupby("measureid"):
            for sectionid, sgroup in mgroup.groupby("sectionid"):
                input_keys = []
                df_combi_check = None
                for fmid, fgroup in sgroup.groupby("failuremechanismid"):
                    if fmid == 1:
                        df_combi_check = fgroup[usecols].copy()
                    else:
                        input_key = f"fmid{fmid}"
                        data_adapter.set_dataframe_adapter(
                            input_key,
                            fgroup[usecols].copy(),
                            if_not_exist="create",
                        )
                        input_keys.append(input_key)

                combine_fc = CombineFragilityCurvesIndependent(
                    data_adapter=data_adapter
                )
                combine_fc.run(input=input_keys, output="fragility_curves")
                df_combi = combine_fc.df_out[usecols].copy()
                results.append(
                    np.isclose(
                        df_combi.to_numpy(),
                        df_combi_check.to_numpy(),
                        atol=1e-8,
                        rtol=1e-2,
                    ).all()
                )

        return results

    results = benchmark(perf_test)
    assert all(results)
