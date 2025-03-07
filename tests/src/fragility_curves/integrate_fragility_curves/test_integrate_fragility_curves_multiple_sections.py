from pathlib import Path

import numpy as np
from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    IntegrateFragilityCurveMultiple,
)

# %%
expected_res_1 = np.array(
    [
        4.78349412e-10,
        1.05452552e-09,
        1.08938891e-09,
        1.11134461e-09,
        1.13374281e-09,
        1.14137763e-09,
        1.14906386e-09,
        1.14197919e-09,
        1.13641249e-09,
        1.18377684e-09,
        1.22109239e-09,
        1.11452817e-09,
        1.04073835e-09,
        9.33994910e-10,
        8.02195884e-10,
        6.77002306e-10,
        5.49536765e-10,
        4.41083902e-10,
        3.46048138e-10,
        2.66185492e-10,
        2.15149422e-10,
        1.64675613e-10,
        1.29869582e-10,
        1.17157779e-10,
    ]
)
# %%


def test_integrate_statistics_multiple_sections():
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / "test_integrate_fragility_curves_multiple.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    integrate_statistics_per_section = IntegrateFragilityCurveMultiple(
        data_adapter=data_adapter
    )
    integrate_statistics_per_section.run(
        input=["exceedance_curve_csv", "fragility_curve_multi_csv"], output="result"
    )
    df_out = integrate_statistics_per_section.df_out
    assert len(df_out.section_id.unique()) == 2
    section_1 = df_out[df_out["section_id"] == 1]
    arr_sec_1 = section_1["probability_contribution"]
    assert np.isclose(
        arr_sec_1[arr_sec_1 > 1e-10],
        expected_res_1,
    ).all()

    section_2 = df_out[df_out["section_id"] == 2]
    arr_sec_2 = section_2["probability_contribution"]
    assert np.isclose(
        arr_sec_2[arr_sec_2 > 1e-10],
        expected_res_1,
    ).all()
