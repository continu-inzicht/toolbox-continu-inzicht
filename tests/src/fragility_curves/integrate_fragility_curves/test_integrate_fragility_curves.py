from pathlib import Path

import numpy as np
from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht.fragility_curves import IntegrateFragilityCurve

# %%
expected_result = np.array(
    [
        3.26327834e-10,
        2.82796836e-09,
        6.38427249e-09,
        6.16190850e-09,
        5.00777273e-09,
        2.59171866e-09,
        9.81142477e-10,
        4.92021365e-10,
        3.52932440e-10,
        2.90168635e-10,
        2.22426980e-10,
        1.59652777e-10,
        1.07869740e-10,
    ]
)
# %%


def tests_integrate_statistics_per_section():
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / "test_integrate_fragility_curves.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    integrate_statistics_per_section = IntegrateFragilityCurve(
        data_adapter=data_adapter
    )
    integrate_statistics_per_section.run(
        input=["exceedance_curve_csv", "fragility_curve_csv"], output="result"
    )
    df_out = integrate_statistics_per_section.df_out
    array = df_out["probability_contribution"]
    assert np.isclose(
        array[array > 1e-10],
        expected_result,
    ).all()
