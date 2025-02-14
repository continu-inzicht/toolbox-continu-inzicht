from pathlib import Path

import numpy as np
from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht.fragility_curves import IntegrateFragilityCurve


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

    assert np.isclose(
        integrate_statistics_per_section.df_out.loc[0, "result"],
        np.float64(2.01314963024323e-08),
    )
