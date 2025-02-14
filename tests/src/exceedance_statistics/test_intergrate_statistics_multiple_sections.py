from pathlib import Path

import numpy as np
from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht.exceedance_statistics import IntegrateStatistics


def test_integrate_statistics_multiple_sections():
    path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=path / "test_integrate_statistics_multiple_sections.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    integrate_statistics_per_section = IntegrateStatistics(data_adapter=data_adapter)
    integrate_statistics_per_section.run(
        input=["exceedance_curve_csv", "fragility_curve_multi_csv"], output="result"
    )

    assert np.isclose(
        integrate_statistics_per_section.df_out.result.to_numpy(),
        np.array([2.014131e-08, 2.014131e-08]),
    ).all()
