import numpy as np
import pandas as pd

from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.pydra_legacy import (
    bretschneider,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.wave_provider import (
    BretschneiderWaveProvider,
    WaveDataProvider,
)


def test_bretschneider_wave_provider_matches_bretschneider():
    windrichtingen = np.array([0.0, 90.0, 180.0])
    bedlevel = np.array([1.0, 2.0, 3.0])
    fetch = np.array([100.0, 200.0, 300.0])
    tp_tspec = 1.1
    provider = BretschneiderWaveProvider(
        bedlevel=bedlevel,
        fetch=fetch,
        windrichtingen=windrichtingen,
        tp_tspec=tp_tspec,
    )

    windspeed = 10.0
    waterlevel = 5.0
    hs, tspec, wave_dir = provider.get_wave_conditions_for_directions(
        windspeed, windrichtingen, waterlevel
    )

    depth = waterlevel - bedlevel
    hs_exp, tp_exp = bretschneider(
        d=depth, fe=fetch, u=np.ones_like(bedlevel) * windspeed
    )
    tspec_exp = tp_exp / tp_tspec

    assert np.allclose(hs, hs_exp)
    assert np.allclose(tspec, tspec_exp)
    assert np.allclose(wave_dir, windrichtingen)

    direction = 90.0
    waterlevels = np.array([4.0, 5.0])
    hs_lvl, tspec_lvl, wave_dir_lvl = provider.get_wave_conditions_for_levels(
        windspeed, direction, waterlevels
    )

    bedlevel_interp = np.interp([direction], windrichtingen, bedlevel, period=360)[0]
    fetch_interp = np.interp([direction], windrichtingen, fetch, period=360)[0]
    depth = waterlevels - bedlevel_interp
    hs_exp, tp_exp = bretschneider(
        d=depth,
        fe=np.ones_like(waterlevels) * fetch_interp,
        u=np.ones_like(waterlevels) * windspeed,
    )
    tspec_exp = tp_exp / tp_tspec

    assert np.allclose(hs_lvl, hs_exp)
    assert np.allclose(tspec_lvl, tspec_exp)
    assert np.allclose(wave_dir_lvl, np.ones_like(waterlevels) * direction)


def test_wavedata_wave_provider_interpolation():
    rows = []
    for waveval_type in [2, 6, 7]:
        for ws in [10.0, 20.0]:
            for wd in [0.0, 90.0]:
                for wl in [1.0, 2.0]:
                    if waveval_type == 2:
                        waveval = ws + wd / 100.0 + wl / 10.0
                    elif waveval_type == 6:
                        waveval = 2.0 * ws + wd / 100.0 + wl / 10.0
                    else:
                        waveval = wd
                    rows.append(
                        {
                            "waveval_type": waveval_type,
                            "windspeed": ws,
                            "winddir": wd,
                            "waterlevel": wl,
                            "waveval": waveval,
                        }
                    )
    waveval_df = pd.DataFrame(rows)
    id_cols = ["waveval_type", "windspeed", "winddir"]
    waveval_id_df = waveval_df[id_cols].drop_duplicates().reset_index(drop=True)
    waveval_id_df["waveval_id"] = np.arange(len(waveval_id_df))
    waveval_df = waveval_df.merge(waveval_id_df, on=id_cols)[
        ["waveval_id", "waterlevel", "waveval"]
    ]
    provider = WaveDataProvider(waveval_id_df, waveval_df)

    windspeed = 10.0
    windrichtingen = np.array([0.0, 90.0])
    waterlevel = 1.0
    hs, tspec, wave_dir = provider.get_wave_conditions_for_directions(
        windspeed, windrichtingen, waterlevel
    )

    assert np.allclose(hs, np.array([10.1, 11.0]))
    assert np.allclose(tspec, np.array([20.1, 21.0]))
    assert np.allclose(wave_dir, windrichtingen)

    direction = 0.0
    waterlevels = np.array([1.0, 2.0])
    hs_lvl, tspec_lvl, wave_dir_lvl = provider.get_wave_conditions_for_levels(
        windspeed, direction, waterlevels
    )

    assert np.allclose(hs_lvl, np.array([10.1, 10.2]))
    assert np.allclose(tspec_lvl, np.array([20.1, 20.2]))
    assert np.allclose(wave_dir_lvl, np.zeros_like(waterlevels))


def test_wavedata_wave_provider_circular_direction():
    rows = []
    for waveval_type in [2, 6, 7]:
        for ws in [10.0, 20.0]:
            for wd in [350.0, 10.0]:
                for wl in [1.0, 2.0]:
                    waveval = wd if waveval_type == 7 else 1.0
                    rows.append(
                        {
                            "waveval_type": waveval_type,
                            "windspeed": ws,
                            "winddir": wd,
                            "waterlevel": wl,
                            "waveval": waveval,
                        }
                    )
    waveval_df = pd.DataFrame(rows)
    id_cols = ["waveval_type", "windspeed", "winddir"]
    waveval_id_df = waveval_df[id_cols].drop_duplicates().reset_index(drop=True)
    waveval_id_df["waveval_id"] = np.arange(len(waveval_id_df))
    waveval_df = waveval_df.merge(waveval_id_df, on=id_cols)[
        ["waveval_id", "waterlevel", "waveval"]
    ]
    provider = WaveDataProvider(waveval_id_df, waveval_df)

    windspeed = 10.0
    windrichtingen = np.array([0.0])
    waterlevel = 1.0
    _, _, wave_dir = provider.get_wave_conditions_for_directions(
        windspeed, windrichtingen, waterlevel
    )

    assert np.isclose(wave_dir[0], 0.0, atol=1e-6)
