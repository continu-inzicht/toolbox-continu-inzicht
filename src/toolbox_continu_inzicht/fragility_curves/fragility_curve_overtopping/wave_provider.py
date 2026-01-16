from __future__ import annotations

from enum import Enum
from typing import Protocol

import numpy as np
import pandas as pd

from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.pydra_legacy import (
    bretschneider,
)
from toolbox_continu_inzicht.utils.interpolate import (
    bracketing_indices,
    circular_interpolate_1d,
    interpolate_1d,
)


class WaveType(Enum):
    SIGNIFICANT_WAVEHEIGHT = 2
    SPECTRAL_WAVEPERIOD = 6
    WAVEDIRECTION = 7


class WaveProvider(Protocol):
    """
    Interface voor het leveren van golfcondities voor overtopping berekeningen.
    """

    def get_wave_conditions_for_directions(
        self,
        windspeed: float,
        windrichtingen: np.ndarray,
        waterlevel: float,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Geef hs, tspec en wave_direction per windrichting.

        Parameters
        ----------
        windspeed : float
            Windsnelheid.
        windrichtingen : np.ndarray
            Windrichtingen (graden).
        waterlevel : float
            Waterniveau.

        Returns
        -------
        tuple[np.ndarray, np.ndarray, np.ndarray]
            hs, tspec en wave_direction per windrichting.
        """

    def get_wave_conditions_for_levels(
        self,
        windspeed: float,
        direction: float,
        waterlevels: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Geef hs, tspec en wave_direction per waterlevel.

        Parameters
        ----------
        windspeed : float
            Windsnelheid.
        direction : float
            Windrichting (graden).
        waterlevels : np.ndarray
            Waterniveaus.

        Returns
        -------
        tuple[np.ndarray, np.ndarray, np.ndarray]
            hs, tspec en wave_direction per waterlevel.
        """


class BretschneiderWaveProvider(WaveProvider):
    """
    WaveProvider implementatie op basis van Bretschneider.
    """

    def __init__(
        self,
        bedlevel: np.ndarray,
        fetch: np.ndarray,
        windrichtingen: np.ndarray,
        tp_tspec: float = 1.1,
    ) -> None:
        self.bedlevel = np.asarray(bedlevel, dtype=float)
        self.fetch = np.asarray(fetch, dtype=float)
        self.windrichtingen = np.asarray(windrichtingen, dtype=float)
        self.tp_tspec = tp_tspec

    def get_wave_conditions_for_directions(
        self,
        windspeed: float,
        windrichtingen: np.ndarray,
        waterlevel: float,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        bedlevel = np.interp(
            windrichtingen, self.windrichtingen, self.bedlevel, period=360
        )
        fetch = np.interp(windrichtingen, self.windrichtingen, self.fetch, period=360)
        depth = waterlevel - bedlevel
        hss, tps = bretschneider(
            d=depth,
            fe=fetch,
            u=np.ones_like(bedlevel) * windspeed,
        )
        tspec = tps / self.tp_tspec
        wave_direction = np.asarray(windrichtingen, dtype=float)
        return hss, tspec, wave_direction

    def get_wave_conditions_for_levels(
        self,
        windspeed: float,
        direction: float,
        waterlevels: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        waterlevels = np.asarray(waterlevels, dtype=float)
        bedlevel = float(
            np.interp([direction], self.windrichtingen, self.bedlevel, period=360)[0]
        )
        fetch = float(
            np.interp([direction], self.windrichtingen, self.fetch, period=360)[0]
        )
        depth = waterlevels - bedlevel
        hss, tps = bretschneider(
            d=depth,
            fe=np.ones_like(waterlevels) * fetch,
            u=np.ones_like(waterlevels) * windspeed,
        )
        tspec = tps / self.tp_tspec
        wave_direction = np.ones_like(waterlevels) * direction
        return hss, tspec, wave_direction


class PreCalculatedWaveProvider(WaveProvider):
    """
    WaveProvider implementatie op basis van voorberekende golfcondities.
    """

    def __init__(self, df_waveval_id: pd.DataFrame, df_waveval: pd.DataFrame) -> None:
        self.waveval_by_type = {}
        for wvt, group in df_waveval_id.groupby("waveval_type"):
            self.waveval_by_type[int(wvt)] = group.merge(df_waveval, on="waveval_id")

        # Check that all required wavevaltypes are present
        for required_wvt in WaveType:
            if required_wvt.value not in self.waveval_by_type:
                raise KeyError(f"{required_wvt} is not present in df_waveval_id")

    def _interpolate_type_for_directions(
        self,
        waveval_type: int,
        windspeed: float,
        windrichtingen: np.ndarray,
        waterlevel: float,
    ) -> np.ndarray:
        group = self.waveval_by_type[waveval_type]
        ws = group["windspeed"].to_numpy()
        wd = group["winddir"].to_numpy()
        wl = group["waterlevel"].to_numpy()
        wy = group["waveval"].to_numpy()

        wsv, ws_idx = np.unique(ws, return_inverse=True)
        wdv, wd_idx = np.unique(wd, return_inverse=True)
        wlv, wl_idx = np.unique(wl, return_inverse=True)

        grid_wswdwl = np.full((wsv.size, wdv.size, wlv.size), np.nan, dtype=float)
        grid_wswdwl[ws_idx, wd_idx, wl_idx] = wy

        i1, i2, fws = bracketing_indices(wsv, windspeed)
        grid_wdwl = (1 - fws) * grid_wswdwl[i1, :, :] + fws * grid_wswdwl[i2, :, :]

        i3, i4, fwl = bracketing_indices(wlv, waterlevel)
        grid_wd = (1 - fwl) * grid_wdwl[:, i3] + fwl * grid_wdwl[:, i4]

        wd_ext = np.concatenate([wdv - 360.0, wdv, wdv + 360.0])
        grid_wd_ext = np.concatenate([grid_wd, grid_wd, grid_wd])
        if waveval_type == WaveType.WAVEDIRECTION.value:
            return circular_interpolate_1d(windrichtingen, wd_ext, grid_wd_ext)
        return interpolate_1d(windrichtingen, wd_ext, grid_wd_ext, ll=-np.inf)

    def _interpolate_type_for_levels(
        self,
        waveval_type: int,
        windspeed: float,
        direction: float,
        waterlevels: np.ndarray,
    ) -> np.ndarray:
        group = self.waveval_by_type[waveval_type]
        ws = group["windspeed"].to_numpy()
        wd = group["winddir"].to_numpy()
        wl = group["waterlevel"].to_numpy()
        wy = group["waveval"].to_numpy()

        wsv, ws_idx = np.unique(ws, return_inverse=True)
        wdv, wd_idx = np.unique(wd, return_inverse=True)
        wlv, wl_idx = np.unique(wl, return_inverse=True)

        grid_wswdwl = np.full((wsv.size, wdv.size, wlv.size), np.nan, dtype=float)
        grid_wswdwl[ws_idx, wd_idx, wl_idx] = wy

        i1, i2, fws = bracketing_indices(wsv, windspeed)
        grid_wdwl = (1 - fws) * grid_wswdwl[i1, :, :] + fws * grid_wswdwl[i2, :, :]

        wd_ext = np.concatenate([wdv - 360.0, wdv, wdv + 360.0])
        grid_wd_ext = np.concatenate([grid_wdwl, grid_wdwl, grid_wdwl], axis=0)
        grid_wl = interpolate_1d(
            np.array([direction]), wd_ext, grid_wd_ext, ll=-np.inf
        )[0]

        if waveval_type == WaveType.WAVEDIRECTION.value:
            return circular_interpolate_1d(waterlevels, wlv, grid_wl, ll=-np.inf)
        return interpolate_1d(waterlevels, wlv, grid_wl, ll=-np.inf)

    def get_wave_conditions_for_directions(
        self,
        windspeed: float,
        windrichtingen: np.ndarray,
        waterlevel: float,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        hs = self._interpolate_type_for_directions(
            WaveType.SIGNIFICANT_WAVEHEIGHT.value, windspeed, windrichtingen, waterlevel
        )
        tspec = self._interpolate_type_for_directions(
            WaveType.SPECTRAL_WAVEPERIOD.value, windspeed, windrichtingen, waterlevel
        )
        wave_direction = self._interpolate_type_for_directions(
            WaveType.WAVEDIRECTION.value, windspeed, windrichtingen, waterlevel
        )
        return hs, tspec, wave_direction

    def get_wave_conditions_for_levels(
        self,
        windspeed: float,
        direction: float,
        waterlevels: np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        hs = self._interpolate_type_for_levels(
            WaveType.SIGNIFICANT_WAVEHEIGHT.value, windspeed, direction, waterlevels
        )
        tspec = self._interpolate_type_for_levels(
            WaveType.SPECTRAL_WAVEPERIOD.value, windspeed, direction, waterlevels
        )
        wave_direction = self._interpolate_type_for_levels(
            WaveType.WAVEDIRECTION.value, windspeed, direction, waterlevels
        )
        return hs, tspec, wave_direction
