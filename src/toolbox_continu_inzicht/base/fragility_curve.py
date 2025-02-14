from abc import abstractmethod
from typing import Optional

import numpy as np
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter
from toolbox_continu_inzicht.utils.interpolate import log_interpolate_1d


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurve:
    """
    Class met een aantal gemakkelijke methoden om fragility curves
    op te slaan en aan te passen
    """

    data_adapter: DataAdapter
    waterlevels: Optional[np.ndarray] | None = None
    failure_probability: Optional[np.ndarray] | None = None
    fragility_curve_schema = {
        "waterlevels": float,
        "failure_probability": float,
    }
    lower_limit = 1e-20

    def run(self, *args, **kwargs):
        self.calculate_fragility_curve(*args, **kwargs)

    @abstractmethod
    def calculate_fragility_curve(self, *args, **kwargs):
        pass

    def as_array(self):
        """Geef curve terug als numpy array, deze kunnen vervolgens worden gestacked en in een database geplaatst"""
        return np.vstack(
            [
                self.waterlevels,
                self.failure_probability,
            ]
        ).T

    def as_dataframe(self):
        """Geef curve terug als pandas dataframe"""
        return pd.DataFrame(
            {
                "waterlevels": self.waterlevels,
                "failure_probability": self.failure_probability,
            }
        )

    def from_dataframe(self, df):
        """Zet een dataframe om naar een fragility curve"""
        self.waterlevels = df["waterlevels"].to_numpy()
        self.failure_probability = df["failure_probability"].to_numpy()

    def load(self, input: str):
        """Laad een fragility curve in"""
        df_in = self.data_adapter.input(input, schema=self.fragility_curve_schema)
        self.from_dataframe(df_in)

    def shift(self, effect):
        """Schuif een fragility curve op

        Schuift de waterstanden van de fragility curve op (voor bijvoorbeeld
        een noodmaatregel), en interpoleer de faalkansen op het oorspronkelijke
        waterstandsgrid
        """
        if effect == 0.0:
            return None
        # For now okay, consider later: Log or not? ideally interpolate beta values
        x = self.waterlevels
        fp = self.failure_probability
        xp = x + effect
        self.failure_probability = log_interpolate_1d(x, xp, fp, ll=1e-20, clip01=True)

    def refine(self, waterlevels):
        """Interpoleer de fragility curve op de gegeven waterstanden"""
        refined_failure_probability = log_interpolate_1d(
            waterlevels,
            self.waterlevels,
            self.failure_probability,
            ll=self.lower_limit,
            clip01=True,
        )
        self.waterlevels = waterlevels
        self.failure_probability = refined_failure_probability

    def reliability_update(self, update_level, trust_factor=1):
        """Voer een versimpelde reliability updating uit

        Parameters
        ----------
        update_level : _type_
            _description_
        trust_factor : int, optional
            _description_, by default 1
        """
        wl_grid = self.waterlevels
        fp_grid = self.failure_probability

        sel_update = wl_grid < update_level
        wl_steps = np.diff(wl_grid[sel_update])
        wl_steps = np.hstack([wl_steps[0], wl_steps])
        F_update = trust_factor * (fp_grid[sel_update] * wl_steps).sum()

        fp_grid[sel_update] = (1 - trust_factor) * fp_grid[sel_update]
        fp_grid[~sel_update] = (fp_grid[~sel_update] - F_update) / (1 - F_update)
        self.failure_probability = fp_grid
