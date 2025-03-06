from abc import abstractmethod
from typing import Callable, ClassVar, Optional

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

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object om data in te laden
    hydraulicload: Optional[np.ndarray] | None
        Array met de belastingen
    failure_probability: Optional[np.ndarray] | None
        Array met de faalkansen
    lower_limit: float
        Ondergrens voor de faalkans, standaard 1e-20
    interp_func: Callable
        Functie waarmee geinterpoleerd wordt
    fragility_curve_schema: ClassVar[dict[str, str]]
        Schema waaraan de fragility curve moet voldoen:{hydraulicload: float, failure_probability: float}
    """

    data_adapter: DataAdapter
    hydraulicload: Optional[np.ndarray] | None = None
    failure_probability: Optional[np.ndarray] | None = None
    lower_limit: float = 1e-20
    interp_func: Callable = log_interpolate_1d
    fragility_curve_schema: ClassVar[dict[str, str]] = {
        "hydraulicload": "float",
        "failure_probability": "float",
    }

    def run(self, *args, **kwargs):
        self.calculate_fragility_curve(*args, **kwargs)

    @abstractmethod
    def calculate_fragility_curve(self, *args, **kwargs):
        pass

    def as_array(self):
        """Geef curve terug als NumPy array. Deze kunnen vervolgens worden gestacked en in een database geplaatst"""
        return np.vstack(
            [
                self.hydraulicload,
                self.failure_probability,
            ]
        ).T

    def as_dataframe(self):
        """Geef curve terug als pandas dataframe"""
        return pd.DataFrame(
            {
                "hydraulicload": self.hydraulicload,
                "failure_probability": self.failure_probability,
            }
        )

    def from_dataframe(self, df: pd.DataFrame):
        """Zet een dataframe om naar een fragility curve"""
        self.hydraulicload = df["hydraulicload"].to_numpy()
        self.failure_probability = df["failure_probability"].to_numpy()

    def load(self, input: str):
        """Laadt een fragility curve in"""
        df_in = self.data_adapter.input(input, schema=self.fragility_curve_schema)
        self.from_dataframe(df_in)

    def shift(self, effect: float):
        """Schuif een fragility curve op

        Schuift de belasting van de fragility curve op (voor bijvoorbeeld
        een noodmaatregel), en interpoleer de faalkansen op het oorspronkelijke
        waterstandsgrid
        """
        if effect == 0.0:
            return None
        # For now okay, consider later: Log or not? ideally interpolate beta values
        x = self.hydraulicload
        fp = self.failure_probability
        xp = x + effect
        self.failure_probability = self.interp_func(x, xp, fp, ll=1e-20, clip01=True)

    def refine(self, new_hydraulicload: np.ndarray | list[float] | float):
        """Interpoleert de fragility curve op de gegeven waterstanden"""
        refined_failure_probability = self.interp_func(
            new_hydraulicload,
            self.hydraulicload,
            self.failure_probability,
            ll=self.lower_limit,
            clip01=True,
        )
        self.hydraulicload = new_hydraulicload
        self.failure_probability = refined_failure_probability

    def reliability_update(
        self, update_level: int | float, trust_factor: int | float = 1
    ):
        """Voer een versimpelde reliability updating uit

        Parameters
        ----------
        update_level : int | float
            hydraulic load level to which the fragility curve is updated
        trust_factor : int | float, optional
            by default 1
        """
        wl_grid = self.hydraulicload
        fp_grid = self.failure_probability

        sel_update = wl_grid < update_level
        wl_steps = np.diff(wl_grid[sel_update])
        wl_steps = np.hstack([wl_steps[0], wl_steps])
        F_update = trust_factor * (fp_grid[sel_update] * wl_steps).sum()

        fp_grid[sel_update] = (1 - trust_factor) * fp_grid[sel_update]
        fp_grid[~sel_update] = (fp_grid[~sel_update] - F_update) / (1 - F_update)
        self.failure_probability = fp_grid
