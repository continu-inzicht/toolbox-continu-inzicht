from abc import abstractmethod
from pydantic.dataclasses import dataclass
import numpy as np
from toolbox_continu_inzicht import DataAdapter
from typing import Optional
import pandas as pd


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurve:
    """
    Class met een aantal gemakkelijke methoden om fragility curves
    op te slaan en aan te passen
    """

    data_adapter: DataAdapter
    df_out: Optional[pd.DataFrame] | None = None

    fragility_curve_schema = {
        "waterlevels": float,
        "failure_probability": float,
    }

    def run(self, *args, **kwargs):
        self.calculate_fragility_curve(*args, **kwargs)

    @abstractmethod
    def calculate_fragility_curve(self, *args, **kwargs):
        pass

    def as_array(self):
        """Geeft curve terug als numpy array, deze kunnen vervolgens worden gestacked en in een database geplaatst"""
        arr = self.df_out[["waterlevels", "failure_probability"]].to_numpy()
        return arr

    def load(self, input: str):
        """Laad een fragility curve in"""
        self.df_out = self.data_adapter.input(input, schema=self.fragility_curve_schema)

    def shift(self, effect):
        """Schuift de waterstanden van de fragility curve op (voor een noodmaatregel), en interpoleer de faalkansen
        op het oorspronkelijke waterstandsgrid"""
        if effect == 0.0:
            return None
        # For now okay, consider later: Log or not? ideally interpolate beta values
        self.df_out["failure_probability"] = np.maximum(
            0.0,
            np.minimum(
                1.0,
                log_interpolate_1d(
                    self.df_out["waterlevels"].to_numpy(),
                    self.df_out["waterlevels"].to_numpy() + effect,
                    self.df_out["failure_probability"].to_numpy(),
                ),
            ),
        )

    def refine(self, waterlevels):
        """Interpolleer de fragility curve op de gegeven waterstanden"""
        df_new = pd.DataFrame(
            {
                "waterlevels": waterlevels,
                "failure_probability": 0.0,
            }
        )
        df_new["failure_probability"] = np.maximum(
            0.0,
            np.minimum(
                1.0,
                log_interpolate_1d(
                    waterlevels,
                    self.df_out["waterlevels"].to_numpy(),
                    self.df_out["failure_probability"].to_numpy(),
                ),
            ),
        )
        self.df_out = df_new


def interpolate_1d(x, xp, fp):
    """
    Interpolate an array along the given axis.
    Similar to np.interp, but with extrapolation outside range.

    Parameters
    ----------
    x : np.array
        Array with positions to interpolate at
    xp : np.array
        Array with positions of known values
    fp : np.array
        Array with values as known positions to interpolate between

    Returns
    -------
    np.array
        interpolated array
    """
    # Determine lower bounds
    intidx = np.minimum(np.maximum(0, np.searchsorted(xp, x) - 1), len(xp) - 2)
    # Determine interpolation fractions
    fracs = (x - xp[intidx]) / (xp[intidx + 1] - xp[intidx])
    # Interpolate (1-frac) * f_low + frac * f_up
    f = (1 - fracs) * fp[intidx] + fp[intidx + 1] * fracs

    return f


def log_interpolate_1d(x, xp, fp):
    """Similar to interpolate_1d, but interpolates in log-space"""
    fp[fp < 1e-20] = 1e-20
    return np.exp(interpolate_1d(x, xp, np.log(fp)))
