from abc import abstractmethod
from pydantic.dataclasses import dataclass
import numpy as np
from toolbox_continu_inzicht import DataAdapter
from toolbox_continu_inzicht.utils.interpolate import log_interpolate_1d
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

    def run(self, *args, **kwargs):
        self.calculate_fragility_curve(*args, **kwargs)

    @abstractmethod
    def calculate_fragility_curve(self, *args, **kwargs):
        pass

    def as_array(self):
        """Geeft curve terug als numpy array, deze kunnen vervolgens worden gestacked en in een database geplaatst"""
        arr = self.df_out[["waterlevels", "failure_probability"]].to_numpy()
        return arr

    def shift(self, effect):
        """Schuift de waterstanden van de fragility curve op (voor een noodmaatregel), en interpoleer de faalkansen
        op het oorspronkelijke waterstandsgrid"""
        if effect == 0.0:
            return None
        # TODO: Log or not? ideally interpolate beta values
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
        df_new = pd.Dataframe(
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
