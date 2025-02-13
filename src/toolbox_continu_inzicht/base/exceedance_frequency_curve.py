from abc import abstractmethod
from pydantic.dataclasses import dataclass
import numpy as np
from toolbox_continu_inzicht import DataAdapter
from typing import Optional
import pandas as pd
from toolbox_continu_inzicht.utils.interpolate import log_interpolate_1d


@dataclass(config={"arbitrary_types_allowed": True})
class ExceedanceFrequencyCurve:
    """
    Class met een aantal gemakkelijke methoden om overschrijdingsfrequentiecurves
    op te slaan en aan te passen
    """

    data_adapter: DataAdapter
    df_out: Optional[pd.DataFrame] | None = None

    exceedance_frequency_curve_schema = {
        "waterlevels": float,
        "probability_exceedance": float,
    }

    def run(self, *args, **kwargs):
        self.calculate_exceedance_frequency_curve(*args, **kwargs)

    @abstractmethod
    def calculate_exceedance_frequency_curve(self, *args, **kwargs):
        pass

    def as_array(self):
        """Geeft curve terug als numpy array, deze kunnen vervolgens worden gestacked en in een database geplaatst"""
        arr = self.df_out[["waterlevels", "probability_exceedance"]].to_numpy()
        return arr

    def load(self, input: str):
        """Laad een fragility curve in"""
        self.df_out = self.data_adapter.input(
            input, schema=self.exceedance_frequency_curve_schema
        )

    def shift(self, effect):
        """Schuift de waterstanden van de overschrijdingsfrequentiecurve op  en interpoleer de faalkansen
        op het oorspronkelijke waterstandsgrid"""
        if effect == 0.0:
            return None
        # For now okay, consider later: Log or not? ideally interpolate beta values
        self.df_out["probability_exceedance"] = np.maximum(
            0.0,
            np.minimum(
                1.0,
                log_interpolate_1d(
                    self.df_out["waterlevels"].to_numpy(),
                    self.df_out["waterlevels"].to_numpy() + effect,
                    self.df_out["probability_exceedance"].to_numpy(),
                ),
            ),
        )

    def refine(self, waterlevels):
        """Interpolleer de fragility curve op de gegeven waterstanden"""
        df_new = pd.DataFrame(
            {
                "waterlevels": waterlevels,
                "probability_exceedance": 0.0,
            }
        )
        df_new["probability_exceedance"] = np.maximum(
            0.0,
            np.minimum(
                1.0,
                log_interpolate_1d(
                    waterlevels,
                    self.df_out["waterlevels"].to_numpy(),
                    self.df_out["probability_exceedance"].to_numpy(),
                ),
            ),
        )
        self.df_out = df_new
