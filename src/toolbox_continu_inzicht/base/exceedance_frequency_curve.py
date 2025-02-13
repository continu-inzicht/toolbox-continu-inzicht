from abc import abstractmethod
from typing import Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter
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
        """Geef curve terug als numpy array, deze kunnen vervolgens worden gestacked en in een database geplaatst"""
        arr = self.df_out[["waterlevels", "probability_exceedance"]].to_numpy()
        return arr

    def load(self, input: str):
        """Laad een overschrijdingsfrequentielijn in"""
        self.df_out = self.data_adapter.input(
            input, schema=self.exceedance_frequency_curve_schema
        )

    def refine(self, waterlevels):
        """Interpoleer de overschrijdingsfrequentielijn op de gegeven waterstanden"""
        df_new = pd.DataFrame(
            {
                "waterlevels": waterlevels,
                "probability_exceedance": log_interpolate_1d(
                    waterlevels,
                    self.df_out["waterlevels"].to_numpy(),
                    self.df_out["probability_exceedance"].to_numpy(),
                    ll=1e-20,
                    clip01=True,
                ),
            }
        )
        self.df_out = df_new
