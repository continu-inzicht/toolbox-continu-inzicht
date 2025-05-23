from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import ToolboxBase, DataAdapter
from toolbox_continu_inzicht.utils.interpolate import log_interpolate_1d


@dataclass(config={"arbitrary_types_allowed": True})
class ExceedanceFrequencyCurve(ToolboxBase):
    """
    Class met een aantal gemakkelijke methoden om overschrijdingsfrequentiecurves
    op te slaan en aan te passen.

    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object om data in te laden
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de overschrijdingsfrequentiecurve
    lower_limit: float
        Ondergrens voor de overschrijdingsfrequentie, standaard 1e-200
    exceedance_frequency_curve_schema: ClassVar[dict[str, str]]
        Schema waaraan de overschrijdingsfrequentiecurve moet voldoen: {hydraulicload: float, probability_exceedance: float}
    """

    data_adapter: DataAdapter
    df_out: Optional[pd.DataFrame] | None = None
    lower_limit: float = 1e-200
    exceedance_frequency_curve_schema: ClassVar[dict[str, str]] = {
        "hydraulicload": float,
        "probability_exceedance": float,
    }

    def run(self, *args, **kwargs):
        self.calculate_exceedance_frequency_curve(*args, **kwargs)

    def calculate_exceedance_frequency_curve(self, *args, **kwargs):
        # Dit is geen abstractmethod omdat anders deze class altijd als
        # subclass gebruikt moet worden. Raise daarom alleen een TypeError
        # als deze methode wordt aangeroepen zonder subclass.
        raise TypeError(
            "De methode 'calculate_exceedance_frequency_curve' is niet geimplementeerd"
        )

    def as_array(self):
        """Geef curve terug als numpy array, deze kunnen vervolgens worden gestacked en in een database geplaatst"""
        arr = self.df_out[["hydraulicload", "probability_exceedance"]].to_numpy()
        return arr

    def load(self, input: str):
        """Laad een overschrijdingsfrequentielijn in"""
        self.df_out = self.data_adapter.input(
            input, schema=self.exceedance_frequency_curve_schema
        )

    def refine(self, hydraulicload):
        """Interpoleer de overschrijdingsfrequentielijn op de gegeven waterstanden"""
        df_new = pd.DataFrame(
            {
                "hydraulicload": hydraulicload,
                "probability_exceedance": log_interpolate_1d(
                    hydraulicload,
                    self.df_out["hydraulicload"].to_numpy(),
                    self.df_out["probability_exceedance"].to_numpy(),
                    ll=self.lower_limit,
                    clip01=True,
                ),
            }
        )
        self.df_out = df_new
