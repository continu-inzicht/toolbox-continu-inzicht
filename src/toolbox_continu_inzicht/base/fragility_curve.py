from typing import Callable, ClassVar, Optional
import warnings

import numpy as np
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import ToolboxBase, DataAdapter
from toolbox_continu_inzicht.utils.interpolate import log_interpolate_1d


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurve(ToolboxBase):
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
        Ondergrens voor de interpolatie van de faalkans, standaard 1e-200
    interp_func: Callable
        Functie waarmee geinterpoleerd wordt
    enforce_monotonic: bool
        Forceert monotoon stijgende faalkansen, standaard True
    fragility_curve_schema: ClassVar[dict[str, str]]
        Schema waaraan de fragility curve moet voldoen:{hydraulicload: float, failure_probability: float}
    """

    data_adapter: DataAdapter
    hydraulicload: Optional[np.ndarray] | None = None
    failure_probability: Optional[np.ndarray] | None = None
    lower_limit: float = 1e-200
    interp_func: Callable = log_interpolate_1d
    enforce_monotonic: bool = True
    fragility_curve_schema: ClassVar[dict[str, str]] = {
        "hydraulicload": "float",
        "failure_probability": "float",
    }

    def run(self, *args, **kwargs):
        self.calculate_fragility_curve(*args, **kwargs)

    def calculate_fragility_curve(self, *args, **kwargs):
        # Dit is geen abstractmethod omdat anders deze class altijd als
        # subclass gebruikt moet worden. Raise daarom alleen een TypeError
        # als deze methode wordt aangeroepen zonder subclass.
        raise TypeError(
            "De methode 'calculate_fragility_curve' is niet geïmplementeerd"
        )

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
        self.check_monotonic_curve()

    def load(self, input: str):
        """Laadt een fragility curve in"""
        df_in = self.data_adapter.input(input, schema=self.fragility_curve_schema)
        self.from_dataframe(df_in)

    def shift(self, effect: float):
        """Schuift de hydraulische belasting van de fragility curve op om
        bijvoorbeeld het effect van een noodmaatregel te implementeren. Een
        positieve verschuiving levert bij dezelfde faalkans dan een hogere
        hydraulische belasting op. Of bij dezelfde hydraulische belasting een
        lagere faalkans.
        """
        if effect == 0.0:
            return None
        # For now okay, consider later: Log or not? ideally interpolate beta values
        x = self.hydraulicload
        fp = self.failure_probability
        xp = x + effect
        self.failure_probability = self.interp_func(
            x, xp, fp, ll=self.lower_limit, clip01=True
        )

    def check_monotonic_curve(self):
        """Forceert monotoon stijgende faalkansen"""
        if self.enforce_monotonic:
            # Forceer dat de faalkansen monotoon stijgend zijn
            self.sort_curve()
            self.failure_probability = np.maximum.accumulate(self.failure_probability)

    def find_jump_indices(self):
        stepsize = np.diff(self.hydraulicload)
        jumps = np.nonzero(stepsize == 0)[0]
        idxs = np.vstack([jumps, jumps + 1]).flatten(order="F")

        return idxs

    def sort_curve(self):
        """Sorteert de fragility curve eerst op waterstand en vervolgens op faalkans"""
        lexsort = np.lexsort((self.failure_probability, self.hydraulicload))
        self.hydraulicload = self.hydraulicload[lexsort]
        self.failure_probability = self.failure_probability[lexsort]

    def refine(
        self,
        new_hydraulicload: np.ndarray | list[float] | float,
        add_steps: bool = True,
    ):
        """Interpoleert de fragility curve op de gegeven waterstanden"""
        new_failure_probability = self.interp_func(
            new_hydraulicload,
            self.hydraulicload,
            self.failure_probability,
            ll=self.lower_limit,
            clip01=True,
        )

        if add_steps:
            idxs = self.find_jump_indices()
            if len(idxs) > 0:
                # Voeg sprongen toe aan de nieuwe waterstanden
                new_hydraulicload = np.hstack(
                    [new_hydraulicload, self.hydraulicload[idxs]]
                )
                new_failure_probability = np.hstack(
                    [new_failure_probability, self.failure_probability[idxs]]
                )

                # Verwijder eventuele dubbelingen
                data = np.vstack([new_hydraulicload, new_failure_probability])
                data = np.unique(data, axis=1)
                new_hydraulicload = data[0, :]
                new_failure_probability = data[1, :]

        self.hydraulicload = new_hydraulicload
        self.failure_probability = new_failure_probability
        self.sort_curve()

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
        if len(wl_steps) == 0:
            msg = "Geen waardes om aan te passen, originele curve blijft geldig"
            self.data_adapter.logger.warning(msg)
            warnings.warn(msg, UserWarning)
        else:
            wl_steps = np.hstack([wl_steps[0], wl_steps])
            F_update = trust_factor * (fp_grid[sel_update] * wl_steps).sum()

            fp_grid[sel_update] = (1 - trust_factor) * fp_grid[sel_update]
            fp_grid[~sel_update] = (fp_grid[~sel_update] - F_update) / (1 - F_update)
            self.failure_probability = fp_grid
