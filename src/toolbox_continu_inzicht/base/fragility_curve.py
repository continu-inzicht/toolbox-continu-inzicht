from typing import Callable, ClassVar, Optional
import warnings

import numpy as np
import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import ToolboxBase, DataAdapter
from toolbox_continu_inzicht.utils.interpolate import (
    _interpolate_1d,
    log_interpolate_1d,
)
from pydantic import TypeAdapter


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
    cached_fragility_curves: Optional[dict[str | int, str | pd.DataFrame]] | None
        Cache voor fragility curves die al zijn berekend, de key is een string of int met een bijbehorende DataAdapter naam
        Afhankelijk van de implementatie kan deze cache worden gebruikt om fragility curves in te laden zonder deze opnieuw te berekenen.
        De logica om de selectie van de cache te kiezen moet op een hoger abstractieniveau worden geïmplementeerd.
    measure_to_effect: Optional[dict[str | int, float]] | None
        Mapping van maatregel id's naar effect waarden (float) om verschuivingen van de fragility curve te bepalen.
        Deze mapping kan worden gebruikt in combinatie met de 'shift' methode om fragility curves aan te passen op basis van specifieke maatregelen.
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
    cached_fragility_curves: Optional[dict[str | int, str | pd.DataFrame]] | None = None
    measure_to_effect: Optional[dict[str | int, float]] | None = None

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

    def load_effect_from_dataframe(self, cached_value: int | str):
        """Gebruik een zelf opgegeven DataAdapter om de fragility curve in te laden"""
        if cached_value in self.cached_fragility_curves:
            df_in = self.cached_fragility_curves[cached_value]
            self.from_dataframe(df_in)
        else:
            self.data_adapter.logger.info(
                f"Fragility curve with key {cached_value} not found in cache: {self.cached_fragility_curves.keys()}, cannot load. so shifting instead."
            )
            if self.measure_to_effect is None:
                raise ValueError(
                    "measure_to_effect is not defined, cannot shift fragility curve."
                )
            effect = self.measure_to_effect[cached_value]
            self.shift(effect=effect)

    def load_effect_from_data_adapter(self, cached_value: int | str):
        """Gebruik een zelf opgegeven DataAdapter om de fragility curve in te laden"""
        if cached_value in self.cached_fragility_curves:
            data_adapter_to_load = self.cached_fragility_curves[cached_value]
            df_in = self.data_adapter.input(data_adapter_to_load)
            self.from_dataframe(df_in)
        else:
            self.data_adapter.logger.info(
                f"Fragility curve with key {cached_value} not found in cache: {self.cached_fragility_curves.keys()}, cannot load. so shifting instead."
            )
            if self.measure_to_effect is None:
                raise ValueError(
                    "measure_to_effect is not defined, cannot shift fragility curve."
                )
            effect = self.measure_to_effect[cached_value]
            self.shift(effect=effect)

    def copy(self):
        """Maak een kopie van de fragility curve"""
        # Get all field values as dict and create new instance
        adapter = TypeAdapter(FragilityCurve)
        data = adapter.dump_python(self, exclude={"data_adapter"})
        new_curve = FragilityCurve(data_adapter=self.data_adapter, **data)
        return new_curve

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
            # TODO: raise warning if needed.

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

    def water_level_from_failure_probability(
        self, failure_probability_value: float
    ) -> float:
        """Zoek de hydraulische belasting behorende bij een faalkans

        Parameters
        ----------
        failure_probability_value : float
            Faalkans waarvoor de hydraulische belasting gezocht wordt

        Returns
        -------
        float
            Hydraulische belasting behorende bij de faalkans
        """
        failure_probability = np.array(
            sorted(list(self.failure_probability) + [failure_probability_value])
        )
        waterlevel = self._interpolate_water_for_failure_probability(
            failure_probability
        )

        index = np.argmin(np.abs(self.failure_probability - failure_probability_value))
        error = failure_probability_value - self.failure_probability[index]
        waterlevel = self.hydraulicload[index]
        return waterlevel, error

    def _interpolate_water_for_failure_probability(
        self, new_failure_probability: np.ndarray
    ) -> np.ndarray:
        """Interpoleer de hydraulische belasting behorende bij een faalkans"""
        wl = _interpolate_1d(
            new_failure_probability,
            self.failure_probability,
            self.hydraulicload,
        )
        return wl
