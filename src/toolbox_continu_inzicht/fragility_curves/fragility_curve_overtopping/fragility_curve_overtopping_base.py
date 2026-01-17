from __future__ import annotations

from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter, FragilityCurve
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.overtopping_utils import (
    build_pydra_profiles,
    get_overtopping_options,
    parse_profile_dataframe,
    validate_slopes,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.wave_overtopping_calculation import (
    WaveOvertoppingCalculation,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.wave_provider import (
    WaveProvider,
)


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingBase(FragilityCurve):
    """
    Template base class voor overtopping fragility curves.
    Subclasses laden hun input en leveren een WaveProvider.
    """

    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_profile: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    options_key: ClassVar[str] = ""

    def _load_inputs(self, input: list[str]) -> None:
        raise NotImplementedError

    def _build_wave_provider(self, options: dict) -> WaveProvider:
        raise NotImplementedError

    def _get_options_key(self) -> str:
        if self.options_key:
            return self.options_key
        return type(self).__name__

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        da = self.data_adapter
        self._load_inputs(input)

        profile_series = parse_profile_dataframe(self.df_profile)
        validate_slopes(self.df_slopes)

        options = get_overtopping_options(
            da.config.global_variables, self._get_options_key()
        )

        basis_profiel, overtopping = build_pydra_profiles(
            self.df_slopes, profile_series
        )
        wave_provider = self._build_wave_provider(options)

        niveaus, ovkansqcr = WaveOvertoppingCalculation.calculate_overtopping_curve(
            profile_series["windspeed"],
            profile_series["sectormin"],
            profile_series["sectorsize"],
            overtopping,
            basis_profiel,
            qcr=profile_series["qcr"],
            closing_situation=profile_series["closing_situation"],
            options=options,
            wave_provider=wave_provider,
        )

        self.hydraulicload = niveaus
        self.failure_probability = ovkansqcr

        da.output(output=output, df=self.as_dataframe())
