from __future__ import annotations

from typing import ClassVar, Optional

import pandas as pd
from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht import DataAdapter, FragilityCurve
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.overtopping_utils import (
    build_pydra_profiles,
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
    default_options: ClassVar[dict] = {
        "tp_tspec": 1.1,
        "lower_limit_coarse": 4.0,
        "upper_limit_coarse": 2.0,
        "upper_limit_fine": 1.01,
        "hstap": 0.05,
        "gh_onz_aantal": 7,
        "gp_onz_aantal": 7,
    }

    def _load_inputs(self, input: list[str]) -> None:
        raise NotImplementedError

    def _build_wave_provider(self, options: dict) -> WaveProvider:
        raise NotImplementedError

    @classmethod
    def get_overtopping_options(
        cls, global_variables: dict, key: str, defaults: dict
    ) -> dict:
        options = defaults.copy()
        options.update(global_variables.get(key, {}))
        return options

    def _build_options(
        self,
        overrides: dict | None = None,
        context: dict | None = None,
    ) -> dict:
        options_key = self.options_key
        defaults = self.__class__.default_options.copy()
        options = self.get_overtopping_options(
            self.data_adapter.config.global_variables, options_key, defaults
        )
        if not overrides:
            WaveOvertoppingCalculation.validate_model_uncertainty_options(
                options, options_key
            )
            return options

        should_log = options_key in self.data_adapter.config.global_variables
        logger = self.data_adapter.logger
        closing_situation = None
        if context:
            closing_situation = context.get("closing_situation")

        for key, value in overrides.items():
            if should_log and logger is not None:
                logger.info(
                    "Overtopping model uncertainty override: closing_situation=%s, "
                    "key=%s, global=%s, override=%s",
                    closing_situation,
                    key,
                    options.get(key),
                    value,
                )
            options[key] = value
        WaveOvertoppingCalculation.validate_model_uncertainty_options(
            options, options_key
        )
        return options

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        da = self.data_adapter
        self._load_inputs(input)

        profile_series = parse_profile_dataframe(self.df_profile)
        validate_slopes(self.df_slopes)

        options = self._build_options()
        closing_situation = options.get("closing_situation")
        if closing_situation is None:
            raise KeyError(
                f"Missing overtopping config option 'closing_situation' for '{self.options_key}'."
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
            closing_situation=closing_situation,
            options=options,
            wave_provider=wave_provider,
        )

        self.hydraulicload = niveaus
        self.failure_probability = ovkansqcr

        da.output(output=output, df=self.as_dataframe())
