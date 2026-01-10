from typing import Optional

import pandas as pd
import numpy as np
from pydantic.dataclasses import dataclass
from toolbox_continu_inzicht import DataAdapter, FragilityCurve
from toolbox_continu_inzicht.utils.interpolate import (
    circular_interpolate_1d,
    interpolate_1d,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.calculate_fragility_curve_overtopping import (
    CustomModelUncertainty,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.overtopping_utils import (
    build_waterlevel_grid,
    build_pydra_profiles,
    compute_failure_probability,
    get_overtopping_options,
    parse_profile_dataframe,
    validate_slopes,
)


@dataclass(config={"arbitrary_types_allowed": True})
class FragilityCurveOvertoppingPreCalculated(FragilityCurve):
    """
    Maakt een enkele fragility curve voor golfoverslag.
    Attributes
    ----------
    data_adapter: DataAdapter
        DataAdapter object
    df_slopes: Optional[pd.DataFrame] | None
        DataFrame met helling data.
    df_profile: Optional[pd.DataFrame] | None
        DataFrame met profiel data.
    df_out: Optional[pd.DataFrame] | None
        DataFrame met de resultaten van de berekening.

    Notes
    -----
    Deze implementatie gebruikt pre-berekende golfcondities (Hs, Tm-1,0 en
    golf richting) via DataAdapters i.p.v. Bretschneider.

    """

    data_adapter: DataAdapter
    df_slopes: Optional[pd.DataFrame] | None = None
    df_profile: Optional[pd.DataFrame] | None = None
    df_out: Optional[pd.DataFrame] | None = None

    def run(self, input: list[str], output: str) -> None:
        """
        Runt de berekening van de fragility curve voor golfoverslag

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters:
            slopes, profile, waveval_unique_windspeed, waveval_unique_winddir,
            waveval_unique_waveval_id en pre_calculated_filter
        output: str
            Naam van de DataAdapter Fragility curve output

        Notes
        -----
        De inputvolgorde is vrij specifiek, vandaar de extra details.
        Als er geen type is opgegeven, wordt standaard het type float gebruikt.

        De eerste (slopes) DataAdapter met hellingsdata moet de volgende kolommen bevatten:

        1. x, x-coördinaat
        1. y, y-coördinaat
        1. r, roughness
        1. slopetypeid, id de helling type (int, 1: dike or 2: slope)

        De tweede (profile) DataAdapter met profieldata moet de volgende kolommen bevatten:

        1. windspeed, windsnelheid
        1. sectormin, de minimale sectorhoek.
        1. sectorsize, de grootte van de sectorhoek.
        1. orientation, oriëntatie van het profiel in graden
        1. crestlevel, kruinhoogte in meters
        1. dam, wel of geen dam (int, 0: geen dam or 1: dam)
        1. damheight, dam hoogte in meters
        1. qcr, mag een van 3 zijn: een waarde in m^3/s (float), open of niet (str: close | open) of de waarden van mu en sigma (tuple).

        De derde tot en met zesde DataAdapter leveren pre-berekende golfcondities
        met kolommen zoals waveval_type, windspeed, winddir, waterlevel en waveval.
        """
        self.calculate_fragility_curve(input, output)

    @staticmethod
    def bracketing_indices(xvec: np.ndarray, x: float, wrap: bool = False):
        n = xvec.size
        if n < 2:
            raise ValueError("x_vec must contain at least two values")

        # Normalize target only for circular semantics
        x = x % 360 if wrap else float(x)

        pos = np.searchsorted(xvec, x, side="left")

        if wrap:
            i0 = (pos - 1) % n
            i1 = pos % n

            a0 = xvec[i0]
            a1 = xvec[i1]

            # forward arc length from a0 to a1 (in [0, 360))
            span = (a1 - a0) % 360.0
            if span == 0.0:
                # degenerate: identical xvec (or full circle); choose f=0 deterministically
                return int(i0), int(i1), 0.0

            # forward distance from a0 to target
            dt = (x - a0) % 360.0
            f = dt / span

        else:
            if pos <= 0:
                i0, i1 = 0, 1
            elif pos >= n:
                i0, i1 = n - 2, n - 1
            else:
                i0, i1 = pos - 1, pos

            a0 = xvec[i0]
            a1 = xvec[i1]
            denom = a1 - a0
            if denom == 0.0:
                return int(i0), int(i1), 0.0

            f = (x - a0) / denom

        return int(i0), int(i1), float(f)

    def calculate_fragility_curve(self, input: list[str], output: str) -> None:
        """
        Bereken de fragility curve op basis van de opgegeven input en sla het resultaat op in het opgegeven outputbestand.

        Parameters
        ----------
        input: list[str]
            Lijst namen van de input DataAdapters:
            slopes, profile, waveval_unique_windspeed, waveval_unique_winddir,
            waveval_unique_waveval_id en pre_calculated_filter
        output: str
            Naam van de DataAdapter Fragility curve output

        """
        # @TODO replace with real location
        location = "034-02_0228_9_HD_km0998"

        # haal input op
        self.df_slopes = self.data_adapter.input(input[0])
        self.df_profile = self.data_adapter.input(input[1])

        profile_series = parse_profile_dataframe(self.df_profile)

        windspeed = profile_series["windspeed"]
        sectormin = profile_series["sectormin"]
        sectorsize = profile_series["sectorsize"]
        windrichtingen = (
            np.linspace(sectormin, sectormin + sectorsize, int(round(sectorsize))) % 360
        )
        qcr = profile_series["qcr"]
        closing_situation = profile_series["closing_situation"]

        global_variables = self.data_adapter.config.global_variables
        options = get_overtopping_options(
            global_variables, "FragilityCurveOvertoppingPreCalculated"
        )

        validate_slopes(self.df_slopes)

        basis_profiel, overtopping = build_pydra_profiles(
            self.df_slopes, profile_series
        )

        overtopping.closing_situation = closing_situation
        basis_profiel.closing_situation = closing_situation

        da = self.data_adapter

        # unique windspeeds
        with da.temporary_adapter_config(input[2], {"location": location}):
            uniq_windspeed = da.input(input[2])["windspeed"].to_numpy()

        # unique winddirections
        with da.temporary_adapter_config(input[3], {"location": location}):
            uniq_winddir = da.input(input[3])["winddir"].to_numpy()

        # Get brackets
        i1a, i1b, _ = self.bracketing_indices(uniq_windspeed, windspeed)
        windspeed_idx = uniq_windspeed[[i1a, i1b]]
        winddir_idx = []
        for winddir in windrichtingen:
            i2a, i2b, _ = self.bracketing_indices(uniq_winddir, winddir, wrap=True)
            winddir_idx += [i2a, i2b]
        winddir_idx = uniq_winddir[np.unique(winddir_idx)]

        # Unique waveval_id
        with da.temporary_adapter_config(
            input[4],
            {
                "location": location,
                "windspeed_bracket": windspeed_idx,
                "winddir_bracket": winddir_idx,
            },
        ):
            df_waveval_id = da.input(input[4])
        uniq_waveval_id = np.sort(df_waveval_id["waveval_id"].unique())

        # get waveval data, merge with waveval_id
        with da.temporary_adapter_config(
            input[5], {"waveval_bracket": uniq_waveval_id}
        ):
            df = da.input(input[5])

        # @TODO following code up until and including dom_vals = pd.DataFrame takes on average 0.003 seconds
        df_data = df_waveval_id.drop(columns="location").merge(df, on="waveval_id")
        df_data = df_data.drop(columns="waveval_id")

        def interpolate_waveval_for_direction(
            group: pd.DataFrame,
            windspeed_val: float,
            winddir_val: float,
            waterlevels: np.ndarray,
        ) -> np.ndarray:
            ws = group["windspeed"].to_numpy()
            wd = group["winddir"].to_numpy()
            wl = group["waterlevel"].to_numpy()
            wy = group["waveval"].to_numpy()

            wsv, ws_idx = np.unique(ws, return_inverse=True)
            wdv, wd_idx = np.unique(wd, return_inverse=True)
            wlv, wl_idx = np.unique(wl, return_inverse=True)

            grid_wswdwl = np.full((wsv.size, wdv.size, wlv.size), np.nan, dtype=float)
            grid_wswdwl[ws_idx, wd_idx, wl_idx] = wy

            i1, i2, fws = self.bracketing_indices(wsv, windspeed_val)
            grid_wdwl = (1 - fws) * grid_wswdwl[i1, :, :] + fws * grid_wswdwl[i2, :, :]

            wd_ext = np.concatenate([wdv - 360.0, wdv, wdv + 360.0])
            grid_wd_ext = np.concatenate([grid_wdwl, grid_wdwl, grid_wdwl], axis=0)
            grid_wl = interpolate_1d(
                np.array([winddir_val]), wd_ext, grid_wd_ext, ll=-np.inf
            )[0]

            return interpolate_1d(waterlevels, wlv, grid_wl, ll=-np.inf)

        dom_vals = {}
        dom_wl = overtopping.dike_crest_level - 0.5
        for waveval_type, group in df_data.groupby("waveval_type", group_keys=False):
            ws = group["windspeed"].to_numpy()
            wd = group["winddir"].to_numpy()
            wl = group["waterlevel"].to_numpy()
            wy = group["waveval"].to_numpy()

            wsv, ws_idx = np.unique(ws, return_inverse=True)
            wdv, wd_idx = np.unique(wd, return_inverse=True)
            wlv, wl_idx = np.unique(wl, return_inverse=True)

            grid_wswdwl = np.full((wsv.size, wdv.size, wlv.size), np.nan, dtype=float)
            grid_wswdwl[ws_idx, wd_idx, wl_idx] = wy

            i1, i2, fws = self.bracketing_indices(wsv, windspeed)
            grid_wdwl = (1 - fws) * grid_wswdwl[i1, :, :] + fws * grid_wswdwl[i2, :, :]
            i3, i4, fwl = self.bracketing_indices(wlv, dom_wl)
            grid_wd = (1 - fwl) * grid_wdwl[:, i3] + fwl * grid_wdwl[:, i4]

            wd_ext = np.concatenate([wdv - 360.0, wdv, wdv + 360.0])
            grid_wd_ext = np.concatenate([grid_wd, grid_wd, grid_wd])
            if waveval_type == 7:
                dom_vals[waveval_type] = circular_interpolate_1d(
                    windrichtingen, wd_ext, grid_wd_ext, ll=-np.inf
                )
            else:
                dom_vals[waveval_type] = interpolate_1d(
                    windrichtingen, wd_ext, grid_wd_ext, ll=-np.inf
                )

        dom_hs, dom_tspec, dom_wavedir = dom_vals[2], dom_vals[6], dom_vals[7]
        qov_dom = basis_profiel.calculate_overtopping(
            water_level=np.full_like(dom_hs, dom_wl, dtype=float),
            significant_wave_height=dom_hs,
            spectral_wave_period=dom_tspec,
            wave_direction=dom_wavedir,
        )
        ir = int(np.argmax(qov_dom))
        dominant_winddir = windrichtingen[ir]

        waterlevels = build_waterlevel_grid(overtopping.dike_crest_level, options)

        hs_group = df_data[df_data["waveval_type"] == 2]
        tspec_group = df_data[df_data["waveval_type"] == 6]
        dir_group = df_data[df_data["waveval_type"] == 7]

        hs = interpolate_waveval_for_direction(
            hs_group, windspeed, dominant_winddir, waterlevels
        )
        tspec = interpolate_waveval_for_direction(
            tspec_group, windspeed, dominant_winddir, waterlevels
        )
        wave_direction = interpolate_waveval_for_direction(
            dir_group, windspeed, dominant_winddir, waterlevels
        )

        standaard_model_onzekerheden = {
            "gh_onz_mu": 0.96,
            "gh_onz_sigma": 0.27,
            "gp_onz_mu_tp": 1.03,
            "gp_onz_sigma_tp": 0.13,
            "gp_onz_mu_tspec": 1.03,
            "gp_onz_sigma_tspec": 0.13,
            "gh_onz_aantal": 7,
            "gp_onz_aantal": 7,
            "closing_situation": closing_situation,
        }
        for onzekerheid in standaard_model_onzekerheden:
            if onzekerheid in options:
                standaard_model_onzekerheden[onzekerheid] = options[onzekerheid]

        modelonzekerheid = CustomModelUncertainty(standaard_model_onzekerheden)

        ovkansqcr = np.zeros(len(waterlevels))
        t_tspec = options.get("tp_tspec", 1.1)

        for (
            factor_hs,
            factor_tspec,
            onzkans,
        ) in modelonzekerheid.iterate_model_uncertainty_wave_conditions(
            closing_situation=closing_situation
        ):
            hs_adj = hs * factor_hs
            tspec_adj = tspec * factor_tspec
            qov = overtopping.calculate_overtopping(
                water_level=waterlevels,
                significant_wave_height=hs_adj,
                spectral_wave_period=tspec_adj,
                wave_direction=wave_direction,
                tp_tspec=t_tspec,
                dll_settings=None,
            )
            qov = np.array(qov)
            ovkansqcr += compute_failure_probability(qov, qcr, hs_adj, onzkans)

        self.hydraulicload = waterlevels
        self.failure_probability = ovkansqcr

        self.data_adapter.output(output=output, df=self.as_dataframe())
