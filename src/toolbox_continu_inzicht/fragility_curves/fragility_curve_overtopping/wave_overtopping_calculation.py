from typing import Any, Dict, List, Tuple
import numpy as np
import pandas as pd

import pydra_core
import pydra_core.location
from pydra_core.location.model.statistics.stochastics.model_uncertainty import (
    ModelUncertainty,
    DistributionUncertainty,
)

from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.overtopping_utils import (
    build_waterlevel_grid,
    compute_failure_probability,
)
from toolbox_continu_inzicht.fragility_curves.fragility_curve_overtopping.wave_provider import (
    WaveProvider,
)


class WaveOvertoppingCalculation:
    """
    Variant op de Pydra-berekening die gebruikt wordt om on-the-fly
    fragility curves te berekenen voor Continu Inzicht Rivierenland.
    De golfcondities worden geleverd via een WaveProvider.
    """

    def __init__(self, profile, options, wave_provider: WaveProvider):
        self.profile: pydra_core.Profile = profile
        self.wave_provider = wave_provider
        # Stel standaardonzekerheden voor Bretschneider in
        standaard_model_onzekerheden = {}
        # gh =  golfhoogte onzekerheid mu/sigma/aantal;

        standaard_model_onzekerheden["gh_onz_mu"] = 0.96
        standaard_model_onzekerheden["gh_onz_sigma"] = 0.27

        standaard_model_onzekerheden["gp_onz_mu_tp"] = 1.03
        standaard_model_onzekerheden["gp_onz_sigma_tp"] = 0.13

        standaard_model_onzekerheden["gp_onz_mu_tspec"] = 1.03
        standaard_model_onzekerheden["gp_onz_sigma_tspec"] = 0.13

        standaard_model_onzekerheden["gh_onz_aantal"] = 7
        standaard_model_onzekerheden["gp_onz_aantal"] = 7

        standaard_model_onzekerheden["closing_situation"] = profile.closing_situation

        # Overschrijf standaardwaarden met gebruikersinput
        for onzekerheid in standaard_model_onzekerheden:
            if onzekerheid in options:
                standaard_model_onzekerheden[onzekerheid] = options[onzekerheid]

        self.modelonzekerheid: CustomModelUncertainty = CustomModelUncertainty(
            standaard_model_onzekerheden
        )
        self.qov = []
        self.kansen = []

    @classmethod
    def calculate_overtopping_curve(
        cls,
        windspeed: float,
        sectormin: float,
        sectorsize: float,
        overtopping: object,
        basis_profiel: object,
        qcr: float,
        closing_situation: object,
        options: Dict[str, Any],
        wave_provider: WaveProvider,
    ) -> Tuple[List[float], List[float]]:
        """
        Berekent de overloopcurve voor overtopping.
        Parameters:
        -----------
        cls : class
            De klasse van het berekeningsobject.
        windspeed : float
            De windsnelheid.
        sectormin : float
            De minimale sectorhoek.
        sectorsize : float
            De grootte van de sectorhoek.
        overtopping : object
            Het overtopping object.
        basis_profiel : object
            Het basisprofiel object.
        qcr : float
            De kritieke afvoer.
        closing_situation : object
            De sluitsituatie.
        options : Dict[str, Any]
            Optionele parameters uit de config.
        wave_provider : WaveProvider
            Leverancier van golfcondities.

        Returns:
        --------
        niveaus : List[float]
            De niveaus.
        ovkansqcr : List[float]
            De overloopkansen.
        """
        # Pas profiel aan voor maatregel (via kruinhoogte)
        overtopping.closing_situation = (
            closing_situation  # niet zo netjes maar het werkt
        )
        # Berekening
        berekening = cls(overtopping, options, wave_provider)

        # Bereken fragility curve
        windrichtingen = (
            np.linspace(sectormin, sectormin + sectorsize, int(round(sectorsize))) % 360
        )
        # Voor het bepalen van de dominante windrichting wordt het profiel zonder voorland gebruikt
        basis_profiel.closing_situation = (
            closing_situation  # niet zo netjes maar het werkt
        )
        # Maak een berekeningobject met het basisprofiel aan (WaveOvertoppingCalculation)
        berekening_basis = cls(basis_profiel, options, wave_provider)
        t_tspec = 1.1
        if "tp_tspec" in options:
            t_tspec = options["tp_tspec"]

        # Bepaal dominante windrichting
        ir = berekening_basis.bepaal_dominante_richting(
            overtopping.dike_crest_level - 0.5,
            windspeed,
            windrichtingen,
            t_tspec,
        )
        # Bereken fragility curve
        niveaus, ovkansqcr = berekening.bereken_fc_cond(
            richting=windrichtingen[ir],
            windsnelheid=windspeed,
            qcr=qcr,
            crestlevel=overtopping.dike_crest_level,
            closing_situation=closing_situation,
            t_tspec=t_tspec,
            options=options,
        )

        return niveaus, ovkansqcr

    def bepaal_dominante_richting(
        self,
        level: float,
        windspeed: float,
        richtingen: List[float],
        t_tspec: float,
    ) -> int:
        """
        Parameters
        ----------
        level : float
            Het waterpeil.
        windspeed : float
            De windsnelheid.
        richtingen : List[float]
            De array met richtingen.
        t_tspec : float
            De spectrale golfperiode.
        Returns
        -------
        int
            De index van de dominante richting.
        """
        hss, tspec, wave_direction = (
            self.wave_provider.get_wave_conditions_for_directions(
                windspeed=windspeed,
                windrichtingen=np.asarray(richtingen, dtype=float),
                waterlevel=level,
            )
        )
        # Bereken overslagdebieten
        qov = []
        for r, hs, tp in zip(wave_direction, hss, tspec):
            qov.append(
                self.profile.calculate_overtopping(
                    water_level=level,
                    significant_wave_height=hs,
                    spectral_wave_period=tp,
                    wave_direction=r,
                )
            )

        return np.argmax(qov)

    def bereken_fc_cond(
        self,
        richting: float,
        windsnelheid: float,
        qcr: float,
        t_tspec: float,
        crestlevel: float,
        closing_situation: object,
        options: Dict[str, Any],
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Berekent de overloopcurve voor overtopping.

        Parameters:
        -----------
        richting : float
            De windrichting.
        windsnelheid : float
            De windsnelheid.
        qcr : float
            De kritieke afvoer.
        t_tspec : float
            De spectrale golfperiode.
        crestlevel : float
            De kruinhoogte.
        closing_situation : object
            De sluitsituatie.
        options : Dict[str, Any]
            Optionele parameters uit de config.

        Returns:
        --------
        Tuple[np.ndarray, np.ndarray]
            Een tuple met de niveaus en overloopkansen.
        """
        # Stel standaardwaarden in
        waterlevels = build_waterlevel_grid(crestlevel, options)[:, None]

        hs_dw, tspec_dw, richting = self.wave_provider.get_wave_conditions_for_levels(
            windspeed=windsnelheid,
            direction=richting,
            waterlevels=waterlevels,
        )

        # Alloceer lege array om kansen aan toe te kennen
        ovkansqcr = np.zeros(len(waterlevels))

        # Voor elke combinatie van modelonzekerheid
        for (
            factor_hs,
            factor_tspec,
            onzkans,
        ) in self.modelonzekerheid.iterate_model_uncertainty_wave_conditions(
            closing_situation=closing_situation
        ):
            self.kansen.append((factor_hs, factor_tspec, onzkans))
            hs, tspec = (
                hs_dw * factor_hs,
                tspec_dw * factor_tspec,
            )
            qov = self.profile.calculate_overtopping(
                water_level=waterlevels.flatten(),
                significant_wave_height=hs.flatten(),
                spectral_wave_period=tspec.flatten(),
                wave_direction=richting.flatten(),
                tp_tspec=t_tspec,
                dll_settings=None,
            )
            qov = np.array(qov)
            self.qov.append(qov * 1000)
            ovkansqcr += compute_failure_probability(qov, qcr, hs, onzkans)
        return waterlevels.squeeze(), ovkansqcr


class CustomModelUncertainty(ModelUncertainty):
    """
    Aangepaste versie van de oorspronkelijke ModelUncertainty-klasse.
    De oorspronkelijke klasse maakt veel gebruik van database-interacties, die we hier vermijden. Gebruikt de dictionary 'model_uncertainties' als invoer.
    Er wordt aangenomen dat de correlaties nul zijn.

    Attributen
    ----------
    model_uncertainties : dict
        Een dictionary met modelonzekerheden.
    """

    model_uncertainties = {}
    correlations = {}

    def __init__(self, standaard_model_onzekerheden):
        self.step_size = {
            "hs": standaard_model_onzekerheden["gh_onz_aantal"],
            "tspec": standaard_model_onzekerheden["gp_onz_aantal"],
        }

        mu = pd.DataFrame(columns=["k", "rvid", "mean", "stdev"])
        mu.loc[0, "k"] = standaard_model_onzekerheden["closing_situation"]
        mu.loc[0, "rvid"] = "hs"
        mu.loc[0, "mean"] = standaard_model_onzekerheden["gh_onz_mu"]
        mu.loc[0, "stdev"] = standaard_model_onzekerheden["gh_onz_sigma"]

        mu.loc[1, "k"] = standaard_model_onzekerheden["closing_situation"]
        mu.loc[1, "rvid"] = "tp"
        mu.loc[1, "mean"] = standaard_model_onzekerheden["gp_onz_mu_tp"]
        mu.loc[1, "stdev"] = standaard_model_onzekerheden["gp_onz_sigma_tp"]

        mu.loc[2, "k"] = standaard_model_onzekerheden["closing_situation"]
        mu.loc[2, "rvid"] = "tspec"
        mu.loc[2, "mean"] = standaard_model_onzekerheden["gp_onz_mu_tspec"]
        mu.loc[2, "stdev"] = standaard_model_onzekerheden["gp_onz_sigma_tspec"]

        # Zorg ervoor dat de index voor alle punten hetzelfde is, zodat deze overeenkomt met de oorspronkelijke implementatie
        mu.index = [0, 0, 0]
        mu.index.name = "HRDLocationId"

        for comb, uncertainty in mu.groupby(["k", "rvid"]):
            self.model_uncertainties[comb] = DistributionUncertainty(
                uncertainty.to_numpy()[0]
            )
