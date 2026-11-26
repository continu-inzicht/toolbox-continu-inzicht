"""DataAdapter input type `damlive_stix`: read-only access to D-Stability `.stix` files,
including DamLive's reduced result files.

A `.stix` is a zip of JSON files. Each JSON is validated into GEOLib's typed classes.
GEOLib's `DStabilityModel` is not used: it rejects the reduced `.stix` files DamLive
writes (missing files such as `soilcorrelations.json`, `null` sub-objects in
`soils.json` and `null` stage references such as `DecorationsId`).
"""

import json
import re
import warnings
import zipfile
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from geolib.models.dstability.internal import (
    CalculationSettings,
    Geometry,
    PersistableCalculation,
    PersistableSoil,
    Scenario,
    SoilCollection,
    SoilLayerCollection,
    Stage,
    Waternet,
)
from pydantic import BaseModel, ValidationError

# DamLive names its results
# Dik(<dike>)_Loc(<location>)_Stp(<step>)_Mdl(<model>)_<yyyy-mm-ddTHH_MM_SS>_Pro(<profile>)_result.stix
_DAMLIVE_NAME = re.compile(
    r"""
    _Loc\((?P<location>.+?)\)                              # _Loc(<location>)
    _Stp\((?P<step>\d+)\)                                  # _Stp(<step>)
    _Mdl\((?P<model>[^)]+)\)                               # _Mdl(<model>)
    _(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2})    # _<yyyy-mm-ddTHH_MM_SS>
    _Pro\(                                                 # _Pro(<profile>)...
    """,
    re.VERBOSE,
)


class StixFile(BaseModel):
    """Read-only, typed view of one `.stix` file.

    Holds the parts of the model the toolbox uses, as GEOLib objects. It can't write,
    calculate or read calculation results; DamLive's result `.stix` files hold no
    results anyway (DamLive's numbers are in its FEWS output time series).

    For files named by DamLive the name is parsed into `location`, `step`,
    `calculation_model` (Bishop or UpliftVan) and `timestamp`; for other files these
    are None. `timestamp` is naive: the local time of the DamLive run as written in the
    file name, which is the same time zone as DamLive's output time series.
    """

    path: Path
    scenarios: list[Scenario]
    geometries: list[Geometry]
    soillayers: list[SoilLayerCollection]
    soils: SoilCollection
    waternets: list[Waternet]
    calculationsettings: list[CalculationSettings]

    location: str | None = None
    step: int | None = None
    calculation_model: str | None = None
    timestamp: datetime | None = None

    @classmethod
    def from_path(cls, path: Path | str) -> "StixFile":
        """Read a `.stix` file, and its DamLive metadata from the file name if present.

        Raises
        ------
        FileNotFoundError
            If the file does not exist.
        ValueError
            If the file is not a valid `.stix`; the message names the file.
        """
        path = Path(path)
        if not path.is_file():
            raise FileNotFoundError(f"Stix-bestand niet gevonden: {path}")
        try:
            with zipfile.ZipFile(path) as zf:
                content = {
                    "scenarios": cls._read_folder(zf, "scenarios", Scenario),
                    "geometries": cls._read_folder(zf, "geometries", Geometry),
                    "soillayers": cls._read_folder(
                        zf, "soillayers", SoilLayerCollection
                    ),
                    # Only soils need `drop_nulls`: DAM Engine writes `null` there
                    # for sub-objects GEOLib requires.
                    "soils": cls._read_json(
                        zf, "soils.json", SoilCollection, drop_nulls=True
                    ),
                    "waternets": cls._read_folder(zf, "waternets", Waternet),
                    "calculationsettings": cls._read_folder(
                        zf, "calculationsettings", CalculationSettings
                    ),
                }
        except (zipfile.BadZipFile, KeyError, ValidationError, ValueError) as e:
            raise ValueError(f"Kan stix-bestand niet lezen: {path}\n{e}") from e

        # GEOLib accepts `null` here, but D-Stability always writes both
        for settings in content["calculationsettings"]:
            if settings.AnalysisType is None or settings.CalculationType is None:
                raise ValueError(
                    f"Kan stix-bestand niet lezen: {path}\n"
                    f"Rekeninstellingen {settings.Id} zonder AnalysisType of CalculationType"
                )

        name = _DAMLIVE_NAME.search(path.name)
        if name:
            content.update(
                {
                    "location": name["location"],
                    "step": int(name["step"]),
                    "calculation_model": name["model"],
                    "timestamp": datetime.strptime(
                        name["timestamp"], "%Y-%m-%dT%H_%M_%S"
                    ),
                }
            )
        return cls(path=path, **content)

    @staticmethod
    def _read_folder(zf: zipfile.ZipFile, folder: str, structure) -> list:
        """Read every JSON file in a folder of the zip into `structure`, sorted by name."""
        items = []
        for name in sorted(zf.namelist()):
            if name.startswith(f"{folder}/") and name.endswith(".json"):
                items.append(StixFile._read_json(zf, name, structure))
        return items

    @staticmethod
    def _read_json(zf: zipfile.ZipFile, name: str, structure, drop_nulls=False):
        """Read one JSON file of the zip and validate it into `structure` (a GEOLib class).

        With `drop_nulls`, `null` values are removed before validation, so GEOLib's
        defaults apply instead (see `_drop_nulls`).
        """
        data = json.loads(zf.read(name))
        if drop_nulls:
            data = StixFile._drop_nulls(data)
        return structure.model_validate(data)

    @staticmethod
    def _drop_nulls(value):
        """Remove all `null` (None) values from parsed JSON, recursively."""
        if isinstance(value, dict):
            return {
                k: StixFile._drop_nulls(v) for k, v in value.items() if v is not None
            }
        if isinstance(value, list):
            return [StixFile._drop_nulls(v) for v in value]
        return value

    def stages(self) -> Iterator[tuple[Scenario, Stage]]:
        """All stages, with the scenario they belong to."""
        for scenario in self.scenarios:
            for stage in scenario.Stages:
                yield scenario, stage

    def geometry_of(self, stage: Stage) -> Geometry:
        """The geometry the stage refers to."""
        return self._by_id(self.geometries, stage.GeometryId, "geometry")

    def soillayers_of(self, stage: Stage) -> SoilLayerCollection:
        """The stage's own soil-layer collection."""
        return self._by_id(self.soillayers, stage.SoilLayersId, "soil-layer collection")

    def soil_of_layer(self, stage: Stage, layer_id: str) -> PersistableSoil | None:
        """The soil of a layer, looked up in the stage's own soil-layer collection."""
        # find the soil id linked to this layer in the stage's soil-layer collection
        soil_id = None
        for soil_layer in self.soillayers_of(stage).SoilLayers:
            if soil_layer.LayerId == layer_id:
                soil_id = soil_layer.SoilId
                break
        if soil_id is None:
            return None

        # find the soil with that id among the model's soils
        for soil in self.soils.Soils:
            if soil.Id == soil_id:
                return soil
        return None

    def waternet_of(self, stage: Stage) -> Waternet:
        """The waternet the stage refers to."""
        return self._by_id(self.waternets, stage.WaternetId, "waternet")

    def settings_of(self, calculation: PersistableCalculation) -> CalculationSettings:
        """The calculation settings a calculation refers to."""
        return self._by_id(
            self.calculationsettings,
            calculation.CalculationSettingsId,
            "calculation settings",
        )

    def _by_id(self, items: list, item_id: str | None, kind: str):
        """The item with `Id == item_id`; a KeyError naming the file if there is none."""
        for item in items:
            if item.Id == item_id:
                return item
        raise KeyError(f"Geen {kind} met id {item_id!r} in {self.path}")


class StixCollection:
    """The `.stix` files of one DamLive run, or a single `.stix`.

    Maps file name (without extension) to `StixFile`; `len()` is the number of files.
    Use `select()` to pick one file by DamLive location and time.
    """

    def __init__(self, files: dict[str, StixFile]):
        self.files = files

    def __len__(self) -> int:
        return len(self.files)

    def __iter__(self) -> Iterator[str]:
        return iter(self.files)

    def __getitem__(self, name: str) -> StixFile:
        return self.files[name]

    def select(
        self,
        location: str,
        timestamp: datetime | str,
        calculation_model: str | None = None,
    ) -> StixFile:
        """The single file for a DamLive location and time.

        Parameters
        ----------
        location : str
            DamLive location, e.g. `PU0013_87074-1_DWP001`.
        timestamp : datetime | str
            Time of the DamLive run (naive, see `StixFile`), or an ISO string such as
            `"2024-11-06T07:00"`.
        calculation_model : str, optional
            Bishop or UpliftVan; only needed when a location and time has both.

        Raises
        ------
        KeyError
            If no file, or more than one file, matches.
        """
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        matches = [
            f
            for f in self.files.values()
            if f.location == location
            and f.timestamp == timestamp
            and (calculation_model is None or f.calculation_model == calculation_model)
        ]
        wanted = f"location={location!r}, timestamp={timestamp}, calculation_model={calculation_model!r}"
        if not matches:
            raise KeyError(f"Geen stix-bestand gevonden voor {wanted}")
        if len(matches) > 1:
            names = ", ".join(f.path.name for f in matches)
            raise KeyError(f"Meerdere stix-bestanden voor {wanted}: {names}")
        return matches[0]


def input_damlive_stix(input_config: dict) -> StixCollection:
    """Laad een `.stix` bestand, of alle DamLive resultaten in een map.

    Parameters
    ----------
    input_config : dict
        DataAdapter-configuratie met `abs_path` naar een `.stix` bestand of naar een
        map. In een map worden alle `*_result.stix` bestanden gezocht, ook in
        submappen, zodat de `<project>.Calc` map van DamLive direct werkt.

    Returns
    -------
    StixCollection
        Per bestandsnaam (zonder extensie) een `StixFile`.

    Raises
    ------
    UserWarning
        Als het pad niet bestaat, de map geen `*_result.stix` bestanden bevat, twee
        bestanden dezelfde naam hebben of een bestand geen geldig `.stix` bestand is.

    Warns
    -----
    UserWarning
        Als een bestand in een map de DamLive naamgeving niet volgt: locatie en tijd
        zijn dan onbekend.
    """
    path = Path(input_config["abs_path"])
    if path.is_file():
        paths = [path]
    elif path.is_dir():
        paths = sorted(path.rglob("*_result.stix"))
        if not paths:
            raise UserWarning(f"Geen `*_result.stix` bestanden gevonden in map: {path}")
    else:
        raise UserWarning(f"Stix-bestand of -map bestaat niet: {path}")

    files = {}
    for stix in paths:
        if stix.stem in files:
            raise UserWarning(f"Dubbele stix-bestandsnaam: {stix.stem}")
        try:
            files[stix.stem] = StixFile.from_path(stix)
        except (FileNotFoundError, ValueError) as e:
            raise UserWarning(str(e)) from e
        # in a folder of DamLive results every name should give location and time
        if path.is_dir() and files[stix.stem].location is None:
            msg = f"Naam volgt de DamLive naamgeving niet, locatie en tijd onbekend: {stix}"
            warnings.warn(msg, UserWarning)
    return StixCollection(files)
