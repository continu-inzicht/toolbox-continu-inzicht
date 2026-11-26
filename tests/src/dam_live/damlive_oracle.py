"""Reference implementation (test oracle) of the original DamLive table building.

A slim, standalone port of the original JSON-folder parsers and merges of
`CombineDamLiveResults`, reading the JSON straight from a `.stix` zip. Tests
compare the production tables against these, so the expected output is produced
on the spot instead of stored.

It deliberately reproduces the *original* behaviour, including its known quirks:

- `merged_soil` joins soil layers on `layer_id` only, ignoring the stage's own
  soil-layer collection; stages are repeated once per calculation.
- `damlive_soil_color` puts the hex colour in `soil_name` and `stb_name`.
- `damlive_soil` is returned without the `datetime` column (the original used the
  wall-clock time of the run).

These quirks only show with several stages or calculations, or in the colour
table; tests apply intended corrections explicitly on top of the oracle.
"""

import json
import zipfile
from pathlib import Path

import pandas as pd


def _read_folder(zf: zipfile.ZipFile, folder: str) -> list[dict]:
    names = sorted(
        n for n in zf.namelist() if n.startswith(f"{folder}/") and n.endswith(".json")
    )
    return [json.loads(zf.read(n)) for n in names]


def _stages(scenarios: list[dict]) -> pd.DataFrame:
    rows = []
    for data in scenarios:
        calculations = data.get("Calculations") or [None]
        for stage in data.get("Stages", []):
            for calc in calculations:
                rows.append(
                    {
                        "stage_id": stage.get("Id"),
                        "stage_label": stage.get("Label"),
                        "scenario_id": data.get("Id"),
                        "scenario_label": data.get("Label"),
                        "geometry_id": stage.get("GeometryId"),
                        "soillayers_id": stage.get("SoilLayersId"),
                        "waternet_id": stage.get("WaternetId"),
                        "calculationsettings_id": (
                            calc.get("CalculationSettingsId") if calc else None
                        ),
                    }
                )
    return pd.DataFrame(rows)


def _geometries(geometries: list[dict]) -> pd.DataFrame:
    rows = [
        {
            "geometry_id": data.get("Id"),
            "layer_id": layer.get("Id"),
            "layer_label": layer.get("Label"),
            "points": layer.get("Points"),
        }
        for data in geometries
        for layer in data.get("Layers", [])
    ]
    return pd.DataFrame(rows)


def _soillayers(soillayers: list[dict]) -> pd.DataFrame:
    rows = [
        {
            "soillayers_id": data.get("Id"),
            "layer_id": layer.get("LayerId"),
            "soil_id": layer.get("SoilId"),
        }
        for data in soillayers
        for layer in data.get("SoilLayers", [])
    ]
    return pd.DataFrame(rows)


def _soils(soils: dict) -> pd.DataFrame:
    rows = [
        {"soil_id": s.get("Id"), "name": s.get("Name"), "code": s.get("Code")}
        for s in soils.get("Soils", [])
    ]
    return pd.DataFrame(rows)


def _waternets(waternets: list[dict]) -> pd.DataFrame:
    rows = []
    for data in waternets:
        lines = [("Head", line) for line in data.get("HeadLines", [])] + [
            ("Reference", line) for line in data.get("ReferenceLines", [])
        ]
        for line_type, line in lines:
            for p in line.get("Points", []):
                rows.append(
                    {
                        "waternet_id": data.get("Id"),
                        "line_type": line_type,
                        "line_id": line.get("Id"),
                        "line_label": line.get("Label"),
                        "x": p.get("X"),
                        "z": p.get("Z"),
                        "top_headline_id": line.get("TopHeadLineId"),
                        "bottom_headline_id": line.get("BottomHeadLineId"),
                        "content_version": data.get("ContentVersion"),
                    }
                )
    return pd.DataFrame(rows)


def _circles(settings: list[dict]) -> pd.DataFrame:
    rows = []
    for data in settings:
        analysis_type = data.get("AnalysisType")
        method = data.get(analysis_type)
        if not isinstance(method, dict):
            continue
        if analysis_type == "Bishop":
            circle = method.get("Circle")
            circles = (
                [(circle.get("Center", {}), circle.get("Radius"))]
                if isinstance(circle, dict)
                else []
            )
        elif analysis_type == "UpliftVan":
            plane = method.get("SlipPlane", {})
            circles = [
                (plane.get(f"{n}CircleCenter", {}), plane.get(f"{n}CircleRadius"))
                for n in ("First", "Second")
                if plane.get(f"{n}CircleCenter", {})
            ]
        else:
            continue
        for center, radius in circles:
            rows.append(
                {
                    "calculationsettings_id": data.get("Id"),
                    "analysis_type": analysis_type,
                    "calculation_type": data.get("CalculationType"),
                    "model_factor_mean": data.get("ModelFactorMean"),
                    "model_factor_std": data.get("ModelFactorStandardDeviation"),
                    "circle_center_x": center.get("X"),
                    "circle_center_z": center.get("Z"),
                    "circle_radius": radius,
                }
            )
    return pd.DataFrame(rows)


def expected_tables(stix: Path) -> dict[str, pd.DataFrame]:
    """Tables the original `CombineDamLiveResults` produces for `stix`.

    Keys: merged_soil, merged_waternet, merged_calculations, damlive_soil.
    """
    with zipfile.ZipFile(stix) as zf:
        stages = _stages(_read_folder(zf, "scenarios"))
        geometries = _geometries(_read_folder(zf, "geometries"))
        soillayers = _soillayers(_read_folder(zf, "soillayers"))
        soils = _soils(json.loads(zf.read("soils.json")))
        waternets = _waternets(_read_folder(zf, "waternets"))
        circles = _circles(_read_folder(zf, "calculationsettings"))

    merged_soil = (
        stages.merge(geometries, how="left", on="geometry_id")
        .merge(soillayers, how="left", on="layer_id", suffixes=("", "_soillayers"))
        .merge(soils, how="left", on="soil_id")
    )[
        [
            "stage_id",
            "stage_label",
            "scenario_id",
            "scenario_label",
            "geometry_id",
            "layer_id",
            "layer_label",
            "points",
            "soillayers_id",
            "soil_id",
            "name",
            "code",
        ]
    ]

    stage_cols = ["stage_id", "stage_label", "scenario_id", "scenario_label"]
    merged_waternet = (
        stages[stage_cols + ["waternet_id"]]
        .drop_duplicates()
        .merge(waternets, how="left", on="waternet_id")
        .sort_values(by=["stage_id", "line_type", "line_id", "x"])
        .reset_index(drop=True)
    )

    merged_calculations = stages.merge(
        circles, how="left", on="calculationsettings_id"
    )[
        stage_cols
        + [
            "calculationsettings_id",
            "analysis_type",
            "calculation_type",
            "model_factor_mean",
            "model_factor_std",
            "circle_center_x",
            "circle_center_z",
            "circle_radius",
        ]
    ]

    return {
        "merged_soil": merged_soil,
        "merged_waternet": merged_waternet,
        "merged_calculations": merged_calculations,
        "damlive_soil": _damlive_soil(stages, geometries, soillayers, soils),
    }


def _damlive_soil(stages, geometries, soillayers, soils) -> pd.DataFrame:
    rows = []
    for _, stage in stages.iterrows():
        for _, layer in geometries[
            geometries["geometry_id"] == stage["geometry_id"]
        ].iterrows():
            match = soillayers[
                (soillayers["soillayers_id"] == stage["soillayers_id"])
                & (soillayers["layer_id"] == layer["layer_id"])
            ]
            if match.empty:
                continue
            soil = soils[soils["soil_id"] == match.iloc[0]["soil_id"]]
            layer_name = (
                soil.iloc[0]["name"] if not soil.empty else layer["layer_label"]
            )
            for index, point in enumerate(layer["points"]):
                rows.append(
                    {
                        "measuringstationid": 1,
                        "parameterid": 200,
                        "itemid": int(layer["layer_id"]),
                        "layername": layer_name,
                        "soils.color": None,
                        "index": index,
                        "x": float(point.get("X", point.get("x", 0.0))),
                        "y": 0.0,
                        "z": float(point.get("Z", point.get("z", 0.0))),
                    }
                )
    return pd.DataFrame(rows).astype(
        {
            "measuringstationid": "int64",
            "parameterid": "int64",
            "itemid": "int64",
            "index": "int64",
            "x": "float64",
            "y": "float64",
            "z": "float64",
        }
    )


def expected_soil_color(colors: pd.DataFrame) -> pd.DataFrame:
    """Soil-colour table the original `CombineDamLiveResults` produces from the colours CSV."""
    rows = []
    for _, row in colors[colors["type"] == "soil"].iterrows():
        hex_code = str(row["color"]).strip()
        digits = hex_code.lstrip("#")
        r, g, b = (
            (int(digits[0:2], 16), int(digits[2:4], 16), int(digits[4:6], 16))
            if len(digits) == 6
            else (128, 128, 128)
        )
        rows.append(
            {
                "soil_name": row["color"],
                "r": r,
                "g": g,
                "b": b,
                "color": hex_code if hex_code.startswith("#") else f"#{hex_code}",
                "stb_name": row["color"],
            }
        )
    return pd.DataFrame(rows).astype({"r": "int64", "g": "int64", "b": "int64"})
