"""`CombineDamLiveResults` on DamLive result `.stix` files, checked against the test oracle."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from damlive_oracle import expected_soil_color, expected_tables
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.dam_live.combine_results import CombineDamLiveResults

CALC = "WV2030_Purmer.Calc_tests_sets"
OUTPUTS = [
    "merge_soil",
    "merge_waternet",
    "merge_calculations",
    "damlive_soil",
    "damlive_soil_color",
]
# (location, timestamp) of the DamLive result files in CALC
RESULTS = [
    ("PU0013_87074-1_DWP001", "2024-11-06T07:00:00"),
    ("PU0013_87074-1_DWP001", "2024-11-06T08:00:00"),
    ("PU0021_87074-1_DWP002", "2024-11-06T07:00:00"),
    ("PU0021_87074-1_DWP002", "2024-11-06T08:00:00"),
]


def setup_data_adapter(rootdir: Path) -> DataAdapter:
    data_sets_path = Path(__file__).parent / "data_sets" / "run_damlive"
    config = Config(config_path=data_sets_path / "test_combine_damlive.yaml")
    config.lees_config()
    config.global_variables["rootdir"] = rootdir
    return DataAdapter(config)


def combine(data_adapter: DataAdapter, stix: Path | str) -> CombineDamLiveResults:
    """Run `CombineDamLiveResults` with the `.stix` input pointed at `stix`."""
    combine_results = CombineDamLiveResults(data_adapter=data_adapter)
    with data_adapter.temporary_adapter_config("damlive_results", {"path": str(stix)}):
        combine_results.run(input=["damlive_results", "colors"], output=OUTPUTS)
    return combine_results


def select(data_adapter: DataAdapter, location: str, timestamp: str) -> None:
    data_adapter.set_global_variable(
        "CombineDamLiveResults", {"location": location, "timestamp": timestamp}
    )


def normalised(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    """Stable sort on `by` and the same null (NaN) everywhere, for comparing tables."""
    df = df.sort_values(by=by, kind="stable").reset_index(drop=True)
    return df.where(df.notna(), np.nan)


@pytest.mark.parametrize(("location", "timestamp"), RESULTS)
def test_tables_match_original_implementation(run_damlive_dir, location, timestamp):
    data_adapter = setup_data_adapter(run_damlive_dir)
    select(data_adapter, location, timestamp)
    combine_results = combine(data_adapter, CALC)
    expected = expected_tables(combine_results.stix.path)

    pd.testing.assert_frame_equal(
        normalised(combine_results.df_merged_soils, ["stage_id", "layer_id"]),
        normalised(expected["merged_soil"], ["stage_id", "layer_id"]),
    )
    waternet_order = ["stage_id", "line_type", "line_id", "x"]
    pd.testing.assert_frame_equal(
        normalised(combine_results.df_merged_waternet, waternet_order),
        normalised(expected["merged_waternet"], waternet_order),
    )
    calculations_order = ["stage_id", "calculationsettings_id", "circle_center_x"]
    pd.testing.assert_frame_equal(
        normalised(combine_results.df_merged_calculations, calculations_order),
        normalised(expected["merged_calculations"], calculations_order),
    )
    # UpliftVan: one row per circle
    n_circles = 2 if combine_results.stix.calculation_model == "UpliftVan" else 1
    assert len(combine_results.df_merged_calculations) == n_circles


@pytest.mark.parametrize(("location", "timestamp"), RESULTS)
def test_database_tables_match_original_with_corrections(
    run_damlive_dir, location, timestamp
):
    data_adapter = setup_data_adapter(run_damlive_dir)
    select(data_adapter, location, timestamp)
    combine_results = combine(data_adapter, CALC)
    expected = expected_tables(combine_results.stix.path)

    # datetime is DamLive's calculation moment (UTC+1) in epoch milliseconds
    soil = combine_results.create_df_damlive_soil()
    moment = datetime.fromisoformat(timestamp).replace(
        tzinfo=timezone(timedelta(hours=1))
    )
    assert (soil["datetime"] == int(moment.timestamp() * 1000)).all()
    assert soil["datetime"].dtype == "int64"
    pd.testing.assert_frame_equal(
        soil.drop(columns=["datetime"]), expected["damlive_soil"]
    )

    # the original put the hex colour in soil_name and stb_name instead of the name
    colors = pd.read_csv(run_damlive_dir / "colors.csv")
    expected_color = expected_soil_color(colors)
    soil_names = colors[colors["type"] == "soil"]["name"].to_list()
    expected_color["soil_name"] = soil_names
    expected_color["stb_name"] = soil_names
    pd.testing.assert_frame_equal(
        combine_results.create_df_damlive_soil_color(), expected_color
    )


def test_datetime_falls_back_to_now_without_damlive_timestamp(run_damlive_dir):
    # a single DamLive result under a name without DamLive metadata
    damlive = next((run_damlive_dir / CALC).rglob("*_result.stix"))
    renamed = run_damlive_dir / "renamed_result.stix"
    renamed.write_bytes(damlive.read_bytes())
    data_adapter = setup_data_adapter(run_damlive_dir)
    data_adapter.set_global_variable("CombineDamLiveResults", {})

    before = datetime.now().timestamp() * 1000
    combine_results = combine(data_adapter, renamed)
    after = datetime.now().timestamp() * 1000

    assert combine_results.stix.timestamp is None
    soil = combine_results.create_df_damlive_soil()
    assert soil["datetime"].between(before, after).all()


def test_calc_folder_needs_a_matching_selection(run_damlive_dir):
    data_adapter = setup_data_adapter(run_damlive_dir)

    select(data_adapter, "PU0021_87074-1_DWP002", "2030-01-01T00:00:00")
    with pytest.raises(UserWarning, match="Geen stix-bestand"):
        combine(data_adapter, CALC)

    data_adapter.set_global_variable("CombineDamLiveResults", {})
    with pytest.raises(UserWarning, match="kies er één"):
        combine(data_adapter, CALC)


@pytest.mark.parametrize(("location", "timestamp"), [RESULTS[0], RESULTS[-1]])
def test_plot_stage_draws_soils_waterlines_and_slip_surface(
    run_damlive_dir, location, timestamp
):
    data_adapter = setup_data_adapter(run_damlive_dir)
    select(data_adapter, location, timestamp)
    combine_results = combine(data_adapter, CALC)
    stage_id = combine_results.df_merged_soils["stage_id"].iloc[0]

    fig, ax = combine_results.plot_stage(stage_id=stage_id, xlim=(0, 65), ylim=(-15, 6))

    assert ax.get_xlim() == (0, 65)
    assert ax.get_ylim() == (-15, 6)
    assert len(ax.patches) == len(combine_results.df_merged_soils)
    slip_lines = [line for line in ax.lines if line.get_color() == "red"]
    # Bishop: one arc; UpliftVan: two arcs and the tangent between them
    upliftvan = combine_results.stix.calculation_model == "UpliftVan"
    assert len(slip_lines) == (3 if upliftvan else 1)
    plt.close(fig)
