from pathlib import Path

from toolbox_continu_inzicht import Config, DataAdapter
from toolbox_continu_inzicht.fragility_curves import (
    LoadCachedFragilityCurveOneFailureMechanism,
    LoadCachedFragilityCurve,
    LoadCachedFragilityCurveMultiple,
)


def load_data_adapter(name: str):
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / name)
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    return data_adapter


def tests_load_cached_one_section_one_failure_mechanism():
    data_adapter = load_data_adapter(
        "test_fragility_curve_from_cache_one_section_one_failure.yaml"
    )
    load_cached_fragility_curve = LoadCachedFragilityCurveOneFailureMechanism(
        data_adapter=data_adapter
    )
    load_cached_fragility_curve.run(
        input=["fragility_curve_one_section_one_failure", "measures_to_effect"],
        output="resulting_fragility_curve",
        measure_id=1,
    )
    # load_cached_fragility_curve
    assert (load_cached_fragility_curve.df_out["measure_id"] == 1).all()


def tests_load_cached_one_section_multi_failure_mechanism_one_measure_id():
    data_adapter = load_data_adapter(
        "test_fragility_curve_from_cache_one_section_multi_failure.yaml"
    )
    load_cached_fragility_curve = LoadCachedFragilityCurve(data_adapter=data_adapter)
    load_cached_fragility_curve.run(
        input=["fragility_curve_one_section_multi_failure", "measures_to_effect"],
        output="resulting_fragility_curve",
        measure_id=1,
    )
    # load_cached_fragility_curve
    assert (load_cached_fragility_curve.df_out["measure_id"] == 1).all()
    assert not (
        load_cached_fragility_curve.df_out["failuremechanism_id"] == 1
    ).all()  # check actually two failure mechanisms are present


def tests_load_cached_multi_section_multi_failure_mechanism_one_measure_id():
    data_adapter = load_data_adapter(
        "test_fragility_curve_from_cache_multi_section_multi_failure.yaml"
    )
    load_cached_fragility_curve = LoadCachedFragilityCurveMultiple(
        data_adapter=data_adapter
    )
    load_cached_fragility_curve.run(
        input=["fragility_curve_multi_section_multi_failure", "measures_to_effect"],
        output="resulting_fragility_curve",
        measure_id=1,
    )
    assert (load_cached_fragility_curve.df_out["measure_id"] == 1).all()
    assert not (load_cached_fragility_curve.df_out["failuremechanism_id"] == 1).all()
    assert not (load_cached_fragility_curve.df_out["section_id"] == 1).all()


def tests_load_cached_multi_section_multi_failure_mechanism_diff_measure_id():
    """checks that different measure_ids are used per section_id when specified in section_id_to_measure_id"""
    data_adapter = load_data_adapter(
        "test_fragility_curve_from_cache_multi_section_multi_failure.yaml"
    )
    load_cached_fragility_curve = LoadCachedFragilityCurveMultiple(
        data_adapter=data_adapter
    )
    load_cached_fragility_curve.run(
        input=[
            "fragility_curve_multi_section_multi_failure",
            "measures_to_effect",
            "section_id_to_measure_id",
        ],
        output="resulting_fragility_curve",
    )
    # opposite of previous test, now different measure_ids per section
    assert not (load_cached_fragility_curve.df_out["measure_id"] == 1).all()
    # still all different
    assert not (load_cached_fragility_curve.df_out["failuremechanism_id"] == 1).all()
    assert not (load_cached_fragility_curve.df_out["section_id"] == 1).all()


def tests_loadcached_multi_section_multi_failure_mechanism_missing_cached_curve():
    """checks that different measure_ids are used per section_id when specified in section_id_to_measure_id, even when missing in cache"""
    data_adapter = load_data_adapter(
        "test_fragility_curve_from_cache_multi_section_multi_failure.yaml"
    )
    load_cached_fragility_curve = LoadCachedFragilityCurveMultiple(
        data_adapter=data_adapter
    )
    load_cached_fragility_curve.run(
        input=[
            "fragility_curve_multi_section_multi_failure",
            "measures_to_effect",
            "section_id_to_measure_id_not_cached",
        ],
        output="resulting_fragility_curve",
    )
    # See notebook 6 for visual check: TODO: add more detailed tests here
    # opposite of previous test, now different measure_ids per section
    assert not (load_cached_fragility_curve.df_out["measure_id"] == 1).all()
    # still all different
    assert not (load_cached_fragility_curve.df_out["failuremechanism_id"] == 1).all()
    assert not (load_cached_fragility_curve.df_out["section_id"] == 1).all()


# TODO add non-trivial tests with different measure_ids per section and failure mechanism
