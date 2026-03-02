from toolbox_continu_inzicht.loads import get_waterinfo_thresholds


def test_get_waterinfo_locations():
    df = get_waterinfo_thresholds(
        location_code="hoekvanholland", parameter_id="waterhoogte"
    )
    assert df is not None
    assert df is not df.empty
