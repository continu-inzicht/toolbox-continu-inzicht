from toolbox_continu_inzicht.loads import get_matroos_locations


def test_get_matroos_locations():
    gdf = get_matroos_locations()
    assert gdf is not None
