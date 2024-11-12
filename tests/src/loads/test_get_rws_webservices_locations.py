from toolbox_continu_inzicht.loads import get_rws_webservices_locations


def test_get_rws_webservices_locations():
    df = get_rws_webservices_locations()
    assert df is not None
