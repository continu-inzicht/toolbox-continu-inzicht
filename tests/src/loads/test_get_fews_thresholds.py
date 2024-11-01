import os
import pytest
from toolbox_continu_inzicht.loads import get_fews_thresholds


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Not possible in FEWS",
)
async def test_get_fews_thresholds():
    print(os.getenv("GITHUB_ACTIONS"))
    df = await get_fews_thresholds(
        host="https://fews.hhnk.nl",
        port=443,
        region="fewspiservice",
        filter_id="HHNK_WEB",
        parameter_id=["EGVms_m.meting"],
        location_id="MPN-AS-2426",
    )

    assert df is not None
    assert df is not df.empty


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Not possible in FEWS",
)
async def test_get_fews_thresholds_invalid_host():
    with pytest.raises(Exception):
        await get_fews_thresholds(
            host="https://invalid-host",
            port=443,
            region="fewspiservice",
            filter_id="HHNK_WEB",
            parameter_id=["EGVms_m.meting"],
            location_id="MPN-AS-2426",
        )


@pytest.mark.skipif(
    os.getenv("GITHUB_ACTIONS") == "true",
    reason="Not possible in FEWS",
)
async def test_get_fews_thresholds_invalid_location():
    with pytest.raises(Exception):
        await get_fews_thresholds(
            host="https://fews.hhnk.nl",
            port=443,
            region="fewspiservice",
            filter_id="HHNK_WEB",
            parameter_id=["EGVms_m.meting"],
            location_id="invalid-location",
        )
