import pytest
from toolbox_continu_inzicht.loads import get_fews_thresholds


@pytest.mark.asyncio
async def test_get_waterinfo_locations():
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
