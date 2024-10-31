import pytest
from toolbox_continu_inzicht.loads import get_fews_locations


@pytest.mark.asyncio
async def test_get_fews_locations():
    df = await get_fews_locations(
        host="https://fews.hhnk.nl",
        port=443,
        region="fewspiservice",
        filter_id="HHNK_WEB",
    )

    assert df is not None
    assert df is not df.empty
