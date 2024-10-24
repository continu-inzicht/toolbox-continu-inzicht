import pytest
from toolbox_continu_inzicht.loads import get_matroos_locations


@pytest.mark.asyncio
async def test_get_matroos_locations():
    gdf = await get_matroos_locations()
    assert gdf is not None
