import pytest
from toolbox_continu_inzicht.loads import get_rws_webservices_locations

@pytest.mark.asyncio
async def test_get_rws_webservices_locations():
  
  df = await get_rws_webservices_locations()
  assert df is not None
