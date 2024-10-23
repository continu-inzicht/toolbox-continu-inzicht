import pytest
from toolbox_continu_inzicht.loads import get_waterinfo_locations

@pytest.mark.asyncio
async def test_get_waterinfo_locations():
  
  df = await get_waterinfo_locations(parameter_id="waterhoogte")
  assert df is not None
  assert df is not df.empty