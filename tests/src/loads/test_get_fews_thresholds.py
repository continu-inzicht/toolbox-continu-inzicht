import pytest
from toolbox_continu_inzicht.loads import get_fews_thresholds

@pytest.mark.skip(reason="Eerst een Fews rest service definieren")
@pytest.mark.asyncio
async def test_get_waterinfo_locations():
  
  df = await get_fews_thresholds(
    host="https://*****************", 
    port=8443, 
    region="fewspiservice", 
    filter_id="HKV_WV_1", 
    parameter_id="WNSHDB1", 
    location_id="VOV9345"
  )
  
  assert df is not None
  assert df is not df.empty