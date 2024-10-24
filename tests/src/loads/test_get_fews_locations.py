import pytest
from toolbox_continu_inzicht.loads import get_fews_locations

@pytest.mark.skip(reason="Eerst een Fews rest service definieren")
@pytest.mark.asyncio
async def test_get_fews_locations():
  
  df = await get_fews_locations(
     host="https://*****************", 
     port=8443, 
     region="fewspiservice", 
     filter_id="HKV_WV_1"
    )
  
  assert df is not None
  assert df is not df.empty
