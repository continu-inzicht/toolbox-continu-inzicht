import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_post


async def get_rws_webservices_locations() -> pd.DataFrame:
    """Haal locaties die bekend zijn bij de RWS webservice."""

    url_catalog: str = "https://waterwebservices.rijkswaterstaat.nl/METADATASERVICES_DBO/OphalenCatalogus"
    # haal voor all locaties de informatie op: catalogus met data
    body_catalog: dict = {
        "CatalogusFilter": {"Compartimenten": True, "Grootheden": True}
    }

    _, catalog_data = await fetch_data_post(url_catalog, body_catalog, mime_type="json")

    if catalog_data is not None:
        df_available_locations = pd.DataFrame(catalog_data["LocatieLijst"])
        df_available_locations = df_available_locations.set_index("Locatie_MessageID")
    else:
        raise ConnectionError("Catalog not found")

    return df_available_locations
