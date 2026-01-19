import pandas as pd
from toolbox_continu_inzicht.utils.fetch_functions import fetch_data_post


def get_rws_webservices_locations():
    """Haal locaties op die bekend zijn bij de RWS webservice."""

    url_catalog: str = "https://ddapi20-waterwebservices.rijkswaterstaat.nl/METADATASERVICES/OphalenCatalogus"
    # haal voor all locaties de informatie op: catalogus met data
    body_catalog: dict = {
        "CatalogusFilter": {
            "Compartimenten": True,
            "Grootheden": True,
            "Eenheden": True,
            ### mogelijke opties
            # "Hoedanigheden": True,
            # "Typeringen": True,
            # "WaardeBewerkingsMethoden": True,
            # "BioTaxon": True,
            # "Organen": True,
            # "ProcesTypes": True,
        }
    }

    status, catalog_data = fetch_data_post(url_catalog, body_catalog, mime_type="json")

    if status is None and catalog_data is not None:
        df_available_locations = pd.DataFrame(catalog_data["LocatieLijst"])
        df_available_locations = df_available_locations.rename(
            columns={
                "Locatie_MessageID": "measurement_location_code",
                "Naam": "measurement_location_description",
            }
        )
        df_available_locations = df_available_locations.set_index(
            "measurement_location_code"
        )
    else:
        raise ConnectionError("Catalog not found")

    return df_available_locations
