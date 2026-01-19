# Flood risk data adapter

Let op: de data adapter specifiek voor `calculate_flood_risk.py`.

Alle andere geven nagenoeg altijd een (geo)pandas object terug, deze datae adapter geeft een numpy array en [rasterio.Affine](https://github.com/rasterio/affine).
