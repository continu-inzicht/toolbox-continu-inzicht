def import_folium():
    try:
        import folium
    except ImportError:
        folium = None
        raise ImportError(
            "Folium is not installed, use the dev pixi environment or install folium"
        )
    return folium


def import_rasterstats():
    try:
        from rasterstats import zonal_stats
    except ImportError:
        zonal_stats = None
        raise ImportError(
            "Rasterio or zonalstats is not installed, use the dev pixi environment or install rasterio and rasterstats"
        )
    return zonal_stats
