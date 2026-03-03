import pandas as pd
import xml.etree.ElementTree as ET


def input_xml_timeseries(input_config: dict) -> pd.DataFrame:
    """Schrijft een XML-bestand in gegeven een pad

    Notes:
    ------
    Gebruikt een custom XML-formaat dat compatibel is met FEWS.

    Returns:
    --------
    None
    """
    # Data checks worden gedaan in de functies zelf, hier alleen geladen
    path = input_config["abs_path"]

    tree = ET.parse(path)
    root = tree.getroot()

    namespace = {"ns": "http://www.wldelft.nl/fews/PI"}
    df_list = []
    for series in root.findall("ns:series", namespace):
        header = series.find("ns:header", namespace)
        location = header.find("ns:stationName", namespace).text
        parameter = header.find("ns:parameterId", namespace).text
        unit = header.find("ns:units", namespace).text

        for event in series.findall("ns:event", namespace):
            date_str = event.get("date")
            time_str = event.get("time")
            value = event.get("value")

            df_list.append(
                {
                    "date_time": pd.to_datetime(f"{date_str} {time_str}"),
                    "measurement_location_code": location,
                    "parameter_code": parameter,
                    "unit": unit,
                    "value": float(value),
                }
            )

    df = pd.DataFrame(df_list)
    return df
