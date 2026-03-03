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


def input_xml_calculation_parameters(input_config: dict) -> pd.DataFrame:
    """Reads an XML calculation parameters file and returns a flattened DataFrame

    Notes:
    ------
    Parses XML structure and flattens nested elements into a single row.

    Example XML structure:
    <?xml version="1.0" encoding="utf-8"?>
    <XmlCalculationParameters xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
        <CalculationModules>
            <StabilityInside>1</StabilityInside>
            <StabilityOutside>0</StabilityOutside>
            <PipingBligh>0</PipingBligh>
            <PipingWti>0</PipingWti>
        </CalculationModules>
        <StabilityParameters>
            <CalculationModel>Bishop</CalculationModel>
            <SearchMethod>Grid</SearchMethod>
        </StabilityParameters>
    </XmlCalculationParameters>

    Returns:
    --------
    pd.DataFrame
    """
    path = input_config["abs_path"]

    tree = ET.parse(path)
    root = tree.getroot()

    parameters = {}
    for module in ["CalculationModules", "StabilityParameters"]:
        for parent in root.findall(module):
            for child in parent.iter():
                # only keep usefule values, skip empty text and newlines
                if "\n" not in child.text and child.text is not None:
                    parameters[f"{parent.tag}_{child.tag}"] = child.text

    df = pd.DataFrame([parameters], index=["parameter_values"])
    df_out = df.T
    df_out.index.name = "parameter_names"
    return df_out.reset_index()
