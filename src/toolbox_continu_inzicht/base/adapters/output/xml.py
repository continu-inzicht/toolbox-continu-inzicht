import pandas as pd


def output_xml_timeseries(output_config: dict, df: pd.DataFrame):
    """Schrijft een XML-bestand in gegeven een pad

    Notes:
    ------
    Gebruikt een custom XML-formaat dat compatibel is met FEWS.
    Gaat uit van een tijdreeks met de volgende kolommen: 'date_time', 'measurement_location_code', 'parameter_code', 'value', 'unit'.
    Opties om dit aan te passen kunnen worden meegegeven in het configuratiebestand.
    Er kan ook een parameter_mapping worden meegegeven in het configuratiebestand om parameter_codes te mappen naar andere waarden.

    Returns:
    --------
    None
    """
    # Data checks worden gedaan in de functies zelf, hier alleen geladen
    path = output_config["abs_path"]
    output_xml_str = """<?xml version="1.0" encoding="UTF-8"?>
<TimeSeries xmlns="http://www.wldelft.nl/fews/PI" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.wldelft.nl/fews/PI https://fewsdocs.deltares.nl/schemas/version1.0/pi-schemas/pi_timeseries.xsd" version="1.2">
	<timeZone>1.0</timeZone>\n"""
    df.reset_index(inplace=True)  # some case datetime is the index
    assert "date_time" in df.columns, "DataFrame moet een 'date_time' kolom bevatten"
    assert "measurement_location_code" in df.columns, (
        "DataFrame moet een 'measurement_location_code' kolom bevatten"
    )
    assert "parameter_code" in df.columns, (
        "DataFrame moet een 'parameter_code' kolom bevatten"
    )
    assert "unit" in df.columns, "DataFrame moet een 'unit' kolom bevatten"

    if "parameter_mapping" in output_config:
        parameter_mapping = output_config["parameter_mapping"]
        df["parameter_code"] = (
            df["parameter_code"].map(parameter_mapping).fillna(df["parameter_code"])
        )
    if "location_mapping" in output_config:
        location_mapping = output_config["location_mapping"]
        df["measurement_location_code"] = (
            df["measurement_location_code"]
            .map(location_mapping)
            .fillna(df["measurement_location_code"])
        )

    for location in df["measurement_location_code"].unique():
        df_subset = df[df["measurement_location_code"] == location]
        # Extract date and time components for XML formatting
        start_date = df_subset["date_time"].min()
        end_date = df_subset["date_time"].max()

        # Create XML for this location
        xml_output = f"""    <series>
        <header>
            <type>instantaneous</type>
            <locationId>{location}</locationId>
            <parameterId>{df_subset["parameter_code"].iloc[0]}</parameterId>
            <timeStep unit="second" multiplier="3600"/>
            <startDate date="{start_date.strftime("%Y-%m-%d")}" time="{start_date.strftime("%H:%M:%S")}"/>
            <endDate date="{end_date.strftime("%Y-%m-%d")}" time="{end_date.strftime("%H:%M:%S")}"/>
            <missVal>-999.0</missVal>
            <stationName>{location}</stationName>
            <units>{df_subset["unit"].iloc[0]}</units>
        </header>"""

        # Add events
        for _, row in df_subset.iterrows():
            dt = row["date_time"]
            xml_output += f'\n        <event date="{dt.strftime("%Y-%m-%d")}" time="{dt.strftime("%H:%M:%S")}" value="{row["value"]}" flag="0"/>'

        xml_output += "\n    </series>"
        output_xml_str += xml_output + "\n"

    output_xml_str += "</TimeSeries>"
    with open(path, "w") as f:
        f.write(output_xml_str)


def output_xml_calculation_parameters(output_config: dict, df) -> None:
    """writes an XML calculation parameters file given the dataframe


    Notes:
    ------
    The dataframe should contain two columns: parameters_names and parameters_values.

    The understore in the parameter names will be used to create the XML structure,
    where the part before the understore is the parent element
    and the part after the understore is the child element.

    Example input dataframe:
    Parameter_names,Parameters
    CalculationModules_StabilityInside,1
    CalculationModules_StabilityOutside,0
    CalculationModules_PipingBligh,0
    CalculationModules_PipingWti,0
    StabilityParameters_CalculationModel,Bishop
    StabilityParameters_SearchMethod,Grid

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
    """
    path = output_config["abs_path"]
    output_xml_str = """<?xml version="1.0" encoding="utf-8"?>
    <XmlCalculationParameters xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">"""
    assert "parameter_names" in df.columns and "parameter_values" in df.columns, (
        "DataFrame moet een 'parameter_names' en 'parameter_values' kolom bevatten"
    )
    assert all(df["parameter_names"].str.contains("_")), (
        "Alle parameter_names moeten een onderstore bevatten om de XML structuur te bepalen"
    )

    for parent in df["parameter_names"].str.split("_").str[0].unique():
        output_xml_str += f"\n    <{parent}>"
        df_subset = df[df["parameter_names"].str.startswith(parent)]
        for _, row in df_subset.iterrows():
            child = row["parameter_names"].split("_")[1]
            value = row["parameter_values"]
            output_xml_str += f"\n        <{child}>{value}</{child}>"
        output_xml_str += f"\n    </{parent}>"

    output_xml_str += "\n</XmlCalculationParameters>"
    with open(path, "w") as f:
        f.write(output_xml_str)
