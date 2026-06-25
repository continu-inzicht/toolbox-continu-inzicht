# %% [markdown]
# # DEMO sprint 8 - Deel 1 (Afronding DAM-Live berekening)

# %% [markdown]
# ### Benodigde generieke Python packages

# %%
from pathlib import Path
import datetime
import matplotlib.pyplot as plt


def main():
    # ## Ophalen van belastingen vanuit FEWS (voorbeeld HHNK)
    #
    # We gebruiken HHNK FEWS als voorbeeld op 'actuele' (grond)waterstanden op te halen.
    # Benodigde bestanden:
    # - Een CSV bestand met FEWS locaties (4 peilbuizen en 1 oppervlaktewater meetpunt)
    #
    # <details>
    # <summary>Configuratie</summary>
    #
    # ```yaml
    # GlobalVariables:
    #     rootdir: "data_demo/preprocess"
    #     moments: [0, 24]
    #     calc_time: "2024-11-06 08:00:00"
    #     aquo_alias:
    #         H.meting: "WATHTE"
    #         WATHTE [m][NAP][OW]: "WATHTE"
    #
    #     LoadsFews:
    #         host: "https://fews.hhnk.nl"
    #         port: 443
    #         region: "fewspiservice"
    #         version: "1.25"
    #         parameters: ["H.meting", "WATHTE [m][NAP][OW]"]
    #
    # DataAdapter:
    #     default_options:
    #         csv:
    #             sep: ","
    #     locaties:
    #         type: csv
    #         path: "locations_fews.csv"
    #     waterstanden:
    #         type: csv
    #         path: "hidden_waterstanden_fews.csv"
    #     waterstanden_xml:
    #         type: xml_timeseries
    #         path: "waterstanden_fews.xml"
    #         parameter_mapping:
    #             WATHTE: H.meting
    #         location_mapping:
    #             MPN-N-24: BP
    #
    # ```
    #
    # </details>

    # ### Lees configuratie in en maak dictionary base.Config aan

    from toolbox_continu_inzicht import Config

    yaml_config_file = "loads_fews_grondwater.yaml"
    path = Path.joinpath(Path.cwd(), "data_demo/preprocess", yaml_config_file)
    config = Config(config_path=path)
    config.lees_config()

    # ### Geef config mee aan base.Adapter

    from toolbox_continu_inzicht import DataAdapter

    data_adapter = DataAdapter(config)
    print(f"Folder met data: {data_adapter.config.config_path}")
    print(f"Globale variabelen: {data_adapter.config.global_variables}")

    # ### Laad en run module (functie) LoadFews voor ophalen 'actuele' (grond)waterstand uit FEWS

    from toolbox_continu_inzicht.loads.loads_fews.loads_fews import LoadsFews

    loads_fews = LoadsFews(data_adapter=data_adapter)
    df_out = loads_fews.run(input="locaties", output="waterstanden_xml")

    if df_out is not None:
        print("Data succesvol opgehaald uit FEWS.")
        df_waterstanden = data_adapter.input("waterstanden_xml")
    else:
        print(
            "Er is een fout opgetreden bij het ophalen van data uit FEWS. Gebruik een lokaal testbestand"
        )
        data_adapter.input("waterstanden_xml")
        file = data_adapter.config.data_adapters["waterstanden_xml"]["path"]
        df_waterstanden = data_adapter.input("waterstanden_xml")

    print(df_waterstanden.head())

    # ### Plot de ingelezen 'actuele'(grond)waterstanden in een grafiek

    fig, ax = plt.subplots()
    for station in df_waterstanden["measurement_location_code"].unique():
        df_station = df_waterstanden[
            df_waterstanden["measurement_location_code"] == station
        ].copy()
        df_station.rename(columns={"value": station}, inplace=True)
        df_station.plot(x="date_time", y=station, ax=ax, legend=station, marker=".")

    # ### De 'actuele' (grond)waterstanden uitlezen op de gedefinieerde momenten

    from toolbox_continu_inzicht.loads import LoadsToMoments

    loads_to_moments = LoadsToMoments(data_adapter=data_adapter)
    loads_to_moments.run(input="waterstanden_xml", output="waterstanden_xml_hourly")

    # ### Plot de 'actuele'(grond)waterstanden voor de momenten in een grafiek

    fig, ax = plt.subplots()
    df_waterstanden_moments = loads_to_moments.df_out
    for station in df_waterstanden_moments["measurement_location_code"].unique():
        df_station = df_waterstanden_moments[
            df_waterstanden_moments["measurement_location_code"] == station
        ].copy()
        df_station.rename(columns={"value": station}, inplace=True)
        df_station.plot(x="date_time", y=station, ax=ax, legend=station, marker=".")

    # ### Voer een DAM-Live berekening uit
    # Let op: Om DAM-Live te kunnen gebruiken is een licentie van Deltares nodig!
    # Dit voorbeeld is gemaakt met DAM-Live versie 26.1.0.7015.
    # Kopieer de resultaten met waterstanden uit preprocess (FEWS) naar de process map!
    #
    #
    # <details>
    # <summary> Configuratie</summary>
    #
    # ```yaml
    # GlobalVariables:
    #     rootdir: "data_demo/process"
    #     moments: [0, 24]
    #     calc_time: "2024-11-06 08:00:00"
    #     UpdateDamLive:
    #         DAMLIVE_FILE: 'WV2030_Purmer.damx'
    #         delete_output_folder: False
    #
    # DataAdapter:
    #     default_options:
    #         csv:
    #             sep: ","
    #     waterstanden_xml_uur:
    #         type: xml_timeseries
    #         path: "waterstanden_fews_hourly.xml"
    #     parameters_bishop_csv:
    #         type: csv
    #         index: True
    #         path: "parameters_bishop.csv"
    #     parameters_uplift_csv:
    #         type: csv
    #         index: True
    #         path: "parameters_uplift.csv"
    #     output_file:
    #         type: csv
    #         path: test.csv
    #
    # ```
    # </details>
    #

    # ### Lees configuratie in en maak dictionary base.Config aan

    yaml_config_file = "run_damlive.yaml"
    path = Path.joinpath(Path.cwd(), "data_demo/process", yaml_config_file)
    config = Config(config_path=path)
    config.lees_config()

    # ### Geef config mee aan base.Adapter

    data_adapter = DataAdapter(config)
    print(f"Folder met data: {data_adapter.config.config_path}")
    print(f"Globale variabelen: {data_adapter.config.global_variables}")

    # ### Inlezen van (grond)waterstanden en parameters voor de DAM-Live berekening
    # Benodigd:
    # - waterstanden_fews_hourly.xml met (grond)waterstanden
    # - parameters_bishop.csv met instelling voor Bishop berekening in DAM-Live

    data_adapter.input("waterstanden_xml_uur")
    data_adapter.input("parameters_bishop_csv")

    # ### Benodigde databestanden voor DAM-Live berekening
    # - Een {prjectnaam}.damx bestand voor de DAM-Live berekening
    # - Een folder {prjectnaam},geometries2D}
    #

    # ### Geef de invoer door aan DAM-Live

    from toolbox_continu_inzicht.dam_live import UpdateDamLive

    update_dam_live = UpdateDamLive(data_adapter=data_adapter)

    # ### Start de DAM-Live berekening (indien DAM-Live licentie beschikbaar)

    # om meer informatie terug te krijgen van DAMLIVE, kan de logging level op INFO worden gezet.
    # update_dam_live.data_adapter.set_global_variable("logging", {"level": "INFO"})
    # update_dam_live.data_adapter.init_logging(re_initialize=True)
    update_dam_live.run(
        input=["waterstanden_xml_uur", "parameters_bishop_csv"], output="output_file"
    )

    # ### Samenvatting van de berekeningsesultaten
    # Resultaatbestanden (Resultaat.csv en WV2030_Purmer.Calc (resultaat reeds uitgepakt)) zijn onderdeel van deze demo (beschikbaar zonder DAM-Live uit te voeren i.v.m. benodigde licentie DAM-Live)

    df_output_damlive = update_dam_live.data_adapter.input("output_file")
    df_output_damlive.head(10)

    # ### Verwerk de DAM-Live resultaten

    # Eerst moeten de resultaten uitgepakt worden. Mocht je problemen hebben met het uitpakken van de bestanden, kan het door de maximale pad naam lengthe van windows komen. De namen van de DamLive bestanden zijn vrij lang. Zie deze [gist](https://gist.github.com/leodutra/a25bc1f51e8779943df0a95d5a4839d1#windows-11) voor een oplossing
    #
    # <details>
    # <summary> Configuratie</summary>
    #
    # ```yaml
    # GlobalVariables:
    #   rootdir: "data_demo/postprocess/WV2030_Purmer.Calc/Stability/Bishop/PU0013_87074-1_DWP001_0_2024-11-06T07"
    #
    # DataAdapter:
    #   scenario:
    #     type: stages
    #     path: scenarios
    #
    #   geometries:
    #     type: geometries
    #     path: geometries
    #
    #   soillayers:
    #     type: soillayers
    #     path: soillayers
    #
    #   soils:
    #     type: soils
    #     path: soils.json
    #
    #   waternets:
    #     type: waternets
    #     path: waternets
    #
    #   calculationsettings:
    #     type: calculationsettings
    #     path: calculationsettings
    #
    #   colors:
    #     type: csv
    #     relative_file: data_demo/postprocess/colors.csv
    #
    #   merge_soil:
    #     type: csv
    #     path: merged_soil.csv
    #
    #   merge_waternet:
    #     type: csv
    #     path: merged_waternet.csv
    #
    #   merge_calculations:
    #     type: csv
    #     path: merged_calculations.csv
    # ```
    # </details>

    # ### Lees configuratie in en maak dictionary base.Config aan

    yaml_config_file = "dam_live_CI_database_config.yaml"
    path = Path.joinpath(Path.cwd(), "data_demo/postprocess", yaml_config_file)
    config = Config(config_path=path)
    config.lees_config()

    # ### Geef config mee aan base.Adapter

    data_adapter = DataAdapter(config)
    print(f"Folder met data: {data_adapter.config.config_path}")
    print(f"Globale variabelen: {data_adapter.config.global_variables}")

    # ### Verwerk resultaten in grafiek
    # Ook de kleuren van de bodemlagen worden meergenomen in de resutaat; ie hiervoor colors.csv en de configuratie in het yaml bestand

    from toolbox_continu_inzicht.dam_live import CombineDamLiveResults

    combine_damlive_results = CombineDamLiveResults(data_adapter)

    combine_damlive_results.run(
        input=[
            "scenario",
            "geometries",
            "soils",
            "soillayers",
            "waternets",
            "calculationsettings",
            "colors",
        ],
        output=[
            "merge_soil",
            "merge_waternet",
            "merge_calculations",
            "ci_database_damlive_soil",
            "ci_database_damlive_soil_color",
        ],
    )

    # ### Demo alleen voor locatie PU0013_87074-1_DWP001 en tijdstap 2024-11-06T07

    loc = "PU0013_87074-1_DWP001"
    time = "2024-11-06T07"

    # in dit geval is de tijdzone van de output +1 uur, dus die wordt hier nog goed gezet voor het filteren
    tz = datetime.timezone(datetime.timedelta(hours=1))

    date_time = df_output_damlive["date_time"].apply(
        lambda x: datetime.datetime.fromisoformat(x)
        .astimezone(tz)
        .strftime("%Y-%m-%dT%H")
    )

    df_corresponding_output = df_output_damlive[
        (df_output_damlive["measurement_location_code"] == loc) & (date_time == time)
    ]
    df_corresponding_output



if __name__ == "__main__":
    main()
