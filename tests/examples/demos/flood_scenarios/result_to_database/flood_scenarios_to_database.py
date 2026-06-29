# ---
# title : "Real-time of what-if overstromingsrisico's"
# ---


# Risico's worden berekend door kansen te combineren met gevolgen. In de toolbox Continu Inzicht worden fragilitycurves gebruikt om de conditionele kans op falen van een sectie (per faalmechanisme) te bepalen. De gevolgen van een overstroming worden bepaald met behulp van overstromingsscenarioberekeningen, ook wel aangeduid met scenario's. Uit een overstroomd gebied worden gevolgen afgeleid zoals economische schade, slachtoffers, en getroffenen. <br>
# Door conditionele kansen met gevolgen te combineren kunnen conditionele risico's worden bepaald, bestaande uit de verwachte economische schade van een gebied, het verwachte aantal slachtoffers en/of getroffen in een gebied en/of de plaatsgebonden overstromingskans van een locatie. <br>
#
# Deze risico's kunnen helpen om prioriteiten te stellen tijdens een hoogewatern, waarbij er niet alleen een kans of een afzonderlijk gevolg wordt beschouwd. <br>
#
# Om de scenarios te kunnen combineren moeten er een aantal stappen worden doorlopen:
# - Omrekenen van dijkvakkansen naar trajectdeelkansen ([CalculateFloodScenarioProbability](#sec-CalculateFloodScenarioProbability))
# - Belasting per deeltraject bepalen ([LoadFromFloodScenarioProbability](#sec-LoadFromFloodScenarioProbability))
# - Met de belasting een bijpassende overstromingscenario selecteren ([SelectFloodScenarioFromLoad ](#sec-SelectFloodScenarioFromLoad))
# - Van het scenario en de curve een geagregeerd risico bepalen ([CalculateFloodRisk](#sec-CalculateFloodRisk))
# - Een nabewerking van het risico om dit terug te koppelen aan het maatgevende vak ([PostProcessFloodRisk](#sec-PostProcessFloodRisk))
#
# ```{mermaid}
# graph TD
#     A[CalculateFloodScenarioProbability] --> B[LoadFromFloodScenarioProbability]
#     B --> C[SelectFloodScenarioFromLoad]
#     C --> D[CalculateFloodRisk]
#     D --> E[PostProcessFloodRisk]
#     A --> E
#
# ```

# %%
# initialiseer de (toolbox continu inzicht) modules
from pathlib import Path

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter


# ## CalculateFloodScenarioProbability{#sec-CalculateFloodScenarioProbability}
#
# Dit voorbeeld laat zien hoe met behulp van de `CalculateFloodScenarioProbability` scenariokansen berekend worden uit kansen per sectie en per faalmachanisme. <br>
# Eerst worden de kansen per sectie en faalmachanisme ingeladen, daarna worden deze gecombineerd naar een kans over alle secties (voorheen aangeduid als de ringkans) per faalmechanisme. Daarna worden de kansen over alle secties per faalmechanisme gecombineerd naar 1 kans over alle secties en over alle faalmechanismen. <br>
# ```{mermaid}
# graph TD
#     A1[Dijkvak/section] --> D[Dijktraject deel/segment]
#     B1[Dijkvak/section] --> D
#     B2[Dijkvak/section] --> D
#     C1[Dijkvak/section] --> E[Dijktraject deel/segment]
#     C2[Dijkvak/section] --> E
#     E --> F
#     D --> F[Overstromingskans gebied/Dikesystem]
# ```
# Deze volgorde (eerst combineren over alle sectie en daarna over alle faalmechanismen) is bewust gekozen omdat per faalmechanisme verschillende met andere correlaties wordt gerekend. Voor het faalmechanisme GEKB wordt verondersteld dat alle secties volledig afhankelijk falen en voor de overige faalmechanismen wordt verondersteld dat de secties volledig onafhankelijk falsen. Door gekozen volgorde bij het combineren van faalkansen aan te houden, kan dit onderscheid geborgd worden. Tussen de verschillende faalmechanismen wordt volledige onafhankelijkheid verondersteld. N.B. in een volledige probabilistische berekening (waarbij niet met fragility curves wordt gerekend) kan de daadwerkelijke correlatie worden berekend en is het ook mogelijk om te rekenen met gedeeltelijke afhankelijkheid. Dat is in een werkwijze met fragilitycurves niet mogelijk. <br>
#
# <details>
# <summary>Configuratie</summary>
#
# ```yaml
# ```yaml
# GlobalVariables:
#     rootdir: "data_sets/7.flood_scenarios"
#     moments: [-24,0,24,36]
#
# DataAdapter:
#     default_options:
#         csv:
#             sep: ","
#     input_failuremechanism:
#         type: csv
#         file: "rekentest_failuremechanism.csv"
#     input_probabilities_failuremechanisme_sections:
#         type: csv
#         file: "rekentest_probabilities_failuremechanisme_sections.csv"
#     input_sections_in_segment:
#         type: csv
#         file: "rekentest_sections_in_segment.csv"
#     output_scenario_failure_prob_segments:
#         type: csv
#         index: false
#         file: "hidden_rekentest_scenario_failure_prob_segments.csv"
#     output_combined_failure_prob_all_sections:
#         type: csv
#         index: false
#         file: "hidden_rekentest_combined_failure_prob_all_sections.csv"
# ```
# </details>


# Op dit momemt moet het voorbeeld nog worden uitgebreid met:
# - loop over momenten inbouwen
# - Mee nemen van een lengte-effect factor bij het bepalen van de faalkans per segment
def main():
    # %%
    # laadt de configuratie en initialiseer de data adapter
    path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=path / "rekentest_calculate_flood_scenario_probability.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # %%
    # druk een overzicht van de faalmechanismen af
    data_adapter.input("input_failuremechanism")

    # %%
    # druk een overzicht van de secties per segment af
    data_adapter.input("input_sections_in_segment")

    # %%
    # druk een overzicht van de faalkansen per sectie en faalmechanisme af
    data_adapter.input("input_probabilities_failuremechanisme_sections")

    # %%
    # importeer en initialiseer de module voor het berekenen van de scenariokansen per segment
    from toolbox_continu_inzicht.flood_scenarios import (
        CalculateFloodScenarioProbability,
    )

    # %%
    # maak een instantie van de klasse CalculateFloodScenarioProbability
    calculate_flood_scenario_probability = CalculateFloodScenarioProbability(
        data_adapter=data_adapter
    )

    # %%
    # voer de berekening van de scenariokansen uit
    calculate_flood_scenario_probability.run(
        input=[
            "input_failuremechanism",
            "input_probabilities_failuremechanisme_sections",
            "input_sections_in_segment",
        ],
        output=[
            "output_scenario_failure_prob_segments",
            "output_combined_failure_prob_all_sections",
        ],
    )

    # %%
    # druk een overzicht van de scenariokansen faalkansen per segment en over alle faalmechanismen af
    calculate_flood_scenario_probability.df_out_scenario_failure_prob_segments

    # %%
    # druk een overzicht van de gecombineerde faalkansen over alle secties en over alle faalmechanismen af
    calculate_flood_scenario_probability.df_out_combined_failure_prob_all_sections

    # ## LoadFromFloodScenarioProbability
    #
    # Voor het bepalen van de risico van een overstroming is het nodig om een bijpassend overstromingsscenario te selecteren. Er zijn meerdere overstromingsscenario's beschikbaar die verschillende hydraulische belastingen representeren. <br>
    # Afhankelijk van de scenariokans van een deeltraject (segment) kan een bijpassend overstromingsscenario worden geselecteerd. Hiertoe is het nodig om de hydraulische belasting per deeltraject te bepalen. Dat kan met behulp van de fragilitycurves. Hiertoe is het noodzakelijk om 1 representatieve fragilitycurve per deeltraject te selecteren. Dat doen we door voor elk deeltraject (segment) 1 sectie te selecteren. De fragilitycurve van deze sectie wordt dan gebruikt om de scenariekans van het deeltraject (segement) om te zetten in een hydraulische belasting, die daarna gebruik kan worden om een bijpassend overstromingsscenario te selecteren voor het deeltraject (segment). <br>
    #
    # <details>
    # <summary>Configuratie</summary>
    #
    # ```yaml
    # ```yaml
    # GlobalVariables:
    #     rootdir: "data_sets/7.flood_scenarios"
    #     moments: [ -24, 0, 24, 36 ]
    #     LoadFromFloodScenarioProbability:
    #         failuremechanism_id_combined: 1
    #
    # DataAdapter:
    #     default_options:
    #         csv:
    #             sep: ","
    #     input_scenario_failure_prob_segments:
    #         type: csv
    #         file: "hidden_rekentest_scenario_failure_prob_segments.csv"
    #     input_section_to_segment:
    #         type: csv
    #         file: "rekentest_section_to_segment.csv"
    #     input_section_fragility_curves:
    #         type: csv
    #         file: "rekentest_fragilitycurves.csv"
    #     output_scenario_loads:
    #         type: csv
    #         path: "hidden_rekentest_scenario_loads.csv"
    #
    # ```
    # </details>

    # Dit is specifiek voor een moment, over meerdere momenten loopen moet op een hoger viveau.

    # %%
    # laadt de configuratie en initialiseer de data adapter

    config = Config(
        config_path=path / "rekentest_load_from_flood_scenario_probability.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # %%
    # druk een overzicht van de scenariokansen per segment af
    data_adapter.input("input_scenario_failure_prob_segments")

    # %%
    # druk een overzicht van de mapping van sectie naar segment af
    data_adapter.input("input_section_to_segment")

    # %%
    # druk een overzicht van de fragiliteitscurves per faalmechanisme af
    data_adapter.input("input_section_fragility_curves")

    # %%
    # importeer en initialiseer de module voor het bepalen van de belastingen vanuit scenariokansen
    from toolbox_continu_inzicht.flood_scenarios import LoadFromFloodScenarioProbability

    # %%
    # voer de bepaling van de hydraulische belastingen uit
    load_from_flood_scenario_probability = LoadFromFloodScenarioProbability(
        data_adapter=data_adapter
    )
    load_from_flood_scenario_probability.run(
        input=[
            "input_scenario_failure_prob_segments",
            "input_section_to_segment",
            "input_section_fragility_curves",
        ],
        output="output_scenario_loads",
    )

    # %%
    # druk een overzicht van de belastingen overeenkomstig de scenariokansen (per segment) af
    load_from_flood_scenario_probability.df_out_scenario_loads

    # ## SelectFloodScenarioFromLoad
    #
    # Voor het bepalen van de risico van een overstroming is het nodig om een bijpassend overstromingsscenario te selecteren. Er zijn meerdere overstromingsscenario's beschikbaar die verschillende hydraulische belastingen representeren. Per scenario is een hydraulische belasting (per deeltraject) bepaald. Met deze hydraulische belasting kan een bijpassend overstromingsscenario geselcteerd worden voor het deeltraject (segment). <br>
    #
    # <details>
    # <summary>Configuratie</summary>
    #
    # ```yaml
    # GlobalVariables:
    #     rootdir: "data_sets/7.flood_scenarios"
    #     moments: [ -24, 0, 24, 36 ]
    #
    # DataAdapter:
    #     default_options:
    #         csv:
    #             sep: ","
    #     input_scenarios_loads:
    #         type: csv
    #         path: "hidden_rekentest_scenario_loads.csv"
    #     input_consequences_loads:
    #         type: csv
    #         path: "rekentest_consequences_loads.csv"
    #     output_scenario_consequences_grids:
    #         type: csv
    #         path: "hidden_rekentest_scenario_consequences_grids.csv"
    # ```
    # </details>

    # %%
    # laadt de configuratie en initialiseer de data adapter

    config = Config(config_path=path / "rekentest_select_flood_scenario_from_load.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # %%
    # druk een overzicht van de belastingen overeenkomstig de scenariokansen (per segment) af
    data_adapter.input("input_scenarios_loads")

    # %%
    # druk een overzicht van de gevolgen voor verschillende hydraulische belastingniveau's af
    data_adapter.input("input_consequences_loads")

    # %%
    # importeer en initialiseer de module voor het bepalen van de bijbehorende scenario's vanuit belastingen
    from toolbox_continu_inzicht.flood_scenarios import SelectFloodScenarioFromLoad

    # %%
    # voer de bepaling van het bijbehorende gevolggrid uit
    select_flood_scenario_from_load = SelectFloodScenarioFromLoad(
        data_adapter=data_adapter
    )
    select_flood_scenario_from_load.run(
        input=[
            "input_scenarios_loads",
            "input_consequences_loads",
        ],
        output="output_scenario_consequences_grids",
    )

    # %%
    # druk een overzicht van de bijbehorende gevolgen per scenario af
    select_flood_scenario_from_load.df_out_scenario_consequences_grids

    # ### Alternatief: keuze uit twee scenarios
    #
    # Standaard wordt er per scenariokans, één overstromingsscenario geselecteerd op basis van de hydraulische belasting. Met de parameter `return_two_scenarios` kan ervoor gekozen worden om twee overstromingsscenarios te selecteren. De gebruiker kan hiermee zelf bepalen welk scenario hij wil gebruiken voor de risico berekening. Of eventueel de gemiddelde schade van de twee scenario's gebruiken, of beide tonen. <br>
    #
    # <details>
    # <summary>Configuratie</summary>
    #
    # ```yaml
    # GlobalVariables:
    #     rootdir: "data_sets/7.flood_scenarios"
    #     moments: [ -24, 0, 24, 36 ]
    #     SelectFloodScenarioFromLoad:
    #         return_two_scenarios: True
    #
    # DataAdapter:
    #     default_options:
    #         csv:
    #             sep: ","
    #     input_scenarios_loads:
    #         type: csv
    #         path: "hidden_rekentest_scenario_loads.csv"
    #     input_consequences_loads:
    #         type: csv
    #         path: "rekentest_consequences_loads.csv"
    #     output_scenario_consequences_grids:
    #         type: csv
    #         path: "hidden_rekentest_scenario_consequences_grids.csv"
    # ```
    #
    # </details>
    #

    # %%
    # laadt de configuratie en initialiseer de data adapter

    config = Config(
        config_path=path
        / "rekentest_select_flood_scenario_from_load_two_scenarios.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # %%
    # druk een overzicht van de belastingen overeenkomstig de scenariokansen (per segment) af
    data_adapter.input("input_scenarios_loads")

    # %%
    # druk een overzicht van de gevolgen voor verschillende hydraulische belastingniveau's af
    data_adapter.input("input_consequences_loads")

    # %%
    # importeer en initialiseer de module voor het bepalen van de bijbehorende scenario's vanuit belastingen
    from toolbox_continu_inzicht.flood_scenarios import SelectFloodScenarioFromLoad

    # %%
    # voer de bepaling van het bijbehorende gevolggrid uit
    select_flood_scenario_from_load = SelectFloodScenarioFromLoad(
        data_adapter=data_adapter
    )
    select_flood_scenario_from_load.run(
        input=[
            "input_scenarios_loads",
            "input_consequences_loads",
        ],
        output="output_scenario_consequences_grids",
    )

    # %%
    # druk een overzicht van de bijbehorende gevolgen per scenario af
    # TODO verwijder de index kolom in de CSV output
    select_flood_scenario_from_load.df_out_scenario_consequences_grids

    # ## CalculateFloodRisk
    #
    # Het (conditionele) risico wordt bepaald door de (conditionele) kans te combineren met het gevolg. In dit geval worden de gevolgen van een overstroming gecombineerd met de scenariokans van een deeltraject (segment). <br>
    # Met behulp van de `CalculateFloodRisk` module worden risico's berekend voor 4 verschillende risicomate: (1) verwachte economische schade, (2) verwacht aantal slachtoffers, (3) verwacht aantal getroffenen, en (4) plaatsgebonden overstromingskans. <br>
    # In eerste instantie wordt het risico berekend per gridcel. Daarna worden de risico's geaggregeerd naar gebieden die gedefinieerd zijn in een shape-bestand.
    #
    #
    # <details>
    # <summary>Configuratie</summary>
    #
    # ```yaml
    # GlobalVariables:
    #     rootdir: "data_sets/7.flood_scenarios"
    #     moments: [ -24, 0, 24, 36 ]
    #     CalculateFloodRisk:
    #         aggregate_methods: # dit zijn de standaard aggregatie-methoden per risicomaat (kunnen worden overschreven in de config)
    #             casualties: sum
    #             damage: sum
    #             flooding: median
    #             affected_people: sum
    #             waterdepth: sum
    #         per_hectare: False
    #
    # DataAdapter:
    #     default_options:
    #         csv:
    #             sep: ","
    #             Index: False
    #     input_scenario_failure_prob_segments:
    #         type: csv
    #         path: "hidden_rekentest_scenario_failure_prob_segments.csv"
    #     input_scenario_consequences_grids:
    #         type: csv
    #         path: "hidden_rekentest_scenario_consequences_grids.csv"
    #     input_areas_to_aggregate:
    #         type: shape
    #         path: "rekentest_areas_to_aggregate.geojson"
    #     flood_risk_local_file:
    #         type: flood_risk_local_file
    #         path: '' # gebruik de data_dir
    #         scenario_path: "flood_scenarios" # en een map dieper kijk in de floodscenarios dir
    #         #abs_path_user: "tests/src/flood_scenarios/data_sets/flood_scenarios" # dit kan ook gebruikt worden
    #     output_flood_risk_results:
    #         type: csv
    #         path: "hidden_rekentest_flood_risk_results.csv"
    #
    # ```
    #
    # </details>
    #

    # %%
    # laadt de configuratie en initialiseer de data adapter

    config = Config(config_path=path / "rekentest_calculate_flood_risk.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # %%
    # druk een overzicht van de scenariokansen per segment af
    data_adapter.input("input_scenario_failure_prob_segments")

    # %%
    # druk een overzicht van de bijbehorende gevolgen per scenario af
    data_adapter.input("input_scenario_consequences_grids")

    # %%
    # druk de gebieden af waarvoor het overstromingsrisico wordt geaggregeerd
    data_adapter.input("input_areas_to_aggregate")

    # %%
    # importeer en initialiseer de module voor het berekenen van het overstromingsrisico in 4 verschillende formaten
    from toolbox_continu_inzicht.flood_scenarios.calculate_flood_risk import (
        CalculateFloodRisk,
    )

    # %%
    # voer de berekening van het risico op grid niveau uit
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "input_scenario_failure_prob_segments",
            "input_scenario_consequences_grids",
            "input_areas_to_aggregate",
            "flood_risk_local_file",
        ],
        output=["output_flood_risk_results", "output_flood_risk_results_per_segment"],
    )

    # %%
    data_adapter.input("flood_risk_local_file")

    # %%
    calculate_flood_risk.df_out_flood_risk_results.head(5)

    # %%
    calculate_flood_risk.df_out_flood_risk_results_per_segment.head(5)

    # %%
    import matplotlib.pyplot as plt

    gdf_results = calculate_flood_risk.df_out_flood_risk_results

    def make_plot(gdf_results):
        columns = ["casualties", "damage", "flooding", "affected_people"]
        rename = {
            "casualties": "Slachtoffers",
            "damage": "Schade",
            "flooding": "Plaatgebonden overstromingskans",
            "affected_people": "Getroffen mensen",
        }
        fig, axs = plt.subplots(2, 2, figsize=(7, 5))
        ax = axs.flatten()
        for index, column in enumerate(columns):
            gdf_results.plot(column=column, ax=ax[index])
            plt.colorbar(
                ax[index].collections[0], ax=ax[index], orientation="horizontal"
            )
            ax[index].set_title(rename[column])
        plt.tight_layout()
        return fig, ax

    make_plot(gdf_results)

    # Herhaal per ha
    # ```yaml
    # GlobalVariables:
    #     ...
    #     CalculateFloodRisk:
    #         ...
    #         per_hectare: True
    #         columns_per_hectare:
    #             - casualties
    #             - damage
    #             - affected_people
    #
    # ```
    #

    # %%
    config = Config(config_path=path / "rekentest_calculate_flood_risk_per_ha.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    calculate_flood_risk = CalculateFloodRisk(data_adapter=data_adapter)
    calculate_flood_risk.run(
        input=[
            "input_scenario_failure_prob_segments",
            "input_scenario_consequences_grids",
            "input_areas_to_aggregate",
            "flood_risk_local_file",
        ],
        output=["output_flood_risk_results", "output_flood_risk_results_per_segment"],
    )

    # %%
    gdf_results = calculate_flood_risk.df_out_flood_risk_results

    columns = [
        "casualties_per_ha",
        "damage_per_ha",
        "flooding",
        "affected_people_per_ha",
    ]
    rename = {
        "casualties_per_ha": "Slachtoffers per hectare",
        "damage_per_ha": "Schade per hectare",
        "flooding": "Plaatgebonden overstromingskans",
        "affected_people_per_ha": "Getroffen mensen per hectare",
    }
    fig, axs = plt.subplots(2, 2, figsize=(7, 5))
    ax = axs.flatten()
    for index, column in enumerate(columns):
        gdf_results.plot(column=column, ax=ax[index])
        plt.colorbar(ax[index].collections[0], ax=ax[index], orientation="horizontal")
        ax[index].set_title(rename[column])
    plt.tight_layout()

    # Naast het samengestelde risico slaan we ook het aandeel in risico per segment op

    # %%
    gdf_results_per_segment = (
        calculate_flood_risk.df_out_flood_risk_results_per_segment.copy()
    )

    segment_id_0 = gdf_results_per_segment["segment_id"].unique()[0]
    gdf_results_section_0 = gdf_results_per_segment[
        gdf_results_per_segment["segment_id"] == segment_id_0
    ]

    segment_id_1 = gdf_results_per_segment["segment_id"].unique()[1]
    gdf_results_section_1 = gdf_results_per_segment[
        gdf_results_per_segment["segment_id"] == segment_id_1
    ]

    # %%
    fig, ax = make_plot(gdf_results_section_0)
    fig.suptitle(f"Bijdrage segment {segment_id_0}")

    # Zo zien we dat de bijdragen van een segment aan het risico verschilt per gebied

    # %%
    fig, ax = make_plot(gdf_results_section_1)
    fig.suptitle(f"Bijdrage segment {segment_id_1}")

    # ## PostProcessFloodRisk
    #
    # Het resultaat van de overstromingsberekening kunnen we verder verwerken om het overstromingsrisico van een gebeied te relateren aan het maatgevende vak. Hiermee wordt het inzichtelijk welk vak kritiek is voor een bepaald gevolg.  We selecteren eerst het traject met de meeste bijdragen aan het risio van een gebied.
    # Omdat we risico het risico niet per vak berekenen, maar voor een traject, gebruiken we de faalkans van een vak. Hierbij worden de faalkansen van de verschillende faalmechanismes onafhankelijk gecombineerd naar een totale vak kans. Het vak met de hoogste faalkans wordt gekoppeld aan het gebied, deze kan dan eenvoudig getoond worden in een een viewer.
    #
    # In een grafiek ziet dit er ongeveer zo uit:
    # ```mermaid
    # graph TD;
    #     A[Risico per gebied]-->B[Dijktraject deel 1];
    #     A --> C[Dijktraject deel 2];
    #     A --> D[Dijktraject deel 3: maatgevend];
    #     D --> E[Dijkvak 1];
    #     D --> F[Dijkvak 2];
    #     D --> G[Dijkvak 3: maatgevend];
    #     G <--> |Koppeling gebied-dijkvak|A;
    # ```
    # Dit is bijna het zelfde process als in [CalculateFloodScenarioProbability](#sec-CalculateFloodScenarioProbability), maar dan vanuit risico terug naar de kans.
    #
    # <details>
    # <summary>Configuratie</summary>
    #
    # ```yaml
    # GlobalVariables:
    #     rootdir: "data_sets/7.flood_scenarios"
    #     moments: [ -24, 0, 24, 48 ]
    #     PostProcessFloodRisk:
    #         risk_metric_columns:
    #           - casualties
    #           - damage
    #           - flooding
    #           - affected_people
    #           - waterdepth
    #
    # DataAdapter:
    #     default_options:
    #         csv:
    #             sep: ","
    #             index: False
    #     section_id_to_segment_id:
    #         type: csv
    #         path: "rekentest_section_to_segment.csv"
    #     combined_failure_probability_data:
    #         type: csv
    #         path: "rekentest_probabilities_failuremechanisme_sections.csv"
    #     scenario_failure_prob_segments:
    #         type: csv
    #         file: "rekentest_scenario_failure_prob_segments.csv"
    #     flood_risk_results_per_segment:
    #         type: csv
    #         path: "rekentest_flood_risk_results_per_segment.csv"
    #     areas_to_determining_sections:
    #         type: csv
    #         path: "hidden_rekentest_areas_to_determining_sections_min.csv"
    # ```
    #
    # </details>

    # %%
    from toolbox_continu_inzicht.flood_scenarios import PostProcessFloodRisk

    config = Config(config_path=path / "rekentest_postprocess_flood_risk.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    postprocess_flood_risk = PostProcessFloodRisk(data_adapter=data_adapter)
    postprocess_flood_risk.run(
        input=[
            "combined_failure_probability_data",
            "section_id_to_segment_id",
            "flood_risk_results_per_segment",
        ],
        output="areas_to_determining_sections",
    )

    # %%
    df_out = postprocess_flood_risk.gdf_out_areas_to_determining_sections
    # maak de tabel beter leesbaar door alleen de relevante kolommen te tonen
    df_out[
        [
            "area_id",
            "segment_id_casualties",
            "section_id_casualties",
            "segment_id_damage",
            "section_id_damage",
            "segment_id_flooding",
            "section_id_flooding",
        ]
    ]

    # %%
    # Heel snel voorbeeld van hoe dit in een viewer kan worden getoond
    # in een uitgewerkte versie zou het ook het oplichten van het vak inplaats van de 'Mouse over' hier.
    postprocess_flood_risk.make_map(risk_metric="waterdepth")


if __name__ == "__main__":
    main()
