from pathlib import Path

import matplotlib.pyplot as plt

from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.flood_scenarios import CalculateFloodScenarioProbability
from toolbox_continu_inzicht.flood_scenarios import LoadFromFloodScenarioProbability
from toolbox_continu_inzicht.flood_scenarios import SelectFloodScenarioFromLoad

# importeer en initialiseer de module voor het berekenen van het overstromingsrisico in 4 verschillende formaten
from toolbox_continu_inzicht.flood_scenarios.calculate_flood_risk import (
    CalculateFloodRisk,
)
from toolbox_continu_inzicht.helpers import calculation_start


def main():
    """Ensure your cwd is tests/examples/demos/flood_scenarios, when debugging add to launch.json:
    "cwd": "${workspaceFolder}/tests/examples/demos/flood_scenarios" """

    # laadt de configuratie en initialiseer de data adapter
    path = Path(__file__).parent / "data_sets"
    config = Config(config_path=path / "demo_calculate_flood_scenario_probability.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    start, end = calculation_start(
        data_adapter=data_adapter, output="calculation_start_config", calc_time=None
    )
    print(f"Start: {start}, End: {end}")

    # # druk een overzicht van de faalmechanismen af
    # data_adapter.input("input_failuremechanism")

    # # druk een overzicht van de secties per segment af
    # data_adapter.input("input_sections_in_segment")

    # # druk een overzicht van de faalkansen per sectie en faalmechanisme af
    # data_adapter.input("input_probabilities_failuremechanisme_sections")

    # maak een instantie van de klasse CalculateFloodScenarioProbability
    calculate_flood_scenario_probability = CalculateFloodScenarioProbability(
        data_adapter=data_adapter
    )

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

    # TODO: output doorzetten

    # druk een overzicht van de scenariokansen faalkansen per segment en over alle faalmechanismen af
    calculate_flood_scenario_probability.df_out_scenario_failure_prob_segments

    # druk een overzicht van de gecombineerde faalkansen over alle secties en over alle faalmechanismen af
    calculate_flood_scenario_probability.df_out_combined_failure_prob_all_sections

    # Dit is specifiek voor een moment, over meerdere momenten loopen moet op een hoger viveau.

    # laadt de configuratie en initialiseer de data adapter
    config = Config(config_path=path / "demo_load_from_flood_scenario_probability.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # druk een overzicht van de scenariokansen per segment af
    data_adapter.input("input_scenario_failure_prob_segments")

    # druk een overzicht van de mapping van sectie naar segment af
    data_adapter.input("input_section_to_segment")

    # druk een overzicht van de fragiliteitscurves per faalmechanisme af
    data_adapter.input("input_section_fragility_curves")

    # importeer en initialiseer de module voor het bepalen van de belastingen vanuit scenariokansen

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

    # druk een overzicht van de belastingen overeenkomstig de scenariokansen (per segment) af
    load_from_flood_scenario_probability.df_out_scenario_loads

    # laadt de configuratie en initialiseer de data adapter
    config = Config(config_path=path / "rekentest_select_flood_scenario_from_load.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # druk een overzicht van de belastingen overeenkomstig de scenariokansen (per segment) af
    data_adapter.input("input_scenarios_loads")

    # druk een overzicht van de gevolgen voor verschillende hydraulische belastingniveau's af
    data_adapter.input("input_consequences_loads")

    # importeer en initialiseer de module voor het bepalen van de bijbehorende scenario's vanuit belastingen

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

    # druk een overzicht van de bijbehorende gevolgen per scenario af
    select_flood_scenario_from_load.df_out_scenario_consequences_grids

    # laadt de configuratie en initialiseer de data adapter
    config = Config(
        config_path=path
        / "rekentest_select_flood_scenario_from_load_two_scenarios.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # druk een overzicht van de belastingen overeenkomstig de scenariokansen (per segment) af
    data_adapter.input("input_scenarios_loads")

    # druk een overzicht van de gevolgen voor verschillende hydraulische belastingniveau's af
    data_adapter.input("input_consequences_loads")

    # importeer en initialiseer de module voor het bepalen van de bijbehorende scenario's vanuit belastingen

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

    # druk een overzicht van de bijbehorende gevolgen per scenario af
    # TODO verwijder de index kolom in de CSV output
    select_flood_scenario_from_load.df_out_scenario_consequences_grids

    # laadt de configuratie en initialiseer de data adapter
    config = Config(config_path=path / "rekentest_calculate_flood_risk.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    # druk een overzicht van de scenariokansen per segment af
    data_adapter.input("input_scenario_failure_prob_segments")

    # druk een overzicht van de bijbehorende gevolgen per scenario af
    data_adapter.input("input_scenario_consequences_grids")

    # druk de gebieden af waarvoor het overstromingsrisico wordt geaggregeerd
    data_adapter.input("input_areas_to_aggregate")

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

    data_adapter.input("flood_risk_local_file")

    calculate_flood_risk.df_out_flood_risk_results.head(5)

    gdf_results = calculate_flood_risk.df_out_flood_risk_results

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
        plt.colorbar(ax[index].collections[0], ax=ax[index], orientation="horizontal")
        ax[index].set_title(rename[column])
    plt.tight_layout()

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


if __name__ == "__main__":
    main()
