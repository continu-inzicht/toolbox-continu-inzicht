from pathlib import Path
from toolbox_continu_inzicht.base.data_adapter import Config, DataAdapter
from toolbox_continu_inzicht.loads import LoadsToMoments
from toolbox_continu_inzicht.loads import LoadsClassify
from toolbox_continu_inzicht.sections import SectionsTechnicalFailureprobability
from toolbox_continu_inzicht.sections.sections_loads.sections_loads import SectionsLoads


def test_quick_start_guide_extra_columns_parameter():
    """Test the quick start guide.
    In de quick start guide notebook worden express de waardes niet doorgegeven, hier wel om afhankelijkheden te testen.
    De modules anzich worden wel getest in de unittests, hier testen we of ze ook samenwerken.


    """
    path = Path(__file__).parent / "data_sets"

    ### Alleen ophalen van RWS waterstanden doen we niet.
    # config = Config(config_path=path / "loads.yaml")
    # config.lees_config()
    # data_adapter = DataAdapter(config=config)

    # waterwebservices_rws = LoadsWaterwebservicesRWS(data_adapter=data_adapter)
    # waterwebservices_rws.run(
    #     input="belasting_locaties_waterwebservices_RWS",
    #     output="waterstanden_waterwebservices",
    # )

    # Loads to moments

    config = Config(config_path=path / "test_loads_to_moments_extra_columns.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    load_moments = LoadsToMoments(data_adapter=data_adapter)
    load_moments.run(
        input="waterstanden_waterwebservices_met_wind",
        output="moments_waterstanden_waterwebservices",
    )

    # Loads classify

    config = Config(config_path=path / "loads_classify.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    loads_classify = LoadsClassify(data_adapter=data_adapter)

    loads_classify.run(
        input=["belasting_locaties_grenzen", "momenten_waterstanden"],
        output="geclassificeerde_waterstanden",
    )

    # Sections Loads

    config = Config(config_path=path / "sections_loads.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)

    sections_loads = SectionsLoads(data_adapter=data_adapter)
    sections_loads.run(
        input=[
            "dijkvak_namen",
            "momenten_waterstanden",
            "koppeling_dijkvak_belastinglocatie",
        ],
        output="waterstanden_per_dijkvak",
    )

    # Fragility Curves

    config = Config(config_path=path / "technical_failure_probability.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    sections_failureprobability = SectionsTechnicalFailureprobability(
        data_adapter=data_adapter
    )

    sections_failureprobability.run(
        input=[
            "technical_failure_probability_fragility_curves",
            "waterstanden_per_dijkvak",
        ],
        output="technical_failure_probability_data",
    )
    sections_failureprobability.df_out
