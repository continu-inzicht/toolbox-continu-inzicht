import logging
import logging.handlers
from pathlib import Path

from toolbox_continu_inzicht import Config, DataAdapter


def test_setup_logging():
    """Check of logging wordt aangemaakt"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(config_path=test_data_sets_path / "test_config.yaml")
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    assert data_adapter.logger is not None
    assert data_adapter.logger.name == "toolbox_continu_inzicht"
    assert data_adapter.logger.level == logging.WARNING
    assert data_adapter.logger.hasHandlers()


def test_setup_logging_file():
    """Check of logging file wordt aangemaakt met juiste instellingen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_config_advanced_logging.yaml"
    )
    config.lees_config()
    data_adapter = DataAdapter(config=config)
    assert data_adapter.logger is not None
    assert data_adapter.logger.name == "toolbox_continu_inzicht"
    assert data_adapter.logger.level == logging.DEBUG
    assert data_adapter.logger.hasHandlers()
    assert isinstance(data_adapter.logger.handlers[0], logging.FileHandler)
    assert data_adapter.logger.handlers[0].baseFilename == str(
        test_data_sets_path / "hidden_logfile_advanced.log"
    )


def test_setup_logging_file_history():
    """Check of logging file wordt aangemaakt met juiste instellingen"""
    test_data_sets_path = Path(__file__).parent / "data_sets"
    config = Config(
        config_path=test_data_sets_path / "test_config_advanced_logging.yaml"
    )
    config.lees_config()
    config.global_variables["logging"]["name"] = "toolbox_continu_inzicht_2"
    config.global_variables["logging"]["level"] = "ERROR"
    config.global_variables["logging"]["history_file"] = (
        "hidden_logfile_history_advanced.log"
    )
    data_adapter = DataAdapter(config=config)

    assert data_adapter.logger is not None
    assert data_adapter.logger.name == "toolbox_continu_inzicht_2"
    assert data_adapter.logger.level == logging.ERROR
    assert data_adapter.logger.hasHandlers()
    assert isinstance(data_adapter.logger.handlers[0], logging.FileHandler)
    assert data_adapter.logger.handlers[0].baseFilename == str(
        test_data_sets_path / "hidden_logfile_advanced.log"
    )
    assert data_adapter.logger.handlers[1].baseFilename == str(
        test_data_sets_path / "hidden_logfile_history_advanced.log"
    )
