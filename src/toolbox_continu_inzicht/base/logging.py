import logging.handlers
from pathlib import Path
from pydantic import BaseModel as PydanticBaseModel


class Logger(PydanticBaseModel):
    """logging voor Toolbox CI

    Wordt aangemaakt bij het aanmaken van de DataAdapter

    Grove richtlijnen:
    - DEBUG: voor het development team
    - INFO: voor de technische gebruiker hoofdlijnen wat de functie doet
    - WARNING: Er klopt iets niet, maar de functie kan door
    - ERROR: Er klopt iets niet, en de functie kan niet door
    """

    @staticmethod
    def set_up_logging_to_stream(name: str, level: int | str):
        logger = logging.getLogger(name)
        logger.setLevel(level)
        if not logger.hasHandlers():
            log_stream_handler = logging.StreamHandler()
            log_stream_handler.setFormatter(Logger.get_formatter())
            logger.addHandler(log_stream_handler)
        return logger

    @staticmethod
    def set_up_logging_to_file(
        name: str,
        logfile: Path,
        loghistoryfile: Path | None,
        level: int | str,
        mode: str,
        maxBytes: int,
    ):
        logger = Logger.set_up_single_file_logging(name, logfile, level, mode=mode)

        no_handler = logging.handlers.RotatingFileHandler not in [
            type(handler) for handler in logger.handlers
        ]
        if loghistoryfile is not None and no_handler:
            history_log_handler = logging.handlers.RotatingFileHandler(
                loghistoryfile, maxBytes=maxBytes, backupCount=5
            )
            history_log_handler.setFormatter(Logger.get_formatter())
            logger.addHandler(history_log_handler)

        return logger

    @staticmethod
    def set_up_single_file_logging(
        name: str, logfile: Path, level: int | str, mode="w"
    ):
        logger = logging.getLogger(name)
        logger.setLevel(level)

        if logging.FileHandler not in [type(handler) for handler in logger.handlers]:
            current_log_handler = logging.FileHandler(filename=logfile, mode=mode)
            current_log_handler.setFormatter(Logger.get_formatter())
            logger.addHandler(current_log_handler)
        return logger

    @staticmethod
    def get_handler(logfile: Path, mode: str = "w"):
        current_log_handler = logging.FileHandler(filename=logfile, mode=mode)
        current_log_handler.setFormatter(Logger.get_formatter())
        return current_log_handler

    @staticmethod
    def get_formatter():
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        fmt = r"%(asctime)s %(levelname)s - %(module)s: %(message)s"
        dfmt = r"%Y-%m-%d %H:%M:%S"
        return logging.Formatter(fmt, datefmt=dfmt)
