import functools
from abc import ABC, abstractmethod

from pydantic.dataclasses import dataclass

from toolbox_continu_inzicht.base.data_adapter import DataAdapter


@dataclass(config={"arbitrary_types_allowed": True})
class ToolboxBase(ABC):
    data_adapter: DataAdapter  # Een DataAdapter is verplicht voor een subclass

    def __init_subclass__(cls, *args, **kwargs):
        super().__init_subclass__(**kwargs)
        # Voeg het loggen van exceptions altijd toe aan de run methode
        if hasattr(cls, "run"):
            cls.run = ToolboxBase.log_exceptions(getattr(cls, "run"))

    @staticmethod
    def log_exceptions(method):
        """Stuurt exceptions eerst naar de logger van de DataAdapter"""

        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            logger = None
            try:
                if hasattr(args[0], "data_adapter"):
                    logger = args[0].data_adapter.logger
                return method(*args, **kwargs)
            except Exception as e:
                if logger is not None:
                    logger.error(f"{type(args[0]).__name__} - {e}", exc_info=True)
                raise

        return wrapper

    @abstractmethod
    def run(self):
        """De run methode moet altijd gedefinieerd worden in een subclass"""
        pass
