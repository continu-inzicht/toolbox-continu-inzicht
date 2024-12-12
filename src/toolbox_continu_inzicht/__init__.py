__version__ = "0.0.2"

# Hier alleen base functies
from toolbox_continu_inzicht.base import config
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base import data_adapter
from toolbox_continu_inzicht.base.data_adapter import DataAdapter

# hier de hoofd modules, sub modules in de mapjes zelf
from toolbox_continu_inzicht import base
from toolbox_continu_inzicht import loads
from toolbox_continu_inzicht import sections
from toolbox_continu_inzicht import proof_of_concept

__all__ = [
    "config",
    "data_adapter",
    "Config",
    "DataAdapter",
    "loads",
    "sections",
    "proof_of_concept",
    "base",
]
