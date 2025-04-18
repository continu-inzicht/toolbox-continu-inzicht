## Voeg later dynamische setuptools toe
# import pkg_resources  # part of setuptools
# __version__ = pkg_resources.get_distribution("toolbox_continu_inzicht").version
__version__ = "0.1.1"

# Hier alleen base functies
from toolbox_continu_inzicht.base import config
from toolbox_continu_inzicht.base.config import Config
from toolbox_continu_inzicht.base import data_adapter
from toolbox_continu_inzicht.base.base_module import ToolboxBase
from toolbox_continu_inzicht.base.data_adapter import DataAdapter
from toolbox_continu_inzicht.base import fragility_curve
from toolbox_continu_inzicht.base.fragility_curve import FragilityCurve

# hier de hoofd modules, sub modules in de mapjes zelf
from toolbox_continu_inzicht import (
    base,
    loads,
    sections,
    fragility_curves,
    proof_of_concept,
    inspections,
)


__all__ = [
    "__version__",
    "ToolboxBase",
    "config",
    "Config",
    "data_adapter",
    "DataAdapter",
    "fragility_curve",
    "FragilityCurve",
    "base",
    "loads",
    "sections",
    "fragility_curves",
    "proof_of_concept",
    "inspections",
]
