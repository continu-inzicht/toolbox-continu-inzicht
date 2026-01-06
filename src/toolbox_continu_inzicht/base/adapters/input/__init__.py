import warnings
from toolbox_continu_inzicht.base.adapters.input.csv import *  # noqa: F403
from toolbox_continu_inzicht.base.adapters.input.python import *  # noqa: F403

# t
try:
    from toolbox_continu_inzicht.base.adapters.input.netcdf import *  # noqa: F403
except ImportError as e:
    warnings.warn(f"{e}.\n Some features may not be available.")

try:
    from toolbox_continu_inzicht.base.adapters.input.continu_inzicht_postgresql import *  # noqa: F403
    from toolbox_continu_inzicht.base.adapters.input.postgresql import *  # noqa: F403
    from toolbox_continu_inzicht.base.adapters.input.shape import *  # noqa: F403
except ImportError as e:
    warnings.warn(f"{e}.\n Some features may not be available.")

try:
    from toolbox_continu_inzicht.base.adapters.input.excel import *  # noqa: F403
except ImportError as e:
    warnings.warn(f"{e}.\n Some features may not be available.")

try:
    from toolbox_continu_inzicht.base.adapters.input.flood_risk import *  # noqa: F403
except ImportError as e:
    warnings.warn(f"{e}.\n Some features may not be available.")

try:
    from toolbox_continu_inzicht.base.adapters.input.pre_calculated import *  # noqa: F403
except ImportError as e:
    warnings.warn(f"{e}.\n Some features may not be available.")
