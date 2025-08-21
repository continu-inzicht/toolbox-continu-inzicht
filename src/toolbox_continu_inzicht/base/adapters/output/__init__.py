import warnings
from toolbox_continu_inzicht.base.adapters.output.csv import *  # noqa: F403
from toolbox_continu_inzicht.base.adapters.output.python import *  # noqa: F403

try:
    from toolbox_continu_inzicht.base.adapters.output.continu_inzicht_postgresql import *  # noqa: F403
    from toolbox_continu_inzicht.base.adapters.output.postgresql import *  # noqa: F403
    from toolbox_continu_inzicht.base.adapters.output.shape import *  # noqa: F403
except ImportError as e:
    warnings.warn(f"{e}.\n Some features may not be available.")

try:
    from toolbox_continu_inzicht.base.adapters.output.netcdf import *  # noqa: F403
except ImportError as e:
    warnings.warn(f"{e}.\n Some features may not be available.")
