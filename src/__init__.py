"""Mini-FRED Python package namespace."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("mini-fred")
except PackageNotFoundError:  # pragma: no cover - best effort during dev install
    __version__ = "0.0.0"

__all__ = ["__version__"]
