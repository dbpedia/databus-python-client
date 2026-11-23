"""Version helpers for databusclient."""

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
import tomllib


# Source checkouts do not always have current package metadata installed, so
# prefer pyproject.toml locally and fall back to installed metadata for wheels.
def get_version() -> str:
    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    if pyproject.exists():
        with pyproject.open("rb") as f:
            return tomllib.load(f)["tool"]["poetry"]["version"]

    try:
        return version("databusclient")
    except PackageNotFoundError:
        return "0.0.0"


__version__ = get_version()
