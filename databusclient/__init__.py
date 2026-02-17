"""Top-level package for the databus Python client.

This module exposes a small set of convenience functions and the CLI
entrypoint so the package can be used as a library or via
``python -m databusclient``.
"""

from databusclient import cli
from databusclient.api.deploy import create_dataset, create_distribution, deploy

__version__ = "0.15"
__all__ = ["create_dataset", "deploy", "create_distribution"]


def run():
    """Start the Click CLI application.

    This function is used by the ``__main__`` module and the package
    entrypoint to invoke the command line interface.
    """

    cli.app()
