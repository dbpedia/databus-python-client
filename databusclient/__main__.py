"""Module used for ``python -m databusclient`` execution.

Runs the package's CLI application.
"""

from databusclient import cli


def main():
	"""Invoke the CLI application.

	Kept as a named function for easier testing and clarity.
	"""

	cli.app()


if __name__ == "__main__":
	main()
