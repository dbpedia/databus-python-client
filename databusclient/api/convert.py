from databusclient.filehandling.format import convert_file, get_converted_filename
from databusclient.filehandling import mapping as _mapping

from databusclient.filehandling.format import (
    QuadHandler,
    TSDHandler,
    TripleHandler,
    _quad_handler,
    _tsd_handler,
    _triple_handler,
)

__all__ = [
    "convert_file",
    "get_converted_filename",
    "QuadHandler",
    "TSDHandler",
    "TripleHandler",
]

convert_rdf_to_csv = _mapping.convert_rdf_to_csv


def convert_rdf_triple_format(
    source: str,
    target: str,
    input_format: str,
    output_format: str,
) -> None:
    _triple_handler.convert(source, target, input_format, output_format)


def convert_rdf_quad_format(
    source: str,
    target: str,
    input_format: str,
    output_format: str,
) -> None:
    _quad_handler.convert(source, target, input_format, output_format)


def convert_tabular_format(
    source: str,
    target: str,
    input_format: str,
    output_format: str,
) -> None:
    _tsd_handler.convert(source, target, input_format, output_format)