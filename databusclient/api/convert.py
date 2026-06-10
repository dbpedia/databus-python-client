from databusclient.filehandling.format import convert_file, get_converted_filename
from databusclient.filehandling import mapping as _mapping

from databusclient.filehandling.format import (  # noqa: F401
    ALL_FORMATS,
    EXTENSION_TO_FORMAT,
    FORMAT_TO_EXTENSION,
    RDF_QUAD_FORMATS,
    RDF_TRIPLE_FORMATS,
    TABULAR_FORMATS,
    QuadHandler,
    TSDHandler,
    TripleHandler,
    _quad_handler,
    _tsd_handler,
    _triple_handler,
    detect_format_from_filename,
    get_format_class,
)

__all__ = ["convert_file", "get_converted_filename"]

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