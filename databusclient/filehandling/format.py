"""Format and Mapping Conversion Layer.

This module implements the format conversion pipeline for the Databus Python Client

Layer 2: Within-class format conversion (lossless).
    - TripleHandler: RDF triple formats (turtle, ntriples, rdf-xml)
    - QuadHandler:   RDF quad formats (nquads, trig, trix, json-ld)
    - TSDHandler:    Tabular formats (csv, tsv)

Each handler provides read() -> IR, write(IR) -> file, convert() -> chains both.
The IR (intermediate representation) returned by read() is designed to be passed
to future mapping classes (TripleToQuadMapper, TripleToTSDMapper, etc.).
"""

import csv
import os
import shutil
import warnings
from typing import Optional

from rdflib import Dataset, Graph

# Suppress rdflib internal DeprecationWarning for Dataset API.
# rdflib is mid-migration from ConjunctiveGraph to Dataset in 7.x.
# These warnings originate from rdflib internals, not our code.
# Can be removed when rdflib completes their Dataset API migration.
warnings.filterwarnings("ignore", category=DeprecationWarning, module="rdflib")
warnings.filterwarnings("ignore", category=UserWarning, module="rdflib")


# ---------------------------------------------------------------------------
# Format registries
# ---------------------------------------------------------------------------

# Maps CLI format name -> rdflib format string
RDF_TRIPLE_FORMATS = {
    "ntriples": "ntriples",
    "turtle": "turtle",
    "rdf-xml": "xml",
}

RDF_QUAD_FORMATS = {
    "nquads": "nquads",
    "trig": "trig",
    "trix": "trix",
    "json-ld": "json-ld",
}

TABULAR_FORMATS = {
    "csv": ",",
    "tsv": "\t",
}

ALL_FORMATS = (
    list(RDF_TRIPLE_FORMATS)
    + list(RDF_QUAD_FORMATS)
    + list(TABULAR_FORMATS)
)

# Maps short CLI aliases -> canonical format name
FORMAT_ALIASES = {
    "nt": "ntriples",
    "ttl": "turtle",
    "rdf": "rdf-xml",
    "xml": "rdf-xml",
    "nq": "nquads",
    "jsonld": "json-ld",
}

def normalize_format(fmt: str) -> str:
    """Normalize a format name or alias to its canonical form.

    Accepts both full names (e.g. 'ntriples') and short aliases (e.g. 'nt').
    Canonical names pass through unchanged. Unknown values raise ValueError.

    Args:
        fmt: Format name or alias string (case-insensitive).

    Returns:
        Canonical format name string.

    Raises:
        ValueError: If fmt is not a recognised format name or alias.
    """
    fmt_lower = fmt.lower()
    # Resolve alias first
    canonical = FORMAT_ALIASES.get(fmt_lower, fmt_lower)
    if canonical not in ALL_FORMATS:
        raise ValueError(
            f"Unknown format: '{fmt}'. "
            f"Supported formats: {ALL_FORMATS}. "
            f"Supported aliases: {list(FORMAT_ALIASES.keys())}"
        )
    return canonical

# Maps file extension -> CLI format name
EXTENSION_TO_FORMAT = {
    ".ttl": "turtle",
    ".nt": "ntriples",
    ".rdf": "rdf-xml",
    ".xml": "rdf-xml",
    ".owl": "rdf-xml",
    ".nq": "nquads",
    ".trig": "trig",
    ".trix": "trix",
    ".jsonld": "json-ld",
    ".json": "json-ld",
    ".csv": "csv",
    ".tsv": "tsv",
}

# Maps format name -> file extension
FORMAT_TO_EXTENSION = {
    "ntriples": ".nt",
    "turtle": ".ttl",
    "rdf-xml": ".rdf",
    "nquads": ".nq",
    "trig": ".trig",
    "trix": ".trix",
    "json-ld": ".jsonld",
    "csv": ".csv",
    "tsv": ".tsv",
}


# ---------------------------------------------------------------------------
# Format detection helpers
# ---------------------------------------------------------------------------

def detect_format_from_filename(filename: str) -> Optional[str]:
    """Detect format from file extension, ignoring compression extensions.

    Args:
        filename: File name or path.

    Returns:
        Format name string or None if not detectable.
    """
    name = filename.lower()

    # strip compression extension first
    for ext in (".bz2", ".gz", ".xz"):
        if name.endswith(ext):
            name = name[: -len(ext)]
            break

    # match longest extension first to avoid .json matching before .jsonld
    for ext in sorted(EXTENSION_TO_FORMAT.keys(), key=len, reverse=True):
        if name.endswith(ext):
            return EXTENSION_TO_FORMAT[ext]

    return None


def get_format_class(fmt: str) -> str:
    """Return equivalence class for a format name.

    Args:
        fmt: Format name (e.g. 'turtle', 'nquads', 'csv').

    Returns:
        'triples', 'quads', or 'tabular'.

    Raises:
        ValueError: If format is not recognised.
    """
    if fmt in RDF_TRIPLE_FORMATS:
        return "triples"
    if fmt in RDF_QUAD_FORMATS:
        return "quads"
    if fmt in TABULAR_FORMATS:
        return "tabular"
    raise ValueError(
        f"Unknown format: '{fmt}'. Supported formats: {ALL_FORMATS}"
    )


def get_converted_filename(original_filename: str, convert_format: str) -> str:
    """Generate output filename after format conversion.

    Strips compression extension if present, then replaces the format
    extension with the target format extension. Accepts format aliases.

    Args:
        original_filename: Original file name (basename only, not full path).
        convert_format: Target format name or alias.

    Returns:
        New filename with updated extension.
    """
    # Normalize alias to canonical name
    convert_format = normalize_format(convert_format)

    name = original_filename

    # strip compression extension
    for ext in (".bz2", ".gz", ".xz"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break

    # strip existing format extension (longest first)
    for old_ext in sorted(FORMAT_TO_EXTENSION.values(), key=len, reverse=True):
        if name.lower().endswith(old_ext):
            name = name[: -len(old_ext)]
            break

    target_ext = FORMAT_TO_EXTENSION.get(convert_format, f".{convert_format}")
    return name + target_ext


# ---------------------------------------------------------------------------
# Layer 2 Handlers
# ---------------------------------------------------------------------------

class TripleHandler:
    """Handler for RDF triple formats (Layer 2).

    Uses rdflib.Graph as the intermediate representation (IR).
    Supports: ntriples, turtle, rdf-xml.

    The IR returned by read() can be passed to future mapping classes
    such as TripleToQuadMapper or TripleToTSDMapper for Layer 3 conversions.
    """

    def read(self, source: str, input_format: str) -> Graph:
        """Parse an RDF triples file into a Graph (IR).

        Args:
            source: Path to input file.
            input_format: Source format name (e.g. 'turtle', 'ntriples', 'rdf-xml').

        Returns:
            rdflib.Graph containing all parsed triples.

        Raises:
            ValueError: If input_format is not a recognised triple format.
        """
        if input_format not in RDF_TRIPLE_FORMATS:
            raise ValueError(
                f"'{input_format}' is not a triple format. "
                f"Supported: {list(RDF_TRIPLE_FORMATS)}"
            )
        g = Graph()
        g.parse(source, format=RDF_TRIPLE_FORMATS[input_format])
        return g

    def write(self, data: Graph, target: str, output_format: str) -> None:
        """Serialize a Graph (IR) to a file.

        Args:
            data: rdflib.Graph to serialize.
            target: Path to output file.
            output_format: Target format name (e.g. 'ntriples', 'turtle').

        Raises:
            ValueError: If output_format is not a recognised triple format.
        """
        if output_format not in RDF_TRIPLE_FORMATS:
            raise ValueError(
                f"'{output_format}' is not a triple format. "
                f"Supported: {list(RDF_TRIPLE_FORMATS)}"
            )
        parent = os.path.dirname(target)
        if parent:
            os.makedirs(parent, exist_ok=True)
        # Explicitly specify utf-8 encoding to avoid NTSerializer warning
        data.serialize(
            destination=target,
            format=RDF_TRIPLE_FORMATS[output_format],
            encoding="utf-8",
        )

    def convert(
        self,
        source: str,
        target: str,
        input_format: str,
        output_format: str,
    ) -> None:
        """Convert between RDF triple formats (Layer 2, lossless).

        Chains read() -> write(). Both formats must be in the same
        equivalence class (RDF triples).

        Args:
            source: Path to input file.
            target: Path to output file.
            input_format: Source format name.
            output_format: Target format name.
        """
        graph = self.read(source, input_format)
        self.write(graph, target, output_format)
        print(
            f"Converted {input_format} -> {output_format}: "
            f"{os.path.basename(target)}"
        )


class QuadHandler:
    """Handler for RDF quad formats (Layer 2).

    Uses rdflib.Dataset as the intermediate representation (IR).
    Supports: nquads, trig, trix, json-ld.

    Named graph information is preserved through the Dataset IR.
    The IR returned by read() can be passed to future mapping classes
    such as QuadToTripleMapper or QuadToTSDMapper for Layer 3 conversions.
    """

    def read(self, source: str, input_format: str) -> Dataset:
        """Parse an RDF quads file into a Dataset (IR).

        Args:
            source: Path to input file.
            input_format: Source format name (e.g. 'nquads', 'trig', 'trix', 'json-ld').

        Returns:
            rdflib.Dataset containing all parsed quads with named graphs.

        Raises:
            ValueError: If input_format is not a recognised quad format.
        """
        if input_format not in RDF_QUAD_FORMATS:
            raise ValueError(
                f"'{input_format}' is not a quad format. "
                f"Supported: {list(RDF_QUAD_FORMATS)}"
            )
        d = Dataset()
        d.parse(source, format=RDF_QUAD_FORMATS[input_format])
        return d

    def write(self, data: Dataset, target: str, output_format: str) -> None:
        """Serialize a Dataset (IR) to a file.

        Args:
            data: rdflib.Dataset to serialize.
            target: Path to output file.
            output_format: Target format name.

        Raises:
            ValueError: If output_format is not a recognised quad format.
        """
        if output_format not in RDF_QUAD_FORMATS:
            raise ValueError(
                f"'{output_format}' is not a quad format. "
                f"Supported: {list(RDF_QUAD_FORMATS)}"
            )
        parent = os.path.dirname(target)
        if parent:
            os.makedirs(parent, exist_ok=True)
        data.serialize(
            destination=target,
            format=RDF_QUAD_FORMATS[output_format],
        )

    def convert(
        self,
        source: str,
        target: str,
        input_format: str,
        output_format: str,
    ) -> None:
        """Convert between RDF quad formats (Layer 2, lossless).

        Chains read() -> write(). Both formats must be in the same
        equivalence class (RDF quads). Named graph information is preserved.

        Args:
            source: Path to input file.
            target: Path to output file.
            input_format: Source format name.
            output_format: Target format name.
        """
        dataset = self.read(source, input_format)
        self.write(dataset, target, output_format)
        print(
            f"Converted {input_format} -> {output_format}: "
            f"{os.path.basename(target)}"
        )


class TSDHandler:
    """Handler for tabular structured data formats (Layer 2).

    Uses list[list[str]] as the intermediate representation (IR).
    Supports: csv, tsv.

    The IR returned by read() can be passed to future mapping classes
    such as TSDToTripleMapper for Layer 3 conversions.
    """

    def read(self, source: str, input_format: str) -> list:
        """Parse a tabular file into a list of rows (IR).

        Each row is a list of string values. First row is the header.

        Args:
            source: Path to input file.
            input_format: Source format name ('csv' or 'tsv').

        Returns:
            list[list[str]] where first element is the header row.

        Raises:
            ValueError: If input_format is not a recognised tabular format.
        """
        if input_format not in TABULAR_FORMATS:
            raise ValueError(
                f"'{input_format}' is not a tabular format. "
                f"Supported: {list(TABULAR_FORMATS)}"
            )
        delimiter = TABULAR_FORMATS[input_format]
        with open(source, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=delimiter)
            return list(reader)

    def write(self, data: list, target: str, output_format: str) -> None:
        """Serialize a list of rows (IR) to a tabular file.

        Args:
            data: list[list[str]] to write.
            target: Path to output file.
            output_format: Target format name ('csv' or 'tsv').

        Raises:
            ValueError: If output_format is not a recognised tabular format.
        """
        if output_format not in TABULAR_FORMATS:
            raise ValueError(
                f"'{output_format}' is not a tabular format. "
                f"Supported: {list(TABULAR_FORMATS)}"
            )
        parent = os.path.dirname(target)
        if parent:
            os.makedirs(parent, exist_ok=True)
        delimiter = TABULAR_FORMATS[output_format]
        with open(target, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerows(data)

    def convert(
        self,
        source: str,
        target: str,
        input_format: str,
        output_format: str,
    ) -> None:
        """Convert between tabular formats (Layer 2, lossless).

        Chains read() -> write(). Both formats must be in the same
        equivalence class (tabular).

        Args:
            source: Path to input file.
            target: Path to output file.
            input_format: Source format name.
            output_format: Target format name.
        """
        rows = self.read(source, input_format)
        self.write(rows, target, output_format)
        print(
            f"Converted {input_format} -> {output_format}: "
            f"{os.path.basename(target)}"
        )


# ---------------------------------------------------------------------------
# Main dispatcher — called from download pipeline
# ---------------------------------------------------------------------------

# Handler instances — created once, reused
_triple_handler = TripleHandler()
_quad_handler = QuadHandler()
_tsd_handler = TSDHandler()


def convert_file(
    input_file: str,
    output_file: str,
    convert_format: str,
    graph_name: str = None,
    base_uri: str = None,
) -> None:
    """Main conversion dispatcher called from the download pipeline.

    Detects the input format from the file extension, determines whether
    this is a Layer 2 (within-class) or Layer 3 (cross-class) conversion,
    and delegates to the appropriate handler.

    Accepts both canonical format names and short aliases (e.g. 'nt' for
    'ntriples', 'ttl' for 'turtle'). See normalize_format() for full list.

    For Layer 3 cross-class conversions:
        - Triple -> Quad requires graph_name (--graph-name <uri>).
        - CSV -> Triple requires base_uri (--base-uri <uri>).
        - Quad -> Triple produces multiple files in a subdirectory; output_file
          is used as the subdirectory path.

    Args:
        input_file: Path to the input file (must be decompressed).
        output_file: Path to write the converted output file.
                     For Quad -> Triple, this is the output subdirectory path.
        convert_format: Target format name or alias (CLI format string).
        graph_name: Named graph URI for Triple -> Quad conversion.
        base_uri: Base URI for CSV -> Triple conversion.

    Raises:
        ValueError: If input format cannot be detected or conversion
                    is not supported.
    """
    # Normalize alias to canonical name before any processing
    convert_format = normalize_format(convert_format)

    input_format = detect_format_from_filename(input_file)

    if input_format is None:
        raise ValueError(
            f"Could not detect input format from filename: "
            f"'{os.path.basename(input_file)}'. "
            f"Supported extensions: {list(EXTENSION_TO_FORMAT.keys())}"
        )

    if input_format == convert_format:
        # Input and target format are identical.
        # Copy input to output path so the caller always receives an output file.
        if input_file != output_file:
            shutil.copy2(input_file, output_file)
            print(
                f"Input and target format are both '{input_format}'. "
                f"Copied to output path: {os.path.basename(output_file)}"
            )
        return

    input_class = get_format_class(input_format)
    output_class = get_format_class(convert_format)

    # --- Layer 2: within-class ---
    if input_class == output_class:
        if input_class == "triples":
            _triple_handler.convert(
                input_file, output_file, input_format, convert_format
            )
        elif input_class == "quads":
            _quad_handler.convert(
                input_file, output_file, input_format, convert_format
            )
        elif input_class == "tabular":
            _tsd_handler.convert(
                input_file, output_file, input_format, convert_format
            )
        return

    # --- Layer 3: cross-class ---
    from databusclient.filehandling import mapping as _mapping

    # Triple -> Quad
    if input_class == "triples" and output_class == "quads":
        _mapping.convert_triples_to_quads(
            input_file, output_file, input_format, convert_format, graph_name
        )
        return

    # Quad -> Triple (output_file used as output subdirectory)
    if input_class == "quads" and output_class == "triples":
        _mapping.convert_quads_to_triples(
            input_file, output_file, input_format, convert_format
        )
        return

    # Triple -> TSD
    if input_class == "triples" and output_class == "tabular":
        _mapping.convert_rdf_to_csv(
            input_file, output_file, input_format, convert_format
        )
        return

    # TSD -> Triple
    if input_class == "tabular" and output_class == "triples":
        _mapping.convert_csv_to_rdf(
            input_file, output_file, input_format, convert_format, base_uri
        )
        return

    # Quad -> TSD
    if input_class == "quads" and output_class == "tabular":
        _mapping.convert_quads_to_csv(
            input_file, output_file, input_format, convert_format
        )
        return

    raise ValueError(
        f"Conversion from '{input_format}' ({input_class}) to "
        f"'{convert_format}' ({output_class}) is not supported."
    )