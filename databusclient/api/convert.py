"""Format and Mapping Conversion Layer.

Layer 2: Within-class format conversion (lossless).
Layer 3: Cross-class mapping conversion (quasi-equal for RDF <-> Tabular).
"""

import csv
import json
import os
from typing import Optional

from rdflib import Dataset, Graph


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


# ---------------------------------------------------------------------------
# Format detection
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


# ---------------------------------------------------------------------------
# Output filename helper
# ---------------------------------------------------------------------------

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


def get_converted_filename(original_filename: str, convert_format: str) -> str:
    """Generate output filename after format conversion.

    Strips compression extension if present, then replaces the format
    extension with the target format extension.

    Args:
        original_filename: Original file name (basename only, not full path).
        convert_format: Target format name.

    Returns:
        New filename with updated extension.
    """
    name = original_filename

    # strip compression extension
    for ext in (".bz2", ".gz", ".xz"):
        if name.lower().endswith(ext):
            name = name[: -len(ext)]
            break

    # strip existing format extension
    for old_ext in sorted(FORMAT_TO_EXTENSION.values(), key=len, reverse=True):
        if name.lower().endswith(old_ext):
            name = name[: -len(old_ext)]
            break

    target_ext = FORMAT_TO_EXTENSION.get(convert_format, f".{convert_format}")
    return name + target_ext


# ---------------------------------------------------------------------------
# Layer 2 — within-class format conversion
# ---------------------------------------------------------------------------

def convert_rdf_triple_format(
    input_file: str,
    output_file: str,
    input_format: str,
    output_format: str,
) -> None:
    """Convert between RDF triple serialization formats (Layer 2).

    Handles: ntriples, turtle, rdf-xml.
    Uses rdflib Graph as internal representation.

    Args:
        input_file: Path to input file.
        output_file: Path to write converted output.
        input_format: Source format name (must be in RDF_TRIPLE_FORMATS).
        output_format: Target format name (must be in RDF_TRIPLE_FORMATS).
    """
    g = Graph()
    g.parse(input_file, format=RDF_TRIPLE_FORMATS[input_format])
    g.serialize(destination=output_file, format=RDF_TRIPLE_FORMATS[output_format])
    print(
        f"Converted {input_format} -> {output_format}: {os.path.basename(output_file)}"
    )


def convert_rdf_quad_format(
    input_file: str,
    output_file: str,
    input_format: str,
    output_format: str,
) -> None:
    """Convert between RDF quad serialization formats (Layer 2).

    Handles: nquads, trig, trix, json-ld.
    Uses rdflib Dataset as internal representation
    to preserve named graph information.

    Args:
        input_file: Path to input file.
        output_file: Path to write converted output.
        input_format: Source format name (must be in RDF_QUAD_FORMATS).
        output_format: Target format name (must be in RDF_QUAD_FORMATS).
    """
    g = Dataset()
    g.parse(input_file, format=RDF_QUAD_FORMATS[input_format])
    g.serialize(destination=output_file, format=RDF_QUAD_FORMATS[output_format])
    print(
        f"Converted {input_format} -> {output_format}: {os.path.basename(output_file)}"
    )


def convert_tabular_format(
    input_file: str,
    output_file: str,
    input_format: str,
    output_format: str,
) -> None:
    """Convert between tabular formats (Layer 2).

    Handles: csv <-> tsv.
    Uses Python built-in csv module.

    Args:
        input_file: Path to input file.
        output_file: Path to write converted output.
        input_format: Source format name ('csv' or 'tsv').
        output_format: Target format name ('csv' or 'tsv').
    """
    input_delimiter = TABULAR_FORMATS[input_format]
    output_delimiter = TABULAR_FORMATS[output_format]

    with open(input_file, "r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile, delimiter=input_delimiter)
        rows = list(reader)

    with open(output_file, "w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile, delimiter=output_delimiter)
        writer.writerows(rows)

    print(
        f"Converted {input_format} -> {output_format}: {os.path.basename(output_file)}"
    )


# ---------------------------------------------------------------------------
# Layer 3 — cross-class mapping conversion
# ---------------------------------------------------------------------------

def convert_rdf_to_csv(
    input_file: str,
    output_file: str,
    input_format: str,
) -> None:
    """Map RDF triples to a wide CSV table (Layer 3).

    Each unique subject becomes a row. Each unique predicate becomes a column.
    Multi-valued predicates are pipe-separated.
    A companion .meta.json file is generated alongside the CSV to preserve
    RDF datatype and language tag information for lossless round trips.

    Args:
        input_file: Path to input RDF triples file.
        output_file: Path to write output CSV file.
        input_format: Source triple format name (must be in RDF_TRIPLE_FORMATS).
    """
    g = Graph()
    g.parse(input_file, format=RDF_TRIPLE_FORMATS[input_format])

    predicates = sorted(set(str(p) for s, p, o in g))

    subjects: dict = {}
    column_metadata: dict = {}

    for s, p, o in g:
        subj = str(s)
        pred = str(p)

        # capture datatype or language tag for companion file
        if hasattr(o, "datatype") and o.datatype:
            column_metadata[pred] = {"datatype": str(o.datatype)}
        elif hasattr(o, "language") and o.language:
            column_metadata[pred] = {"language": str(o.language)}

        if subj not in subjects:
            subjects[subj] = {}
        if pred not in subjects[subj]:
            subjects[subj][pred] = []
        subjects[subj][pred].append(str(o))

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["resource"] + predicates)
        for subj, pred_map in subjects.items():
            row = [subj]
            for pred in predicates:
                values = pred_map.get(pred, [])
                row.append("|".join(values))
            writer.writerow(row)

    companion_file = output_file + ".meta.json"
    with open(companion_file, "w", encoding="utf-8") as f:
        json.dump({"columns": column_metadata}, f, indent=2)

    print(f"Converted RDF -> CSV: {os.path.basename(output_file)}")
    print(f"Companion metadata: {os.path.basename(companion_file)}")


# ---------------------------------------------------------------------------
# Main dispatcher — called from download pipeline
# ---------------------------------------------------------------------------

def convert_file(
    input_file: str,
    output_file: str,
    convert_format: str,
) -> None:
    """Main conversion dispatcher called from the download pipeline.

    Detects the input format from the file extension, determines whether
    this is a Layer 2 (within-class) or Layer 3 (cross-class) conversion,
    and delegates to the appropriate conversion function.

    For Layer 2: lossless, same equivalence class.
    For Layer 3: quasi-equal for RDF <-> Tabular, lossless for Triples <-> Quads.

    Args:
        input_file: Path to the input file (must be decompressed).
        output_file: Path to write the converted output file.
        convert_format: Target format name (CLI format string).

    Raises:
        ValueError: If the input format cannot be detected or if the
                    requested conversion is not supported.
    """
    input_format = detect_format_from_filename(input_file)

    if input_format is None:
        raise ValueError(
            f"Could not detect input format from filename: '{os.path.basename(input_file)}'. "
            f"Supported extensions: {list(EXTENSION_TO_FORMAT.keys())}"
        )

    if input_format == convert_format:
        print(
            f"WARNING: Input and target format are both '{input_format}'. "
            "Skipping conversion."
        )
        return

    input_class = get_format_class(input_format)
    output_class = get_format_class(convert_format)

    # --- Layer 2: within-class ---
    if input_class == output_class:
        if input_class == "triples":
            convert_rdf_triple_format(
                input_file, output_file, input_format, convert_format
            )
        elif input_class == "quads":
            convert_rdf_quad_format(
                input_file, output_file, input_format, convert_format
            )
        elif input_class == "tabular":
            convert_tabular_format(
                input_file, output_file, input_format, convert_format
            )
        return

    # --- Layer 3: cross-class ---
    if input_class == "triples" and output_class == "tabular":
        convert_rdf_to_csv(input_file, output_file, input_format)
        return

    raise ValueError(
        f"Conversion from '{input_format}' ({input_class}) to "
        f"'{convert_format}' ({output_class}) is not yet implemented. "
        f"Supported Layer 3 conversions: RDF Triples -> CSV/TSV."
    )