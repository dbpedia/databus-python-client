"""Layer 3 Mapping Conversion — cross-class conversions between RDF and tabular formats.

Supported mapping directions:
    Triple -> Quad   : Assigns a named graph to all triples (requires graph_name).
    Quad -> Triple   : Splits quads into one file per named graph (in a subdirectory),
                       written in the triple format specified by output_format.
    Triple -> TSD    : Maps RDF triples to wide CSV table (quasi-equal, companion .meta.json).
    TSD -> Triple    : Reconstructs RDF triples from wide CSV (lossless with companion file).
    Quad -> TSD      : Maps RDF quads to wide CSV table with extra graph column.

Data loss and quasi-equality:
    RDF -> CSV conversion is quasi-equal. RDF datatypes (xsd:integer etc.) and language
    tags (@en) cannot be represented in plain CSV. A companion .meta.json file is generated
    alongside the CSV to preserve this information. When converting back (CSV -> RDF), if the
    companion file is present, datatypes and language tags are restored for full lossless
    round trips. Without the companion file, all values are restored as plain xsd:string.

    Note: a string literal whose lexical value itself looks like a URI (e.g. a literal
    "http://example.com/text") cannot be distinguished from an actual URI reference in
    the CSV representation. This is an inherent limitation of the wide-table CSV format
    and matches the level of fidelity of the Java client's TSD mapping.

Blank node handling:
    Blank node subjects and objects are serialized to CSV cells as '_:label' (matching
    N-Triples notation). This is essential for round trips: without the '_:' marker,
    convert_csv_to_rdf() cannot distinguish a blank node reference from a URI or string
    literal. On CSV -> RDF, any cell value starting with '_:' is reconstructed as a BNode
    with the same label, preserving links between blank nodes and their properties.

Per-predicate metadata granularity:
    The companion .meta.json stores one datatype/language entry per predicate (the last
    value seen during conversion). This assumes a predicate's values share a consistent
    type, which holds for typical RDF datasets (e.g. DBpedia mappings) where a given
    predicate has a consistent range.
"""

import json
import os

from rdflib import BNode, Dataset, Graph, Literal, URIRef
from rdflib.namespace import XSD

from databusclient.filehandling.format import (
    QuadHandler,
    TSDHandler,
    TripleHandler,
    FORMAT_TO_EXTENSION,
)

# ---------------------------------------------------------------------------
# Module-level handler instances — reuse across calls
# ---------------------------------------------------------------------------

_triple_handler = TripleHandler()
_quad_handler = QuadHandler()
_tsd_handler = TSDHandler()


# ---------------------------------------------------------------------------
# Shared helper — RDF term to CSV cell string
# ---------------------------------------------------------------------------

def _term_to_str(term) -> str:
    """Convert an RDF term (URIRef, BNode, or Literal) to its CSV cell string.

    Blank nodes are prefixed with '_:' (matching N-Triples notation) so they
    can be correctly distinguished from URIs and literals when reconstructing
    RDF from CSV in convert_csv_to_rdf(). Without this prefix, a blank node
    label like 'address1' would be indistinguishable from a relative resource
    identifier, breaking the link between a blank node and its properties.

    Literals are represented by their lexical form (the string value as
    written), regardless of datatype. This avoids conversion-related
    discrepancies (e.g. datetime formatting via .toPython()) and matches
    what is restored via Literal(value, datatype=...) on the reverse direction.

    Args:
        term: An rdflib term (URIRef, BNode, or Literal).

    Returns:
        String representation suitable for a CSV cell.
    """
    if isinstance(term, BNode):
        return f"_:{term}"
    return str(term)


# ---------------------------------------------------------------------------
# Direction 1 — Triple -> Quad
# ---------------------------------------------------------------------------

def convert_triples_to_quads(
    input_file: str,
    output_file: str,
    input_format: str,
    output_format: str,
    graph_name: str,
) -> None:
    """Promote RDF triples to named graph quads (Layer 3, lossless).

    All triples are assigned to the named graph specified by graph_name.
    Requires --graph-name to be provided.

    Args:
        input_file: Path to input RDF triples file.
        output_file: Path to write output quads file.
        input_format: Source triple format name (e.g. 'turtle', 'ntriples').
        output_format: Target quad format name (e.g. 'nquads', 'trig').
        graph_name: URI string for the named graph to assign all triples to.

    Raises:
        ValueError: If graph_name is empty or None.
    """
    if not graph_name:
        raise ValueError(
            "graph_name is required for Triple -> Quad conversion. "
            "Use --graph-name <uri> to specify the target named graph."
        )

    g = _triple_handler.read(input_file, input_format)
    d = Dataset()
    graph_uri = URIRef(graph_name)
    named_graph = d.graph(graph_uri)

    for triple in g:
        named_graph.add(triple)

    _quad_handler.write(d, output_file, output_format)
    print(
        f"Converted {input_format} -> {output_format} "
        f"(graph: {graph_name}): {os.path.basename(output_file)}"
    )


# ---------------------------------------------------------------------------
# Direction 2 — Quad -> Triple
# ---------------------------------------------------------------------------

def convert_quads_to_triples(
    input_file: str,
    output_dir: str,
    input_format: str,
    output_format: str,
) -> list:
    """Split RDF quads into per-graph triple files (Layer 3, lossless).

    Each named graph in the quads file becomes a separate file, written in
    output_format (e.g. 'ntriples', 'turtle', 'rdf-xml' — whatever was
    specified via --format). Output files are written to output_dir, named
    after the last segment of the graph URI (e.g. 'people.ttl' for graph
    'https://example.org/graph/people' when output_format='turtle').

    Default graph triples (no named graph) are written to
    'default_graph.<ext>'.

    Args:
        input_file: Path to input quads file.
        output_dir: Directory to write one file per named graph.
        input_format: Source quad format name (e.g. 'nquads', 'trig').
        output_format: Target triple format name (e.g. 'ntriples', 'turtle',
            'rdf-xml'). Required — no default, matches whatever the user
            specified via --format.

    Returns:
        List of output file paths created.

    Raises:
        ValueError: If no named graphs with triples are found in input.
    """
    os.makedirs(output_dir, exist_ok=True)

    d = _quad_handler.read(input_file, input_format)
    output_files = []

    file_ext = FORMAT_TO_EXTENSION.get(output_format, f".{output_format}")

    for named_graph in d.graphs():
        graph_id = str(named_graph.identifier)

        # Skip empty graphs (e.g. an unused default graph)
        if len(named_graph) == 0:
            continue

        # Determine output filename from graph URI last segment
        if graph_id in ("urn:x-rdflib:default", ""):
            file_stem = "default_graph"
        else:
            file_stem = graph_id.rstrip("/").split("/")[-1]
            # Sanitize: replace characters invalid in filenames
            file_stem = "".join(
                c if c.isalnum() or c in "-_." else "_" for c in file_stem
            )
            if not file_stem:
                file_stem = "graph"

        out_path = os.path.join(output_dir, file_stem + file_ext)

        # Handle duplicate filenames by appending a counter
        counter = 1
        original_out_path = out_path
        while os.path.exists(out_path):
            out_path = original_out_path[: -len(file_ext)] + f"_{counter}{file_ext}"
            counter += 1

        _triple_handler.write(named_graph, out_path, output_format)
        output_files.append(out_path)
        print(f"Written graph '{graph_id}' -> {os.path.basename(out_path)}")

    if not output_files:
        raise ValueError(
            f"No named graphs with triples found in '{os.path.basename(input_file)}'. "
            "Nothing to split."
        )

    print(
        f"Quad -> Triple split complete: {len(output_files)} file(s) "
        f"({output_format}) in '{os.path.basename(output_dir)}/'"
    )
    return output_files


# ---------------------------------------------------------------------------
# Direction 3 — Triple -> TSD (CSV/TSV)
# ---------------------------------------------------------------------------

def convert_rdf_to_csv(
    input_file: str,
    output_file: str,
    input_format: str,
    output_format: str,
) -> None:
    """Map RDF triples to a wide tabular table (Layer 3, quasi-equal).

    Each unique RDF subject becomes one row. Each unique predicate becomes
    a column header (full predicate URI). Object values fill the cells.
    Multi-valued predicates are pipe-separated (|) to enable unambiguous
    splitting on round trip.

    A companion .meta.json file is generated alongside the output file
    to preserve RDF datatype and language tag information, enabling
    lossless round trips when convert_csv_to_rdf() is called with the
    same companion file present.

    Blank node subjects and objects are serialized as '_:label' (see
    _term_to_str). This is essential for correct round trips.

    Args:
        input_file: Path to input RDF triples file.
        output_file: Path to write output CSV or TSV file.
        input_format: Source triple format name (must be in RDF_TRIPLE_FORMATS).
        output_format: Target tabular format ('csv' or 'tsv').
    """
    g = _triple_handler.read(input_file, input_format)

    # Collect all unique predicates (sorted for deterministic column order)
    predicates = sorted(set(str(p) for s, p, o in g))

    # Group objects by (subject, predicate)
    subjects: dict = {}
    # column_metadata: predicate URI -> {datatype: ...} or {language: ...}
    # Only the LAST seen value's metadata is stored per predicate (see
    # module docstring on per-predicate metadata granularity).
    column_metadata: dict = {}

    for s, p, o in g:
        subj = _term_to_str(s)
        pred = str(p)

        # Collect datatype/language metadata for companion file
        if isinstance(o, Literal):
            if o.datatype and str(o.datatype) != str(XSD.string):
                column_metadata[pred] = {"datatype": str(o.datatype)}
            elif o.language:
                column_metadata[pred] = {"language": str(o.language)}

        if subj not in subjects:
            subjects[subj] = {}
        if pred not in subjects[subj]:
            subjects[subj][pred] = []
        subjects[subj][pred].append(_term_to_str(o))

    # Build rows: header + one row per subject
    rows = [["resource"] + predicates]
    for subj, pred_map in subjects.items():
        row = [subj]
        for pred in predicates:
            values = pred_map.get(pred, [])
            row.append("|".join(values))
        rows.append(row)

    _tsd_handler.write(rows, output_file, output_format)

    # Write companion metadata file
    companion_file = output_file + ".meta.json"
    with open(companion_file, "w", encoding="utf-8") as f:
        json.dump({"columns": column_metadata}, f, indent=2)

    print(f"Converted RDF -> {output_format.upper()}: {os.path.basename(output_file)}")
    print(f"Companion metadata: {os.path.basename(companion_file)}")


# ---------------------------------------------------------------------------
# Direction 4 — TSD (CSV/TSV) -> Triple
# ---------------------------------------------------------------------------

def convert_csv_to_rdf(
    input_file: str,
    output_file: str,
    input_format: str,
    output_format: str,
    base_uri: str,
) -> None:
    """Reconstruct RDF triples from a wide tabular file (Layer 3).

    Column headers (except 'resource') become predicate URIs directly.
    Each row becomes one RDF subject. Cell values become object literals,
    URIs, or blank nodes depending on their content.

    If a companion .meta.json file exists alongside the input CSV
    (same path + '.meta.json'), datatypes and language tags are restored
    from it, enabling lossless round trips. Without the companion file,
    all literal values are created as plain xsd:string literals.

    Note: companion file lookup uses input_file + '.meta.json' — the
    companion must be co-located with the exact input file path passed
    here. If the input was downloaded compressed and decompressed to a
    temporary file, no companion will typically be found (this is an
    inherent, documented limitation, not a bug).

    Multi-valued cells (pipe-separated '|') are split back into multiple
    triples per subject-predicate pair.

    Blank node subjects/objects: any value starting with '_:' is
    reconstructed as a BNode with the same label. URI objects: any value
    starting with 'http://' or 'https://' is created as URIRef.

    Args:
        input_file: Path to input CSV or TSV file.
        output_file: Path to write output RDF triples file.
        input_format: Source tabular format ('csv' or 'tsv').
        output_format: Target triple format name (e.g. 'ntriples', 'turtle').
        base_uri: Base URI for constructing subject URIs from relative identifiers.

    Raises:
        ValueError: If base_uri is empty or None.
        ValueError: If input file is empty or missing the 'resource' column.
    """
    if not base_uri:
        raise ValueError(
            "base_uri is required for CSV -> RDF conversion. "
            "Use --base-uri <uri> to specify the base URI for subject construction."
        )

    rows = _tsd_handler.read(input_file, input_format)

    if not rows:
        raise ValueError(f"Input file '{os.path.basename(input_file)}' is empty.")

    header = rows[0]
    if "resource" not in header:
        raise ValueError(
            f"Input CSV missing 'resource' column. "
            f"Found columns: {header}. "
            "The 'resource' column is required and must contain subject identifiers."
        )

    resource_idx = header.index("resource")
    predicate_columns = [
        (i, col) for i, col in enumerate(header) if i != resource_idx
    ]

    # Load companion metadata if present
    companion_path = input_file + ".meta.json"
    column_metadata: dict = {}
    if os.path.exists(companion_path):
        with open(companion_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
            column_metadata = meta.get("columns", {})
        print(f"Loaded companion metadata: {os.path.basename(companion_path)}")
    else:
        print(
            "No companion metadata file found. "
            "All literal values will be created as plain strings."
        )

    base_uri_stripped = base_uri.rstrip("/")
    g = Graph()

    for row in rows[1:]:  # skip header
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))

        resource_val = row[resource_idx].strip()
        if not resource_val:
            continue  # skip empty rows

        # Build subject node
        if resource_val.startswith("_:"):
            subject = BNode(resource_val[2:])
        elif resource_val.startswith("http://") or resource_val.startswith("https://"):
            subject = URIRef(resource_val)
        else:
            subject = URIRef(f"{base_uri_stripped}/{resource_val}")

        # Build triples for each predicate column
        for col_idx, pred_uri in predicate_columns:
            cell = row[col_idx].strip() if col_idx < len(row) else ""
            if not cell:
                continue

            predicate = URIRef(pred_uri)
            meta = column_metadata.get(pred_uri, {})

            # Split multi-valued cells
            for val in cell.split("|"):
                val = val.strip()
                if not val:
                    continue

                obj = _build_object(val, meta)
                g.add((subject, predicate, obj))

    _triple_handler.write(g, output_file, output_format)
    print(
        f"Converted {input_format.upper()} -> {output_format}: "
        f"{os.path.basename(output_file)}"
    )


def _build_object(value: str, meta: dict):
    """Build an RDF object term from a CSV cell string and metadata.

    Args:
        value: String value from CSV cell.
        meta: Metadata dict with optional 'datatype' or 'language' keys.

    Returns:
        rdflib term: URIRef, BNode, or Literal.
    """
    # Blank node
    if value.startswith("_:"):
        return BNode(value[2:])

    # URI
    if value.startswith("http://") or value.startswith("https://"):
        return URIRef(value)

    # Literal with datatype from companion file
    if "datatype" in meta:
        return Literal(value, datatype=URIRef(meta["datatype"]))

    # Literal with language tag from companion file
    if "language" in meta:
        return Literal(value, lang=meta["language"])

    # Plain string literal (no companion metadata)
    return Literal(value)


# ---------------------------------------------------------------------------
# Direction 5 — Quad -> TSD (CSV/TSV)
# ---------------------------------------------------------------------------

def convert_quads_to_csv(
    input_file: str,
    output_file: str,
    input_format: str,
    output_format: str,
) -> None:
    """Map RDF quads to a wide tabular table with a graph column (Layer 3, quasi-equal).

    Extends the Triple -> TSD mapping by adding a 'graph' column containing
    the named graph URI. Each row represents one (subject, graph) pair, with
    one column per predicate (pipe-separated for multi-valued predicates).

    A companion .meta.json file is generated to preserve datatype and
    language tag information.

    The default graph (if present) is skipped — only triples within named
    graphs are represented, since the 'graph' column requires a graph URI.

    Args:
        input_file: Path to input quads file.
        output_file: Path to write output CSV or TSV file.
        input_format: Source quad format name (e.g. 'nquads', 'trig').
        output_format: Target tabular format ('csv' or 'tsv').
    """
    d = _quad_handler.read(input_file, input_format)

    # Collect all predicates across all named graphs (sorted for determinism)
    all_predicates = sorted(
        set(
            str(p)
            for named_graph in d.graphs()
            for s, p, o in named_graph
            if str(named_graph.identifier) not in ("urn:x-rdflib:default", "")
        )
    )

    column_metadata: dict = {}
    # rows_map key: (subject_str, graph_uri_str) -> {predicate_uri: [values]}
    rows_map: dict = {}

    for named_graph in d.graphs():
        graph_id = str(named_graph.identifier)

        # Skip the default graph — no meaningful graph URI for the column
        if graph_id in ("urn:x-rdflib:default", ""):
            continue

        for s, p, o in named_graph:
            subj = _term_to_str(s)
            pred = str(p)
            key = (subj, graph_id)

            if isinstance(o, Literal):
                if o.datatype and str(o.datatype) != str(XSD.string):
                    column_metadata[pred] = {"datatype": str(o.datatype)}
                elif o.language:
                    column_metadata[pred] = {"language": str(o.language)}

            if key not in rows_map:
                rows_map[key] = {}
            if pred not in rows_map[key]:
                rows_map[key][pred] = []
            rows_map[key][pred].append(_term_to_str(o))

    # Build rows: header = resource + graph + all predicates
    header = ["resource", "graph"] + all_predicates
    rows = [header]

    for (subj, graph_id), pred_map in rows_map.items():
        row = [subj, graph_id]
        for pred in all_predicates:
            values = pred_map.get(pred, [])
            row.append("|".join(values))
        rows.append(row)

    _tsd_handler.write(rows, output_file, output_format)

    companion_file = output_file + ".meta.json"
    with open(companion_file, "w", encoding="utf-8") as f:
        json.dump({"columns": column_metadata}, f, indent=2)

    print(
        f"Converted {input_format} -> {output_format.upper()} "
        f"(with graph column): {os.path.basename(output_file)}"
    )
    print(f"Companion metadata: {os.path.basename(companion_file)}")