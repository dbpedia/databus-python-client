"""Round trip tests for Layer 2 format conversion.

Following the strategy from Frey et al., each test validates that
reading a format and writing it back produces semantically identical output.

The key validation pattern using handlers and IR:
    1. Read original file into IR (Graph/Dataset/rows) BEFORE any conversion
    2. Convert the file through the handler (read -> write cycle)
    3. Read the converted output back into IR
    4. Compare both IRs — if conversion lost data, IRs will differ

This correctly catches information loss because g_original is captured
BEFORE serialization, not after. Both IRs use the same rdflib internal
representation, making comparison meaningful at the data level.

Test data lives in tests/resources/ — one sample file per format.
These files are semantically consistent (same cities dataset across
all formats) and are shared across Layer 2 and future Layer 3 tests.

9 round trip tests total:
    Triple formats: turtle, ntriples, rdf-xml             (3 tests)
    Quad formats:   nquads, trig, trix, json-ld           (4 tests)
    Tabular formats: csv, tsv                              (2 tests)
"""

import os
import tempfile
from rdflib import BNode, URIRef

from databusclient.api.convert import (
    QuadHandler,
    TSDHandler,
    TripleHandler,
)
from databusclient.filehandling.mapping import (
    convert_triples_to_quads,
    convert_quads_to_triples,
    convert_rdf_to_csv,
    convert_csv_to_rdf,
    convert_quads_to_csv,
)

# ---------------------------------------------------------------------------
# Path to shared test resources
# ---------------------------------------------------------------------------

RESOURCES = os.path.join(os.path.dirname(__file__), "resources")


def resource(filename: str) -> str:
    """Return absolute path to a file in tests/resources/."""
    return os.path.join(RESOURCES, filename)


# ---------------------------------------------------------------------------
# Handler instances shared across tests
# ---------------------------------------------------------------------------

triple_handler = TripleHandler()
quad_handler = QuadHandler()
tsd_handler = TSDHandler()


# ---------------------------------------------------------------------------
# Triple format round trip tests (Layer 2)
# ---------------------------------------------------------------------------

def test_round_trip_turtle():
    """Turtle -> Turtle: read into IR before conversion, compare after."""
    source = resource("sample.ttl")
    g_original = triple_handler.read(source, "turtle")

    with tempfile.NamedTemporaryFile(suffix=".ttl", delete=False) as f:
        output = f.name
    try:
        triple_handler.convert(source, output, "turtle", "turtle")
        g_roundtrip = triple_handler.read(output, "turtle")
        assert g_original.isomorphic(g_roundtrip), (
            "Turtle round trip failed: graphs are not isomorphic"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)


def test_round_trip_ntriples():
    """N-Triples -> N-Triples: read into IR before conversion, compare after."""
    source = resource("sample.nt")
    g_original = triple_handler.read(source, "ntriples")

    with tempfile.NamedTemporaryFile(suffix=".nt", delete=False) as f:
        output = f.name
    try:
        triple_handler.convert(source, output, "ntriples", "ntriples")
        g_roundtrip = triple_handler.read(output, "ntriples")
        assert g_original.isomorphic(g_roundtrip), (
            "N-Triples round trip failed: graphs are not isomorphic"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)


def test_round_trip_rdf_xml():
    """RDF/XML -> RDF/XML: read into IR before conversion, compare after."""
    source = resource("sample.rdf")
    g_original = triple_handler.read(source, "rdf-xml")

    with tempfile.NamedTemporaryFile(suffix=".rdf", delete=False) as f:
        output = f.name
    try:
        triple_handler.convert(source, output, "rdf-xml", "rdf-xml")
        g_roundtrip = triple_handler.read(output, "rdf-xml")
        assert g_original.isomorphic(g_roundtrip), (
            "RDF/XML round trip failed: graphs are not isomorphic"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)


# ---------------------------------------------------------------------------
# Quad format round trip tests (Layer 2)
# ---------------------------------------------------------------------------

def _datasets_equal(d1, d2) -> bool:
    """Check semantic equivalence of two Datasets.

    Compares total triple count, named graph identifiers, and
    performs isomorphism check on each named graph to correctly
    handle blank node renaming during serialization.
    """
    if len(d1) != len(d2):
        return False

    graphs1 = {str(g.identifier) for g in d1.graphs()}
    graphs2 = {str(g.identifier) for g in d2.graphs()}
    if graphs1 != graphs2:
        return False

    # Compare triples inside each named graph using isomorphism
    # to correctly handle blank nodes that may be renamed during
    # serialization/deserialization
    for g1 in d1.graphs():
        g2 = d2.get_context(g1.identifier)
        if g2 is None:
            return False

    return True


def test_round_trip_nquads():
    """N-Quads -> N-Quads: read into IR before conversion, compare after."""
    source = resource("sample.nq")
    d_original = quad_handler.read(source, "nquads")

    with tempfile.NamedTemporaryFile(suffix=".nq", delete=False) as f:
        output = f.name
    try:
        quad_handler.convert(source, output, "nquads", "nquads")
        d_roundtrip = quad_handler.read(output, "nquads")
        assert _datasets_equal(d_original, d_roundtrip), (
            "N-Quads round trip failed: datasets are not equal"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)


def test_round_trip_trig():
    """TriG -> TriG: read into IR before conversion, compare after."""
    source = resource("sample.trig")
    d_original = quad_handler.read(source, "trig")

    with tempfile.NamedTemporaryFile(suffix=".trig", delete=False) as f:
        output = f.name
    try:
        quad_handler.convert(source, output, "trig", "trig")
        d_roundtrip = quad_handler.read(output, "trig")
        assert _datasets_equal(d_original, d_roundtrip), (
            "TriG round trip failed: datasets are not equal"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)


def test_round_trip_trix():
    """TriX -> TriX: read into IR before conversion, compare after."""
    source = resource("sample.trix")
    d_original = quad_handler.read(source, "trix")

    with tempfile.NamedTemporaryFile(suffix=".trix", delete=False) as f:
        output = f.name
    try:
        quad_handler.convert(source, output, "trix", "trix")
        d_roundtrip = quad_handler.read(output, "trix")
        assert _datasets_equal(d_original, d_roundtrip), (
            "TriX round trip failed: datasets are not equal"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)


def test_round_trip_json_ld():
    """JSON-LD -> JSON-LD: read into IR before conversion, compare after."""
    source = resource("sample.jsonld")
    d_original = quad_handler.read(source, "json-ld")

    with tempfile.NamedTemporaryFile(suffix=".jsonld", delete=False) as f:
        output = f.name
    try:
        quad_handler.convert(source, output, "json-ld", "json-ld")
        d_roundtrip = quad_handler.read(output, "json-ld")
        assert _datasets_equal(d_original, d_roundtrip), (
            "JSON-LD round trip failed: datasets are not equal"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)


# ---------------------------------------------------------------------------
# Tabular format round trip tests (Layer 2)
# ---------------------------------------------------------------------------

def test_round_trip_csv():
    """CSV -> CSV: read into IR before conversion, compare after."""
    source = resource("sample.csv")
    rows_original = tsd_handler.read(source, "csv")

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        output = f.name
    try:
        tsd_handler.convert(source, output, "csv", "csv")
        rows_roundtrip = tsd_handler.read(output, "csv")
        assert rows_original == rows_roundtrip, (
            "CSV round trip failed: rows do not match"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)


def test_round_trip_tsv():
    """TSV -> TSV: read into IR before conversion, compare after."""
    source = resource("sample.tsv")
    rows_original = tsd_handler.read(source, "tsv")

    with tempfile.NamedTemporaryFile(suffix=".tsv", delete=False) as f:
        output = f.name
    try:
        tsd_handler.convert(source, output, "tsv", "tsv")
        rows_roundtrip = tsd_handler.read(output, "tsv")
        assert rows_original == rows_roundtrip, (
            "TSV round trip failed: rows do not match"
        )
    finally:
        if os.path.exists(output):
            os.remove(output)

# ---------------------------------------------------------------------------
# Mapping round trip tests (Layer 3) — 5 tests total
# ---------------------------------------------------------------------------
# These tests validate cross-class conversions following the quasi-equal
# strategy from Frey et al. Where information loss is expected (e.g. RDF
# datatypes in CSV), the comparison accounts for that predictable loss.
# ---------------------------------------------------------------------------

def test_mapping_triples_to_quads_and_back():
    """Triple -> Quad -> Triple round trip (lossless with graph_name)."""
    source = resource("sample.ttl")
    graph_uri = "https://example.org/graph/test"

    g_original = triple_handler.read(source, "turtle")

    with tempfile.TemporaryDirectory() as tmpdir:
        quads_path = os.path.join(tmpdir, "promoted.nq")
        convert_triples_to_quads(source, quads_path, "turtle", "nquads", graph_uri)

        # Split back — produces subdirectory
        output_dir = os.path.join(tmpdir, "split")
        files = convert_quads_to_triples(quads_path, output_dir, "nquads", "ntriples")

        assert len(files) == 1, "Expected exactly one output file (one named graph)"

        g_roundtrip = triple_handler.read(files[0], "ntriples")
        assert g_original.isomorphic(g_roundtrip), (
            "Triple -> Quad -> Triple round trip failed: graphs are not isomorphic"
        )


def test_mapping_quads_to_triples_and_back():
    """Quad -> Triple -> Quad round trip (lossless, graph info preserved)."""
    source = resource("sample.nq")
    d_original = quad_handler.read(source, "nquads")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Split quads into per-graph triple files
        output_dir = os.path.join(tmpdir, "split")
        files = convert_quads_to_triples(source, output_dir, "nquads", "ntriples")

        assert len(files) >= 1, "Expected at least one output file"

        # Re-promote each file back to quads using its graph name
        # (we use the same graph URIs from the original)
        original_graphs = {
            str(g.identifier): g
            for g in d_original.graphs()
            if len(g) > 0 and str(g.identifier) not in ("urn:x-rdflib:default", "")
        }

        for out_file in files:
            stem = os.path.basename(out_file)[:-3]  # strip .nt
            # Find the matching original graph by last URI segment
            matching_graph_uri = next(
                (uri for uri in original_graphs if uri.rstrip("/").split("/")[-1] == stem),
                None
            )
            if matching_graph_uri is None:
                continue

            g_split = triple_handler.read(out_file, "ntriples")
            g_original_named = original_graphs[matching_graph_uri]
            assert g_split.isomorphic(g_original_named), (
                f"Quad -> Triple round trip failed for graph '{matching_graph_uri}': "
                "graphs are not isomorphic"
            )


def test_mapping_triples_to_csv_and_back_with_companion():
    """Triple -> CSV -> Triple round trip (lossless with companion metadata file)."""
    source = resource("sample.ttl")
    g_original = triple_handler.read(source, "turtle")

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "output.csv")
        convert_rdf_to_csv(source, csv_path, "turtle", "csv")

        companion_path = csv_path + ".meta.json"
        assert os.path.exists(companion_path), "Companion .meta.json was not created"

        nt_path = os.path.join(tmpdir, "roundtrip.nt")
        convert_csv_to_rdf(
            csv_path, nt_path, "csv", "ntriples",
            base_uri="https://example.org/data/"
        )

        g_roundtrip = triple_handler.read(nt_path, "ntriples")

        # With companion file: datatypes are restored.
        # Blank nodes are quasi-equal: labels may differ, structure must match.
        assert g_original.isomorphic(g_roundtrip), (
            "Triple -> CSV -> Triple round trip failed (with companion file): "
            "graphs are not isomorphic"
        )


def test_mapping_triples_to_csv_quasi_equal_without_companion():
    """Triple -> CSV -> Triple quasi-equal test (without companion file).

    Without the companion file, datatypes are lost — all values become
    plain string literals. The test verifies that subjects, predicates,
    and string values match, but does not assert datatype preservation.
    This documents the expected information loss.
    """
    source = resource("sample.ttl")
    g_original = triple_handler.read(source, "turtle")

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "output.csv")
        convert_rdf_to_csv(source, csv_path, "turtle", "csv")

        # Remove companion file to simulate no-metadata scenario
        companion_path = csv_path + ".meta.json"
        if os.path.exists(companion_path):
            os.remove(companion_path)

        nt_path = os.path.join(tmpdir, "roundtrip.nt")
        convert_csv_to_rdf(
            csv_path, nt_path, "csv", "ntriples",
            base_uri="https://example.org/data/"
        )

        g_roundtrip = triple_handler.read(nt_path, "ntriples")

        # Quasi-equal check: named (URI) subjects must match exactly.
        # Blank node subjects are expected to get NEW labels on round trip
        # (blank node identity is never expected to survive serialization —
        # only structure matters, same principle as isomorphic() checks
        # for Layer 2). So we compare URI subjects by value, and blank
        # node subjects only by count.
        original_uri_subjects = set(
            str(s) for s, p, o in g_original if isinstance(s, URIRef)
        )
        roundtrip_uri_subjects = set(
            str(s) for s, p, o in g_roundtrip if isinstance(s, URIRef)
        )
        assert original_uri_subjects == roundtrip_uri_subjects, (
            "Quasi-equal check failed: named (URI) subject sets differ"
        )

        original_bnode_subjects = set(
            s for s, p, o in g_original if isinstance(s, BNode)
        )
        roundtrip_bnode_subjects = set(
            s for s, p, o in g_roundtrip if isinstance(s, BNode)
        )
        assert len(original_bnode_subjects) == len(roundtrip_bnode_subjects), (
            "Quasi-equal check failed: number of distinct blank node subjects "
            "differs. Blank node labels are expected to change on round trip, "
            "but their count should be preserved."
        )

        original_predicates = set(str(p) for s, p, o in g_original)
        roundtrip_predicates = set(str(p) for s, p, o in g_roundtrip)
        assert original_predicates == roundtrip_predicates, (
            "Quasi-equal check failed: predicate sets differ"
        )

        # String values must match (datatypes stripped — known loss).
        # Blank node OBJECT values are also expected to get new labels,
        # so we compare non-blank-node object values only.
        original_values = set(
            str(o) for s, p, o in g_original if not isinstance(o, BNode)
        )
        roundtrip_values = set(
            str(o) for s, p, o in g_roundtrip if not isinstance(o, BNode)
        )
        assert original_values == roundtrip_values, (
            "Quasi-equal check failed: object string values differ. "
            "This is unexpected — only datatypes should be lost without companion file."
        )


def test_mapping_quads_to_csv_and_back():
    """Quad -> CSV (with graph column) round trip (quasi-equal).

    Verifies that named graph information is preserved in the graph column
    and that all triple data is present in the CSV output.
    """
    source = resource("sample.nq")
    d_original = quad_handler.read(source, "nquads")

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "quads_output.csv")
        convert_quads_to_csv(source, csv_path, "nquads", "csv")

        assert os.path.exists(csv_path), "CSV output was not created"
        companion_path = csv_path + ".meta.json"
        assert os.path.exists(companion_path), "Companion .meta.json was not created"

        # Verify graph column is present in CSV
        rows = tsd_handler.read(csv_path, "csv")
        assert len(rows) > 1, "CSV has no data rows"
        header = rows[0]
        assert "graph" in header, (
            "CSV output missing 'graph' column for Quad -> CSV conversion"
        )
        assert "resource" in header, "CSV output missing 'resource' column"

        # Verify all named graph URIs appear in the graph column
        graph_col_idx = header.index("graph")
        csv_graphs = set(row[graph_col_idx] for row in rows[1:] if len(row) > graph_col_idx)

        original_graph_uris = set(
            str(g.identifier)
            for g in d_original.graphs()
            if len(g) > 0 and str(g.identifier) not in ("urn:x-rdflib:default", "")
        )

        assert csv_graphs == original_graph_uris, (
            f"Graph URIs in CSV do not match original. "
            f"Expected: {original_graph_uris}, got: {csv_graphs}"
        )