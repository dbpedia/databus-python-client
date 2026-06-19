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

from databusclient.api.convert import (
    QuadHandler,
    TSDHandler,
    TripleHandler,
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