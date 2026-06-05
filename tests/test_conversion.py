"""Round trip tests for Layer 2 format conversion.

Following the strategy from Frey et al., each test validates that
reading a format and writing it back produces semantically identical output.
Pattern: parse(format X) -> serialize(format X) -> parse again -> compare.

9 tests total:
- Triple formats: ntriples, turtle, rdf-xml          (3 tests)
- Quad formats:   nquads, trig, trix, json-ld        (4 tests)
- Tabular formats: csv, tsv                           (2 tests)
"""

import csv
import os
import tempfile

from rdflib import Dataset, Graph

from databusclient.api.convert import (
    convert_rdf_quad_format,
    convert_rdf_triple_format,
    convert_tabular_format,
)

# ---------------------------------------------------------------------------
# Sample RDF data used across all RDF tests
# ---------------------------------------------------------------------------

SAMPLE_TURTLE = """
@prefix ex: <http://example.org/> .
@prefix schema: <http://schema.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Paris schema:isCapitalOf ex:France ;
         schema:population "2161000"^^xsd:integer .

ex:Berlin schema:isCapitalOf ex:Germany ;
          schema:population "3645000"^^xsd:integer .
"""

SAMPLE_NQUADS = """
<http://example.org/Paris> <http://schema.org/isCapitalOf> <http://example.org/France> <http://example.org/graph1> .
<http://example.org/Berlin> <http://schema.org/isCapitalOf> <http://example.org/Germany> <http://example.org/graph1> .
<http://example.org/Rome> <http://schema.org/isCapitalOf> <http://example.org/Italy> <http://example.org/graph2> .
"""

SAMPLE_CSV = """resource,name,population
http://example.org/Paris,Paris,2161000
http://example.org/Berlin,Berlin,3645000
"""

SAMPLE_TSV = "resource\tname\tpopulation\nhttp://example.org/Paris\tParis\t2161000\nhttp://example.org/Berlin\tBerlin\t3645000\n"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_temp(content: str, suffix: str) -> str:
    """Write content to a named temp file and return its path."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def _graphs_are_isomorphic(g1: Graph, g2: Graph) -> bool:
    """Check semantic equivalence of two rdflib Graphs."""
    return g1.isomorphic(g2)


def _datasets_equal(g1: Dataset, g2: Dataset) -> bool:
    """Check semantic equivalence of two Datasets by triple count and graph names."""
    if len(g1) != len(g2):
        return False
    graphs1 = {str(c.identifier) for c in g1.graphs()}
    graphs2 = {str(c.identifier) for c in g2.graphs()}
    return graphs1 == graphs2


# ---------------------------------------------------------------------------
# Triple format round trip tests (Layer 2)
# ---------------------------------------------------------------------------

def test_round_trip_turtle():
    """Turtle -> Turtle: parse, serialize, reparse, compare."""
    input_path = _write_temp(SAMPLE_TURTLE, ".ttl")
    output_path = input_path + ".rt.ttl"

    try:
        convert_rdf_triple_format(input_path, output_path, "turtle", "turtle")

        g_original = Graph()
        g_original.parse(input_path, format="turtle")

        g_roundtrip = Graph()
        g_roundtrip.parse(output_path, format="turtle")

        assert _graphs_are_isomorphic(g_original, g_roundtrip), (
            "Turtle round trip failed: graphs are not isomorphic"
        )
    finally:
        for p in (input_path, output_path):
            if os.path.exists(p):
                os.remove(p)


def test_round_trip_ntriples():
    """N-Triples -> N-Triples: parse, serialize, reparse, compare."""
    # first produce an ntriples file from turtle
    turtle_path = _write_temp(SAMPLE_TURTLE, ".ttl")
    nt_path = turtle_path + ".nt"
    output_path = nt_path + ".rt.nt"

    try:
        convert_rdf_triple_format(turtle_path, nt_path, "turtle", "ntriples")
        convert_rdf_triple_format(nt_path, output_path, "ntriples", "ntriples")

        g_original = Graph()
        g_original.parse(nt_path, format="ntriples")

        g_roundtrip = Graph()
        g_roundtrip.parse(output_path, format="ntriples")

        assert _graphs_are_isomorphic(g_original, g_roundtrip), (
            "N-Triples round trip failed: graphs are not isomorphic"
        )
    finally:
        for p in (turtle_path, nt_path, output_path):
            if os.path.exists(p):
                os.remove(p)


def test_round_trip_rdf_xml():
    """RDF/XML -> RDF/XML: parse, serialize, reparse, compare."""
    turtle_path = _write_temp(SAMPLE_TURTLE, ".ttl")
    rdf_path = turtle_path + ".rdf"
    output_path = rdf_path + ".rt.rdf"

    try:
        convert_rdf_triple_format(turtle_path, rdf_path, "turtle", "rdf-xml")
        convert_rdf_triple_format(rdf_path, output_path, "rdf-xml", "rdf-xml")

        g_original = Graph()
        g_original.parse(rdf_path, format="xml")

        g_roundtrip = Graph()
        g_roundtrip.parse(output_path, format="xml")

        assert _graphs_are_isomorphic(g_original, g_roundtrip), (
            "RDF/XML round trip failed: graphs are not isomorphic"
        )
    finally:
        for p in (turtle_path, rdf_path, output_path):
            if os.path.exists(p):
                os.remove(p)


# ---------------------------------------------------------------------------
# Quad format round trip tests (Layer 2)
# ---------------------------------------------------------------------------

def test_round_trip_nquads():
    """N-Quads -> N-Quads: parse, serialize, reparse, compare."""
    input_path = _write_temp(SAMPLE_NQUADS, ".nq")
    output_path = input_path + ".rt.nq"

    try:
        convert_rdf_quad_format(input_path, output_path, "nquads", "nquads")

        g_original = Dataset()
        g_original.parse(input_path, format="nquads")

        g_roundtrip = Dataset()
        g_roundtrip.parse(output_path, format="nquads")

        assert _datasets_equal(g_original, g_roundtrip), (
            "N-Quads round trip failed: graphs are not equal"
        )
    finally:
        for p in (input_path, output_path):
            if os.path.exists(p):
                os.remove(p)


def test_round_trip_trig():
    """TriG -> TriG: parse, serialize, reparse, compare."""
    # produce trig from nquads
    nq_path = _write_temp(SAMPLE_NQUADS, ".nq")
    trig_path = nq_path + ".trig"
    output_path = trig_path + ".rt.trig"

    try:
        convert_rdf_quad_format(nq_path, trig_path, "nquads", "trig")
        convert_rdf_quad_format(trig_path, output_path, "trig", "trig")

        g_original = Dataset()
        g_original.parse(trig_path, format="trig")

        g_roundtrip = Dataset()
        g_roundtrip.parse(output_path, format="trig")

        assert _datasets_equal(g_original, g_roundtrip), (
            "TriG round trip failed: graphs are not equal"
        )
    finally:
        for p in (nq_path, trig_path, output_path):
            if os.path.exists(p):
                os.remove(p)


def test_round_trip_trix():
    """TriX -> TriX: parse, serialize, reparse, compare."""
    nq_path = _write_temp(SAMPLE_NQUADS, ".nq")
    trix_path = nq_path + ".trix"
    output_path = trix_path + ".rt.trix"

    try:
        convert_rdf_quad_format(nq_path, trix_path, "nquads", "trix")
        convert_rdf_quad_format(trix_path, output_path, "trix", "trix")

        g_original = Dataset()
        g_original.parse(trix_path, format="trix")

        g_roundtrip = Dataset()
        g_roundtrip.parse(output_path, format="trix")

        assert _datasets_equal(g_original, g_roundtrip), (
            "TriX round trip failed: graphs are not equal"
        )
    finally:
        for p in (nq_path, trix_path, output_path):
            if os.path.exists(p):
                os.remove(p)


def test_round_trip_json_ld():
    """JSON-LD -> JSON-LD: parse, serialize, reparse, compare."""
    nq_path = _write_temp(SAMPLE_NQUADS, ".nq")
    jsonld_path = nq_path + ".jsonld"
    output_path = jsonld_path + ".rt.jsonld"

    try:
        convert_rdf_quad_format(nq_path, jsonld_path, "nquads", "json-ld")
        convert_rdf_quad_format(jsonld_path, output_path, "json-ld", "json-ld")

        g_original = Dataset()
        g_original.parse(jsonld_path, format="json-ld")

        g_roundtrip = Dataset()
        g_roundtrip.parse(output_path, format="json-ld")

        assert _datasets_equal(g_original, g_roundtrip), (
            "JSON-LD round trip failed: graphs are not equal"
        )
    finally:
        for p in (nq_path, jsonld_path, output_path):
            if os.path.exists(p):
                os.remove(p)


# ---------------------------------------------------------------------------
# Tabular format round trip tests (Layer 2)
# ---------------------------------------------------------------------------

def test_round_trip_csv():
    """CSV -> CSV: read, write, reread, compare rows."""
    input_path = _write_temp(SAMPLE_CSV, ".csv")
    output_path = input_path + ".rt.csv"

    try:
        convert_tabular_format(input_path, output_path, "csv", "csv")

        with open(input_path, newline="", encoding="utf-8") as f:
            original_rows = list(csv.reader(f))

        with open(output_path, newline="", encoding="utf-8") as f:
            roundtrip_rows = list(csv.reader(f))

        assert original_rows == roundtrip_rows, (
            "CSV round trip failed: rows do not match"
        )
    finally:
        for p in (input_path, output_path):
            if os.path.exists(p):
                os.remove(p)


def test_round_trip_tsv():
    """TSV -> TSV: read, write, reread, compare rows."""
    input_path = _write_temp(SAMPLE_TSV, ".tsv")
    output_path = input_path + ".rt.tsv"

    try:
        convert_tabular_format(input_path, output_path, "tsv", "tsv")

        with open(input_path, newline="", encoding="utf-8") as f:
            original_rows = list(csv.reader(f, delimiter="\t"))

        with open(output_path, newline="", encoding="utf-8") as f:
            roundtrip_rows = list(csv.reader(f, delimiter="\t"))

        assert original_rows == roundtrip_rows, (
            "TSV round trip failed: rows do not match"
        )
    finally:
        for p in (input_path, output_path):
            if os.path.exists(p):
                os.remove(p)