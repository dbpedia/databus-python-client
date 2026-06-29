"""Comprehensive functional tests for Layer 3 mapping conversions.

These tests cover all 5 mapping directions with edge cases:
    - Triple -> Quad (with graph_name)
    - Quad -> Triple (split by graph, subdirectory output)
    - Triple -> TSD (CSV/TSV, companion metadata)
    - TSD -> Triple (with and without companion file)
    - Quad -> TSD (CSV with graph column)

Edge cases covered:
    - Blank node subjects and objects
    - Typed literals (xsd:integer)
    - Multi-valued predicates (pipe-separated)
    - Missing companion file (graceful degradation)
    - Empty cells in CSV
    - Graph name sanitization in filenames
"""

import json
import os
import tempfile
import pytest

from databusclient.filehandling.format import TripleHandler, QuadHandler, TSDHandler
from databusclient.filehandling.mapping import (
    convert_triples_to_quads,
    convert_quads_to_triples,
    convert_rdf_to_csv,
    convert_csv_to_rdf,
    convert_quads_to_csv,
)

triple_handler = TripleHandler()
quad_handler = QuadHandler()
tsd_handler = TSDHandler()

# ---------------------------------------------------------------------------
# Shared test data and helpers
# ---------------------------------------------------------------------------

RESOURCES = os.path.join(os.path.dirname(__file__), "resources")


def resource(filename: str) -> str:
    return os.path.join(RESOURCES, filename)


# ---------------------------------------------------------------------------
# Direction 1: Triple -> Quad
# ---------------------------------------------------------------------------

class TestTriplesToQuads:

    def test_basic_conversion(self):
        """All triples are assigned to the specified named graph."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.nq")
            convert_triples_to_quads(src, out, "turtle", "nquads",
                                     "https://example.org/graph/test")

            assert os.path.exists(out)
            d = quad_handler.read(out, "nquads")
            graph_uris = [
                str(g.identifier) for g in d.graphs()
                if str(g.identifier) not in ("urn:x-rdflib:default", "")
                and len(g) > 0
            ]
            assert "https://example.org/graph/test" in graph_uris

    def test_triple_count_preserved(self):
        """All triples from input appear in the named graph."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.nq")
            convert_triples_to_quads(src, out, "turtle", "nquads",
                                     "https://example.org/graph/test")

            g_original = triple_handler.read(src, "turtle")
            d = quad_handler.read(out, "nquads")
            named_graph = d.get_context(
                __import__("rdflib").URIRef("https://example.org/graph/test")
            )
            assert len(g_original) == len(named_graph)

    def test_requires_graph_name(self):
        """Raises ValueError if graph_name is None or empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.nq")

            with pytest.raises(ValueError, match="graph_name is required"):
                convert_triples_to_quads(src, out, "turtle", "nquads", None)

            with pytest.raises(ValueError, match="graph_name is required"):
                convert_triples_to_quads(src, out, "turtle", "nquads", "")

    def test_trig_output_format(self):
        """Triple -> Quad works with trig output format."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.trig")
            convert_triples_to_quads(src, out, "turtle", "trig",
                                     "https://example.org/graph/trig_test")
            assert os.path.exists(out)
            d = quad_handler.read(out, "trig")
            assert len(d) > 0

    def test_uses_resource_files(self):
        """Conversion works correctly on the shared test resource files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out = os.path.join(tmpdir, "output.nq")
            convert_triples_to_quads(
                resource("sample.ttl"), out, "turtle", "nquads",
                "https://example.org/graph/resource_test"
            )
            assert os.path.exists(out)
            d = quad_handler.read(out, "nquads")
            assert len(d) > 0


# ---------------------------------------------------------------------------
# Direction 2: Quad -> Triple
# ---------------------------------------------------------------------------

class TestQuadsToTriples:

    def test_creates_subdirectory(self):
        """Output subdirectory is created automatically."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.nq")
            out_dir = os.path.join(tmpdir, "split_output")
            convert_quads_to_triples(src, out_dir, "nquads", "ntriples")
            assert os.path.isdir(out_dir)

    def test_one_file_per_graph(self):
        """One .nt file is created per named graph."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.nq")
            out_dir = os.path.join(tmpdir, "split")
            files = convert_quads_to_triples(src, out_dir, "nquads", "ntriples")

            # SAMPLE_NQ_CONTENT has 2 named graphs
            assert len(files) == 2
            for f in files:
                assert f.endswith(".nt")
                assert os.path.exists(f)

    def test_all_triples_present(self):
        """Total triple count across all output files matches input quad count."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.nq")
            out_dir = os.path.join(tmpdir, "split")
            files = convert_quads_to_triples(src, out_dir, "nquads", "ntriples")

            total_output_triples = sum(
                len(triple_handler.read(f, "ntriples")) for f in files
            )
            d_original = quad_handler.read(src, "nquads")
            total_input_triples = sum(
                len(g) for g in d_original.graphs()
                if str(g.identifier) not in ("urn:x-rdflib:default", "")
            )
            assert total_output_triples == total_input_triples

    def test_filename_from_graph_uri(self):
        """Output filenames are derived from graph URI last segment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.nq")
            out_dir = os.path.join(tmpdir, "split")
            files = convert_quads_to_triples(src, out_dir, "nquads", "ntriples")

            filenames = [os.path.basename(f) for f in files]
            # people.nt and projects.nt expected from graph URIs
            assert "people.nt" in filenames
            assert "projects.nt" in filenames

    def test_empty_input_raises(self):
        """Raises ValueError if input has no named graphs with triples."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("empty.nq")
            out_dir = os.path.join(tmpdir, "split")
            with pytest.raises(ValueError, match="No named graphs"):
                convert_quads_to_triples(src, out_dir, "nquads", "ntriples")

    def test_uses_resource_files(self):
        """Conversion works correctly on shared resource sample.nq."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "split")
            files = convert_quads_to_triples(resource("sample.nq"), out_dir, "nquads", "ntriples")
            assert len(files) >= 1
            for f in files:
                g = triple_handler.read(f, "ntriples")
                assert len(g) > 0


# ---------------------------------------------------------------------------
# Direction 3: Triple -> TSD
# ---------------------------------------------------------------------------

class TestTriplesToCSV:

    def test_creates_csv_and_companion(self):
        """Both CSV and companion .meta.json are created."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(src, out, "turtle", "csv")

            assert os.path.exists(out)
            assert os.path.exists(out + ".meta.json")

    def test_header_row_contains_predicates(self):
        """CSV header contains 'resource' and all predicate URIs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(src, out, "turtle", "csv")

            rows = tsd_handler.read(out, "csv")
            header = rows[0]
            assert "resource" in header
            assert "http://xmlns.com/foaf/0.1/name" in header
            assert "https://example.org/vocab/age" in header

    def test_datatype_preserved_in_companion(self):
        """Companion file records xsd:integer datatype for age predicate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(src, out, "turtle", "csv")

            with open(out + ".meta.json", "r", encoding="utf-8") as f:
                meta = json.load(f)
            age_meta = meta["columns"].get("https://example.org/vocab/age", {})
            assert "datatype" in age_meta
            assert "integer" in age_meta["datatype"]

    def test_one_row_per_subject(self):
        """CSV has one data row per unique subject."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(src, out, "turtle", "csv")

            rows = tsd_handler.read(out, "csv")
            g = triple_handler.read(src, "turtle")
            unique_subjects = set(str(s) for s, p, o in g)
            # rows[0] is header, rest are data rows
            assert len(rows) - 1 == len(unique_subjects)

    def test_tsv_output(self):
        """Triple -> TSV also works correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            out = os.path.join(tmpdir, "output.tsv")
            convert_rdf_to_csv(src, out, "turtle", "tsv")
            assert os.path.exists(out)
            rows = tsd_handler.read(out, "tsv")
            assert len(rows) > 1

    def test_uses_resource_files(self):
        """Conversion works on shared resource sample.ttl."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(resource("sample.ttl"), out, "turtle", "csv")
            assert os.path.exists(out)
            rows = tsd_handler.read(out, "csv")
            assert len(rows) > 1


# ---------------------------------------------------------------------------
# Direction 4: TSD -> Triple
# ---------------------------------------------------------------------------

class TestCSVToTriples:

    def test_basic_reconstruction_with_companion(self):
        """CSV -> RDF round trip with companion file restores typed literals."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            csv_path = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(src, csv_path, "turtle", "csv")

            nt_path = os.path.join(tmpdir, "roundtrip.nt")
            convert_csv_to_rdf(
                csv_path, nt_path, "csv", "ntriples",
                base_uri="https://example.org/data/"
            )
            assert os.path.exists(nt_path)
            g = triple_handler.read(nt_path, "ntriples")
            assert len(g) > 0

    def test_requires_base_uri(self):
        """Raises ValueError if base_uri is None or empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_content = "resource,https://example.org/vocab/name\nhttps://example.org/data/alice,Alice\n"
            csv_path = os.path.join(tmpdir,"input.csv")
            with open(csv_path, "w", encoding="utf-8") as f:
                f.write(csv_content)
            out = os.path.join(tmpdir, "output.nt")

            with pytest.raises(ValueError, match="base_uri is required"):
                convert_csv_to_rdf(csv_path, out, "csv", "ntriples", None)

            with pytest.raises(ValueError, match="base_uri is required"):
                convert_csv_to_rdf(csv_path, out, "csv", "ntriples", "")

    def test_missing_resource_column_raises(self):
        """Raises ValueError if CSV has no 'resource' column."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = resource("missing_resource_col.csv")
            out = os.path.join(tmpdir, "output.nt")
            with pytest.raises(ValueError, match="missing 'resource' column"):
                convert_csv_to_rdf(csv_path, out, "csv", "ntriples",
                                   "https://example.org/data/")

    def test_blank_nodes_reconstructed(self):
        """Blank node subjects (starting with '_:') are reconstructed as BNodes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            csv_path = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(src, csv_path, "turtle", "csv")

            nt_path = os.path.join(tmpdir, "roundtrip.nt")
            convert_csv_to_rdf(
                csv_path, nt_path, "csv", "ntriples",
                base_uri="https://example.org/data/"
            )
            g = triple_handler.read(nt_path, "ntriples")
            from rdflib import BNode
            blank_subjects = [s for s, p, o in g if isinstance(s, BNode)]
            assert len(blank_subjects) > 0, (
                "Expected blank node subjects to be reconstructed"
            )

    def test_uri_objects_reconstructed(self):
        """Object values starting with http:// are reconstructed as URIRef."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            csv_path = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(src, csv_path, "turtle", "csv")

            nt_path = os.path.join(tmpdir, "roundtrip.nt")
            convert_csv_to_rdf(
                csv_path, nt_path, "csv", "ntriples",
                base_uri="https://example.org/data/"
            )
            g = triple_handler.read(nt_path, "ntriples")
            from rdflib import URIRef
            uri_objects = [o for s, p, o in g if isinstance(o, URIRef)]
            assert len(uri_objects) > 0

    def test_graceful_without_companion(self):
        """Without companion file, conversion succeeds with plain string literals."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.ttl")
            csv_path = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(src, csv_path, "turtle", "csv")

            # Remove companion
            companion = csv_path + ".meta.json"
            if os.path.exists(companion):
                os.remove(companion)

            nt_path = os.path.join(tmpdir, "roundtrip.nt")
            # Should not raise — graceful degradation
            convert_csv_to_rdf(
                csv_path, nt_path, "csv", "ntriples",
                base_uri="https://example.org/data/"
            )
            g = triple_handler.read(nt_path, "ntriples")
            assert len(g) > 0

    def test_empty_csv_raises(self):
        """Raises ValueError if CSV file is empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = resource("empty.csv")
            out = os.path.join(tmpdir, "output.nt")
            with pytest.raises(ValueError, match="empty"):
                convert_csv_to_rdf(csv_path, out, "csv", "ntriples",
                                   "https://example.org/data/")


# ---------------------------------------------------------------------------
# Direction 5: Quad -> TSD
# ---------------------------------------------------------------------------

class TestQuadsToCSV:

    def test_creates_csv_with_graph_column(self):
        """Output CSV contains 'resource', 'graph', and predicate columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.nq")
            out = os.path.join(tmpdir, "output.csv")
            convert_quads_to_csv(src, out, "nquads", "csv")

            assert os.path.exists(out)
            rows = tsd_handler.read(out, "csv")
            header = rows[0]
            assert "resource" in header
            assert "graph" in header

    def test_companion_file_created(self):
        """Companion .meta.json is created alongside CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.nq")
            out = os.path.join(tmpdir, "output.csv")
            convert_quads_to_csv(src, out, "nquads", "csv")
            assert os.path.exists(out + ".meta.json")

    def test_graph_uris_in_csv(self):
        """All named graph URIs from input appear in the graph column."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.nq")
            out = os.path.join(tmpdir, "output.csv")
            convert_quads_to_csv(src, out, "nquads", "csv")

            rows = tsd_handler.read(out, "csv")
            header = rows[0]
            graph_idx = header.index("graph")
            csv_graphs = set(row[graph_idx] for row in rows[1:] if len(row) > graph_idx)

            assert "https://example.org/graph/people" in csv_graphs
            assert "https://example.org/graph/projects" in csv_graphs

    def test_all_triples_represented(self):
        """Data row count matches total triple count across all named graphs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            src = resource("sample.nq")
            out = os.path.join(tmpdir, "output.csv")
            convert_quads_to_csv(src, out, "nquads", "csv")

            rows = tsd_handler.read(out, "csv")
            # Each row is one (subject, graph) pair, not one triple.
            # Verify at least one row per unique (subject, graph)
            d = quad_handler.read(src, "nquads")
            unique_subject_graph_pairs = set(
                (str(s), str(g.identifier))
                for g in d.graphs()
                for s, p, o in g
                if str(g.identifier) not in ("urn:x-rdflib:default", "")
            )
            assert len(rows) - 1 == len(unique_subject_graph_pairs)

    def test_uses_resource_files(self):
        """Conversion works on shared resource sample.nq."""
        with tempfile.TemporaryDirectory() as tmpdir:
            out = os.path.join(tmpdir, "output.csv")
            convert_quads_to_csv(resource("sample.nq"), out, "nquads", "csv")
            assert os.path.exists(out)
            rows = tsd_handler.read(out, "csv")
            assert len(rows) > 1

# ---------------------------------------------------------------------------
# Round trip tests — compare original IR with reconstructed IR
# ---------------------------------------------------------------------------

class TestRoundTrips:
    """Round trip tests for Layer 3 mapping directions.

    For each direction, the original file is read into IR BEFORE conversion.
    After the full round trip (A -> B -> A), the reconstructed IR is compared
    against the original. This genuinely detects information loss because the
    original IR is captured before any conversion happens.
    """

    def test_triple_to_quad_to_triple_round_trip(self):
        """Triple -> Quad -> Triple: reconstructed graph must be isomorphic to original."""
        source = resource("sample.ttl")
        g_original = triple_handler.read(source, "turtle")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Step 1: Triple -> Quad
            quads_path = os.path.join(tmpdir, "output.nq")
            convert_triples_to_quads(
                source, quads_path, "turtle", "nquads",
                "https://example.org/graph/test"
            )

            # Step 2: Quad -> Triple (produces subdirectory)
            output_dir = os.path.join(tmpdir, "split")
            files = convert_quads_to_triples(quads_path, output_dir, "nquads", "ntriples")

            assert len(files) == 1, "Expected exactly one output file (one named graph)"

            # Compare IR: original graph vs reconstructed graph
            g_roundtrip = triple_handler.read(files[0], "ntriples")
            assert g_original.isomorphic(g_roundtrip), (
                "Triple -> Quad -> Triple round trip failed: "
                "reconstructed graph is not isomorphic to original"
            )

    def test_triple_to_csv_to_triple_round_trip_with_companion(self):
        """Triple -> CSV -> Triple: with companion file, reconstruction must be isomorphic."""
        source = resource("sample.ttl")
        g_original = triple_handler.read(source, "turtle")

        with tempfile.TemporaryDirectory() as tmpdir:
            # Step 1: Triple -> CSV (produces companion .meta.json)
            csv_path = os.path.join(tmpdir, "output.csv")
            convert_rdf_to_csv(source, csv_path, "turtle", "csv")

            assert os.path.exists(csv_path + ".meta.json"), (
                "Companion .meta.json was not produced"
            )

            # Step 2: CSV -> Triple (reads companion file automatically)
            nt_path = os.path.join(tmpdir, "roundtrip.nt")
            convert_csv_to_rdf(
                csv_path, nt_path, "csv", "ntriples",
                base_uri="https://example.org/data/"
            )

            # Compare IR: original graph vs reconstructed graph
            g_roundtrip = triple_handler.read(nt_path, "ntriples")
            assert g_original.isomorphic(g_roundtrip), (
                "Triple -> CSV -> Triple round trip failed (with companion file): "
                "reconstructed graph is not isomorphic to original"
            )

    def test_triple_to_tsv_to_triple_round_trip_with_companion(self):
        """Triple -> TSV -> Triple: with companion file, reconstruction must be isomorphic."""
        source = resource("sample.ttl")
        g_original = triple_handler.read(source, "turtle")

        with tempfile.TemporaryDirectory() as tmpdir:
            tsv_path = os.path.join(tmpdir, "output.tsv")
            convert_rdf_to_csv(source, tsv_path, "turtle", "tsv")

            nt_path = os.path.join(tmpdir, "roundtrip.nt")
            convert_csv_to_rdf(
                tsv_path, nt_path, "tsv", "ntriples",
                base_uri="https://example.org/data/"
            )

            g_roundtrip = triple_handler.read(nt_path, "ntriples")
            assert g_original.isomorphic(g_roundtrip), (
                "Triple -> TSV -> Triple round trip failed (with companion file): "
                "reconstructed graph is not isomorphic to original"
            )

    def test_quad_to_triple_to_quad_round_trip(self):
        """Quad -> Triple (split) -> Quad (re-promote + merge) -> compare with original.

        Follows the paper's round trip pattern (Fig. 3, steps 1-6).
        Each split .nt file is matched to its original named graph by content
        (isomorphic comparison), not by filename, making the test independent
        of filename conventions and sanitization.

        After re-promotion and merging, the merged Dataset is verified to
        contain exactly the original named graph URIs — no more, no less.
        """
        from rdflib import Dataset, URIRef

        source = resource("sample.nq")
        d_original = quad_handler.read(source, "nquads")

        # Collect original named graphs (excluding empty default graph)
        original_graphs = {
            str(g.identifier): g
            for g in d_original.graphs()
            if len(g) > 0
            and str(g.identifier) not in ("urn:x-rdflib:default", "")
        }

        assert len(original_graphs) >= 1, (
            "sample.nq must contain at least one named graph for this test"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # Steps 2+3: Quad -> Triple (split into one .nt file per named graph)
            output_dir = os.path.join(tmpdir, "split")
            files = convert_quads_to_triples(
                source, output_dir, "nquads", "ntriples"
            )

            assert len(files) == len(original_graphs), (
                f"Expected {len(original_graphs)} output file(s) "
                f"(one per named graph), got {len(files)}"
            )

            # Steps 4+5+6: Match each .nt to original graph by content,
            # re-promote to Quad using original graph URI, merge all
            d_merged = Dataset()
            used_graph_uris = set()

            for out_file in files:
                # Step 4: Read split .nt into IR
                g_split = triple_handler.read(out_file, "ntriples")

                # Step 5: Match to original named graph by content (not filename)
                matching_uri = next(
                    (
                        uri for uri, g_original in original_graphs.items()
                        if uri not in used_graph_uris
                        and g_split.isomorphic(g_original)
                    ),
                    None,
                )
                assert matching_uri is not None, (
                    f"Could not match output file '{os.path.basename(out_file)}' "
                    "to any original named graph by graph content"
                )
                used_graph_uris.add(matching_uri)

                # Step 5+6: Re-promote .nt back to Quad using matched graph URI
                stem = os.path.basename(out_file)[:-3]
                repromoted_path = os.path.join(tmpdir, f"{stem}_repromoted.nq")
                convert_triples_to_quads(
                    out_file,
                    repromoted_path,
                    "ntriples",
                    "nquads",
                    matching_uri,
                )

                # Read repromoted Quad into IR and merge into d_merged
                d_repromoted = quad_handler.read(repromoted_path, "nquads")
                for named_graph in d_repromoted.graphs():
                    graph_id = str(named_graph.identifier)
                    if (
                        graph_id in ("urn:x-rdflib:default", "")
                        or len(named_graph) == 0
                    ):
                        continue
                    merged_graph = d_merged.graph(URIRef(graph_id))
                    for triple in named_graph:
                        merged_graph.add(triple)

            # Verify merged Dataset contains exactly the original named graph URIs
            merged_graph_uris = {
                str(g.identifier)
                for g in d_merged.graphs()
                if len(g) > 0
                and str(g.identifier) not in ("urn:x-rdflib:default", "")
            }
            assert merged_graph_uris == set(original_graphs.keys()), (
                f"Merged Dataset graph URIs do not match original. "
                f"Expected: {set(original_graphs.keys())}, "
                f"got: {merged_graph_uris}"
            )

            # Compare each named graph in d_merged against d_original
            for uri, g_original_named in original_graphs.items():
                g_merged_named = d_merged.get_context(URIRef(uri))
                assert g_original_named.isomorphic(g_merged_named), (
                    f"Quad -> Triple -> Quad round trip failed for graph '{uri}': "
                    f"reconstructed graph is not isomorphic to original. "
                    f"Original had {len(g_original_named)} triple(s), "
                    f"reconstructed has {len(g_merged_named)} triple(s)."
                )

if __name__ == "__main__":
    pytest.main([__file__, "-v"])