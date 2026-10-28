"""UTF-8 signatures must not become part of tabular column names."""

import csv

import pytest
from rdflib import Graph, Literal, URIRef

from databusclient.filehandling.format import TSDHandler
from databusclient.filehandling.mapping import convert_csv_to_rdf


@pytest.mark.parametrize("input_format,delimiter", [("csv", ","), ("tsv", "\t")])
@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig"])
def test_tabular_conversion_accepts_utf8_bom(tmp_path, input_format, delimiter, encoding):
    source = tmp_path / f"input.{input_format}"
    rows = [["resource", "https://example.org/name"], ["alice", "Zoë\ufeff Smith"]]
    with source.open("w", encoding=encoding, newline="") as stream:
        csv.writer(stream, delimiter=delimiter).writerows(rows)

    assert TSDHandler().read(str(source), input_format) == rows

    output = tmp_path / "output.ttl"
    convert_csv_to_rdf(
        str(source), str(output), input_format, "turtle", "https://example.org"
    )
    graph = Graph().parse(output, format="turtle")
    assert set(graph) == {
        (
            URIRef("https://example.org/alice"),
            URIRef("https://example.org/name"),
            Literal("Zoë\ufeff Smith"),
        )
    }
