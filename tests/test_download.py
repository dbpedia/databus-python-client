"""Download Tests"""

import pytest

from databusclient.api.download import download as api_download

# TODO: overall test structure not great, needs refactoring

DEFAULT_ENDPOINT = "https://databus.dbpedia.org/sparql"
TEST_QUERY = """
PREFIX dcat: <http://www.w3.org/ns/dcat#>
SELECT ?file
WHERE {
  ?file dcat:downloadURL ?url ;
        dcat:byteSize ?size .
  FILTER(STRSTARTS(STR(?file), "https://databus.dbpedia.org/dbpedia/"))
  FILTER(xsd:integer(?size) < 104857600)
}
LIMIT 10
"""
TEST_COLLECTION = (
    "https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12"
)


def test_with_query():
    api_download("tmp", DEFAULT_ENDPOINT, [TEST_QUERY])


@pytest.mark.skip(reason="Live collection download is long-running and flakes on network timeouts")
def test_with_collection():
    api_download("tmp", DEFAULT_ENDPOINT, [TEST_COLLECTION])
