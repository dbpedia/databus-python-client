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


@pytest.mark.skip(
    reason="Live collection download is long-running and flakes on network timeouts"
)
@pytest.mark.skip(
    reason="Integration test: requires live databus.dbpedia.org connection"
)
def test_with_collection():
    api_download("tmp", DEFAULT_ENDPOINT, [TEST_COLLECTION])

def test_404_records_failed_manifest_entry(monkeypatch):
    from databusclient.manifest.context import ManifestContext
    import databusclient.api.download as dl

    class FakeHeadResp:
        status_code = 200
        headers = {}

    class FakeGetResp:
        status_code = 404
        headers = {"content-length": "0"}

        def raise_for_status(self):
            import requests
            raise requests.exceptions.HTTPError(response=self)

    monkeypatch.setattr("requests.head", lambda *a, **k: FakeHeadResp())
    monkeypatch.setattr("requests.get", lambda *a, **k: FakeGetResp())

    ctx = ManifestContext(command="download")
    dl._download_file("https://databus.dbpedia.org/account/notexisting", localDir=".", manifest_context=ctx)

    assert len(ctx.files) == 1
    assert ctx.files[0]["status"] == "failed"
    assert ctx.files[0]["error_message"] == "404 Not Found"