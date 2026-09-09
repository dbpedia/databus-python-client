"""Download Tests"""

import bz2

import pytest
from click.testing import CliRunner

from databusclient.api.download import download as api_download
from databusclient.cli import app

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


class FakeHeadResp:
    status_code = 200
    headers = {}


class FakeGetResp:
    status_code = 200

    def __init__(self, content: bytes):
        self.content = content
        self.headers = {"content-length": str(len(content))}

    def iter_content(self, block_size):
        yield self.content

    def raise_for_status(self):
        return None


def _mock_download(monkeypatch, contents_by_url):
    monkeypatch.setattr("requests.head", lambda *a, **k: FakeHeadResp())
    monkeypatch.setattr(
        "requests.get",
        lambda url, *a, **k: FakeGetResp(contents_by_url[url]),
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


def test_default_download_creates_no_graph_file(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/data.ttl"
    _mock_download(monkeypatch, {url: b"<s> <p> <o> .\n"})

    dl._download_file(url, localDir=str(tmp_path))

    assert (tmp_path / "data.ttl").exists()
    assert not (tmp_path / "data.ttl.graph").exists()


def test_graph_mode_creates_sidecar_with_exact_final_filename_and_url(
    monkeypatch, tmp_path
):
    import databusclient.api.download as dl

    url = "https://example.org/mappingbased-objects_lang=en.ttl.bz2"
    _mock_download(monkeypatch, {url: b"compressed bytes are not inspected"})

    dl._download_file(url, localDir=str(tmp_path), graph_mode="download-url")

    downloaded = tmp_path / "mappingbased-objects_lang=en.ttl.bz2"
    sidecar = tmp_path / "mappingbased-objects_lang=en.ttl.bz2.graph"
    assert downloaded.exists()
    assert sidecar.exists()
    assert sidecar.read_text(encoding="utf-8") == url


def test_graph_mode_multiple_downloads_each_get_correct_url(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    urls = ["https://example.org/a.ttl", "https://example.org/b.ttl"]
    _mock_download(monkeypatch, {urls[0]: b"a", urls[1]: b"b"})

    dl._download_files(urls, str(tmp_path), graph_mode="download-url")

    assert (tmp_path / "a.ttl.graph").read_text(encoding="utf-8") == urls[0]
    assert (tmp_path / "b.ttl.graph").read_text(encoding="utf-8") == urls[1]


def test_failed_download_does_not_create_graph_sidecar(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    class FakeGet404Resp:
        status_code = 404
        headers = {"content-length": "0"}

        def raise_for_status(self):
            import requests

            raise requests.exceptions.HTTPError(response=self)

    monkeypatch.setattr("requests.head", lambda *a, **k: FakeHeadResp())
    monkeypatch.setattr("requests.get", lambda *a, **k: FakeGet404Resp())

    dl._download_file(
        "https://example.org/missing.ttl",
        localDir=str(tmp_path),
        graph_mode="download-url",
    )

    assert not (tmp_path / "missing.ttl").exists()
    assert not (tmp_path / "missing.ttl.graph").exists()


def test_invalid_graph_mode_value_is_rejected_by_cli():
    result = CliRunner().invoke(
        app,
        ["download", "https://example.org/data.ttl", "--graph-mode", "invalid"],
    )

    assert result.exit_code != 0
    assert "Invalid value for '--graph-mode'" in result.output


def test_cli_manifest_records_graph_mode(monkeypatch, tmp_path):
    import json

    captured = {}

    def fake_download(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.cli.api_download", fake_download)
    manifest_path = tmp_path / "download.jsonld"

    result = CliRunner().invoke(
        app,
        [
            "download",
            "https://example.org/data.ttl",
            "--graph-mode",
            "download-url",
            "--manifest",
            str(manifest_path),
        ],
    )

    assert result.exit_code == 0
    assert captured["graph_mode"] == "download-url"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["dbus:replayParams"]["graph_mode"] == "download-url"


def test_converted_output_gets_graph_sidecar_at_final_path(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/data.ttl.bz2"
    _mock_download(monkeypatch, {url: bz2.compress(b"<s> <p> <o> .\n")})

    dl._download_file(
        url,
        localDir=str(tmp_path),
        compression="none",
        graph_mode="download-url",
    )

    assert (tmp_path / "data.ttl").exists()
    assert not (tmp_path / "data.ttl.bz2").exists()
    assert not (tmp_path / "data.ttl.bz2.graph").exists()
    assert (tmp_path / "data.ttl.graph").read_text(encoding="utf-8") == url
