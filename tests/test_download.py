"""Download Tests"""

import bz2

import pytest
import requests
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


def test_cli_reports_redirected_download_timeout_without_traceback(monkeypatch):
    request = requests.Request(
        "HEAD", "https://data.dbpedia.org/repo/example/data.ttl"
    ).prepare()

    def timeout(**_kwargs):
        raise requests.exceptions.ConnectTimeout(request=request)

    monkeypatch.setattr("databusclient.cli.api_download", timeout)

    result = CliRunner().invoke(
        app, ["download", "https://databus.dbpedia.org/example/data.ttl"]
    )

    assert result.exit_code == 1
    assert (
        "Error: Could not connect to data.dbpedia.org: "
        "the request timed out."
    ) in result.output
    assert "Traceback" not in result.output


def test_cli_reports_connection_failure_without_traceback(monkeypatch):
    request = requests.Request("GET", "https://files.example.org/data.ttl").prepare()

    def connection_error(**_kwargs):
        raise requests.exceptions.ConnectionError(request=request)

    monkeypatch.setattr("databusclient.cli.api_download", connection_error)

    result = CliRunner().invoke(
        app, ["download", "https://databus.dbpedia.org/example/data.ttl"]
    )

    assert result.exit_code == 1
    assert "Error: Could not connect to files.example.org." in result.output
    assert "Check the server address and your network connection." in result.output
    assert "Traceback" not in result.output


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


def test_resume_partial_download_appends_bytes(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    target_file.write_bytes(b"hello ")

    recorded_headers = {}

    class MockHeadResp:
        status_code = 200
        headers = {"content-length": "12"}

    class MockPartialGetResp:
        status_code = 206

        def __init__(self):
            self.content = b"world!"
            self.headers = {"content-length": str(len(self.content))}

        def iter_content(self, block_size):
            yield self.content

        def raise_for_status(self):
            pass

    def mock_head(req_url, *args, **kwargs):
        return MockHeadResp()

    def mock_get(req_url, *args, **kwargs):
        if "headers" in kwargs:
            recorded_headers.update(kwargs["headers"])
        return MockPartialGetResp()

    monkeypatch.setattr("requests.head", mock_head)
    monkeypatch.setattr("requests.get", mock_get)

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert recorded_headers.get("Range") == "bytes=6-"
    assert target_file.read_bytes() == b"hello world!"


def test_resume_server_falls_back_to_200(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    target_file.write_bytes(b"old_partial_data")

    class MockHeadResp:
        status_code = 200
        headers = {"content-length": "18"}

    class MockFullGetResp:
        status_code = 200

        def __init__(self):
            self.content = b"fresh_full_content"
            self.headers = {"content-length": str(len(self.content))}

        def iter_content(self, block_size):
            yield self.content

        def raise_for_status(self):
            pass

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())
    monkeypatch.setattr("requests.get", lambda *a, **k: MockFullGetResp())

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert target_file.read_bytes() == b"fresh_full_content"


def test_resume_already_complete_file_416(monkeypatch, tmp_path):
    import requests
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    content = b"already completed content"
    target_file.write_bytes(content)

    class MockHeadResp:
        status_code = 200
        headers = {}  # HEAD does not include content-length, forcing GET

    class Mock416GetResp:
        status_code = 416
        headers = {}

        def raise_for_status(self):
            raise requests.exceptions.HTTPError("416 Range Not Satisfiable", response=self)

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())
    monkeypatch.setattr("requests.get", lambda *a, **k: Mock416GetResp())

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert target_file.read_bytes() == content


def test_resume_already_complete_file_via_head(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    content = b"already completed content"
    target_file.write_bytes(content)

    get_called = False

    class MockHeadResp:
        status_code = 200
        headers = {"content-length": str(len(content))}

    def mock_get(*args, **kwargs):
        nonlocal get_called
        get_called = True
        raise AssertionError("GET should not be called when HEAD confirms file is complete")

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())
    monkeypatch.setattr("requests.get", mock_get)

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert not get_called
    assert target_file.read_bytes() == content


def test_cli_resume_flag(monkeypatch, tmp_path):
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
            "--resume",
            "--manifest",
            str(manifest_path),
        ],
    )

    assert result.exit_code == 0
    assert captured["resume"] is True
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["dbus:replayParams"]["resume"] is True

    # Also test short alias -c
    result_short = CliRunner().invoke(
        app,
        [
            "download",
            "https://example.org/data.ttl",
            "-c",
        ],
    )
    assert result_short.exit_code == 0
    assert captured["resume"] is True


def test_resume_content_range_mismatch_restarts(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    target_file.write_bytes(b"corrupt")

    calls = []

    class MockHeadResp:
        status_code = 200
        headers = {"content-length": "10"}

    class MockMismatch206GetResp:
        status_code = 206

        def __init__(self):
            self.content = b"partial_content_wrong_offset"
            self.headers = {
                "content-length": "10",
                "Content-Range": "bytes 0-9/10",
            }

        def iter_content(self, block_size):
            yield self.content

        def raise_for_status(self):
            pass

    class MockFull200GetResp:
        status_code = 200

        def __init__(self):
            self.content = b"0123456789"
            self.headers = {"content-length": "10"}

        def iter_content(self, block_size):
            yield self.content

        def raise_for_status(self):
            pass

    def mock_get(req_url, *args, **kwargs):
        headers = kwargs.get("headers", {})
        if "Range" in headers:
            calls.append("range")
            return MockMismatch206GetResp()
        calls.append("full")
        return MockFull200GetResp()

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())
    monkeypatch.setattr("requests.get", mock_get)

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert calls == ["range", "full"]
    # File should have been restarted and overwritten with the full payload
    assert target_file.read_bytes() == b"0123456789"


def test_resume_416_mismatch_restarts(monkeypatch, tmp_path):
    import requests
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    target_file.write_bytes(b"wrong_length_content_12345")

    calls = []

    class MockHeadResp:
        status_code = 200
        headers = {}

    class Mock416GetResp:
        status_code = 416
        headers = {"Content-Range": "bytes */10"}

        def raise_for_status(self):
            raise requests.exceptions.HTTPError("416", response=self)

    class MockFullGetResp:
        status_code = 200
        headers = {"content-length": "10"}

        def iter_content(self, block_size):
            yield b"0123456789"

        def raise_for_status(self):
            pass

    def mock_get(req_url, *args, **kwargs):
        if not calls:
            calls.append("416")
            return Mock416GetResp()
        calls.append("200")
        return MockFullGetResp()

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())
    monkeypatch.setattr("requests.get", mock_get)

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert calls == ["416", "200"]
    assert target_file.read_bytes() == b"0123456789"


def test_resume_if_range_header_sent(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    target_file.write_bytes(b"hello ")

    recorded_headers = {}

    class MockHeadResp:
        status_code = 200
        headers = {"content-length": "12", "ETag": '"etag-val-123"'}

    class MockPartialGetResp:
        status_code = 206
        headers = {"content-length": "6", "Content-Range": "bytes 6-11/12"}

        def iter_content(self, block_size):
            yield b"world!"

        def raise_for_status(self):
            pass

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())

    def mock_get(req_url, *args, **kwargs):
        if "headers" in kwargs:
            recorded_headers.update(kwargs["headers"])
        return MockPartialGetResp()

    monkeypatch.setattr("requests.get", mock_get)

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert recorded_headers.get("Range") == "bytes=6-"
    assert recorded_headers.get("If-Range") == '"etag-val-123"'
    assert target_file.read_bytes() == b"hello world!"


def test_manifest_replay_forwards_resume():
    from databusclient.manifest.replay import _build_download_kwargs

    manifest = {"@id": "https://example.org/manifest"}
    replay_params = {
        "databusURIs": ["https://databus.dbpedia.org/user/group/artifact/version"],
        "resume": True,
    }
    kwargs = _build_download_kwargs(manifest, replay_params, overrides={})
    assert kwargs.get("resume") is True


def test_resume_chunked_206_unknown_total(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    target_file.write_bytes(b"hello ")

    class MockHeadResp:
        status_code = 200
        headers = {}  # No content-length

    class MockChunked206GetResp:
        status_code = 206

        def __init__(self):
            self.content = b"world!"
            self.headers = {
                "Content-Range": "bytes 6-11/*",
            }

        def iter_content(self, block_size):
            yield self.content

        def raise_for_status(self):
            pass

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())
    monkeypatch.setattr("requests.get", lambda *a, **k: MockChunked206GetResp())

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert target_file.read_bytes() == b"hello world!"


def test_head_request_includes_accept_encoding_identity(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    head_headers = {}

    class MockHeadResp:
        status_code = 200
        headers = {"content-length": "5"}

    class MockGetResp:
        status_code = 200
        headers = {"content-length": "5"}

        def iter_content(self, block_size):
            yield b"12345"

        def raise_for_status(self):
            pass

    def mock_head(req_url, *args, **kwargs):
        head_headers.update(kwargs.get("headers", {}))
        return MockHeadResp()

    monkeypatch.setattr("requests.head", mock_head)
    monkeypatch.setattr("requests.get", lambda *a, **k: MockGetResp())

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    assert head_headers.get("Accept-Encoding") == "identity"


def test_resume_mismatched_range_start_replacement_404_handled(monkeypatch, tmp_path):
    import databusclient.api.download as dl
    from databusclient.manifest.context import ManifestContext

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    target_file.write_bytes(b"hello ")

    class MockHeadResp:
        status_code = 200
        headers = {"content-length": "12"}

    class Mock206WrongStartResp:
        status_code = 206
        headers = {
            "Content-Range": "bytes 0-5/12",
            "content-length": "6",
        }

        def raise_for_status(self):
            pass

    class Mock404Resp:
        status_code = 404
        headers = {}

        def raise_for_status(self):
            import requests
            raise requests.exceptions.HTTPError(response=self)

    get_calls = []

    def mock_get(req_url, *args, **kwargs):
        get_calls.append(kwargs.get("headers", {}))
        if len(get_calls) == 1:
            return Mock206WrongStartResp()
        return Mock404Resp()

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())
    monkeypatch.setattr("requests.get", mock_get)

    ctx = ManifestContext(command="download")
    # Should not raise an unhandled exception when replacement GET returns 404
    dl._download_file(url, localDir=str(tmp_path), resume=True, manifest_context=ctx)
    assert len(ctx.files) == 1
    assert ctx.files[0]["status"] == "failed"


def test_resume_server_ignores_range_returns_200(monkeypatch, tmp_path):
    import databusclient.api.download as dl

    url = "https://example.org/file.bin"
    target_file = tmp_path / "file.bin"
    target_file.write_bytes(b"old partial data")

    class MockHeadResp:
        status_code = 200
        headers = {"content-length": "10"}

    class Mock200GetResp:
        status_code = 200
        headers = {"content-length": "10"}

        def iter_content(self, block_size):
            yield b"0123456789"

        def raise_for_status(self):
            pass

    monkeypatch.setattr("requests.head", lambda *a, **k: MockHeadResp())
    monkeypatch.setattr("requests.get", lambda *a, **k: Mock200GetResp())

    dl._download_file(url, localDir=str(tmp_path), resume=True)

    # File must be overwritten cleanly with full 10 bytes, not appended
    assert target_file.read_bytes() == b"0123456789"

