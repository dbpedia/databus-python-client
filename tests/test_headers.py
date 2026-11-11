"""Tests for overriding HTTP headers and User-Agent configuration."""

import pytest
from click.testing import CliRunner

from databusclient.api.utils import parse_headers, get_default_headers, fetch_databus_jsonld
from databusclient.api.delete import DeleteQueue, delete as api_delete
from databusclient.api.deploy import create_dataset, deploy_from_metadata
from databusclient.api.download import (
    _download_file,
    _query_sparql_endpoint,
    __get_vault_access__,
    download as api_download,
)
from databusclient.cli import app
from databusclient.version import __version__
from databusclient.workflow.context import StepContext
from databusclient.workflow.steps import DownloadStep


def test_parse_headers_none_and_empty():
    assert parse_headers(None) == {}
    assert parse_headers([]) == {}
    assert parse_headers({}) == {}


def test_parse_headers_dict():
    input_dict = {"User-Agent": "MyBot/1.0", "X-Custom": "val123"}
    parsed = parse_headers(input_dict)
    assert parsed == {"User-Agent": "MyBot/1.0", "X-Custom": "val123"}
    # Verify non-string types get stringified
    assert parse_headers({"X-Num": 42}) == {"X-Num": "42"}


def test_parse_headers_iterable_strings():
    header_list = [
        "User-Agent: MyBot/1.0",
        "Authorization: Bearer token123",
        "Accept: application/json",
    ]
    parsed = parse_headers(header_list)
    assert parsed == {
        "User-Agent": "MyBot/1.0",
        "Authorization": "Bearer token123",
        "Accept": "application/json",
    }


def test_parse_headers_with_colons_in_value():
    headers = [
        "User-Agent: MyBot/1.0 (https://example.org/bot; contact: admin@example.org)",
        "X-Custom-Time: 2026-10-25T10:00:00Z",
    ]
    parsed = parse_headers(headers)
    assert (
        parsed["User-Agent"]
        == "MyBot/1.0 (https://example.org/bot; contact: admin@example.org)"
    )
    assert parsed["X-Custom-Time"] == "2026-10-25T10:00:00Z"


def test_parse_headers_invalid_format():
    with pytest.raises(ValueError, match="Invalid header format"):
        parse_headers(["InvalidHeaderWithoutColon"])

    with pytest.raises(ValueError, match="Unexpected header type"):
        parse_headers([123])


def test_get_default_headers_without_overrides(monkeypatch):
    monkeypatch.delenv("DATABUS_USER_AGENT", raising=False)
    headers = get_default_headers()
    assert headers["User-Agent"] == (
        f"databus-python-client/{__version__} "
        "(https://github.com/dbpedia/databus-python-client)"
    )


def test_get_default_headers_env_var_override(monkeypatch):
    monkeypatch.setenv("DATABUS_USER_AGENT", "EnvBot/2.0")
    headers = get_default_headers()
    assert headers["User-Agent"] == "EnvBot/2.0"


def test_get_default_headers_explicit_override():
    headers = get_default_headers({"User-Agent": "WikidataBot/1.0"})
    assert headers["User-Agent"] == "WikidataBot/1.0"


def test_get_default_headers_case_insensitive_user_agent():
    headers = get_default_headers(["user-agent: CustomCaseBot/1.0"])
    # Should not duplicate User-Agent
    assert any(k.lower() == "user-agent" for k in headers)
    ua_keys = [k for k in headers if k.lower() == "user-agent"]
    assert len(ua_keys) == 1
    assert headers[ua_keys[0]] == "CustomCaseBot/1.0"


def test_get_default_headers_preserves_custom_headers():
    headers = get_default_headers({"X-App-ID": "test-app", "Accept-Language": "de"})
    assert headers["X-App-ID"] == "test-app"
    assert headers["Accept-Language"] == "de"
    assert "User-Agent" in headers


def test_fetch_databus_jsonld_sends_headers(monkeypatch):
    captured_headers = {}

    class FakeResponse:
        status_code = 200
        text = '{"@id": "https://databus.dbpedia.org/test"}'

        def raise_for_status(self):
            pass

    def fake_get(url, headers=None, timeout=30):
        captured_headers.update(headers or {})
        return FakeResponse()

    monkeypatch.setattr("requests.get", fake_get)
    fetch_databus_jsonld(
        "https://databus.dbpedia.org/test",
        headers={"User-Agent": "JsonLdBot/1.0", "X-Trace": "abc"},
    )

    assert captured_headers.get("User-Agent") == "JsonLdBot/1.0"
    assert captured_headers.get("X-Trace") == "abc"
    assert captured_headers.get("Accept") == "application/ld+json"


def test_download_file_sends_custom_headers(tmp_path, monkeypatch):
    head_headers = {}
    get_headers = {}

    class FakeHead:
        status_code = 200
        headers = {}

    class FakeGet:
        status_code = 200
        headers = {"content-length": "13"}

        def iter_content(self, chunk_size):
            yield b"dummy content"

        def raise_for_status(self):
            pass

    def fake_head(url, headers=None, timeout=30, allow_redirects=False):
        head_headers.update(headers or {})
        return FakeHead()

    def fake_get(url, headers=None, stream=True, allow_redirects=True, timeout=30):
        get_headers.update(headers or {})
        return FakeGet()

    monkeypatch.setattr("requests.head", fake_head)
    monkeypatch.setattr("requests.get", fake_get)

    target_url = "https://databus.dbpedia.org/org/group/art/1.0/file.ttl"
    _download_file(
        url=target_url,
        localDir=str(tmp_path),
        headers={"User-Agent": "CustomDownloader/1.0", "X-Custom-Auth": "secret123"},
    )

    assert head_headers.get("User-Agent") == "CustomDownloader/1.0"
    assert head_headers.get("X-Custom-Auth") == "secret123"
    assert get_headers.get("User-Agent") == "CustomDownloader/1.0"
    assert get_headers.get("X-Custom-Auth") == "secret123"


def test_query_sparql_endpoint_sets_headers(monkeypatch):
    class MockSPARQL:
        def __init__(self, endpoint):
            self.endpoint = endpoint
            self.method = None
            self.customHttpHeaders = {}
            self.agent = None

        def setQuery(self, q):
            pass

        def setReturnFormat(self, f):
            pass

        def addCustomHttpHeader(self, k, v):
            self.customHttpHeaders[k] = v

        def query(self):
            class R:
                def convert(self):
                    return {"results": {"bindings": []}}

            return R()

    monkeypatch.setattr("databusclient.api.download.SPARQLWrapper", MockSPARQL)

    result = _query_sparql_endpoint(
        "https://query.wikidata.org/sparql",
        "SELECT * WHERE { ?s ?p ?o } LIMIT 1",
        headers={"User-Agent": "WikidataSPARQLBot/1.0", "X-Request-ID": "req-99"},
    )
    assert result == {"results": {"bindings": []}}


def test_cli_download_with_header_flag(tmp_path, monkeypatch):
    captured_headers = None

    def fake_download(*args, **kwargs):
        nonlocal captured_headers
        captured_headers = kwargs.get("headers")

    monkeypatch.setattr("databusclient.cli.api_download", fake_download)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "download",
            "https://databus.dbpedia.org/test/artifact/1.0/file.ttl",
            "--localdir",
            str(tmp_path),
            "-H",
            "User-Agent: CLIBot/1.0",
            "-H",
            "X-Extra: value",
        ],
    )

    assert result.exit_code == 0
    assert captured_headers == {
        "User-Agent": "CLIBot/1.0",
        "X-Extra": "value",
    }


def test_cli_download_invalid_header_error():
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "download",
            "https://databus.dbpedia.org/test/artifact/1.0/file.ttl",
            "-H",
            "MalformedHeaderWithoutColon",
        ],
    )
    assert result.exit_code != 0
    assert "Invalid header format" in result.output


def test_manifest_records_redacted_headers(tmp_path, monkeypatch):
    def fake_download(*args, **kwargs):
        pass

    monkeypatch.setattr("databusclient.cli.api_download", fake_download)

    manifest_file = tmp_path / "test-manifest.jsonld"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "download",
            "https://databus.dbpedia.org/test/artifact/1.0/file.ttl",
            "--manifest",
            str(manifest_file),
            "-H",
            "User-Agent: PublicBot/1.0",
            "-H",
            "Authorization: Bearer sensitive_token",
            "-H",
            "X-API-KEY: secret_key",
        ],
    )
    assert result.exit_code == 0
    assert manifest_file.exists()

    content = manifest_file.read_text(encoding="utf-8")
    assert "PublicBot/1.0" in content
    assert "sensitive_token" not in content
    assert "secret_key" not in content
    assert "***" in content


def test_workflow_step_executes_with_headers(monkeypatch, tmp_path):
    received_kwargs = {}

    def fake_download(*args, **kwargs):
        received_kwargs.update(kwargs)

    monkeypatch.setattr("databusclient.workflow.steps.api_download", fake_download)

    step = DownloadStep()
    ctx = StepContext()

    step.run(
        {
            "name": "download_data",
            "command": "download",
            "uri": "https://databus.dbpedia.org/test/artifact/1.0/file.ttl",
            "localdir": str(tmp_path),
            "headers": {
                "User-Agent": "WorkflowBot/1.0",
                "X-Workflow-Run": "42",
            },
        },
        ctx,
    )

    assert received_kwargs.get("headers") == {
        "User-Agent": "WorkflowBot/1.0",
        "X-Workflow-Run": "42",
    }


def test_fetch_databus_jsonld_preserves_lowercase_accept(monkeypatch):
    captured = {}

    class FakeResp:
        text = "{}"

        def raise_for_status(self):
            pass

    def fake_get(url, headers=None, timeout=30):
        captured.update(headers or {})
        return FakeResp()

    monkeypatch.setattr("requests.get", fake_get)
    fetch_databus_jsonld(
        "https://databus.dbpedia.org/test",
        headers={"accept": "application/json"},
    )
    assert captured.get("accept") == "application/json"
    assert "Accept" not in captured


def test_cross_origin_redirect_strips_credential_headers(tmp_path, monkeypatch):
    seen = []

    class RedirectHead:
        status_code = 302
        headers = {"Location": "https://mirror.example.net/file.ttl"}

    class OkHead:
        status_code = 200
        headers = {}

    class OkGet:
        status_code = 200
        headers = {"content-length": "4"}

        def iter_content(self, chunk_size):
            yield b"data"

        def raise_for_status(self):
            pass

    head_responses = [RedirectHead(), OkHead()]

    def fake_head(url, headers=None, **kwargs):
        seen.append(("HEAD", url, dict(headers or {})))
        return head_responses.pop(0)

    def fake_get(url, headers=None, **kwargs):
        seen.append(("GET", url, dict(headers or {})))
        return OkGet()

    monkeypatch.setattr("requests.head", fake_head)
    monkeypatch.setattr("requests.get", fake_get)

    _download_file(
        "https://databus.dbpedia.org/org/group/art/1.0/file.ttl",
        localDir=str(tmp_path),
        headers={
            "User-Agent": "MirrorSafeBot/1.0",
            "Authorization": "Bearer topsecret",
            "X-API-KEY": "secret-key",
            "Cookie": "session=xyz",
        },
    )

    assert "Authorization" in seen[0][2]
    assert "Authorization" not in seen[1][2]
    assert "X-API-KEY" not in seen[1][2]
    assert "Cookie" not in seen[1][2]
    assert seen[1][2]["User-Agent"] == "MirrorSafeBot/1.0"
    assert "Authorization" not in seen[2][2]
    assert seen[2][2]["User-Agent"] == "MirrorSafeBot/1.0"


def test_vault_access_strips_download_credentials(tmp_path, monkeypatch):
    token_file = tmp_path / "refresh.tok"
    token_file.write_text("x" * 90, encoding="utf-8")
    posted_headers = []

    class TokenResp:
        def raise_for_status(self):
            pass

        def json(self):
            return {"access_token": "minted-token"}

    def fake_post(url, data=None, headers=None, timeout=30):
        posted_headers.append(dict(headers or {}))
        return TokenResp()

    monkeypatch.setattr("requests.post", fake_post)
    tok = __get_vault_access__(
        "https://data.dbpedia.io/org/group/art/1.0/file.ttl",
        str(token_file),
        "https://auth.example.org/token",
        "vault-client",
        headers={
            "User-Agent": "VaultBot/1.0",
            "Authorization": "Bearer download-only",
            "X-API-KEY": "databus-secret",
        },
    )
    assert tok == "minted-token"
    assert len(posted_headers) == 2
    for hdrs in posted_headers:
        assert hdrs["User-Agent"] == "VaultBot/1.0"
        assert "Authorization" not in hdrs
        assert "X-API-KEY" not in hdrs


def test_delete_queue_and_group_forward_headers(monkeypatch):
    deleted = []
    fetched = []

    class FakeResp:
        status_code = 200
        text = "ok"

    monkeypatch.setattr(
        "databusclient.api.delete.fetch_databus_jsonld",
        lambda uri, key=None, headers=None: (
            fetched.append((uri, headers)),
            '{"databus:hasArtifact": [{"@id": "https://databus.dbpedia.org/acc/grp/art"}], "databus:hasVersion": [{"@id": "https://databus.dbpedia.org/acc/grp/art/1.0"}]}',
        )[1],
    )
    monkeypatch.setattr(
        "requests.delete",
        lambda uri, headers=None, timeout=30: (deleted.append((uri, dict(headers or {}))), FakeResp())[1],
    )

    custom = {"User-Agent": "DeleteBot/1.0", "X-Trace": "del-1"}
    q = DeleteQueue("k", headers=custom)
    q.add_uri("https://databus.dbpedia.org/acc/grp/art/1.0")
    q.execute()
    assert deleted[-1][1]["X-Trace"] == "del-1"

    deleted.clear()
    api_delete(
        ["https://databus.dbpedia.org/acc/grp"],
        "k",
        force=True,
        headers=custom,
    )
    assert all(hdrs.get("X-Trace") == "del-1" for _, hdrs in fetched)
    assert all(hdrs.get("X-Trace") == "del-1" for _, hdrs in deleted)


def test_deploy_helpers_forward_headers(monkeypatch):
    seen = []

    class StatResp:
        status_code = 200
        content = b"abc"

    class PostResp:
        status_code = 200
        text = "ok"

    monkeypatch.setattr(
        "requests.get",
        lambda url, headers=None, timeout=30: (seen.append(("GET", dict(headers or {}))), StatResp())[1],
    )
    monkeypatch.setattr(
        "requests.post",
        lambda url, data=None, headers=None, timeout=30: (seen.append(("POST", dict(headers or {}))), PostResp())[1],
    )

    custom = {"User-Agent": "DeployBot/1.0", "X-Deploy": "yes"}
    create_dataset(
        "https://databus.dbpedia.org/acc/grp/art/1.0",
        "Title",
        "Abstract",
        "Description",
        "https://creativecommons.org/licenses/by/4.0/",
        ["https://example.org/file.ttl|lang=en|ttl|none"],
        headers=custom,
    )
    assert seen[-1][0] == "GET" and seen[-1][1]["X-Deploy"] == "yes"

    seen.clear()
    deploy_from_metadata(
        [{"url": "https://example.org/file.ttl", "checksum": "a" * 64, "size": 10, "file_format": "ttl", "compression": "none"}],
        "https://databus.dbpedia.org/acc/grp/art/1.0",
        "Title",
        "Abstract",
        "Description",
        "https://creativecommons.org/licenses/by/4.0/",
        "api-key",
        headers=custom,
    )
    assert seen[-1][0] == "POST" and seen[-1][1]["X-Deploy"] == "yes"


def test_download_group_forwards_headers(tmp_path, monkeypatch):
    head_hdrs = []

    class FakeResp:
        status_code = 200
        headers = {"content-length": "1"}

        def iter_content(self, n):
            yield b"x"

        def raise_for_status(self):
            pass

    monkeypatch.setattr(
        "databusclient.api.download.fetch_databus_jsonld",
        lambda uri, databus_key=None, headers=None: (
            '{"databus:hasArtifact": [{"@id": "https://databus.dbpedia.org/acc/grp/art"}], '
            '"databus:hasVersion": [{"@id": "https://databus.dbpedia.org/acc/grp/art/1.0"}], '
            '"@graph": [{"@type": "Part", "file": "https://databus.dbpedia.org/acc/grp/art/1.0/f.ttl"}]}'
        ),
    )
    monkeypatch.setattr(
        "requests.head",
        lambda url, headers=None, **kw: (head_hdrs.append(dict(headers or {})), FakeResp())[1],
    )
    monkeypatch.setattr("requests.get", lambda url, headers=None, **kw: FakeResp())

    api_download(
        str(tmp_path),
        None,
        ["https://databus.dbpedia.org/acc/grp"],
        headers={"X-Group-Header": "g1"},
    )
    assert head_hdrs and head_hdrs[0]["X-Group-Header"] == "g1"


def test_manifest_redacts_cookie_and_arbitrary_headers(tmp_path, monkeypatch):
    monkeypatch.setattr("databusclient.cli.api_download", lambda *a, **k: None)
    manifest_file = tmp_path / "m.jsonld"
    result = CliRunner().invoke(
        app,
        [
            "download",
            "https://databus.dbpedia.org/test/artifact/1.0/file.ttl",
            "--manifest",
            str(manifest_file),
            "-H",
            "Cookie: session=supersecret",
            "-H",
            "Proxy-Authorization: Basic dXNlcjpwYXNz",
            "-H",
            "X-Auth-Token: customsecret",
        ],
    )
    assert result.exit_code == 0
    content = manifest_file.read_text(encoding="utf-8")
    assert "supersecret" not in content
    assert "dXNlcjpwYXNz" not in content
    assert "customsecret" not in content
