"""User-Agent survives CLI/API routing, redirects, auth retries, and replay."""

import json
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

import databusclient.api.download as dl
from databusclient.cli import app
from databusclient.manifest.replay import replay_manifest
from databusclient.version import __version__


CUSTOM_AGENT = "ExampleDownloadBot/1.0 (mailto:operator@example.org)"
FILE_URL = "https://example.org/account/group/artifact/2026/file.nt"


def response(status=200, headers=None):
    result = Mock(status_code=status, headers=headers or {})
    result.iter_content.return_value = iter([b"data"])
    return result


@pytest.mark.parametrize("agent", [None, CUSTOM_AGENT])
@pytest.mark.parametrize("auth", ["public", "api_key", "vault"])
def test_cli_download_headers_with_redirect_and_auth(tmp_path, monkeypatch, agent, auth):
    target = (
        f"https://{next(iter(dl.VAULT_REQUIRED_HOSTS))}/file.nt"
        if auth == "vault" else "https://example.org/file.nt"
    )
    heads = [response(302, {"Location": target}), response(401 if auth == "api_key" else 200)]
    if auth == "api_key":
        heads.append(response())
    gets = [response(401, {"WWW-Authenticate": "Bearer"}), response()] if auth == "vault" else [response()]
    calls = []

    def head(url, **kwargs):
        calls.append(("HEAD", url, dict(kwargs["headers"])))
        return heads.pop(0)

    def get(url, **kwargs):
        calls.append(("GET", url, dict(kwargs["headers"])))
        return gets.pop(0)

    monkeypatch.setattr(dl.requests, "head", head)
    monkeypatch.setattr(dl.requests, "get", get)
    monkeypatch.setattr(dl, "__get_vault_access__", lambda *args: "access-token")
    args = ["download", FILE_URL, "--localdir", str(tmp_path)]
    if agent is not None:
        args += ["--user-agent", agent]
    if auth == "api_key":
        args += ["--databus-key", "api-key"]
    if auth == "vault":
        args += ["--vault-token", "unused-token-file"]
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    expected = agent if agent is not None else (
        f"databus-python-client/{__version__} "
        "(https://github.com/dbpedia/databus-python-client)"
    )
    assert all(headers["User-Agent"] == expected for _, _, headers in calls)
    assert calls[1][1] == target
    assert not heads and not gets
    assert calls[-1][2]["Accept-Encoding"] == "identity"
    if auth == "api_key":
        assert calls[-1][2]["X-API-KEY"] == "api-key"
    if auth == "vault":
        assert calls[-1][2]["Authorization"] == "Bearer access-token"
    assert next(tmp_path.rglob("file.nt")).read_bytes() == b"data"


@pytest.mark.parametrize("target", [
    "https://example.org/account/group",
    "https://example.org/account/group/artifact",
    "https://example.org/account/group/artifact/2026",
    "https://example.org/account/collections/example",
    "SELECT ?file WHERE { ?file ?p ?o }",
])
def test_api_routes_override_to_file_requests(monkeypatch, tmp_path, target):
    monkeypatch.setattr(dl, "fetch_databus_jsonld", lambda *a, **k: "{}")
    monkeypatch.setattr(dl, "_get_databus_artifacts_of_group", lambda *a: ["https://example.org/account/group/artifact"])
    monkeypatch.setattr(dl, "_get_databus_versions_of_artifact", lambda *a, **k: ["https://example.org/account/group/artifact/2026"])
    monkeypatch.setattr(dl, "_get_file_download_urls_from_artifact_jsonld", lambda *a: [FILE_URL])
    monkeypatch.setattr(dl, "_get_sparql_query_of_collection", lambda *a, **k: "query")
    monkeypatch.setattr(dl, "_get_file_download_urls_from_sparql_query", lambda *a, **k: [FILE_URL])
    head = Mock(return_value=response())
    get = Mock(return_value=response())
    monkeypatch.setattr(dl.requests, "head", head)
    monkeypatch.setattr(dl.requests, "get", get)
    dl.download(str(tmp_path), "https://example.org/sparql", [target], user_agent=CUSTOM_AGENT)
    assert head.call_args.kwargs["headers"]["User-Agent"] == CUSTOM_AGENT
    assert get.call_args.kwargs["headers"]["User-Agent"] == CUSTOM_AGENT


def test_cli_manifest_replays_user_agent(monkeypatch, tmp_path):
    download = Mock()
    monkeypatch.setattr("databusclient.cli.api_download", download)
    monkeypatch.setattr("databusclient.manifest.replay.api_download", download)
    manifest = tmp_path / "download.jsonld"
    result = CliRunner().invoke(app, [
        "download", FILE_URL, "--user-agent", CUSTOM_AGENT, "--manifest", str(manifest),
    ])
    assert result.exit_code == 0, result.output
    assert json.loads(manifest.read_text())["dbus:replayParams"]["user_agent"] == CUSTOM_AGENT
    replay_manifest(str(manifest))
    assert download.call_count == 2
    assert all(call.kwargs["user_agent"] == CUSTOM_AGENT for call in download.call_args_list)


@pytest.mark.parametrize("setting", [None, CUSTOM_AGENT, "${DOWNLOAD_USER_AGENT}"])
def test_workflow_download_user_agent(monkeypatch, tmp_path, setting):
    from databusclient.workflow.context import StepContext
    from databusclient.workflow.parser import parse_workflow
    from databusclient.workflow.steps import DownloadStep

    monkeypatch.setenv("DOWNLOAD_USER_AGENT", CUSTOM_AGENT)
    workflow = tmp_path / "workflow.yml"
    content = (
        "steps:\n"
        "  - name: fetch\n"
        "    command: download\n"
        f"    uri: {FILE_URL}\n"
        f"    localdir: {tmp_path / 'data'}\n"
    )
    if setting is not None:
        content += f'    user_agent: "{setting}"\n'
    workflow.write_text(content)
    download = Mock()
    monkeypatch.setattr("databusclient.workflow.steps.api_download", download)

    step = parse_workflow(str(workflow))["steps"][0]
    DownloadStep().run(step, StepContext())

    download.assert_called_once()
    assert download.call_args.kwargs["user_agent"] == (
        None if setting is None else CUSTOM_AGENT
    )
