"""Tests for version selection in downloads (Issue #17)."""

import json
import pytest
from click.testing import CliRunner

import databusclient.api.download as dl
from databusclient.api.download import (
    _get_databus_versions_of_artifact,
    download as api_download,
)
from databusclient.cli import app
from databusclient.manifest.replay import replay_manifest
from databusclient.workflow.steps import DownloadStep
from databusclient.workflow.context import StepContext


SAMPLE_ARTIFACT_JSONLD = json.dumps({
    "@id": "https://databus.dbpedia.org/user/group/my-artifact",
    "@type": "dataid:Artifact",
    "databus:hasVersion": [
        {"@id": "https://databus.dbpedia.org/user/group/my-artifact/2021.01.01"},
        {"@id": "https://databus.dbpedia.org/user/group/my-artifact/2022.05.01"},
        {"@id": "https://databus.dbpedia.org/user/group/my-artifact/2023.10.15"},
    ],
})

SAMPLE_VERSION_JSONLD = json.dumps({
    "@id": "https://databus.dbpedia.org/user/group/my-artifact/2022.05.01",
    "@type": "dataid:Version",
    "@graph": [
        {
            "@type": "Part",
            "file": "https://example.org/files/data_20220501.ttl",
        }
    ],
})

SAMPLE_GROUP_JSONLD = json.dumps({
    "@id": "https://databus.dbpedia.org/user/group",
    "@type": "dataid:Group",
    "databus:hasArtifact": [
        {"@id": "https://databus.dbpedia.org/user/group/artifact-one"},
        {"@id": "https://databus.dbpedia.org/user/group/artifact-two"},
    ],
})


# ---------------------------------------------------------------------------
# Unit tests: _get_databus_versions_of_artifact
# ---------------------------------------------------------------------------

def test_get_versions_default_returns_latest():
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD)
    assert res == "https://databus.dbpedia.org/user/group/my-artifact/2023.10.15"


def test_get_versions_explicit_latest():
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, version="latest")
    assert res == "https://databus.dbpedia.org/user/group/my-artifact/2023.10.15"


def test_get_versions_all_flag():
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, all_versions=True)
    assert isinstance(res, list)
    assert len(res) == 3
    assert res[0] == "https://databus.dbpedia.org/user/group/my-artifact/2023.10.15"
    assert res[-1] == "https://databus.dbpedia.org/user/group/my-artifact/2021.01.01"


def test_get_versions_version_all_string():
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, version="ALL")
    assert isinstance(res, list)
    assert len(res) == 3


def test_get_versions_specific_version_id():
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, version="2022.05.01")
    assert res == "https://databus.dbpedia.org/user/group/my-artifact/2022.05.01"


def test_get_versions_specific_full_uri():
    res = _get_databus_versions_of_artifact(
        SAMPLE_ARTIFACT_JSONLD,
        version="https://databus.dbpedia.org/user/group/my-artifact/2021.01.01",
    )
    assert res == "https://databus.dbpedia.org/user/group/my-artifact/2021.01.01"


def test_get_versions_not_found_raises():
    with pytest.raises(ValueError, match="Version '1999.01.01' not found"):
        _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, version="1999.01.01")


def test_get_versions_missing_field_raises():
    invalid_json = json.dumps({"@id": "https://databus.dbpedia.org/u/g/a"})
    with pytest.raises(ValueError, match="No 'databus:hasVersion' field"):
        _get_databus_versions_of_artifact(invalid_json)


def test_get_versions_empty_list_raises():
    invalid_json = json.dumps({"@id": "https://databus.dbpedia.org/u/g/a", "databus:hasVersion": []})
    with pytest.raises(ValueError, match="No versions found"):
        _get_databus_versions_of_artifact(invalid_json)


# ---------------------------------------------------------------------------
# Integration tests: _download_artifact and _download_group
# ---------------------------------------------------------------------------

def test_download_artifact_with_specific_version(monkeypatch):
    fetched = []
    downloaded_files = []

    def fake_fetch_jsonld(uri, databus_key=None):
        fetched.append(uri)
        if "2022.05.01" in uri:
            return SAMPLE_VERSION_JSONLD
        return SAMPLE_ARTIFACT_JSONLD

    def fake_download_files(urls, localDir, **kwargs):
        downloaded_files.extend(urls)

    monkeypatch.setattr(dl, "fetch_databus_jsonld", fake_fetch_jsonld)
    monkeypatch.setattr(dl, "_download_files", fake_download_files)

    dl._download_artifact(
        "https://databus.dbpedia.org/user/group/my-artifact",
        localDir="tmp",
        version="2022.05.01",
    )

    assert "https://databus.dbpedia.org/user/group/my-artifact/2022.05.01" in fetched
    assert "https://example.org/files/data_20220501.ttl" in downloaded_files


def test_download_group_with_specific_version_skips_unmatched_artifacts(monkeypatch, capsys):
    artifact_one_json = json.dumps({
        "@id": "https://databus.dbpedia.org/user/group/artifact-one",
        "databus:hasVersion": [{"@id": "https://databus.dbpedia.org/user/group/artifact-one/2022.05.01"}],
    })
    artifact_two_json = json.dumps({
        "@id": "https://databus.dbpedia.org/user/group/artifact-two",
        "databus:hasVersion": [{"@id": "https://databus.dbpedia.org/user/group/artifact-two/2020.01.01"}],
    })

    def fake_fetch_jsonld(uri, databus_key=None):
        if uri.endswith("/group"):
            return SAMPLE_GROUP_JSONLD
        if uri.endswith("artifact-one"):
            return artifact_one_json
        if uri.endswith("artifact-two"):
            return artifact_two_json
        if "2022.05.01" in uri:
            return SAMPLE_VERSION_JSONLD
        return "{}"

    downloaded = []
    monkeypatch.setattr(dl, "fetch_databus_jsonld", fake_fetch_jsonld)
    monkeypatch.setattr(dl, "_download_files", lambda urls, *a, **k: downloaded.extend(urls))

    dl._download_group(
        "https://databus.dbpedia.org/user/group",
        localDir="tmp",
        version="2022.05.01",
    )

    captured = capsys.readouterr().out
    assert "Skipping artifact https://databus.dbpedia.org/user/group/artifact-two" in captured
    assert "https://example.org/files/data_20220501.ttl" in downloaded


# ---------------------------------------------------------------------------
# API download dispatch tests
# ---------------------------------------------------------------------------

def test_api_download_dispatches_version(monkeypatch):
    captured = {}

    def fake_download_artifact(uri, localDir, **kwargs):
        captured["uri"] = uri
        captured["version"] = kwargs.get("version")
        captured["all_versions"] = kwargs.get("all_versions")

    monkeypatch.setattr(dl, "_download_artifact", fake_download_artifact)

    api_download(
        localDir="tmp",
        endpoint=None,
        databusURIs=["https://databus.dbpedia.org/user/group/my-artifact"],
        version="2022.05.01",
    )

    assert captured["version"] == "2022.05.01"
    assert captured["all_versions"] is False


def test_api_download_all_versions_sets_version_all(monkeypatch):
    captured = {}

    def fake_download_artifact(uri, localDir, **kwargs):
        captured["all_versions"] = kwargs.get("all_versions")
        captured["version"] = kwargs.get("version")

    monkeypatch.setattr(dl, "_download_artifact", fake_download_artifact)

    api_download(
        localDir="tmp",
        endpoint=None,
        databusURIs=["https://databus.dbpedia.org/user/group/my-artifact"],
        all_versions=True,
    )

    assert captured["all_versions"] is True
    assert captured["version"] == "all"


# ---------------------------------------------------------------------------
# CLI tests: --version and --target-version
# ---------------------------------------------------------------------------

def test_cli_download_version_flag(monkeypatch):
    captured = {}

    def fake_api_download(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.cli.api_download", fake_api_download)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "download",
            "https://databus.dbpedia.org/user/group/my-artifact",
            "--version",
            "2022.05.01",
        ],
    )

    assert result.exit_code == 0
    assert captured["version"] == "2022.05.01"


def test_cli_download_target_version_flag(monkeypatch):
    captured = {}

    def fake_api_download(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.cli.api_download", fake_api_download)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "download",
            "https://databus.dbpedia.org/user/group/my-artifact",
            "--target-version",
            "2022.05.01",
        ],
    )

    assert result.exit_code == 0
    assert captured["version"] == "2022.05.01"


def test_cli_download_records_version_in_manifest(monkeypatch, tmp_path):
    monkeypatch.setattr("databusclient.cli.api_download", lambda **k: None)

    manifest_file = tmp_path / "manifest.jsonld"
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "download",
            "https://databus.dbpedia.org/user/group/my-artifact",
            "--version",
            "2022.05.01",
            "--manifest",
            str(manifest_file),
        ],
    )

    assert result.exit_code == 0
    data = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert data["dbus:replayParams"]["version"] == "2022.05.01"


# ---------------------------------------------------------------------------
# Manifest replay & Workflow step tests
# ---------------------------------------------------------------------------

def test_manifest_replay_restores_version(monkeypatch, tmp_path):
    captured = {}

    def fake_api_download(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.manifest.replay.api_download", fake_api_download)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "download",
        "dbus:endpoint": "https://databus.dbpedia.org/sparql",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/user/group/my-artifact"],
            "version": "2022.05.01",
        },
    }
    manifest_path = tmp_path / "replay_version.jsonld"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    replay_manifest(str(manifest_path))

    assert captured["version"] == "2022.05.01"


def test_workflow_step_executes_version(monkeypatch, tmp_path):
    captured = {}

    def fake_api_download(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.workflow.steps.api_download", fake_api_download)

    step = {
        "name": "fetch_version",
        "action": "download",
        "uris": ["https://databus.dbpedia.org/user/group/my-artifact"],
        "version": "2022.05.01",
        "localdir": str(tmp_path),
    }
    context = StepContext()
    DownloadStep().run(step, context)

    assert captured["version"] == "2022.05.01"
