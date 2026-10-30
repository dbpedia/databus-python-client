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
from databusclient.manifest.replay import ManifestReplayError, replay_manifest
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
    """Verify default call without version or all_versions returns latest version URL."""
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD)
    assert res == "https://databus.dbpedia.org/user/group/my-artifact/2023.10.15"


def test_get_versions_explicit_latest():
    """Verify version='latest' returns latest version URL."""
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, version="latest")
    assert res == "https://databus.dbpedia.org/user/group/my-artifact/2023.10.15"


def test_get_versions_version_latest_with_all_versions_flag():
    """Verify all_versions=True is honored even when version='latest' is passed."""
    res = _get_databus_versions_of_artifact(
        SAMPLE_ARTIFACT_JSONLD, all_versions=True, version="latest"
    )
    assert isinstance(res, list)
    assert len(res) == 3


def test_get_versions_all_flag():
    """Verify all_versions=True returns list of all version URLs sorted newest-first."""
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, all_versions=True)
    assert isinstance(res, list)
    assert len(res) == 3
    assert res[0] == "https://databus.dbpedia.org/user/group/my-artifact/2023.10.15"
    assert res[-1] == "https://databus.dbpedia.org/user/group/my-artifact/2021.01.01"


def test_get_versions_version_all_string():
    """Verify version='ALL' (case-insensitive) returns all version URLs."""
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, version="ALL")
    assert isinstance(res, list)
    assert len(res) == 3


def test_get_versions_specific_version_id():
    """Verify specific version segment ID matches correct version URL."""
    res = _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, version="2022.05.01")
    assert res == "https://databus.dbpedia.org/user/group/my-artifact/2022.05.01"


def test_get_versions_specific_full_uri():
    """Verify exact matching full version URI matches correct version URL."""
    res = _get_databus_versions_of_artifact(
        SAMPLE_ARTIFACT_JSONLD,
        version="https://databus.dbpedia.org/user/group/my-artifact/2021.01.01",
    )
    assert res == "https://databus.dbpedia.org/user/group/my-artifact/2021.01.01"


def test_get_versions_mismatched_full_uri_raises():
    """Verify full URI for a different artifact does not match despite identical version ID."""
    with pytest.raises(
        ValueError,
        match="Version 'https://databus.dbpedia.org/user/group/other-artifact/2021.01.01' not found",
    ):
        _get_databus_versions_of_artifact(
            SAMPLE_ARTIFACT_JSONLD,
            version="https://databus.dbpedia.org/user/group/other-artifact/2021.01.01",
        )


def test_get_versions_not_found_raises():
    """Verify non-existent version ID raises ValueError with available versions."""
    with pytest.raises(ValueError, match="Version '1999.01.01' not found"):
        _get_databus_versions_of_artifact(SAMPLE_ARTIFACT_JSONLD, version="1999.01.01")


def test_get_versions_missing_field_raises():
    """Verify missing databus:hasVersion raises ValueError."""
    invalid_json = json.dumps({"@id": "https://databus.dbpedia.org/u/g/a"})
    with pytest.raises(ValueError, match="No 'databus:hasVersion' field"):
        _get_databus_versions_of_artifact(invalid_json)


def test_get_versions_empty_list_raises():
    """Verify empty databus:hasVersion list raises ValueError."""
    invalid_json = json.dumps({"@id": "https://databus.dbpedia.org/u/g/a", "databus:hasVersion": []})
    with pytest.raises(ValueError, match="No versions found"):
        _get_databus_versions_of_artifact(invalid_json)


# ---------------------------------------------------------------------------
# Integration tests: _download_artifact and _download_group
# ---------------------------------------------------------------------------

def test_download_artifact_with_specific_version(monkeypatch):
    """Verify _download_artifact downloads only the requested specific version."""
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
    """Verify _download_group skips artifacts not containing the requested version."""
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


def test_download_group_with_empty_version_artifact_skips_and_downloads_matching(monkeypatch, capsys):
    """Verify _download_group skips artifacts with zero versions and continues download."""
    group_json = json.dumps({
        "@id": "https://databus.dbpedia.org/user/group",
        "@type": "dataid:Group",
        "databus:hasArtifact": [
            {"@id": "https://databus.dbpedia.org/user/group/artifact-empty"},
            {"@id": "https://databus.dbpedia.org/user/group/artifact-valid"},
        ],
    })
    artifact_empty_json = json.dumps({
        "@id": "https://databus.dbpedia.org/user/group/artifact-empty",
        "databus:hasVersion": [],
    })
    artifact_valid_json = json.dumps({
        "@id": "https://databus.dbpedia.org/user/group/artifact-valid",
        "databus:hasVersion": [{"@id": "https://databus.dbpedia.org/user/group/artifact-valid/2022.05.01"}],
    })

    def fake_fetch_jsonld(uri, databus_key=None):
        if uri.endswith("/group"):
            return group_json
        if uri.endswith("artifact-empty"):
            return artifact_empty_json
        if uri.endswith("artifact-valid"):
            return artifact_valid_json
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
    assert "Skipping artifact https://databus.dbpedia.org/user/group/artifact-empty" in captured
    assert "https://example.org/files/data_20220501.ttl" in downloaded


# ---------------------------------------------------------------------------
# API download dispatch tests
# ---------------------------------------------------------------------------

def test_api_download_dispatches_version(monkeypatch):
    """Verify api_download passes version parameter to artifact handler."""
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
    """Verify all_versions=True sets version to 'all'."""
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


def test_api_download_positional_arguments_preserve_auth_url(monkeypatch):
    """Verify passing auth_url positionally binds to auth_url and not version."""
    captured = {}

    def fake_download_version(uri, localDir, **kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(dl, "_download_version", fake_download_version)

    # Call with 7 positional arguments: localDir, endpoint, databusURIs, token, databus_key, all_versions, auth_url
    api_download(
        "tmp",
        "https://databus.dbpedia.org/sparql",
        ["https://databus.dbpedia.org/user/group/my-artifact/2022.05.01"],
        "my-token.dat",
        "my-key",
        False,
        "https://custom-auth.org/realms/token",
    )

    assert captured["auth_url"] == "https://custom-auth.org/realms/token"
    assert captured.get("version") is None


# ---------------------------------------------------------------------------
# CLI tests: --version and --target-version
# ---------------------------------------------------------------------------

def test_cli_download_version_flag(monkeypatch):
    """Verify CLI --version flag forwards target version to api_download."""
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
    """Verify CLI --target-version alias forwards target version to api_download."""
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
    """Verify CLI download records version in manifest replay parameters."""
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
    """Verify manifest replay extracts and passes version to api_download."""
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


def test_manifest_replay_rejects_non_string_version(monkeypatch, tmp_path):
    """Verify manifest replay raises ManifestReplayError for non-string version."""
    monkeypatch.setattr("databusclient.manifest.replay.api_download", lambda **k: None)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "download",
        "dbus:endpoint": "https://databus.dbpedia.org/sparql",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/user/group/my-artifact"],
            "version": 20220501,  # Non-string integer
        },
    }
    manifest_path = tmp_path / "replay_invalid_version.jsonld"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(
        ManifestReplayError,
        match="Manifest field dbus:replayParams.version must be a string.",
    ):
        replay_manifest(str(manifest_path))


def test_workflow_step_executes_version(monkeypatch, tmp_path):
    """Verify declarative workflow DownloadStep passes version to api_download."""
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
