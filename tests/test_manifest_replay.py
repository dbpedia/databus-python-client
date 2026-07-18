import json
import pytest
from databusclient.manifest.replay import ManifestReplayError, replay_manifest

def _write_manifest(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f)

def test_replay_download_calls_api_download(tmp_path, monkeypatch):
    captured = {}

    def fake_download(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.manifest.replay.api_download", fake_download)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "download",
        "dbus:endpoint": "https://databus.dbpedia.org/sparql",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/dbpedia/test/artifact/2024.01.01"],
            "all_versions": False,
            "compression": "gz",
            "convert_format": "turtle",
            "graph_name": None,
            "base_uri": None,
            "validate_checksum": True,
            "authurl": "https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token",
            "clientid": "vault-token-exchange",
        },
    }

    path = tmp_path / "run.jsonld"
    _write_manifest(path, manifest)

    result = replay_manifest(str(path))

    assert result["command"] == "download"
    assert captured["databusURIs"] == manifest["dbus:replayParams"]["databusURIs"]
    assert captured["endpoint"] == "https://databus.dbpedia.org/sparql"
    assert captured["compression"] == "gz"
    assert captured["convert_format"] == "turtle"
    assert captured["validate_checksum"] is True
    assert captured["manifest_context"] is None


def test_replay_missing_command_raises(tmp_path):
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:replayParams": {"databusURIs": ["https://example.org/data"]},
    }
    path = tmp_path / "missing-command.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="dbus:command"):
        replay_manifest(str(path))


def test_replay_missing_replay_params_raises(tmp_path):
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "download",
    }
    path = tmp_path / "missing-replay-params.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="dbus:replayParams"):
        replay_manifest(str(path))


def test_replay_unsupported_command_raises(tmp_path):
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "deploy",
        "dbus:replayParams": {"version_id": "https://example.org/version"},
    }
    path = tmp_path / "unsupported.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="not implemented"):
        replay_manifest(str(path))


def test_replay_requires_vault_token_when_manifest_auth_method_is_vault(tmp_path):
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "download",
        "dbus:authMethod": "vault_token",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/dbpedia/test/artifact/2024.01.01"]
        },
    }
    path = tmp_path / "vault-required.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="--vault-token"):
        replay_manifest(str(path))


def test_replay_overrides_are_applied(tmp_path, monkeypatch):
    captured = {}

    def fake_download(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.manifest.replay.api_download", fake_download)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "download",
        "dbus:endpoint": "https://old.example.org/sparql",
        "dbus:authMethod": "databus_key",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/dbpedia/test/artifact/2024.01.01"],
            "validate_checksum": False,
        },
    }

    path = tmp_path / "override.jsonld"
    _write_manifest(path, manifest)

    replay_manifest(
        str(path),
        overrides={
            "endpoint": "https://databus.dbpedia.org/sparql",
            "localDir": "./replay-data",
            "databus_key": "dummy-key",
        },
    )

    assert captured["endpoint"] == "https://databus.dbpedia.org/sparql"
    assert captured["localDir"] == "./replay-data"
    assert captured["databus_key"] == "dummy-key"
