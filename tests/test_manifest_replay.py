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
        "dbus:command": "workflow",
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

def test_replay_delete_confirmed_calls_api_delete(tmp_path, monkeypatch):
    captured = {}

    def fake_delete(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.manifest.replay.api_delete", fake_delete)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "delete",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/acct/grp/art/1.0"],
        },
    }
    path = tmp_path / "delete-run.jsonld"
    _write_manifest(path, manifest)

    result = replay_manifest(
        str(path),
        overrides={"databus_key": "dummy-key"},
        confirm_fn=lambda prompt: "y",
    )

    assert result == {"command": "delete", "executed": True, "dry_run": False}
    assert captured["databusURIs"] == manifest["dbus:replayParams"]["databusURIs"]
    assert captured["databus_key"] == "dummy-key"
    assert captured["dry_run"] is False
    assert captured["force"] is True  # replay already confirmed, skip inner prompt


def test_replay_delete_declined_does_not_call_api_delete(tmp_path, monkeypatch):
    called = {"value": False}

    def fake_delete(**kwargs):
        called["value"] = True

    monkeypatch.setattr("databusclient.manifest.replay.api_delete", fake_delete)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "delete",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/acct/grp/art/1.0"],
        },
    }
    path = tmp_path / "delete-decline.jsonld"
    _write_manifest(path, manifest)

    result = replay_manifest(
        str(path),
        overrides={"databus_key": "dummy-key"},
        confirm_fn=lambda prompt: "n",
    )

    assert result == {"command": "delete", "executed": False, "dry_run": False}
    assert called["value"] is False


def test_replay_delete_force_skips_prompt(tmp_path, monkeypatch):
    prompt_called = {"value": False}

    def fake_delete(**kwargs):
        pass

    def fake_confirm(prompt):
        prompt_called["value"] = True
        return "n"  # should never be reached

    monkeypatch.setattr("databusclient.manifest.replay.api_delete", fake_delete)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "delete",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/acct/grp/art/1.0"],
        },
    }
    path = tmp_path / "delete-force.jsonld"
    _write_manifest(path, manifest)

    result = replay_manifest(
        str(path),
        overrides={"databus_key": "dummy-key", "force": True},
        confirm_fn=fake_confirm,
    )

    assert result == {"command": "delete", "executed": True, "dry_run": False}
    assert prompt_called["value"] is False


def test_replay_delete_dry_run_from_manifest_skips_prompt(tmp_path, monkeypatch):
    """dry_run recorded in the manifest (from the original delete run) is honored automatically."""
    captured = {}
    prompt_called = {"value": False}

    def fake_delete(**kwargs):
        captured.update(kwargs)

    def fake_confirm(prompt):
        prompt_called["value"] = True
        return "y"

    monkeypatch.setattr("databusclient.manifest.replay.api_delete", fake_delete)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "delete",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/acct/grp/art/1.0"],
            "dry_run": True,
        },
    }
    path = tmp_path / "delete-dryrun-manifest.jsonld"
    _write_manifest(path, manifest)

    result = replay_manifest(
        str(path),
        overrides={"databus_key": "dummy-key"},
        confirm_fn=fake_confirm,
    )

    assert result == {"command": "delete", "executed": False, "dry_run": True}
    assert prompt_called["value"] is False
    assert captured["dry_run"] is True


def test_replay_delete_dry_run_override_forces_preview(tmp_path, monkeypatch):
    """--dry-run at replay time can force a preview even if original wasn't a dry run."""
    captured = {}
    prompt_called = {"value": False}

    def fake_delete(**kwargs):
        captured.update(kwargs)

    def fake_confirm(prompt):
        prompt_called["value"] = True
        return "y"

    monkeypatch.setattr("databusclient.manifest.replay.api_delete", fake_delete)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "delete",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/acct/grp/art/1.0"],
            "dry_run": False,
        },
    }
    path = tmp_path / "delete-dryrun-override.jsonld"
    _write_manifest(path, manifest)

    result = replay_manifest(
        str(path),
        overrides={"databus_key": "dummy-key", "dry_run": True},
        confirm_fn=fake_confirm,
    )

    assert result == {"command": "delete", "executed": False, "dry_run": True}
    assert prompt_called["value"] is False
    assert captured["dry_run"] is True


def test_replay_delete_requires_databus_key(tmp_path):
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "delete",
        "dbus:replayParams": {
            "databusURIs": ["https://databus.dbpedia.org/acct/grp/art/1.0"],
        },
    }
    path = tmp_path / "delete-no-key.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="--databus-key"):
        replay_manifest(str(path), overrides={})

def test_replay_deploy_classic_mode(tmp_path, monkeypatch):
    captured = {}

    def fake_create_dataset(**kwargs):
        captured["create_dataset_kwargs"] = kwargs
        return {"@graph": [{"@id": "fake"}]}

    def fake_deploy(dataid, api_key):
        captured["deploy_dataid"] = dataid
        captured["deploy_api_key"] = api_key

    monkeypatch.setattr("databusclient.manifest.replay.api_create_dataset", fake_create_dataset)
    monkeypatch.setattr("databusclient.manifest.replay.api_deploy_call", fake_deploy)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "deploy",
        "dbus:replayParams": {
            "version_id": "https://databus.dbpedia.org/acct/grp/art/1.0",
            "title": "T", "abstract": "A", "description": "D",
            "license_url": "https://license.example.org",
            "deploy_mode": "classic",
            "resolved_distributions": [
                {
                    "downloadURL": "https://example.org/file.nt",
                    "formatExtension": "nt",
                    "compression": "none",
                    "byteSize": 123,
                    "sha256sum": "a" * 64,
                }
            ],
        },
    }
    path = tmp_path / "deploy-classic.jsonld"
    _write_manifest(path, manifest)

    result = replay_manifest(str(path), overrides={"api_key": "dummy-key"})

    assert result == {"command": "deploy", "executed": True}
    assert captured["deploy_api_key"] == "dummy-key"
    assert len(captured["create_dataset_kwargs"]["distributions"]) == 1
    assert "a" * 64 in captured["create_dataset_kwargs"]["distributions"][0]


def test_replay_deploy_metadata_mode(tmp_path, monkeypatch):
    captured = {}

    def fake_create_dataset(**kwargs):
        captured["create_dataset_kwargs"] = kwargs
        return {"@graph": [{"@id": "fake"}]}

    def fake_deploy(dataid, api_key):
        captured["deploy_api_key"] = api_key

    monkeypatch.setattr("databusclient.manifest.replay.api_create_dataset", fake_create_dataset)
    monkeypatch.setattr("databusclient.manifest.replay.api_deploy_call", fake_deploy)

    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "deploy",
        "dbus:replayParams": {
            "version_id": "https://databus.dbpedia.org/acct/grp/art/1.0",
            "title": "T", "abstract": "A", "description": "D",
            "license_url": "https://license.example.org",
            "deploy_mode": "metadata",
            "resolved_metadata": [
                {
                    "checksum": "b" * 64,
                    "size": 456,
                    "url": "https://example.org/data.csv",
                }
            ],
        },
    }
    path = tmp_path / "deploy-metadata.jsonld"
    _write_manifest(path, manifest)

    result = replay_manifest(str(path), overrides={"api_key": "dummy-key"})

    assert result == {"command": "deploy", "executed": True}
    assert len(captured["create_dataset_kwargs"]["distributions"]) == 1


def test_replay_deploy_webdav_mode_raises(tmp_path):
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "deploy",
        "dbus:replayParams": {
            "version_id": "https://databus.dbpedia.org/acct/grp/art/1.0",
            "title": "T", "abstract": "A", "description": "D",
            "license_url": "https://license.example.org",
            "deploy_mode": "webdav",
        },
    }
    path = tmp_path / "deploy-webdav.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="WebDAV"):
        replay_manifest(str(path), overrides={"api_key": "dummy-key"})


def test_replay_deploy_requires_apikey(tmp_path):
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "deploy",
        "dbus:replayParams": {
            "version_id": "https://databus.dbpedia.org/acct/grp/art/1.0",
            "title": "T", "abstract": "A", "description": "D",
            "license_url": "https://license.example.org",
            "deploy_mode": "classic",
            "resolved_distributions": [],
        },
    }
    path = tmp_path / "deploy-no-key.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="--apikey"):
        replay_manifest(str(path), overrides={})


def test_replay_deploy_missing_deploy_mode_raises(tmp_path):
    """Backward-compat: manifests written before deploy replay existed have no deploy_mode."""
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "deploy",
        "dbus:replayParams": {
            "version_id": "https://databus.dbpedia.org/acct/grp/art/1.0",
            "title": "T", "abstract": "A", "description": "D",
            "license_url": "https://license.example.org",
        },
    }
    path = tmp_path / "deploy-old-manifest.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="predate"):
        replay_manifest(str(path), overrides={"api_key": "dummy-key"})

def test_replay_workflow_manifest_gives_clean_not_implemented_error(tmp_path):
    """Workflow manifests have no replayParams (workflows don't call
    record_params()). Confirm this gives the standard 'not implemented'
    message, not a confusing validation error about a missing field."""
    manifest = {
        "@type": "dbus:OperationManifest",
        "dbus:command": "workflow",
    }
    path = tmp_path / "workflow-manifest.jsonld"
    _write_manifest(path, manifest)

    with pytest.raises(ManifestReplayError, match="not implemented"):
        replay_manifest(str(path))