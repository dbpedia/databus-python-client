from __future__ import annotations

import json
from typing import Any, Dict, Optional

from databusclient.api.download import download as api_download


class ManifestReplayError(Exception):
    """Raised when replay manifest is invalid or cannot be replayed safely."""


def _load_manifest(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except FileNotFoundError as e:
        raise ManifestReplayError(f"Manifest file not found: {path}") from e
    except json.JSONDecodeError as e:
        raise ManifestReplayError(f"Manifest is not valid JSON: {path}") from e
    except OSError as e:
        raise ManifestReplayError(f"Failed to read manifest: {e}") from e

    if not isinstance(data, dict):
        raise ManifestReplayError("Manifest root must be a JSON object.")
    return data


def _validate_replay_params(params: Any) -> Dict[str, Any]:
    if not isinstance(params, dict):
        raise ManifestReplayError("Manifest field dbus:replayParams must be an object.")
    return params


def _build_download_kwargs(
    manifest: Dict[str, Any],
    replay_params: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    databus_uris = replay_params.get("databusURIs")
    if not isinstance(databus_uris, list) or not databus_uris:
        raise ManifestReplayError(
            "Manifest replay for download requires non-empty replayParams.databusURIs."
        )

    auth_method = manifest.get("dbus:authMethod")
    token = overrides.get("token")
    databus_key = overrides.get("databus_key")

    if auth_method == "vault_token" and not token:
        raise ManifestReplayError(
            "Manifest uses vault_token authentication. Provide --vault-token for replay."
        )
    if auth_method == "databus_key" and not databus_key:
        raise ManifestReplayError(
            "Manifest uses databus_key authentication. Provide --databus-key for replay."
        )

    endpoint = overrides.get("endpoint", manifest.get("dbus:endpoint"))

    return {
        "localDir": overrides.get("localDir"),
        "endpoint": endpoint,
        "databusURIs": databus_uris,
        "token": token,
        "databus_key": databus_key,
        "all_versions": replay_params.get("all_versions"),
        "auth_url": replay_params.get(
            "authurl",
            "https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token",
        ),
        "client_id": replay_params.get("clientid", "vault-token-exchange"),
        "compression": replay_params.get("compression"),
        "convert_format": replay_params.get("convert_format"),
        "graph_name": replay_params.get("graph_name"),
        "base_uri": replay_params.get("base_uri"),
        "validate_checksum": bool(replay_params.get("validate_checksum", False)),
        "manifest_context": None,
    }


def replay_manifest(manifest_path: str, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Replay a previously recorded operation from a JSON-LD manifest.

    Currently supported:
    - download
    """
    overrides = overrides or {}
    manifest = _load_manifest(manifest_path)

    command = manifest.get("dbus:command")
    if not command:
        raise ManifestReplayError("Manifest missing required field dbus:command.")

    replay_params = _validate_replay_params(manifest.get("dbus:replayParams"))

    if command == "download":
        kwargs = _build_download_kwargs(manifest, replay_params, overrides)
        api_download(**kwargs)
        return {"command": "download"}

    raise ManifestReplayError(
        f"Replay for command '{command}' is not implemented yet. "
        "Currently supported: download."
    )