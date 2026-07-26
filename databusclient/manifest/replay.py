from __future__ import annotations
import json
from typing import Any, Dict, Optional
from databusclient.api.download import download as api_download
from databusclient.api.delete import delete as api_delete
from databusclient.api.deploy import (
    create_dataset as api_create_dataset,
    deploy as api_deploy_call,
    create_distribution as api_create_distribution,
    create_distributions_from_metadata,
)


class ManifestReplayError(Exception):
    """Raised when replay manifest is invalid or cannot be replayed safely."""


def load_manifest(path: str) -> Dict[str, Any]:
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

def _build_delete_kwargs(
    replay_params: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    databus_uris = replay_params.get("databusURIs")
    if not isinstance(databus_uris, list) or not databus_uris:
        raise ManifestReplayError(
            "Manifest replay for delete requires non-empty replayParams.databusURIs."
        )

    databus_key = overrides.get("databus_key")
    if not databus_key:
        raise ManifestReplayError(
            "Delete replay requires --databus-key to be provided explicitly. "
            "The API key is never stored in the manifest."
        )

    return {
        "databusURIs": databus_uris,
        "databus_key": databus_key,
    }


def _replay_delete(
    replay_params: Dict[str, Any],
    overrides: Dict[str, Any],
    confirm_fn,
) -> Dict[str, Any]:
    kwargs = _build_delete_kwargs(replay_params, overrides)
    databus_uris = kwargs["databusURIs"]

    # dry_run reflects what actually happened in the ORIGINAL operation
    # (already recorded in replayParams by the plain `delete` command).
    # A replay-time --dry-run flag may only force it ON for an extra
    # safety preview -- it can never turn a recorded dry_run=True back off.
    recorded_dry_run = bool(replay_params.get("dry_run", False))
    dry_run = recorded_dry_run or bool(overrides.get("dry_run", False))

    # force is CLI-only at replay time, never read from the manifest --
    # same pattern as vault_token / databus_key.
    force = bool(overrides.get("force", False))

    if dry_run:
        api_delete(
            databusURIs=databus_uris,
            databus_key=kwargs["databus_key"],
            dry_run=True,
            force=True,
            manifest_context=None,
        )
        return {"command": "delete", "executed": False, "dry_run": True}

    if not force:
        prompt = (
            "About to replay a DELETE operation for the following "
            f"{len(databus_uris)} URI(s):\n"
            + "\n".join(f"  - {u}" for u in databus_uris)
            + "\nThis is irreversible. Proceed? [y/N]: "
        )
        answer = confirm_fn(prompt).strip().lower()
        if answer not in ("y", "yes"):
            return {"command": "delete", "executed": False, "dry_run": False}

    # Replay-level confirmation already happened above (or --force was
    # given). Call delete() with force=True so it doesn't also prompt
    # per-resource internally -- avoids double confirmation.
    api_delete(
        databusURIs=databus_uris,
        databus_key=kwargs["databus_key"],
        dry_run=False,
        force=True,
        manifest_context=None,
    )
    return {"command": "delete", "executed": True, "dry_run": False}

def _reconstruct_distribution_strings(resolved_distributions: list) -> list:
    strings = []
    for part in resolved_distributions:
        url = part.get("downloadURL")
        if not url:
            raise ManifestReplayError(
                "A stored distribution entry is missing 'downloadURL'; "
                "cannot replay this deploy."
            )
        cvs = {
            k[len("dcv:"):]: v
            for k, v in part.items()
            if k.startswith("dcv:")
        }
        sha256sum = part.get("sha256sum")
        byte_size = part.get("byteSize")
        sha_tuple = (
            (sha256sum, byte_size)
            if sha256sum and byte_size is not None
            else None
        )
        strings.append(
            api_create_distribution(
                url=url,
                cvs=cvs,
                file_format=part.get("formatExtension"),
                compression=part.get("compression"),
                sha256_length_tuple=sha_tuple,
            )
        )
    return strings


def _build_deploy_kwargs(replay_params: Dict[str, Any]) -> Dict[str, Any]:
    deploy_mode = replay_params.get("deploy_mode")

    if deploy_mode == "webdav":
        raise ManifestReplayError(
            "Deploy replay is not supported for WebDAV/Nextcloud uploads. "
            "The uploaded files may no longer exist at their original "
            "local paths, so this mode cannot be safely replayed."
        )

    version_id = replay_params.get("version_id")
    title = replay_params.get("title")
    abstract = replay_params.get("abstract")
    description = replay_params.get("description")
    license_url = replay_params.get("license_url")

    missing = [
        name for name, val in [
            ("version_id", version_id),
            ("title", title),
            ("abstract", abstract),
            ("description", description),
            ("license_url", license_url),
        ] if not val
    ]
    if missing:
        raise ManifestReplayError(
            f"Manifest replay for deploy is missing required field(s): "
            f"{', '.join(missing)}."
        )

    if deploy_mode == "classic":
        resolved = replay_params.get("resolved_distributions")
        if not resolved:
            raise ManifestReplayError(
                "Manifest replay for classic-mode deploy requires "
                "replayParams.resolved_distributions."
            )
        distributions = _reconstruct_distribution_strings(resolved)
    elif deploy_mode == "metadata":
        metadata = replay_params.get("resolved_metadata")
        if not metadata:
            raise ManifestReplayError(
                "Manifest replay for metadata-mode deploy requires "
                "replayParams.resolved_metadata."
            )
        distributions = create_distributions_from_metadata(metadata)
    else:
        raise ManifestReplayError(
            f"Manifest replay for deploy has an unknown or missing "
            f"deploy_mode ('{deploy_mode}'). This manifest may predate "
            "deploy replay support and cannot be replayed."
        )

    return {
        "version_id": version_id,
        "artifact_version_title": title,
        "artifact_version_abstract": abstract,
        "artifact_version_description": description,
        "license_url": license_url,
        "distributions": distributions,
    }


def _replay_deploy(
    replay_params: Dict[str, Any],
    overrides: Dict[str, Any],
) -> Dict[str, Any]:
    api_key = overrides.get("api_key")
    if not api_key:
        raise ManifestReplayError(
            "Deploy replay requires --apikey to be provided explicitly. "
            "The API key is never stored in the manifest."
        )

    kwargs = _build_deploy_kwargs(replay_params)
    dataid = api_create_dataset(**kwargs)
    api_deploy_call(dataid=dataid, api_key=api_key)
    return {"command": "deploy", "executed": True}

def replay_manifest(
    manifest_path: str,
    overrides: Optional[Dict[str, Any]] = None,
    confirm_fn=input,
) -> Dict[str, Any]:
    """
    Replay a previously recorded operation from a JSON-LD manifest.

    Currently supported:
    - download
    - delete (interactive y/n confirmation by default; overrides "force"
      skips the prompt, "dry_run" previews without prompting or deleting)
    - deploy (classic and metadata modes only; WebDAV replay is not supported)

    confirm_fn is injectable for testing (defaults to the built-in input()).
    """
    overrides = overrides or {}
    manifest = load_manifest(manifest_path)

    command = manifest.get("dbus:command")
    if not command:
        raise ManifestReplayError("Manifest missing required field dbus:command.")

    replay_params = _validate_replay_params(manifest.get("dbus:replayParams"))

    if command == "download":
        kwargs = _build_download_kwargs(manifest, replay_params, overrides)
        api_download(**kwargs)
        return {"command": "download", "executed": True}

    if command == "delete":
        return _replay_delete(replay_params, overrides, confirm_fn)

    if command == "deploy":
        return _replay_deploy(replay_params, overrides)

    raise ManifestReplayError(
        f"Replay for command '{command}' is not implemented yet. "
        "Currently supported: download, delete, deploy."
    )