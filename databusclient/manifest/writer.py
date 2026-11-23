"""ManifestWriter — serializes a ManifestContext to a JSON-LD file.

Called once at the end of a CLI operation. If writing fails,
a warning is printed and the CLI exits with code 0 (the actual
operation already succeeded).
"""

from __future__ import annotations

import json
import os

from databusclient.manifest.context import ManifestContext
from databusclient.version import __version__

# JSON-LD context using DataID vocabulary — same vocabulary used
# by the Databus platform itself for semantic interoperability.
_JSONLD_CONTEXT = {
    "dataid": "http://dataid.dbpedia.org/ns#",
    "dcat": "http://www.w3.org/ns/dcat#",
    "dcterms": "http://purl.org/dc/terms/",
    "xsd": "http://www.w3.org/2001/XMLSchema#",
    "dbus": "http://databus.dbpedia.org/manifest/ns#",
}

_SCHEMA_VERSION = "1.0"


class ManifestWriter:
    """Serializes a ManifestContext to a JSON-LD manifest file."""

    @staticmethod
    def build_manifest_dict(context: ManifestContext) -> dict:
        """Build the JSON-LD manifest dict from a context, without writing
        to disk. Extracted from write() so callers (like the workflow
        engine's automatic console summary) can get the dict without
        needing a file path.
        """
        summary = context.summary()

        file_entries = []
        for f in context.files:
            entry: dict = {
                "@type": "dataid:File",
                "dcat:downloadURL": f["url"],
                "dbus:status": f["status"],
            }
            if f.get("sha256"):
                entry["dataid:checksum"] = f["sha256"]
            if f.get("size_bytes") is not None:
                entry["dataid:byteSize"] = f["size_bytes"]
            if f.get("compression"):
                entry["dataid:compression"] = f["compression"]
            if f.get("downloaded_at"):
                entry["dbus:processedAt"] = {
                    "@value": f["downloaded_at"],
                    "@type": "xsd:dateTime",
                }
            if f.get("error_message"):
                entry["dbus:errorMessage"] = f["error_message"]
            if f.get("error_traceback"):
                entry["dbus:errorTraceback"] = f["error_traceback"]
            if f.get("retry_count"):
                entry["dbus:retryCount"] = f["retry_count"]
            if f.get("step"):
                entry["dbus:stepName"] = f["step"]
            file_entries.append(entry)

        manifest = {
            "@context": _JSONLD_CONTEXT,
            "@type": "dbus:OperationManifest",
            "dbus:schemaVersion": _SCHEMA_VERSION,
            "dbus:clientVersion": __version__,
            "dbus:command": context.command,
            "dcterms:issued": {
                "@value": context.issued,
                "@type": "xsd:dateTime",
            },
        }

        if context.endpoint:
            manifest["dbus:endpoint"] = context.endpoint
        if context.auth_method:
            manifest["dbus:authMethod"] = context.auth_method
        if context.replay_params:
            manifest["dbus:replayParams"] = context.replay_params

        manifest["dataid:distribution"] = {
            "@type": "dataid:Distribution",
            "dataid:file": file_entries,
        }

        manifest["dbus:executionResult"] = {
            "@type": "dbus:ExecutionSummary",
            "dbus:totalFiles": summary["total"],
            "dbus:succeeded": summary["succeeded"],
            "dbus:failed": summary["failed"],
            "dbus:totalBytes": summary["total_bytes"],
        }

        if context.operation_error:
            manifest["dbus:operationError"] = {
                "@type": "dbus:OperationError",
                "dbus:errorType": context.operation_error["error_type"],
                "dbus:errorMessage": context.operation_error["error_message"],
                "dbus:errorTraceback": context.operation_error["error_traceback"],
            }

        return manifest

    @staticmethod
    def write(context: ManifestContext, path: str) -> str:
        """Write the manifest to a JSON-LD file at the given path.

        Creates parent directories if they do not exist.
        If a file already exists at `path`, auto-suffixes with _1, _2, etc.
        and prints a warning rather than silently overwriting.
        On failure, raises OSError — callers should catch and warn.

        Args:
            context: The completed ManifestContext to serialize.
            path: File path to write the manifest to.

        Raises:
            OSError: If the file cannot be written, or if path is a directory.
        """
        if path.endswith(("/", "\\")) or os.path.isdir(path):
            stripped = path.rstrip("/\\")
            raise OSError(
                f"--manifest path '{path}' is a directory, not a file. "
                f"Please provide a full file path, e.g. '{stripped}/manifest.jsonld'."
            )

        manifest = ManifestWriter.build_manifest_dict(context)

        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)

        final_path = ManifestWriter._resolve_available_path(path)
        if final_path != path:
            print(
                f"WARNING: manifest already exists at '{path}', "
                f"creating '{final_path}' instead"
            )

        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)
        return final_path

    @staticmethod
    def _resolve_available_path(path: str) -> str:
        """Return a non-colliding path, auto-suffixing with _1, _2, ... if needed.

        If `path` does not exist, it is returned unchanged. If it exists,
        appends _1, _2, etc. before the extension until a free path is found.

        Args:
            path: Desired manifest file path.

        Returns:
            A path that does not currently exist on disk.
        """
        if not os.path.exists(path):
            return path

        base, ext = os.path.splitext(path)
        counter = 1
        while True:
            candidate = f"{base}_{counter}{ext}"
            if not os.path.exists(candidate):
                return candidate
            counter += 1
