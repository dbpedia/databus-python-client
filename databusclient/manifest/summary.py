"""manifest summary — formats an existing manifest as readable console output.

Reads already-recorded fields from a manifest JSON-LD file and formats
them for display. No new data is collected, no new file is written, and
no network access happens here at all.
"""

from __future__ import annotations

from typing import Any, Dict


def _format_bytes(total_bytes: int) -> str:
    """Format a byte count as a human-readable MB value."""
    mb = total_bytes / (1024 * 1024)
    return f"{mb:.1f} MB"


def _derive_status(manifest: Dict[str, Any]) -> str:
    """Derive an overall status string for the operation.

    - "failed" if a top-level dbus:operationError was recorded.
    - "completed with errors" if some individual files failed but the
      operation itself did not raise.
    - "completed" otherwise.
    """
    if manifest.get("dbus:operationError"):
        return "failed"

    result = manifest.get("dbus:executionResult", {})
    if result.get("dbus:failed", 0) > 0:
        return "completed with errors"

    return "completed"


def format_summary(manifest: Dict[str, Any]) -> str:
    """Format a loaded manifest dict as a readable multi-line summary string.

    Args:
        manifest: A manifest dict, as produced by loading a manifest
            JSON-LD file (e.g. via replay.load_manifest).

    Returns:
        A formatted multi-line string ready to print to the console.
    """
    lines = []

    command = manifest.get("dbus:command", "unknown")
    lines.append(f"Command  : {command}")

    issued = manifest.get("dcterms:issued", {}).get("@value")
    if issued:
        lines.append(f"Executed : {issued}")

    endpoint = manifest.get("dbus:endpoint")
    if endpoint:
        lines.append(f"Endpoint : {endpoint}")

    auth_method = manifest.get("dbus:authMethod")
    if auth_method:
        lines.append(f"Auth     : {auth_method}")

    result = manifest.get("dbus:executionResult", {})
    succeeded = result.get("dbus:succeeded", 0)
    failed = result.get("dbus:failed", 0)
    lines.append(f"Files    : {succeeded} succeeded \u00b7 {failed} failed")

    total_bytes = result.get("dbus:totalBytes", 0)
    if command == "download" and total_bytes:
        lines.append(f"Total    : {_format_bytes(total_bytes)}")

    lines.append(f"Status   : {_derive_status(manifest)}")

    operation_error = manifest.get("dbus:operationError")
    if operation_error:
        error_type = operation_error.get("dbus:errorType", "")
        error_message = operation_error.get("dbus:errorMessage", "")
        if error_type:
            lines.append(f"Error    : {error_type}: {error_message}")
        else:
            lines.append(f"Error    : {error_message}")

    failed_files = [
        f for f in manifest.get("dataid:distribution", {}).get("dataid:file", [])
        if f.get("dbus:status") == "failed"
    ]
    if failed_files:
        lines.append("")
        lines.append("Failures:")
        for f in failed_files:
            step = f.get("dbus:stepName")
            url = f.get("dcat:downloadURL", "unknown")
            error_message = f.get("dbus:errorMessage", "no error message recorded")
            prefix = f"  [{step}] " if step else "  "
            lines.append(f"{prefix}{url}: {error_message}")

    return "\n".join(lines)