"""ManifestContext — in-memory record of a Databus operation.

Created by the CLI when --manifest is passed. Threaded through
API functions as manifest_context=None. When None, all recording
calls are no-ops and existing behavior is completely unchanged.
"""

from __future__ import annotations

import traceback as tb
from datetime import datetime, timezone
from typing import Optional


class ManifestContext:
    """Records all details of a Databus operation in memory.

    Created once per CLI invocation when --manifest is passed.
    Passed as manifest_context parameter to download(), deploy(),
    delete(). When --manifest is not passed, manifest_context is
    None and all recording is skipped.

    Sensitive fields (vault_token, api_key, local_dir) are never
    passed to this class — they are excluded at the CLI layer.
    """

    def __init__(
        self,
        command: str,
        endpoint: Optional[str] = None,
        auth_method: Optional[str] = None,
    ) -> None:
        """Initialise a new manifest context.

        Args:
            command: CLI command name ('download', 'deploy', 'delete').
            endpoint: SPARQL endpoint URL used (if applicable).
            auth_method: Authentication method used ('vault_token',
                'databus_key', or None for public access).
        """
        self.command = command
        self.endpoint = endpoint
        self.auth_method = auth_method
        self.issued: str = datetime.now(timezone.utc).isoformat()
        self.replay_params: dict = {}
        self.files: list = []
        self.operation_error: Optional[dict] = None

    def record_params(self, params: dict) -> None:
        """Save the replay parameters for this operation.

        Stores CLI-level inputs needed to reconstruct the operation.
        Sensitive fields (vault_token, api_key, local_dir) must be
        excluded by the caller before passing params here.

        Args:
            params: Dict of safe CLI parameters to store for replay.
        """
        self.replay_params = params

    def record_file(
        self,
        url: str,
        status: str,
        sha256: Optional[str] = None,
        size_bytes: Optional[int] = None,
        compression: Optional[str] = None,
        downloaded_at: Optional[str] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None,
        retry_count: int = 0,
    ) -> None:
        """Record the outcome of processing a single file or URI.

        Called after each file download, deploy distribution, or
        delete operation completes (successfully or not).

        Args:
            url: The file URL or Databus URI that was processed.
            status: 'success' or 'failed'.
            sha256: SHA-256 checksum of the file (if available).
            size_bytes: File size in bytes (if available).
            compression: Compression format of the file (if applicable).
            downloaded_at: ISO-8601 timestamp of when file was processed.
            error_message: Error description if status is 'failed'.
            error_traceback: Full traceback string if status is 'failed'.
            retry_count: Number of retries attempted (default 0).
        """
        entry: dict = {
            "url": url,
            "status": status,
        }
        if sha256 is not None:
            entry["sha256"] = sha256
        if size_bytes is not None:
            entry["size_bytes"] = size_bytes
        if compression is not None:
            entry["compression"] = compression
        if downloaded_at is not None:
            entry["downloaded_at"] = downloaded_at
        if error_message is not None:
            entry["error_message"] = error_message
        if error_traceback is not None:
            entry["error_traceback"] = error_traceback
        if retry_count:
            entry["retry_count"] = retry_count

        self.files.append(entry)

    def record_file_error(self, url: str, exc: Exception) -> None:
        """Convenience method to record a failed file from an exception.

        Captures the exception message and full traceback automatically.

        Args:
            url: The file URL or Databus URI that failed.
            exc: The exception that caused the failure.
        """
        self.record_file(
            url=url,
            status="failed",
            error_message=str(exc),
            error_traceback=tb.format_exc(),
        )

    def record_operation_error(self, exc: Exception) -> None:
        """Record a top-level operation failure.

        Used when the entire operation fails (e.g. DeployError, auth failure)
        rather than an individual file failing. Captures the exception message
        and full traceback so the manifest is useful for debugging even when
        no per-file recording happened.

        Args:
            exc: The exception that caused the operation to fail.
        """
        self.operation_error = {
            "error_message": str(exc),
            "error_traceback": tb.format_exc(),
            "error_type": type(exc).__name__,
        }

    def summary(self) -> dict:
        """Return execution summary counts.

        Returns:
            Dict with total, succeeded, failed counts and total_bytes.
        """
        succeeded = sum(1 for f in self.files if f["status"] == "success")
        failed = sum(1 for f in self.files if f["status"] == "failed")
        total_bytes = sum(
            f.get("size_bytes", 0) for f in self.files if f["status"] == "success"
        )
        return {
            "total": len(self.files),
            "succeeded": succeeded,
            "failed": failed,
            "total_bytes": total_bytes,
        }