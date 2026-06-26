"""Tests for the manifest system (Milestone 2).

Unit tests: ManifestContext records correctly, ManifestWriter
produces valid JSON-LD.
Edge case: manifest write failure warns but does not fail operation.
"""

import json
import os
import tempfile

import pytest

from databusclient.manifest.context import ManifestContext
from databusclient.manifest.writer import ManifestWriter


# ---------------------------------------------------------------------------
# ManifestContext tests
# ---------------------------------------------------------------------------

def test_context_records_command_and_timestamp():
    ctx = ManifestContext(command="download")
    assert ctx.command == "download"
    assert ctx.issued is not None
    assert "T" in ctx.issued  # ISO-8601 format


def test_context_records_params():
    ctx = ManifestContext(command="download")
    ctx.record_params({"databusURIs": ["https://example.org/data"], "compression": "gz"})
    assert ctx.replay_params["databusURIs"] == ["https://example.org/data"]
    assert ctx.replay_params["compression"] == "gz"


def test_context_records_successful_file():
    ctx = ManifestContext(command="download")
    ctx.record_file(
        url="https://example.org/file.ttl",
        status="success",
        sha256="abc123",
        size_bytes=1024,
    )
    assert len(ctx.files) == 1
    assert ctx.files[0]["status"] == "success"
    assert ctx.files[0]["sha256"] == "abc123"
    assert ctx.files[0]["size_bytes"] == 1024


def test_context_records_failed_file():
    ctx = ManifestContext(command="download")
    ctx.record_file(
        url="https://example.org/file.ttl",
        status="failed",
        error_message="Connection timeout",
        error_traceback="Traceback...",
    )
    assert ctx.files[0]["status"] == "failed"
    assert ctx.files[0]["error_message"] == "Connection timeout"


def test_context_record_file_error_convenience():
    ctx = ManifestContext(command="download")
    try:
        raise ValueError("test error")
    except ValueError as e:
        ctx.record_file_error("https://example.org/file.ttl", e)
    assert ctx.files[0]["status"] == "failed"
    assert "test error" in ctx.files[0]["error_message"]
    assert ctx.files[0]["error_traceback"] is not None


def test_context_summary_counts():
    ctx = ManifestContext(command="download")
    ctx.record_file(url="https://a.org/1", status="success", size_bytes=100)
    ctx.record_file(url="https://a.org/2", status="success", size_bytes=200)
    ctx.record_file(url="https://a.org/3", status="failed")
    s = ctx.summary()
    assert s["total"] == 3
    assert s["succeeded"] == 2
    assert s["failed"] == 1
    assert s["total_bytes"] == 300


def test_context_sensitive_fields_not_stored():
    """Sensitive fields must never appear in replay_params."""
    ctx = ManifestContext(command="download", auth_method="vault_token")
    ctx.record_params({
        "databusURIs": ["https://example.org"],
        "compression": "gz",
    })
    # auth_method is stored (it's safe — describes the method, not the credential)
    assert ctx.auth_method == "vault_token"
    # but the actual token must not be in replay_params
    assert "vault_token" not in ctx.replay_params
    assert "databus_key" not in ctx.replay_params
    assert "token" not in ctx.replay_params


# ---------------------------------------------------------------------------
# ManifestWriter tests
# ---------------------------------------------------------------------------

def test_writer_produces_valid_jsonld():
    ctx = ManifestContext(command="download", endpoint="https://databus.dbpedia.org/sparql")
    ctx.record_params({"databusURIs": ["https://example.org/data"]})
    ctx.record_file(
        url="https://example.org/file.ttl",
        status="success",
        sha256="abc123",
        size_bytes=1024,
    )

    with tempfile.NamedTemporaryFile(suffix=".jsonld", delete=False) as f:
        path = f.name
    try:
        ManifestWriter.write(ctx, path)
        with open(path, "r", encoding="utf-8") as f:
            manifest = json.load(f)

        assert manifest["@type"] == "dbus:OperationManifest"
        assert manifest["dbus:command"] == "download"
        assert manifest["dbus:schemaVersion"] == "1.0"
        assert "dcterms:issued" in manifest
        assert manifest["dbus:endpoint"] == "https://databus.dbpedia.org/sparql"
        assert manifest["dbus:replayParams"]["databusURIs"] == ["https://example.org/data"]

        files = manifest["dataid:distribution"]["dataid:file"]
        assert len(files) == 1
        assert files[0]["dcat:downloadURL"] == "https://example.org/file.ttl"
        assert files[0]["dbus:status"] == "success"
        assert files[0]["dataid:checksum"] == "abc123"
        assert files[0]["dataid:byteSize"] == 1024

        result = manifest["dbus:executionResult"]
        assert result["dbus:totalFiles"] == 1
        assert result["dbus:succeeded"] == 1
        assert result["dbus:failed"] == 0
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_writer_creates_parent_directories():
    ctx = ManifestContext(command="delete")
    ctx.record_file(url="https://example.org/v1", status="success")

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "nested", "dir", "manifest.jsonld")
        ManifestWriter.write(ctx, path)
        assert os.path.exists(path)


def test_writer_records_failed_file():
    ctx = ManifestContext(command="download")
    ctx.record_file(
        url="https://example.org/file.ttl",
        status="failed",
        error_message="Timeout",
        error_traceback="Traceback...",
    )

    with tempfile.NamedTemporaryFile(suffix=".jsonld", delete=False) as f:
        path = f.name
    try:
        ManifestWriter.write(ctx, path)
        with open(path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        files = manifest["dataid:distribution"]["dataid:file"]
        assert files[0]["dbus:status"] == "failed"
        assert files[0]["dbus:errorMessage"] == "Timeout"
    finally:
        if os.path.exists(path):
            os.remove(path)


def test_writer_failure_raises_oserror():
    """Writer raises OSError on invalid path — caller should catch and warn.

    Uses a file as the parent directory, which is always invalid on all
    platforms (Windows and Unix) since you cannot create a directory
    inside a file.
    """
    ctx = ManifestContext(command="download")
    ctx.record_file(url="https://example.org/f", status="success")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".txt") as f:
        file_as_parent = f.name

    try:
        # Use an existing file as if it were a parent directory —
        # always raises OSError on all platforms
        invalid_path = os.path.join(file_as_parent, "manifest.jsonld")
        with pytest.raises(OSError):
            ManifestWriter.write(ctx, invalid_path)
    finally:
        if os.path.exists(file_as_parent):
            os.remove(file_as_parent)