"""Tests for StepContext (Milestone 4)."""

import pytest

from databusclient.workflow.context import StepContext, StepReferenceError


def test_set_and_get_output():
    ctx = StepContext()
    ctx.set_output("fetch", "output_files", ["/data/a.ttl", "/data/b.ttl"])
    assert ctx.get_output("fetch", "output_files") == ["/data/a.ttl", "/data/b.ttl"]


def test_get_output_unknown_step_raises():
    ctx = StepContext()
    with pytest.raises(StepReferenceError, match="unknown or not-yet-executed"):
        ctx.get_output("nope", "output_files")


def test_get_output_unknown_key_raises():
    ctx = StepContext()
    ctx.set_output("fetch", "output_files", ["/data/a.ttl"])
    with pytest.raises(StepReferenceError, match="no recorded output"):
        ctx.get_output("fetch", "some_other_key")


def test_resolve_exact_token_preserves_list_type():
    """A value that IS exactly one ${steps.x.y} token resolves to the raw list."""
    ctx = StepContext()
    ctx.set_output("fetch", "output_files", ["/data/a.ttl", "/data/b.ttl"])
    resolved = ctx.resolve("${steps.fetch.output_files}")
    assert resolved == ["/data/a.ttl", "/data/b.ttl"]
    assert isinstance(resolved, list)


def test_resolve_embedded_token_in_string():
    ctx = StepContext()
    ctx.set_output("fetch", "version", "2024.01")
    resolved = ctx.resolve("Deployed version ${steps.fetch.version}")
    assert resolved == "Deployed version 2024.01"


def test_resolve_nested_dict_and_list():
    ctx = StepContext()
    ctx.set_output("fetch", "output_files", ["/data/a.ttl"])
    resolved = ctx.resolve({
        "files": "${steps.fetch.output_files}",
        "meta": {"note": "from ${steps.fetch.output_files}"},
    })
    assert resolved["files"] == ["/data/a.ttl"]
    assert resolved["meta"]["note"] == "from ['/data/a.ttl']"


def test_resolve_plain_value_passthrough():
    ctx = StepContext()
    assert ctx.resolve("no tokens here") == "no tokens here"
    assert ctx.resolve(42) == 42
    assert ctx.resolve(None) is None


def test_resolve_unresolvable_reference_raises():
    ctx = StepContext()
    with pytest.raises(StepReferenceError):
        ctx.resolve("${steps.never_ran.output_files}")


def test_manifest_context_defaults_to_none():
    ctx = StepContext()
    assert ctx.manifest_context is None


def test_manifest_context_stored_when_provided():
    sentinel = object()
    ctx = StepContext(manifest_context=sentinel)
    assert ctx.manifest_context is sentinel