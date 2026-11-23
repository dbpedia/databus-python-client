"""Tests for WorkflowParser (Milestone 4)."""

import os
import tempfile

import pytest
import yaml

from databusclient.workflow.parser import (
    MissingEnvVarError,
    WorkflowParseError,
    parse_workflow,
)


def _write_yaml(content: dict) -> str:
    fd, path = tempfile.mkstemp(suffix=".yml")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        yaml.safe_dump(content, f)
    return path


def test_parses_minimal_valid_workflow():
    path = _write_yaml({
        "steps": [
            {"name": "fetch", "command": "download", "uri": "https://example.org/x"},
        ]
    })
    result = parse_workflow(path)
    assert result["manifest"] is None
    assert len(result["steps"]) == 1
    assert result["steps"][0]["name"] == "fetch"


def test_missing_steps_key_raises():
    path = _write_yaml({"manifest": "run.json"})
    with pytest.raises(WorkflowParseError, match="steps"):
        parse_workflow(path)


def test_empty_steps_list_raises():
    path = _write_yaml({"steps": []})
    with pytest.raises(WorkflowParseError, match="non-empty"):
        parse_workflow(path)


def test_step_missing_name_raises():
    path = _write_yaml({"steps": [{"command": "download", "uri": "x"}]})
    with pytest.raises(WorkflowParseError, match="name"):
        parse_workflow(path)


def test_duplicate_step_names_raise():
    path = _write_yaml({
        "steps": [
            {"name": "a", "command": "download", "uri": "x"},
            {"name": "a", "command": "delete", "uris": ["x"]},
        ]
    })
    with pytest.raises(WorkflowParseError, match="Duplicate step name"):
        parse_workflow(path)


def test_invalid_command_raises():
    path = _write_yaml({"steps": [{"name": "a", "command": "bogus"}]})
    with pytest.raises(WorkflowParseError, match="invalid command"):
        parse_workflow(path)


def test_invalid_on_error_raises():
    path = _write_yaml({
        "steps": [{"name": "a", "command": "download", "uri": "x", "on_error": "maybe"}]
    })
    with pytest.raises(WorkflowParseError, match="invalid on_error"):
        parse_workflow(path)


def test_retry_without_config_raises():
    path = _write_yaml({
        "steps": [{"name": "a", "command": "download", "uri": "x", "on_error": "retry"}]
    })
    with pytest.raises(WorkflowParseError, match="retry"):
        parse_workflow(path)


def test_retry_with_invalid_max_attempts_raises():
    path = _write_yaml({
        "steps": [{
            "name": "a", "command": "download", "uri": "x", "on_error": "retry",
            "retry": {"max_attempts": 0, "delay_seconds": 5},
        }]
    })
    with pytest.raises(WorkflowParseError, match="max_attempts"):
        parse_workflow(path)


def test_valid_retry_config_passes(monkeypatch):
    path = _write_yaml({
        "steps": [{
            "name": "a", "command": "download", "uri": "x", "on_error": "retry",
            "retry": {"max_attempts": 3, "delay_seconds": 5},
        }]
    })
    result = parse_workflow(path)
    assert result["steps"][0]["retry"]["max_attempts"] == 3


def test_env_var_substitution(monkeypatch):
    monkeypatch.setenv("MY_API_KEY", "secret123")
    path = _write_yaml({
        "steps": [{"name": "a", "command": "deploy", "api_key": "${MY_API_KEY}"}]
    })
    result = parse_workflow(path)
    assert result["steps"][0]["api_key"] == "secret123"


def test_missing_env_var_raises(monkeypatch):
    monkeypatch.delenv("DOES_NOT_EXIST_VAR", raising=False)
    path = _write_yaml({
        "steps": [{"name": "a", "command": "deploy", "api_key": "${DOES_NOT_EXIST_VAR}"}]
    })
    with pytest.raises(MissingEnvVarError, match="DOES_NOT_EXIST_VAR"):
        parse_workflow(path)


def test_steps_reference_left_untouched():
    """${steps.x.output_files} must NOT be treated as a missing env var."""
    path = _write_yaml({
        "steps": [
            {"name": "fetch", "command": "download", "uri": "x"},
            {"name": "publish", "command": "deploy", "files": "${steps.fetch.output_files}"},
        ]
    })
    result = parse_workflow(path)
    assert result["steps"][1]["files"] == "${steps.fetch.output_files}"


def test_multiple_tokens_in_same_string(monkeypatch):
    monkeypatch.setenv("ACCOUNT", "myaccount")
    monkeypatch.setenv("GROUP", "mygroup")
    path = _write_yaml({
        "steps": [{
            "name": "a", "command": "deploy",
            "version_id": "https://databus.dbpedia.org/${ACCOUNT}/${GROUP}/art/1.0",
        }]
    })
    result = parse_workflow(path)
    assert result["steps"][0]["version_id"] == "https://databus.dbpedia.org/myaccount/mygroup/art/1.0"


def test_nonexistent_file_raises():
    with pytest.raises(WorkflowParseError, match="not found"):
        parse_workflow("does-not-exist.yml")


def test_invalid_yaml_raises():
    fd, path = tempfile.mkstemp(suffix=".yml")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("steps: [unclosed")
    with pytest.raises(WorkflowParseError, match="not valid YAML"):
        parse_workflow(path)

def test_bare_dollar_var_without_braces_passes_through_unchanged():
    """$VAR (no braces) is not a substitution token -- left as literal text."""
    path = _write_yaml({
        "steps": [{"name": "a", "command": "download", "uri": "$HOME/data"}]
    })
    result = parse_workflow(path)
    assert result["steps"][0]["uri"] == "$HOME/data"


def test_empty_braces_pass_through_unchanged():
    """${} has no characters between the braces, so it doesn't match the
    substitution pattern at all (which requires at least one character) --
    it passes through as literal text, same as a bare $VAR without braces."""
    path = _write_yaml({
        "steps": [{"name": "a", "command": "download", "uri": "${}/data"}]
    })
    result = parse_workflow(path)
    assert result["steps"][0]["uri"] == "${}/data"