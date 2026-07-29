"""Tests for step classes (Milestone 4). No live Databus calls -- the
underlying api_download/api_deploy_call/api_delete functions are mocked."""

import os
import pytest

from databusclient.workflow.context import StepContext
from databusclient.workflow.steps import (
    DeleteStep,
    DeployStep,
    DownloadStep,
    StepValidationError,
)


def test_download_step_requires_uri():
    ctx = StepContext()
    step = DownloadStep()
    with pytest.raises(StepValidationError, match="requires 'uri'"):
        step.run({"name": "fetch", "command": "download"}, ctx)


def test_download_step_calls_api_download_and_collects_files(monkeypatch, tmp_path):
    captured = {}

    def fake_download(**kwargs):
        captured.update(kwargs)
        local_dir = kwargs["localDir"]
        os.makedirs(local_dir, exist_ok=True)
        with open(os.path.join(local_dir, "a.ttl"), "w") as f:
            f.write("data")

    monkeypatch.setattr("databusclient.workflow.steps.api_download", fake_download)

    ctx = StepContext()
    step = DownloadStep()
    step.run(
        {"name": "fetch", "command": "download", "uri": "https://example.org/x",
         "localdir": str(tmp_path)},
        ctx,
    )

    assert captured["databusURIs"] == ["https://example.org/x"]
    output = ctx.get_output("fetch", "output_files")
    assert len(output) == 1
    assert output[0].endswith("a.ttl")


def test_download_step_collects_files_from_subdirectory(monkeypatch, tmp_path):
    """Simulates a Quad -> Triple split producing files in a subdirectory."""
    def fake_download(**kwargs):
        local_dir = kwargs["localDir"]
        sub = os.path.join(local_dir, "split")
        os.makedirs(sub, exist_ok=True)
        with open(os.path.join(sub, "graph1.nt"), "w") as f:
            f.write("data")
        with open(os.path.join(sub, "graph2.nt"), "w") as f:
            f.write("data")

    monkeypatch.setattr("databusclient.workflow.steps.api_download", fake_download)

    ctx = StepContext()
    step = DownloadStep()
    step.run(
        {"name": "fetch", "command": "download", "uri": "x", "localdir": str(tmp_path)},
        ctx,
    )
    output = ctx.get_output("fetch", "output_files")
    assert len(output) == 2
    assert all(isinstance(p, str) for p in output)


def test_deploy_step_requires_fields():
    ctx = StepContext()
    step = DeployStep()
    with pytest.raises(StepValidationError, match="missing required field"):
        step.run({"name": "publish", "command": "deploy"}, ctx)


def test_deploy_step_resolves_step_reference_and_calls_deploy(monkeypatch):
    captured = {}

    def fake_create_dataset(**kwargs):
        captured["create_dataset_kwargs"] = kwargs
        return {"@graph": [{"@id": "fake"}]}

    def fake_deploy(dataid, api_key):
        captured["api_key"] = api_key

    monkeypatch.setattr("databusclient.workflow.steps.create_dataset", fake_create_dataset)
    monkeypatch.setattr("databusclient.workflow.steps.api_deploy_call", fake_deploy)

    ctx = StepContext()
    ctx.set_output("fetch", "output_files", ["/data/a.ttl"])

    step = DeployStep()
    step.run({
        "name": "publish", "command": "deploy",
        "version_id": "https://databus.dbpedia.org/a/b/c/1.0",
        "title": "T", "abstract": "A", "description": "D",
        "license": "https://license.example.org", "api_key": "key123",
        "files": "${steps.fetch.output_files}",
    }, ctx)

    assert captured["create_dataset_kwargs"]["distributions"] == ["/data/a.ttl"]
    assert captured["api_key"] == "key123"


def test_delete_step_always_forces_no_prompt(monkeypatch):
    captured = {}

    def fake_delete(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.workflow.steps.api_delete", fake_delete)

    ctx = StepContext()
    step = DeleteStep()
    step.run({
        "name": "cleanup", "command": "delete",
        "uris": ["https://databus.dbpedia.org/a/b/c/old"],
        "api_key": "key123",
    }, ctx)

    assert captured["force"] is True
    assert captured["dry_run"] is False


def test_delete_step_requires_uris():
    ctx = StepContext()
    step = DeleteStep()
    with pytest.raises(StepValidationError, match="requires 'uris'"):
        step.run({"name": "cleanup", "command": "delete", "api_key": "k"}, ctx)


def test_delete_step_requires_api_key():
    ctx = StepContext()
    step = DeleteStep()
    with pytest.raises(StepValidationError, match="requires 'api_key'"):
        step.run({"name": "cleanup", "command": "delete", "uris": ["x"]}, ctx)

def test_download_step_records_output_urls(monkeypatch, tmp_path):
    def fake_download(**kwargs):
        local_dir = kwargs["localDir"]
        os.makedirs(local_dir, exist_ok=True)
        with open(os.path.join(local_dir, "a.ttl"), "w") as f:
            f.write("data")

    monkeypatch.setattr("databusclient.workflow.steps.api_download", fake_download)

    ctx = StepContext()
    step = DownloadStep()
    step.run(
        {"name": "fetch", "command": "download", "uri": "https://example.org/data/a.ttl",
         "localdir": str(tmp_path)},
        ctx,
    )

    assert ctx.get_output("fetch", "output_urls") == ["https://example.org/data/a.ttl"]