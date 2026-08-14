"""Tests for WorkflowEngine (Milestone 4)."""

import pytest

from databusclient.workflow.engine import WorkflowEngine, WorkflowExecutionError

def test_runs_steps_in_order(monkeypatch):
    order = []

    class OrderedStep:
        def run(self, step_config, context):
            order.append(step_config["name"])

    from databusclient.workflow import steps as steps_module
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "download", OrderedStep)
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "deploy", OrderedStep)

    engine = WorkflowEngine()
    engine.run([
        {"name": "a", "command": "download"},
        {"name": "b", "command": "deploy"},
    ])
    assert order == ["a", "b"]


def test_step_failure_with_default_fail_raises(monkeypatch):
    class FailingStep:
        def run(self, step_config, context):
            raise RuntimeError("boom")

    from databusclient.workflow import steps as steps_module
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "download", FailingStep)

    engine = WorkflowEngine()
    with pytest.raises(WorkflowExecutionError, match="boom"):
        engine.run([{"name": "a", "command": "download"}])


def test_step_failure_with_continue_does_not_raise(monkeypatch):
    class FailingStep:
        def run(self, step_config, context):
            raise RuntimeError("boom")

    class OKStep:
        def run(self, step_config, context):
            pass

    from databusclient.workflow import steps as steps_module
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "download", FailingStep)
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "deploy", OKStep)

    engine = WorkflowEngine()
    results = engine.run([
        {"name": "a", "command": "download", "on_error": "continue"},
        {"name": "b", "command": "deploy"},
    ])
    assert results[0].status == "skipped_error"
    assert results[1].status == "success"


def test_retry_succeeds_on_second_attempt(monkeypatch):
    attempts = {"count": 0}

    class FlakyStep:
        def run(self, step_config, context):
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise RuntimeError("transient failure")

    from databusclient.workflow import steps as steps_module
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "download", FlakyStep)

    engine = WorkflowEngine()
    results = engine.run([{
        "name": "a", "command": "download", "on_error": "retry",
        "retry": {"max_attempts": 3, "delay_seconds": 0},
    }])
    assert results[0].status == "success"
    assert results[0].attempts == 2
    assert attempts["count"] == 2


def test_retry_exhausts_attempts_and_fails(monkeypatch):
    class AlwaysFailsStep:
        def run(self, step_config, context):
            raise RuntimeError("permanent failure")

    from databusclient.workflow import steps as steps_module
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "download", AlwaysFailsStep)

    engine = WorkflowEngine()
    with pytest.raises(WorkflowExecutionError, match="permanent failure"):
        engine.run([{
            "name": "a", "command": "download", "on_error": "retry",
            "retry": {"max_attempts": 2, "delay_seconds": 0},
        }])


def test_step_chaining_end_to_end(monkeypatch):
    """A download step's output is available to a deploy step via StepContext."""
    class FetchStep:
        def run(self, step_config, context):
            context.set_output(step_config["name"], "output_files", ["/data/a.ttl"])

    captured = {}

    class PublishStep:
        def run(self, step_config, context):
            resolved = context.resolve(step_config)
            captured["files"] = resolved["files"]

    from databusclient.workflow import steps as steps_module
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "download", FetchStep)
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "deploy", PublishStep)

    engine = WorkflowEngine()
    engine.run([
        {"name": "fetch", "command": "download"},
        {"name": "publish", "command": "deploy", "files": "${steps.fetch.output_files}"},
    ])
    assert captured["files"] == ["/data/a.ttl"]

def test_unknown_step_reference_surfaces_as_workflow_execution_error(monkeypatch):
    class PublishStep:
        def run(self, step_config, context):
            context.resolve(step_config)  # will raise, since "nonexistent" never ran

    from databusclient.workflow import steps as steps_module
    monkeypatch.setitem(steps_module.STEP_REGISTRY, "deploy", PublishStep)

    engine = WorkflowEngine()
    with pytest.raises(WorkflowExecutionError, match="unknown or not-yet-executed"):
        engine.run([
            {"name": "publish", "command": "deploy",
             "files": "${steps.nonexistent.output_files}"},
        ])