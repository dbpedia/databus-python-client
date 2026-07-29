"""WorkflowEngine — executes a sequence of parsed workflow steps in order.

Applies each step's on_error behavior (fail/continue/retry) around a call
to the step's run() method. Retries operate at the whole-step level --
the engine has no visibility into partial failures inside a step (e.g.
one file out of several failing during a download), since download(),
deploy(), and delete() are called as single atomic operations.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List

from databusclient.workflow.context import StepContext
from databusclient.workflow.steps import STEP_REGISTRY


class WorkflowExecutionError(Exception):
    """Raised when a workflow step fails and on_error is 'fail' (or defaults to it)."""


class StepResult:
    """Outcome of running a single step."""

    def __init__(self, name: str, status: str, error: Exception | None = None,
                 attempts: int = 1) -> None:
        self.name = name
        self.status = status  # "success", "failed", "skipped_error"
        self.error = error
        self.attempts = attempts


class WorkflowEngine:
    """Runs a parsed workflow's steps in order, handling errors per step."""

    def __init__(self, context: StepContext | None = None) -> None:
        self.context = context or StepContext()
        self.results: List[StepResult] = []

    def run(self, steps: List[Dict[str, Any]]) -> List[StepResult]:
        """Execute all steps in order.

        Args:
            steps: List of validated, environment-substituted step dicts
                (as produced by WorkflowParser.parse_workflow).

        Returns:
            List of StepResult, one per step actually attempted.

        Raises:
            WorkflowExecutionError: If a step with on_error 'fail' (the
                default) ultimately fails.
        """
        for step_config in steps:
            result = self._run_step_with_error_handling(step_config)
            self.results.append(result)
            if result.status == "failed":
                # on_error was 'fail' (or defaulted to it) -- stop the workflow.
                raise WorkflowExecutionError(
                    f"Step '{result.name}' failed: {result.error}"
                )
        return self.results

    def _run_step_with_error_handling(self, step_config: Dict[str, Any]) -> StepResult:
        name = step_config["name"]
        command = step_config["command"]
        on_error = step_config.get("on_error", "fail")

        step_class = STEP_REGISTRY.get(command)
        if step_class is None:
            # Should already be caught by the parser, but defend anyway.
            raise WorkflowExecutionError(
                f"Step '{name}' has unknown command '{command}'."
            )
        step = step_class()

        if on_error == "retry":
            return self._run_with_retry(name, step, step_config)

        try:
            step.run(step_config, self.context)
            return StepResult(name, "success")
        except Exception as exc:
            if on_error == "continue":
                print(f"WARNING: step '{name}' failed and on_error is 'continue': {exc}")
                return StepResult(name, "skipped_error", error=exc)
            # on_error == "fail" (or missing/defaulted to fail)
            return StepResult(name, "failed", error=exc)

    def _run_with_retry(self, name: str, step: Any, step_config: Dict[str, Any]) -> StepResult:
        retry_config = step_config["retry"]
        max_attempts = retry_config["max_attempts"]
        delay_seconds = retry_config["delay_seconds"]

        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                step.run(step_config, self.context)
                return StepResult(name, "success", attempts=attempt)
            except Exception as exc:
                last_error = exc
                print(
                    f"WARNING: step '{name}' attempt {attempt}/{max_attempts} "
                    f"failed: {exc}"
                )
                if attempt < max_attempts:
                    time.sleep(delay_seconds)

        return StepResult(name, "failed", error=last_error, attempts=max_attempts)