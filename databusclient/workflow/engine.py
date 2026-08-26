"""WorkflowEngine — executes a sequence of parsed workflow steps in order.

Applies each step's on_error behavior (fail/continue/retry) around a call
to the step's run() method. Retries operate at the whole-step level --
the engine has no visibility into partial failures inside a step (e.g.
one file out of several failing during a download), since download(),
deploy(), and delete() are called as single atomic operations.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from databusclient.manifest.context import ManifestContext
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
    """Runs a parsed workflow's steps in order, handling errors per step.

    If manifest_context is given, one unified manifest is built for the
    entire workflow run: each step gets its own temporary ManifestContext
    (isolating its recorded files/errors), which is merged into
    manifest_context afterward, tagged with the step's name. This lets a
    single workflow manifest remain traceable to which step produced or
    failed on which file, without touching download.py/deploy.py/delete.py
    at all -- those already accept manifest_context=None as a no-op, and
    here they simply receive a real (temporary, per-step) one instead.
    """

    def __init__(
        self,
        context: StepContext | None = None,
        manifest_context: Optional[ManifestContext] = None,
    ) -> None:
        self.context = context or StepContext()
        self.manifest_context = manifest_context
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

        step_manifest_ctx = self._start_step_manifest(command)
        try:
            step.run(step_config, self.context)
            self._finish_step_manifest(step_manifest_ctx, name)
            return StepResult(name, "success")
        except Exception as exc:
            self._finish_step_manifest(step_manifest_ctx, name, error=exc)
            if on_error == "continue":
                print(f"WARNING: step '{name}' failed and on_error is 'continue': {exc}")
                return StepResult(name, "skipped_error", error=exc)
            # on_error == "fail" (or missing/defaulted to fail)
            return StepResult(name, "failed", error=exc)

    def _start_step_manifest(self, command: str) -> Optional[ManifestContext]:
        """If a workflow-level manifest is active, give this step its own
        temporary ManifestContext to record into. Returns None if no
        workflow manifest was requested -- in that case self.context's
        manifest_context is left as whatever it already was (e.g. a step's
        own throwaway context, like DownloadStep uses for output_urls).
        """
        if self.manifest_context is None:
            return None
        step_ctx = ManifestContext(command=command)
        self.context.manifest_context = step_ctx
        return step_ctx

    def _finish_step_manifest(
        self,
        step_manifest_ctx: Optional[ManifestContext],
        step_name: str,
        error: Optional[Exception] = None,
    ) -> None:
        """Merge a completed step's temporary manifest entries into the
        workflow-level master manifest, tagged with the step name. If the
        step failed, also record a synthetic entry so the failure is
        visible in the manifest even if the step recorded no per-file
        entries before failing. The synthetic entry is tagged with the
        same "step" field merge_from() uses, so format_summary()'s
        [stepname] prefix mechanism works consistently for BOTH per-file
        failures (merged from a step's own context) and whole-step
        failures (no file-level detail available at all) -- previously
        only the merged case was tagged, so whole-step failures (like an
        auth error before any file work happens) showed up without the
        [stepname] prefix, relying on the step name being embedded in a
        fake url string instead.
        """
        if self.manifest_context is None or step_manifest_ctx is None:
            return
        self.manifest_context.merge_from(step_manifest_ctx, step_name=step_name)
        if error is not None:
            self.manifest_context.record_file(
                url="(no file-level detail -- step failed before producing one)",
                status="failed",
                error_message=str(error),
            )
            self.manifest_context.files[-1]["step"] = step_name

    def _run_with_retry(self, name: str, step: Any, step_config: Dict[str, Any]) -> StepResult:
        retry_config = step_config["retry"]
        max_attempts = retry_config["max_attempts"]
        delay_seconds = retry_config["delay_seconds"]
        command = step_config["command"]

        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            step_manifest_ctx = self._start_step_manifest(command)
            try:
                step.run(step_config, self.context)
                self._finish_step_manifest(step_manifest_ctx, name)
                return StepResult(name, "success", attempts=attempt)
            except Exception as exc:
                last_error = exc
                print(
                    f"WARNING: step '{name}' attempt {attempt}/{max_attempts} "
                    f"failed: {exc}"
                )
                if attempt == max_attempts:
                    self._finish_step_manifest(step_manifest_ctx, name, error=exc)
                if attempt < max_attempts:
                    time.sleep(delay_seconds)

        return StepResult(name, "failed", error=last_error, attempts=max_attempts)