"""StepContext — tracks step outputs and resolves ${steps.name.key} references at runtime.

WorkflowParser resolves ${VAR_NAME} environment variables at parse time,
but deliberately leaves ${steps.step_name.output_files} tokens untouched,
since those values don't exist until the referenced step has actually run.
StepContext is what resolves them, once the WorkflowEngine has executed
each step in order.
"""

from __future__ import annotations

import re
from typing import Any, Dict

# Matches a single ${steps.step_name.key} token.
_STEP_REF_RE = re.compile(r"\$\{steps\.([^.}]+)\.([^}]+)\}")


class StepReferenceError(Exception):
    """Raised when a ${steps.name.key} reference cannot be resolved."""


class StepContext:
    """Stores per-step outputs and resolves ${steps.name.key} references.

    manifest_context is accepted but unused in Milestone 4 -- it exists as
    a seam so Milestone 5 can wire in manifest recording without changing
    this class's structure. When None, it has zero effect, matching the
    manifest_context=None pattern already used throughout download.py,
    deploy.py, and delete.py.
    """

    def __init__(self, manifest_context=None) -> None:
        self._outputs: Dict[str, Dict[str, Any]] = {}
        self.manifest_context = manifest_context

    def set_output(self, step_name: str, key: str, value: Any) -> None:
        """Record an output value produced by a step.

        Args:
            step_name: Name of the step that produced this output.
            key: Output key, e.g. "output_files".
            value: The value to store (e.g. a list of file paths).
        """
        self._outputs.setdefault(step_name, {})[key] = value

    def get_output(self, step_name: str, key: str) -> Any:
        """Retrieve a previously recorded output value.

        Raises:
            StepReferenceError: If the step or key is unknown.
        """
        if step_name not in self._outputs:
            raise StepReferenceError(
                f"Reference to unknown or not-yet-executed step '{step_name}'."
            )
        if key not in self._outputs[step_name]:
            raise StepReferenceError(
                f"Step '{step_name}' has no recorded output '{key}'. "
                f"Available outputs: {sorted(self._outputs[step_name].keys())}."
            )
        return self._outputs[step_name][key]

    def resolve(self, value: Any) -> Any:
        """Recursively resolve ${steps.name.key} references in a value.

        A value that is EXACTLY a single ${steps.name.key} token (nothing
        else in the string) resolves to the raw stored value (e.g. a list),
        preserving its type. A token embedded inside a larger string is
        resolved by inserting str(value) in place, same as environment
        variable substitution.

        Args:
            value: A string, list, dict, or scalar value from a step config.

        Returns:
            The value with all ${steps.*} references resolved.

        Raises:
            StepReferenceError: If a referenced step/key is unknown.
        """
        if isinstance(value, str):
            full_match = _STEP_REF_RE.fullmatch(value)
            if full_match:
                step_name, key = full_match.group(1), full_match.group(2)
                return self.get_output(step_name, key)

            def _replace(match: re.Match) -> str:
                step_name, key = match.group(1), match.group(2)
                return str(self.get_output(step_name, key))

            return _STEP_REF_RE.sub(_replace, value)

        if isinstance(value, list):
            return [self.resolve(item) for item in value]

        if isinstance(value, dict):
            return {k: self.resolve(v) for k, v in value.items()}

        return value