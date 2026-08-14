"""WorkflowParser — loads and validates a YAML workflow pipeline file.

Parses a YAML file describing a sequence of steps (download/deploy/delete),
validates its structure, and substitutes environment variables of the form
${VAR_NAME}. References of the form ${steps.step_name.output_files} are
left untouched here -- those are resolved at runtime by StepContext once
each step has actually run, since their values don't exist yet at parse time.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List

import yaml

VALID_COMMANDS = {"download", "deploy", "delete"}
VALID_ON_ERROR = {"fail", "continue", "retry"}

# Matches ${...} tokens. The captured group is everything between the braces.
_TOKEN_RE = re.compile(r"\$\{([^}]+)\}")


class WorkflowParseError(Exception):
    """Raised when a workflow YAML file is invalid or fails validation."""


class MissingEnvVarError(WorkflowParseError):
    """Raised when a workflow references an environment variable that is not set."""


def _load_yaml(path: str) -> Any:
    """Load a YAML file using safe_load (never load arbitrary Python objects)."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            return yaml.safe_load(f)
    except FileNotFoundError as e:
        raise WorkflowParseError(f"Workflow file not found: {path}") from e
    except yaml.YAMLError as e:
        raise WorkflowParseError(f"Workflow file is not valid YAML: {path}\n{e}") from e


def _substitute_value(value: Any, step_name: str) -> Any:
    """Recursively substitute ${VAR_NAME} environment variables in a value.

    Tokens of the form ${steps.*} are left untouched -- they are resolved
    later, at runtime, by StepContext once earlier steps have produced
    their outputs. Only non-"steps."-prefixed tokens are treated as
    environment variables here.

    Args:
        value: A string, list, dict, or scalar value from the parsed YAML.
        step_name: Name of the step this value belongs to (for error messages).

    Returns:
        The value with environment variables substituted.

    Raises:
        MissingEnvVarError: If a referenced environment variable is not set.
    """
    if isinstance(value, str):
        def _replace(match: re.Match) -> str:
            token = match.group(1)
            if token.startswith("steps."):
                # Leave step-output references untouched for runtime resolution.
                return match.group(0)
            env_value = os.environ.get(token)
            if env_value is None:
                raise MissingEnvVarError(
                    f"Step '{step_name}' references environment variable "
                    f"'{token}' which is not set."
                )
            return env_value

        return _TOKEN_RE.sub(_replace, value)

    if isinstance(value, list):
        return [_substitute_value(item, step_name) for item in value]

    if isinstance(value, dict):
        return {k: _substitute_value(v, step_name) for k, v in value.items()}

    return value


def _validate_step(step: Any, index: int, seen_names: set) -> Dict[str, Any]:
    """Validate the generic structure of a single step.

    Only validates fields common to all step types (name, command, on_error,
    retry config). Command-specific required fields (e.g. 'uri' for download)
    are validated later, when the step actually executes.

    Args:
        step: The raw step dict from the parsed YAML.
        index: Position of this step in the steps list (for error messages).
        seen_names: Set of step names already seen, for duplicate detection.

    Returns:
        The validated step dict (unchanged, just checked).

    Raises:
        WorkflowParseError: If the step is structurally invalid.
    """
    if not isinstance(step, dict):
        raise WorkflowParseError(f"Step at index {index} must be a mapping/object.")

    name = step.get("name")
    if not name or not isinstance(name, str):
        raise WorkflowParseError(f"Step at index {index} is missing a valid 'name'.")

    if name in seen_names:
        raise WorkflowParseError(f"Duplicate step name '{name}'. Step names must be unique.")
    seen_names.add(name)

    command = step.get("command")
    if command not in VALID_COMMANDS:
        raise WorkflowParseError(
            f"Step '{name}' has invalid command '{command}'. "
            f"Must be one of: {sorted(VALID_COMMANDS)}."
        )

    on_error = step.get("on_error", "fail")
    if on_error not in VALID_ON_ERROR:
        raise WorkflowParseError(
            f"Step '{name}' has invalid on_error '{on_error}'. "
            f"Must be one of: {sorted(VALID_ON_ERROR)}."
        )

    if on_error == "retry":
        retry_config = step.get("retry")
        if not isinstance(retry_config, dict):
            raise WorkflowParseError(
                f"Step '{name}' has on_error: retry but is missing a 'retry' "
                f"configuration block with 'max_attempts' and 'delay_seconds'."
            )
        max_attempts = retry_config.get("max_attempts")
        if not isinstance(max_attempts, int) or max_attempts < 1:
            raise WorkflowParseError(
                f"Step '{name}' retry.max_attempts must be a positive integer."
            )
        delay_seconds = retry_config.get("delay_seconds")
        if not isinstance(delay_seconds, (int, float)) or delay_seconds < 0:
            raise WorkflowParseError(
                f"Step '{name}' retry.delay_seconds must be a non-negative number."
            )

    return step


def parse_workflow(path: str) -> Dict[str, Any]:
    """Load, validate, and substitute environment variables in a workflow YAML file.

    Args:
        path: Path to the workflow YAML file.

    Returns:
        A dict with keys:
            "manifest": Optional manifest output path (str or None).
            "steps": List of validated, environment-substituted step dicts.

    Raises:
        WorkflowParseError: If the file is missing, invalid YAML, or fails
            structural validation.
        MissingEnvVarError: If a step references an unset environment variable.
    """
    raw = _load_yaml(path)

    if not isinstance(raw, dict):
        raise WorkflowParseError("Workflow file root must be a mapping/object.")

    steps = raw.get("steps")
    if not isinstance(steps, list) or not steps:
        raise WorkflowParseError(
            "Workflow file must have a non-empty 'steps' list."
        )

    seen_names: set = set()
    validated_steps: List[Dict[str, Any]] = []
    for index, step in enumerate(steps):
        validated = _validate_step(step, index, seen_names)
        substituted = _substitute_value(validated, validated["name"])
        validated_steps.append(substituted)

    return {
        "manifest": raw.get("manifest"),
        "steps": validated_steps,
    }