"""Step classes — adapt a workflow step config into a call to the existing
download()/deploy()/delete() API functions.

Each step class resolves any ${steps.name.key} references in its config via
StepContext, calls the existing, unmodified API function, and records its
output back into StepContext so later steps can reference it.

No new business logic lives here. Steps are thin adapters only.
"""

from __future__ import annotations
import os
from typing import Any, Dict

from databusclient.api.delete import delete as api_delete
from databusclient.api.deploy import create_dataset, deploy as api_deploy_call
from databusclient.api.download import download as api_download

from databusclient.workflow.context import StepContext


class StepValidationError(Exception):
    """Raised when a step's config is missing a required, command-specific field."""


class DownloadStep:
    """Adapts a workflow step to a call to download()."""

    def run(self, step_config: Dict[str, Any], context: StepContext) -> None:
        resolved = context.resolve(step_config)
        name = resolved["name"]

        uri = resolved.get("uri")
        if not uri:
            raise StepValidationError(f"Step '{name}': download step requires 'uri'.")

        local_dir = resolved.get("localdir")
        if local_dir is None:
            local_dir = os.path.join(os.getcwd(), ".workflow", name)

        api_download(
            localDir=local_dir,
            endpoint=resolved.get("databus"),
            databusURIs=[uri],
            token=resolved.get("vault_token"),
            databus_key=resolved.get("databus_key"),
            all_versions=resolved.get("all_versions", False),
            compression=resolved.get("convert_to") or resolved.get("compression"),
            convert_format=resolved.get("format"),
            graph_name=resolved.get("graph_name"),
            base_uri=resolved.get("base_uri"),
            validate_checksum=resolved.get("validate_checksum", False),
            manifest_context=context.manifest_context,
        )

        output_files = self._collect_output_files(local_dir)
        context.set_output(name, "output_files", output_files)
        context.set_output(name, "output_urls", [uri])

    @staticmethod
    def _collect_output_files(local_dir: str) -> list:
        """Walk local_dir and return all file paths produced by the download.

        Always returns a flat list of file paths, even if the download
        produced files nested in subdirectories (e.g. a Quad -> Triple
        split, which writes multiple files into a subdirectory).
        """
        if not os.path.isdir(local_dir):
            return []
        return sorted(
            os.path.join(root, filename)
            for root, _dirs, filenames in os.walk(local_dir)
            for filename in filenames
        )


class DeployStep:
    """Adapts a workflow step to a call to create_dataset() + deploy()."""

    def run(self, step_config: Dict[str, Any], context: StepContext) -> None:
        resolved = context.resolve(step_config)
        name = resolved["name"]

        required = ["version_id", "title", "abstract", "description", "license", "api_key"]
        missing = [f for f in required if not resolved.get(f)]
        if missing:
            raise StepValidationError(
                f"Step '{name}': deploy step is missing required field(s): "
                f"{', '.join(missing)}."
            )

        files = resolved.get("files")
        if not files:
            raise StepValidationError(f"Step '{name}': deploy step requires 'files'.")
        if isinstance(files, str):
            files = [files]

        dataid = create_dataset(
            version_id=resolved["version_id"],
            artifact_version_title=resolved["title"],
            artifact_version_abstract=resolved["abstract"],
            artifact_version_description=resolved["description"],
            license_url=resolved["license"],
            distributions=files,
        )
        api_deploy_call(dataid=dataid, api_key=resolved["api_key"])

        context.set_output(name, "output_files", files)
        context.set_output(name, "version_id", resolved["version_id"])


class DeleteStep:
    """Adapts a workflow step to a call to delete().

    Workflows are meant to run unattended -- a delete step never triggers
    the interactive confirmation prompt that the plain `delete` CLI command
    uses. force is always effectively True here; dry_run must be set
    explicitly in the step config if a preview-only run is wanted.
    """

    def run(self, step_config: Dict[str, Any], context: StepContext) -> None:
        resolved = context.resolve(step_config)
        name = resolved["name"]

        uris = resolved.get("uris")
        if not uris:
            raise StepValidationError(f"Step '{name}': delete step requires 'uris'.")
        if isinstance(uris, str):
            uris = [uris]

        api_key = resolved.get("api_key")
        if not api_key:
            raise StepValidationError(f"Step '{name}': delete step requires 'api_key'.")

        api_delete(
            databusURIs=uris,
            databus_key=api_key,
            dry_run=resolved.get("dry_run", False),
            force=True,
            manifest_context=context.manifest_context,
        )

        context.set_output(name, "output_files", [])


STEP_REGISTRY = {
    "download": DownloadStep,
    "deploy": DeployStep,
    "delete": DeleteStep,
}