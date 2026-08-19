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
from databusclient.api.deploy import (
    create_dataset,
    deploy as api_deploy_call,
    deploy_from_metadata,
)
from databusclient.api.download import download as api_download
from databusclient.extensions import webdav
from databusclient.manifest.context import ManifestContext

from databusclient.workflow.context import StepContext


class StepValidationError(Exception):
    """Raised when a step's config is missing a required, command-specific field."""


class DownloadStep:
    """Adapts a workflow step to a call to download().

    Accepts either a single URI ('uri') or multiple ('uris') -- the
    underlying download() function already supports a list.

    output_urls records the ACTUAL, final URL each downloaded file was
    fetched from -- after any HTTP redirect. download.py's _download_file
    already resolves redirects internally and reports the final url via
    manifest_context.record_file(); this step supplies a ManifestContext
    (the user's real one if set on StepContext, otherwise a throwaway one
    used purely to capture this information) and reads the resolved URLs
    back from it, rather than re-deriving redirects itself. Re-deriving
    was tried first and found to be wrong: a version/artifact/group URI
    does not redirect the same way an individual file URL does, so
    checking the input URI directly gives the wrong (un-redirected)
    answer. Reading what download.py already resolved is correct
    regardless of whether the input was a single file, version, artifact,
    or group URI, and regardless of how many files it expanded to.
    """

    def run(self, step_config: Dict[str, Any], context: StepContext) -> None:
        resolved = context.resolve(step_config)
        name = resolved["name"]

        uri_value = resolved.get("uri") or resolved.get("uris")
        if not uri_value:
            raise StepValidationError(
                f"Step '{name}': download step requires 'uri' (single) or "
                f"'uris' (list)."
            )
        uris = [uri_value] if isinstance(uri_value, str) else list(uri_value)

        local_dir = resolved.get("localdir")
        if local_dir is None:
            local_dir = os.path.join(os.getcwd(), ".workflow", name)

        capture_context = context.manifest_context or ManifestContext(command="download")
        files_before = len(capture_context.files)

        api_download(
            localDir=local_dir,
            endpoint=resolved.get("databus"),
            databusURIs=uris,
            token=resolved.get("vault_token"),
            databus_key=resolved.get("databus_key"),
            all_versions=resolved.get("all_versions", False),
            compression=resolved.get("convert_to") or resolved.get("compression"),
            convert_format=resolved.get("format"),
            graph_name=resolved.get("graph_name"),
            base_uri=resolved.get("base_uri"),
            validate_checksum=resolved.get("validate_checksum", False),
            manifest_context=capture_context,
        )

        new_entries = capture_context.files[files_before:]
        resolved_urls = [e["url"] for e in new_entries if e.get("status") == "success"]

        output_files = self._collect_output_files(local_dir)
        context.set_output(name, "output_files", output_files)
        context.set_output(name, "output_urls", resolved_urls)

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
    """Adapts a workflow step to a call to create_dataset() + deploy(),
    or to webdav.upload_to_webdav() + deploy_from_metadata() in WebDAV mode.

    Classic and metadata-file deploy modes operate on their normal inputs
    (URLs, or an already-resolved metadata list) and must NOT be given
    local file paths from a previous step -- neither mode can turn a local
    path into a fetchable URL. Chaining a previous step's local files into
    a deploy step is only supported via WebDAV mode: local files are
    uploaded first, which produces real URLs, and any local modifications
    (format/compression conversions) are correctly reflected since the
    upload happens after those conversions.
    """

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

        webdav_url = resolved.get("webdav_url")
        remote = resolved.get("remote")
        path = resolved.get("path")
        webdav_fields = [webdav_url, remote, path]

        if any(webdav_fields) and not all(webdav_fields):
            raise StepValidationError(
                f"Step '{name}': WebDAV deploy mode requires 'webdav_url', "
                f"'remote', and 'path' together."
            )

        if all(webdav_fields):
            output_files = self._run_webdav_mode(resolved, name)
        else:
            output_files = self._run_classic_mode(resolved, name)

        context.set_output(name, "output_files", output_files)
        context.set_output(name, "version_id", resolved["version_id"])

        # deploy()/deploy_from_metadata() do not accept manifest_context
        # (unlike download()/delete()) -- manifest recording for deploy is
        # always done manually by the caller. This mirrors exactly what
        # cli.py's own `deploy` command does after a successful deploy.
        if context.manifest_context is not None:
            for url in output_files:
                context.manifest_context.record_file(url=url, status="success")

    def _run_classic_mode(self, resolved: Dict[str, Any], name: str) -> list:
        files = resolved.get("files")
        if not files:
            raise StepValidationError(
                f"Step '{name}': deploy step requires 'files' (a list of URLs)."
            )
        if isinstance(files, str):
            files = [files]

        non_urls = [f for f in files if not str(f).split("|")[0].startswith(("http://", "https://"))]
        if non_urls:
            raise StepValidationError(
                f"Step '{name}': 'files' must be URLs (http:// or https://). "
                f"Found non-URL value(s): {non_urls}. Local file paths from "
                f"a previous download step are not accepted in classic "
                f"deploy mode -- use WebDAV mode ('webdav_url', 'remote', "
                f"'path') to deploy locally modified files."
            )

        dataid = create_dataset(
            version_id=resolved["version_id"],
            artifact_version_title=resolved["title"],
            artifact_version_abstract=resolved["abstract"],
            artifact_version_description=resolved["description"],
            license_url=resolved["license"],
            distributions=files,
        )
        api_deploy_call(dataid=dataid, api_key=resolved["api_key"])
        return files

    def _run_webdav_mode(self, resolved: Dict[str, Any], name: str) -> list:
        local_files = resolved.get("files")
        if not local_files:
            raise StepValidationError(
                f"Step '{name}': WebDAV deploy mode requires 'files' (local "
                f"file paths to upload, e.g. from a previous download step)."
            )
        if isinstance(local_files, str):
            local_files = [local_files]

        metadata = webdav.upload_to_webdav(
            local_files, resolved["remote"], resolved["path"], resolved["webdav_url"]
        )
        deploy_from_metadata(
            metadata,
            resolved["version_id"],
            resolved["title"],
            resolved["abstract"],
            resolved["description"],
            resolved["license"],
            resolved["api_key"],
        )
        return [entry.get("url", "") for entry in metadata]


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