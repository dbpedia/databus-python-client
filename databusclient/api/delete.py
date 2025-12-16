import json
from typing import List

import requests

from databusclient.api.utils import (
    fetch_databus_jsonld,
    get_databus_id_parts_from_file_url,
)


class DeleteQueue:
    """
    A queue to manage multiple Databus resource deletions.
    Allows adding multiple databus URIs to a queue and executing their deletion in batch.
    """

    def __init__(self, databus_key: str):
        self.databus_key = databus_key
        self.queue: set[str] = set()

    def add_uri(self, databusURI: str):
        self.queue.add(databusURI)

    def add_uris(self, databusURIs: List[str]):
        for uri in databusURIs:
            self.queue.add(uri)

    def is_empty(self) -> bool:
        return len(self.queue) == 0

    def is_not_empty(self) -> bool:
        return len(self.queue) > 0

    def execute(self):
        for uri in self.queue:
            print(f"[DELETE] {uri}")
            _delete_resource(
                uri,
                self.databus_key,
                force=True,
            )


def _confirm_delete(databusURI: str) -> str:
    """
    Confirm deletion of a Databus resource with the user.

    Parameters:
    - databusURI: The full databus URI of the resource to delete

    Returns:
    - "confirm" if the user confirms deletion
    - "skip" if the user chooses to skip deletion
    - "cancel" if the user chooses to cancel the entire deletion process
    """
    print(f"Are you sure you want to delete: {databusURI}?")
    print(
        "\nThis action is irreversible and will permanently remove the resource and all its data."
    )
    while True:
        choice = (
            input(
                "Type 'yes'/'y' to confirm, 'skip'/'s' to skip this resource, or 'cancel'/'c' to abort: "
            )
            .strip()
            .lower()
        )
        if choice in ("yes", "y"):
            return "confirm"
        elif choice in ("skip", "s"):
            return "skip"
        elif choice in ("cancel", "c"):
            return "cancel"
        else:
            print("Invalid input. Please type 'yes'/'y', 'skip'/'s', or 'cancel'/'c'.")


def _delete_resource(
    databusURI: str,
    databus_key: str,
    dry_run: bool = False,
    force: bool = False,
    queue: DeleteQueue = None,
):
    """
    Delete a single Databus resource (version, artifact, group).

    Equivalent to:
    curl -X DELETE "<databusURI>" -H "accept: */*" -H "X-API-KEY: <key>"

    Parameters:
    - databusURI: The full databus URI of the resource to delete
    - databus_key: Databus API key to authenticate the deletion request
    - dry_run: If True, do not perform the deletion but only print what would be deleted
    - force: If True, skip confirmation prompt and proceed with deletion
    - queue: If queue is provided, add the URI to the queue instead of deleting immediately
    """

    # Confirm the deletion request, skip the request or cancel deletion process
    if not (dry_run or force):
        action = _confirm_delete(databusURI)
        if action == "skip":
            print(f"Skipping: {databusURI}\n")
            return
        if action == "cancel":
            raise KeyboardInterrupt("Deletion cancelled by user.")

    if databus_key is None:
        raise ValueError("Databus API key must be provided for deletion")

    if dry_run:
        print(f"[DRY RUN] Would delete: {databusURI}")
        return

    if queue is not None:
        queue.add_uri(databusURI)
        return

    headers = {"accept": "*/*", "X-API-KEY": databus_key}
    response = requests.delete(databusURI, headers=headers, timeout=30)

    if response.status_code in (200, 204):
        print(f"Successfully deleted: {databusURI}")
    else:
        raise Exception(
            f"Failed to delete {databusURI}: {response.status_code} - {response.text}"
        )


def _delete_list(
    databusURIs: List[str],
    databus_key: str,
    dry_run: bool = False,
    force: bool = False,
    queue: DeleteQueue = None,
):
    """
    Delete a list of Databus resources.

    Parameters:
    - databusURIs: List of full databus URIs of the resources to delete
    - databus_key: Databus API key to authenticate the deletion requests
    - dry_run: If True, do not perform the deletion but only print what would be deleted
    - force: If True, skip confirmation prompt and proceed with deletion
    - queue: If queue is provided, add the URIs to the queue instead of deleting immediately
    """
    for databusURI in databusURIs:
        _delete_resource(
            databusURI, databus_key, dry_run=dry_run, force=force, queue=queue
        )


def _delete_artifact(
    databusURI: str,
    databus_key: str,
    dry_run: bool = False,
    force: bool = False,
    queue: DeleteQueue = None,
):
    """
    Delete an artifact and all its versions.

    This function first retrieves all versions of the artifact and then deletes them one by one.
    Finally, it deletes the artifact itself.

    Parameters:
    - databusURI: The full databus URI of the artifact to delete
    - databus_key: Databus API key to authenticate the deletion requests
    - dry_run: If True, do not perform the deletion but only print what would be deleted
    - force: If True, skip confirmation prompt and proceed with deletion
    - queue: If queue is provided, add the URI to the queue instead of deleting immediately
    """
    artifact_body = fetch_databus_jsonld(databusURI, databus_key)

    json_dict = json.loads(artifact_body)
    versions = json_dict.get("databus:hasVersion")

    # Single version case {}
    if isinstance(versions, dict):
        versions = [versions]
    # Multiple versions case [{}, {}]

    # If versions is None or empty skip
    if versions is None:
        print(f"No versions found for artifact: {databusURI}")
    else:
        version_uris = [v["@id"] for v in versions if "@id" in v]
        if not version_uris:
            print(f"No version URIs found in artifact JSON-LD for: {databusURI}")
        else:
            # Delete all versions
            _delete_list(
                version_uris, databus_key, dry_run=dry_run, force=force, queue=queue
            )

    # Finally, delete the artifact itself
    _delete_resource(databusURI, databus_key, dry_run=dry_run, force=force, queue=queue)


def _delete_group(
    databusURI: str,
    databus_key: str,
    dry_run: bool = False,
    force: bool = False,
    queue: DeleteQueue = None,
):
    """
    Delete a group and all its artifacts and versions.

    This function first retrieves all artifacts of the group, then deletes each artifact (which in turn deletes its versions).
    Finally, it deletes the group itself.

    Parameters:
    - databusURI: The full databus URI of the group to delete
    - databus_key: Databus API key to authenticate the deletion requests
    - dry_run: If True, do not perform the deletion but only print what would be deleted
    - force: If True, skip confirmation prompt and proceed with deletion
    - queue: If queue is provided, add the URI to the queue instead of deleting immediately
    """
    group_body = fetch_databus_jsonld(databusURI, databus_key)

    json_dict = json.loads(group_body)
    artifacts = json_dict.get("databus:hasArtifact", [])

    artifact_uris = []
    for item in artifacts:
        uri = item.get("@id")
        if not uri:
            continue
        _, _, _, _, version, _ = get_databus_id_parts_from_file_url(uri)
        if version is None:
            artifact_uris.append(uri)

    # Delete all artifacts (which deletes their versions)
    for artifact_uri in artifact_uris:
        _delete_artifact(
            artifact_uri, databus_key, dry_run=dry_run, force=force, queue=queue
        )

    # Finally, delete the group itself
    _delete_resource(databusURI, databus_key, dry_run=dry_run, force=force, queue=queue)


def delete(databusURIs: List[str], databus_key: str, dry_run: bool, force: bool):
    """
    Delete a dataset from the databus.

    Delete a group, artifact, or version identified by the given databus URI.
    Will recursively delete all data associated with the dataset.

    Parameters:
    - databusURIs: List of full databus URIs of the resources to delete
    - databus_key: Databus API key to authenticate the deletion requests
    - dry_run: If True, will only print what would be deleted without performing actual deletions
    - force: If True, skip confirmation prompt and proceed with deletion
    """

    queue = DeleteQueue(databus_key)

    for databusURI in databusURIs:
        _host, _account, group, artifact, version, file = (
            get_databus_id_parts_from_file_url(databusURI)
        )

        if group == "collections" and artifact is not None:
            print(f"Deleting collection: {databusURI}")
            _delete_resource(
                databusURI, databus_key, dry_run=dry_run, force=force, queue=queue
            )
        elif file is not None:
            print(f"Deleting file is not supported via API: {databusURI}")
        elif version is not None:
            print(f"Deleting version: {databusURI}")
            _delete_resource(
                databusURI, databus_key, dry_run=dry_run, force=force, queue=queue
            )
        elif artifact is not None:
            print(f"Deleting artifact and all its versions: {databusURI}")
            _delete_artifact(
                databusURI, databus_key, dry_run=dry_run, force=force, queue=queue
            )
        elif group is not None and group != "collections":
            print(f"Deleting group and all its artifacts and versions: {databusURI}")
            _delete_group(
                databusURI, databus_key, dry_run=dry_run, force=force, queue=queue
            )
        else:
            print(f"Deleting {databusURI} is not supported.")

    # Execute queued deletions
    if queue.is_not_empty():
        print("\nExecuting queued deletions...")
        queue.execute()
