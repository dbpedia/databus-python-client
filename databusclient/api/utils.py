from typing import Optional, Tuple

import requests


def get_databus_id_parts_from_file_url(
    uri: str,
) -> Tuple[
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
    Optional[str],
]:
    """
    Extract databus ID parts from a given databus URI.

    Parameters:
    - uri: The full databus URI of the form
      "http(s)://host/accountId/groupId/artifactId/versionId/fileId"

    Returns:
    A tuple containing (host, accountId, groupId, artifactId, versionId, fileId).
    Each element is a string or None if not present.
    """
    uri = uri.removeprefix("https://").removeprefix("http://")
    parts = uri.strip("/").split("/")
    parts += [None] * (6 - len(parts))  # pad with None if less than 6 parts
    return tuple(parts[:6])  # return only the first 6 parts


def fetch_databus_jsonld(uri: str, databus_key: str | None = None) -> str:
    """
    Retrieve JSON-LD representation of a databus resource.

    Parameters:
    - uri: The full databus URI
    - databus_key: Optional Databus API key for authentication on protected resources

    Returns:
    JSON-LD string representation of the databus resource.
    """
    headers = {"Accept": "application/ld+json"}
    if databus_key is not None:
        headers["X-API-KEY"] = databus_key
    response = requests.get(uri, headers=headers, timeout=30)
    response.raise_for_status()

    return response.text
