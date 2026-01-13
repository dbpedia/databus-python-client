"""Utility helpers used by the API submodules.

Contains small parsing helpers and HTTP helpers that are shared by
`download`, `deploy` and `delete` modules.
"""

from typing import Optional, Tuple
import hashlib
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
    """Split a Databus URI into its six parts.

    The returned tuple is (host, accountId, groupId, artifactId, versionId, fileId).
    Missing parts are returned as ``None``.

    Args:
        uri: The full databus URI of the form
            "http(s)://host/accountId/groupId/artifactId/versionId/fileId".

    Returns:
        A tuple containing (host, accountId, groupId, artifactId, versionId, fileId).
    """
    uri = uri.removeprefix("https://").removeprefix("http://")
    parts = uri.strip("/").split("/")
    parts += [None] * (6 - len(parts))  # pad with None if less than 6 parts
    return tuple(parts[:6])  # return only the first 6 parts


def fetch_databus_jsonld(
    uri: str,
    databus_key: Optional[str] = None,
    verbose: bool = False,
) -> str:
    """Fetch the JSON-LD representation of a Databus resource.

    Args:
        uri: Full Databus resource URI.
        databus_key: Optional API key for protected resources.
        verbose: When True, log redacted HTTP request/response details.

    Returns:
        The response body as a string containing JSON-LD.
    """
    headers = {"Accept": "application/ld+json"}
    if databus_key is not None:
        headers["X-API-KEY"] = databus_key

    if verbose:
        log_http("GET", uri, req_headers=headers)

    response = requests.get(uri, headers=headers, timeout=30)

    if verbose:
        log_http(
            "GET",
            uri,
            req_headers=headers,
            status=response.status_code,
            resp_headers=response.headers,
        )

    response.raise_for_status()
    return response.text
def compute_sha256_and_length(filepath):
    sha256 = hashlib.sha256()
    total_length = 0
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(4096)
            if not chunk:
                break
            sha256.update(chunk)
            total_length += len(chunk)
    return sha256.hexdigest(), total_length
