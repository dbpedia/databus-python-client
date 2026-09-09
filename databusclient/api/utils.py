"""Utility helpers used by the API submodules.

Contains small parsing helpers and HTTP helpers that are shared by
`download`, `deploy` and `delete` modules.
"""

import hashlib
import re
from typing import Optional, Tuple
from urllib.parse import urlparse
import requests

# Regex for Databus identifier components (account, group, artifact, version)
_DATABUS_ID_RE = re.compile(r"^[a-zA-Z0-9_.-]+$")
# Regex for authority (host:port)
_DATABUS_AUTHORITY_RE = re.compile(r"^[a-zA-Z0-9.-]+(?::[0-9]+)?$")



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
    """Extract databus ID parts from a given databus URI.

    Args:
        uri: The full databus URI of the form "http(s)://host/accountId/groupId/artifactId/versionId/fileId".

    Returns:
        A tuple containing (host, accountId, groupId, artifactId, versionId, fileId).
        Each element is a string or None if not present.
    """
    """Split a Databus URI into its six parts.

    The returned tuple is (host, accountId, groupId, artifactId, versionId, fileId).
    Missing parts are returned as ``None``.
    """

    uri = uri.removeprefix("https://").removeprefix("http://")
    parts = uri.strip("/").split("/")
    parts += [None] * (6 - len(parts))  # pad with None if less than 6 parts
    return tuple(parts[:6])  # return only the first 6 parts


def fetch_databus_jsonld(uri: str, databus_key: str | None = None) -> str:
    """Fetch the JSON-LD representation of a Databus resource.

    Args:
        uri: Full Databus resource URI.
        databus_key: Optional API key for protected resources.

    Returns:
        The response body as a string containing JSON-LD.
    """

    headers = {"Accept": "application/ld+json"}
    if databus_key is not None:
        headers["X-API-KEY"] = databus_key
    response = requests.get(uri, headers=headers, timeout=30)
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


def validate_databus_version_uri(uri: str) -> None:
    """Validate a Databus version URI format.

    Expects format: http(s)://<AUTHORITY>/<ACCOUNT>/<GROUP>/<ARTIFACT>/<VERSION>

    Raises:
        ValueError: If URI scheme, authority, trailing slashes, or path components are invalid.
    """
    if not uri or not isinstance(uri, str):
        raise ValueError("Databus version_id must be a non-empty string.")

    if not (uri.startswith("http://") or uri.startswith("https://")):
        raise ValueError(
            f"Invalid version_id URI scheme: '{uri}'. Must start with 'http://' or 'https://'."
        )

    parsed = urlparse(uri)
    if not parsed.netloc or not _DATABUS_AUTHORITY_RE.match(parsed.netloc):
        raise ValueError(
            f"Invalid authority in version_id URI: '{parsed.netloc or uri}'."
        )

    # Preserve slash delimiters: path must start with '/' followed by account/group/artifact/version
    # Any trailing slash, double slash, or extra segment creates empty or extra parts.
    path_segments = parsed.path.split("/")[1:]

    if len(path_segments) != 4:
        raise ValueError(
            f"Invalid version_id format: '{uri}'. Expected format: <BASE>/<ACCOUNT>/<GROUP>/<ARTIFACT>/<VERSION>"
        )

    component_names = ["ACCOUNT", "GROUP", "ARTIFACT", "VERSION"]
    for name, seg in zip(component_names, path_segments):
        if not seg or not _DATABUS_ID_RE.match(seg):
            raise ValueError(
                f"Invalid Databus {name} component '{seg}' in version_id: '{uri}'. "
                "Must be non-empty and contain only alphanumeric characters, underscores, hyphens, or dots."
            )


