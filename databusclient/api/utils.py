"""Utility helpers used by the API submodules.

Contains small parsing helpers and HTTP helpers that are shared by
`download`, `deploy` and `delete` modules.
"""

import hashlib
import posixpath
from typing import Optional, Tuple
from urllib.parse import unquote, urlparse

import requests


def get_filename_from_url(url: str) -> str:
    """Extract a clean, filesystem-safe filename from a URL.

    Strips query parameters and URL fragments, unquotes percent-encoded
    characters, and falls back to 'downloaded_file' if the path is empty.

    Args:
        url: The download URL.

    Returns:
        The clean filename string.
    """
    parsed = urlparse(url)
    clean_path = parsed.path
    basename = posixpath.basename(unquote(clean_path))
    return basename or "downloaded_file"


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

    The returned tuple is (host, accountId, groupId, artifactId, versionId, fileId).
    Missing parts are returned as ``None``. Query strings and fragments are stripped.

    Args:
        uri: The full databus URI of the form "http(s)://host/accountId/groupId/artifactId/versionId/fileId".

    Returns:
        A tuple containing (host, accountId, groupId, artifactId, versionId, fileId).
        Each element is a string or None if not present.
    """
    clean_uri = uri.split("?")[0].split("#")[0]
    clean_uri = clean_uri.removeprefix("https://").removeprefix("http://")
    parts = clean_uri.strip("/").split("/")
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
