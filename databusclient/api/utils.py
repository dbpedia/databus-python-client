"""Utility helpers used by the API submodules.

Contains small parsing helpers and HTTP helpers that are shared by
`download`, `deploy` and `delete` modules.
"""

import hashlib
import re
from typing import Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Regex for Databus identifier components (account, group, artifact, version)
_DATABUS_ID_RE = re.compile(r"^[a-zA-Z0-9_.-]+$")
# Regex for authority (host:port)
_DATABUS_AUTHORITY_RE = re.compile(r"^[a-zA-Z0-9.-]+(?::[0-9]+)?$")


def get_http_session(
    retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: Tuple[int, ...] = (429, 500, 502, 503, 504),
    allowed_methods: Optional[frozenset] = None,
) -> requests.Session:
    """Create and configure a requests Session with HTTP retry strategy and exponential backoff.

    Args:
        retries: Total number of retries to allow.
        backoff_factor: Backoff factor to apply between attempts.
        status_forcelist: Set of HTTP status codes to force retry on.
        allowed_methods: Set of HTTP methods allowed for retry.

    Returns:
        Configured requests.Session object.
    """
    session = requests.Session()
    kwargs = {
        "total": retries,
        "backoff_factor": backoff_factor,
        "status_forcelist": status_forcelist,
        "raise_on_status": False,
    }
    if allowed_methods is not None:
        kwargs["allowed_methods"] = allowed_methods

    retry_strategy = Retry(**kwargs)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


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


def fetch_databus_jsonld(
    uri: str,
    databus_key: str | None = None,
    session: requests.Session | None = None,
    timeout: int = 30,
) -> str:
    """Fetch the JSON-LD representation of a Databus resource.

    Args:
        uri: Full Databus resource URI.
        databus_key: Optional API key for protected resources.
        session: Optional HTTP session to use for requests.
        timeout: Request timeout in seconds.

    Returns:
        The response body as a string containing JSON-LD.
    """
    if session is None:
        session = get_http_session()

    headers = {"Accept": "application/ld+json"}
    if databus_key is not None:
        headers["X-API-KEY"] = databus_key
    response = session.get(uri, headers=headers, timeout=timeout)
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
