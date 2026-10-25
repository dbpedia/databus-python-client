"""Utility helpers used by the API submodules.

Contains small parsing helpers and HTTP helpers that are shared by
`download`, `deploy` and `delete` modules.
"""

from typing import Any, Dict, Iterable, Optional, Tuple, Union
import hashlib
import os
import requests


def parse_headers(
    headers: Union[Dict[str, Any], Iterable[str], None],
) -> Dict[str, str]:
    """Parse HTTP headers from dict, list, tuple, or None into a clean dict of strings.

    Supports:
    - Dict of headers: e.g. {"User-Agent": "MyBot/1.0", "Accept": "*/*"}
    - Iterable of 'Name: Value' strings: e.g. ["User-Agent: MyBot/1.0"]

    Args:
        headers: Dict or iterable of header strings, or None.

    Returns:
        Dict[str, str]: Parsed headers dictionary.
    """
    if not headers:
        return {}

    if isinstance(headers, dict):
        return {str(k).strip(): str(v).strip() for k, v in headers.items()}

    parsed: Dict[str, str] = {}
    for h in headers:
        if isinstance(h, str):
            if ":" not in h:
                raise ValueError(
                    f"Invalid header format '{h}'. Expected 'Header-Name: Header-Value'."
                )
            k, v = h.split(":", 1)
            parsed[k.strip()] = v.strip()
        else:
            raise ValueError(f"Unexpected header type: {type(h).__name__}")
    return parsed


def get_default_headers(
    headers: Union[Dict[str, Any], Iterable[str], None] = None,
) -> Dict[str, str]:
    """Return headers dictionary with default User-Agent and any overrides applied.

    If User-Agent is not provided in headers, uses the DATABUS_USER_AGENT
    environment variable if set, otherwise falls back to databusclient/<version>.

    Args:
        headers: User-supplied headers (dict or iterable of strings).

    Returns:
        Dict[str, str]: Resolved headers dictionary.
    """
    custom = parse_headers(headers)
    has_user_agent = any(k.lower() == "user-agent" for k in custom.keys())

    res: Dict[str, str] = {}
    if not has_user_agent:
        env_ua = os.environ.get("DATABUS_USER_AGENT")
        if env_ua:
            res["User-Agent"] = env_ua
        else:
            from databusclient.version import __version__

            res["User-Agent"] = f"databusclient/{__version__}"

    res.update(custom)
    return res


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
    headers: Union[Dict[str, Any], Iterable[str], None] = None,
) -> str:
    """Fetch the JSON-LD representation of a Databus resource.

    Args:
        uri: Full Databus resource URI.
        databus_key: Optional API key for protected resources.
        headers: Optional custom headers to send with the request.

    Returns:
        The response body as a string containing JSON-LD.
    """

    req_headers = get_default_headers(headers)
    if "Accept" not in req_headers:
        req_headers["Accept"] = "application/ld+json"
    if databus_key is not None:
        req_headers["X-API-KEY"] = databus_key
    response = requests.get(uri, headers=req_headers, timeout=30)
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
