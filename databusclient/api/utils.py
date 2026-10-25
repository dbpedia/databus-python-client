"""Utility helpers used by the API submodules.

Contains small parsing helpers and HTTP helpers that are shared by
`download`, `deploy` and `delete` modules.
"""

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, Union
import hashlib
import os
import requests

SAFE_MANIFEST_HEADERS = frozenset(
    {"user-agent", "accept", "accept-encoding", "accept-language", "content-type"}
)
_SENSITIVE_HEADER_MARKERS = (
    "auth",
    "token",
    "secret",
    "key",
    "cookie",
    "password",
    "credential",
)


def has_header(headers: Mapping[str, Any], name: str) -> bool:
    """Return True if a header name is present (case-insensitive)."""
    target = name.lower()
    return any(k.lower() == target for k in headers)


def set_header_if_missing(headers: Dict[str, str], name: str, value: str) -> None:
    """Set header only if not already present (case-insensitive)."""
    if not has_header(headers, name):
        headers[name] = value


def set_header(headers: Dict[str, str], name: str, value: str) -> None:
    """Set header while removing any case-variant duplicate keys."""
    target = name.lower()
    for existing in [k for k in headers if k.lower() == target]:
        if existing != name:
            del headers[existing]
    headers[name] = value


def is_sensitive_header(name: str) -> bool:
    """Return True if a header name may carry authentication credentials."""
    lower = name.strip().lower()
    return any(marker in lower for marker in _SENSITIVE_HEADER_MARKERS)


def strip_sensitive_headers(headers: Mapping[str, str]) -> Dict[str, str]:
    """Return a copy of headers with credential-bearing headers removed."""
    return {k: v for k, v in headers.items() if not is_sensitive_header(k)}


def sanitize_headers_for_manifest(headers: Mapping[str, str]) -> Dict[str, str]:
    """Keep only explicitly safe non-sensitive header values in manifests."""
    return {
        k: (v if k.strip().lower() in SAFE_MANIFEST_HEADERS else "***")
        for k, v in headers.items()
    }


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
    user_agent: Optional[str] = None,
) -> Dict[str, str]:
    """Return headers dictionary with default User-Agent and any overrides applied.

    Priority for ``User-Agent`` is: explicit header override, explicit
    ``user_agent`` argument, ``DATABUS_USER_AGENT`` environment variable, and
    finally ``databus-python-client/<version> (...)``.

    Args:
        headers: User-supplied headers (dict or iterable of strings).
        user_agent: Optional User-Agent string override.

    Returns:
        Dict[str, str]: Resolved headers dictionary.
    """
    custom = parse_headers(headers)

    res: Dict[str, str] = {}
    if not has_header(custom, "user-agent"):
        if user_agent is not None:
            res["User-Agent"] = user_agent
        else:
            env_ua = os.environ.get("DATABUS_USER_AGENT")
            if env_ua:
                res["User-Agent"] = env_ua
            else:
                from databusclient.version import __version__

                res["User-Agent"] = (
                    f"databus-python-client/{__version__} "
                    "(https://github.com/dbpedia/databus-python-client)"
                )

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
    set_header_if_missing(req_headers, "Accept", "application/ld+json")
    if databus_key is not None:
        set_header(req_headers, "X-API-KEY", databus_key)
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
