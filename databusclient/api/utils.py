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


def fetch_databus_jsonld(uri: str, databus_key: str | None = None, verbose: bool = False) -> str:
    """
    Retrieve JSON-LD representation of a databus resource.

    Parameters:
    - uri: The full databus URI
    - databus_key: Optional Databus API key for authentication on protected resources
    - verbose: when True, print redacted HTTP request/response details

    Returns:
    JSON-LD string representation of the databus resource.
    """
    headers = {"Accept": "application/ld+json"}
    if databus_key is not None:
        headers["X-API-KEY"] = databus_key
    if verbose:
        log_http("GET", uri, req_headers=headers)
    response = requests.get(uri, headers=headers, timeout=30)
    if verbose:
        log_http("GET", uri, req_headers=headers, status=response.status_code, resp_headers=response.headers)
    response.raise_for_status()

    return response.text


def _redact_headers(headers):
    if not headers:
        return headers
    redacted = {}
    for k, v in headers.items():
        key = k.lower()
        if key == "authorization" or key.startswith("x-api-key"):
            redacted[k] = "REDACTED"
        else:
            redacted[k] = v
    return redacted


import logging


def log_http(method, url, req_headers=None, status=None, resp_headers=None, body_snippet=None):
    """Log HTTP request/response details at DEBUG level with sanitized headers."""
    logger = logging.getLogger("databusclient")
    msg_lines = [f"[HTTP] {method} {url}"]
    if req_headers:
        msg_lines.append(f"  Req headers: {_redact_headers(req_headers)}")
    if status is not None:
        msg_lines.append(f"  Status: {status}")
    if resp_headers:
        # try to convert to dict; handle Mock or response objects gracefully
        try:
            resp_dict = dict(resp_headers)
        except Exception:
            # resp_headers might be a Mock or requests.Response; try common attributes
            if hasattr(resp_headers, "items"):
                try:
                    resp_dict = dict(resp_headers.items())
                except Exception:
                    resp_dict = {"headers": str(resp_headers)}
            elif hasattr(resp_headers, "headers"):
                try:
                    resp_dict = dict(getattr(resp_headers, "headers") or {})
                except Exception:
                    resp_dict = {"headers": str(resp_headers)}
            else:
                resp_dict = {"headers": str(resp_headers)}
        msg_lines.append(f"  Resp headers: {_redact_headers(resp_dict)}")
    if body_snippet:
        msg_lines.append("  Body preview: " + body_snippet[:500])
    logger.debug("\n".join(msg_lines))
