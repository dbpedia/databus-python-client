import json
import os
from typing import List
from urllib.parse import urlparse

import requests
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm

from databusclient.api.utils import (
    fetch_databus_jsonld,
    get_databus_id_parts_from_file_url,
)

from databusclient.extensions.webdav import compute_sha256_and_length

def _extract_checksum_from_node(node) -> str | None:
    """
    Try to extract a 64-char hex checksum from a JSON-LD file node.
    Handles these common shapes:
    - checksum or sha256sum fields as plain string
    - checksum fields as dict with '@value'
    - nested values (recursively search strings for a 64-char hex)
    """
    def find_in_value(v):
        if isinstance(v, str):
            s = v.strip()
            if len(s) == 64 and all(c in "0123456789abcdefABCDEF" for c in s):
                return s
        if isinstance(v, dict):
            # common JSON-LD value object
            if "@value" in v and isinstance(v["@value"], str):
                res = find_in_value(v["@value"])
                if res:
                    return res
            # try all nested dict values
            for vv in v.values():
                res = find_in_value(vv)
                if res:
                    return res
        if isinstance(v, list):
            for item in v:
                res = find_in_value(item)
                if res:
                    return res
        return None

    # direct keys to try first
    for key in ("checksum", "sha256sum", "sha256", "databus:checksum"):
        if key in node:
            res = find_in_value(node[key])
            if res:
                return res

    # fallback: search all values recursively for a 64-char hex string
    for v in node.values():
        res = find_in_value(v)
        if res:
            return res
    return None



# Hosts that require Vault token based authentication. Central source of truth.
VAULT_REQUIRED_HOSTS = {
    "data.dbpedia.io",
    "data.dev.dbpedia.link",
}


class DownloadAuthError(Exception):
    """Raised when an authorization problem occurs during download."""



def _download_file(
    url,
    localDir,
    vault_token_file=None,
    databus_key=None,
    auth_url=None,
    client_id=None,
    validate_checksum: bool = False,
    expected_checksum: str | None = None,
) -> None:
    """
    Download a file from the internet with a progress bar using tqdm.

    Parameters:
    - url: the URL of the file to download
    - localDir: Local directory to download file to. If None, the databus folder structure is created in the current working directory.
    - vault_token_file: Path to Vault refresh token file
    - databus_key: Databus API key for protected downloads
    - auth_url: Keycloak token endpoint URL
    - client_id: Client ID for token exchange
    """
    if localDir is None:
        _host, account, group, artifact, version, file = (
            get_databus_id_parts_from_file_url(url)
        )
        localDir = os.path.join(
            os.getcwd(),
            account,
            group,
            artifact,
            version if version is not None else "latest",
        )
        print(f"Local directory not given, using {localDir}")

    file = url.split("/")[-1]
    filename = os.path.join(localDir, file)
    print(f"Download file: {url}")
    dirpath = os.path.dirname(filename)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)  # Create the necessary directories
    # --- 1. Get redirect URL by requesting HEAD ---
    headers = {}

    # --- 1a. public databus ---
    response = requests.head(url, timeout=30, allow_redirects=False)

    # Check for redirect and update URL if necessary
    if response.headers.get("Location") and response.status_code in [
        301,
        302,
        303,
        307,
        308,
    ]:
        url = response.headers.get("Location")
        print("Redirects url: ", url)
        # Re-do HEAD request on redirect URL
        response = requests.head(url, timeout=30)

    # Extract hostname from final URL (after redirect) to check if vault token needed.
    # This is the actual download location that may require authentication.
    parsed = urlparse(url)
    host = parsed.hostname

    # --- 1b. Handle 401 on HEAD request ---
    if response.status_code == 401:
        # Check if this is a vault-required host
        if host in VAULT_REQUIRED_HOSTS:
            # Vault-required host: need vault token
            if not vault_token_file:
                raise DownloadAuthError(
                    f"Vault token required for host '{host}', but no token was provided. Please use --vault-token."
                )
            # Token provided; will handle in GET request below
        else:
            # Not a vault host; might need databus API key
            if not databus_key:
                raise DownloadAuthError("Databus API key not given for protected download")
            headers = {"X-API-KEY": databus_key}
            response = requests.head(url, headers=headers, timeout=30)

    # --- 2. Try direct GET to redirected URL ---
    headers["Accept-Encoding"] = (
        "identity"  # disable gzip to get correct content-length
    )
    response = requests.get(
        url, headers=headers, stream=True, allow_redirects=True, timeout=30
    )
    www = response.headers.get("WWW-Authenticate", "")  # Check if authentication is required

    # --- 3. Handle authentication responses ---
    # 3a. Server requests Bearer auth. Only attempt token exchange for hosts
    # we explicitly consider Vault-protected (VAULT_REQUIRED_HOSTS). This avoids
    # sending tokens to unrelated hosts and makes auth behavior predictable.
    if response.status_code == 401 and "bearer" in www.lower():
        # If host is not configured for Vault, do not attempt token exchange.
        if host not in VAULT_REQUIRED_HOSTS:
            raise DownloadAuthError(
                "Server requests Bearer authentication but this host is not configured for Vault token exchange."
                " Try providing a databus API key with --databus-key or contact your administrator."
            )

        # Host requires Vault; ensure token file provided.
        if not vault_token_file:
            raise DownloadAuthError(
                f"Vault token required for host '{host}', but no token was provided. Please use --vault-token."
            )

        # --- 3b. Fetch Vault token and retry ---
        # Token exchange is potentially sensitive and should only be performed
        # for known hosts. __get_vault_access__ handles reading the refresh
        # token and exchanging it; errors are translated to DownloadAuthError
        # for user-friendly CLI output.
        vault_token = __get_vault_access__(url, vault_token_file, auth_url, client_id)
        headers["Authorization"] = f"Bearer {vault_token}"
        headers.pop("Accept-Encoding", None)

        # Retry with token
        response = requests.get(url, headers=headers, stream=True, timeout=30)

        # Map common auth failures to friendly messages
        if response.status_code == 401:
            raise DownloadAuthError("Vault token is invalid or expired. Please generate a new token.")
        if response.status_code == 403:
            raise DownloadAuthError("Vault token is valid but has insufficient permissions to access this file.")

    # 3c. Generic forbidden without Bearer challenge
    if response.status_code == 403:
        raise DownloadAuthError("Access forbidden: your token or API key does not have permission to download this file.")

    # 3d. Generic unauthorized without Bearer
    if response.status_code == 401:
        raise DownloadAuthError(
            "Unauthorized: access denied. Check your --databus-key or --vault-token settings."
        )

    try:
        response.raise_for_status()  # Raise if still failing
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            print(f"WARNING: Skipping file {url} because it was not found (404).")
            return
        else:
            raise e

    # --- 4. Download with progress bar ---
    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 KiB

    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    with open(filename, "wb") as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()

    # --- 5. Verify download size ---
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        raise IOError("Downloaded size does not match Content-Length header")

    # --- 6. Optional checksum validation ---
    if validate_checksum:
        # reuse compute_sha256_and_length from webdav extension
        try:
            actual, _ = compute_sha256_and_length(filename)
        except Exception:
            actual = None

        if expected_checksum is None:
            print(f"WARNING: no expected checksum available for {filename}; skipping validation")
        elif actual is None:
            print(f"WARNING: could not compute checksum for {filename}; skipping validation")
        else:
            if actual.lower() != expected_checksum.lower():
                raise IOError(
                    f"Checksum mismatch for {filename}: expected {expected_checksum}, got {actual}"
                )


def _download_files(
    urls: List[str],
    localDir: str,
    vault_token_file: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
    validate_checksum: bool = False,
    checksums: dict | None = None,
) -> None:
    """
    Download multiple files from the databus.

    Parameters:
    - urls: List of file download URLs
    - localDir: Local directory to download files to. If None, the databus folder structure is created in the current working directory.
    - vault_token_file: Path to Vault refresh token file
    - databus_key: Databus API key for protected downloads
    - auth_url: Keycloak token endpoint URL
    - client_id: Client ID for token exchange
    """
    for url in urls:
        expected = None
        if checksums and isinstance(checksums, dict):
            expected = checksums.get(url)
        _download_file(
            url=url,
            localDir=localDir,
            vault_token_file=vault_token_file,
            databus_key=databus_key,
            auth_url=auth_url,
            client_id=client_id,
            validate_checksum=validate_checksum,
            expected_checksum=expected,
        )


def _get_sparql_query_of_collection(uri: str, databus_key: str | None = None) -> str:
    """
    Get SPARQL query of collection members from databus collection URI.

    Parameters:
    - uri: The full databus collection URI
    - databus_key: Optional Databus API key for authentication on protected resources

    Returns:
    SPARQL query string to get download URLs of all files in the collection.
    """
    headers = {"Accept": "text/sparql"}
    if databus_key is not None:
        headers["X-API-KEY"] = databus_key

    response = requests.get(uri, headers=headers, timeout=30)
    response.raise_for_status()
    return response.text


def _query_sparql_endpoint(endpoint_url, query, databus_key=None) -> dict:
    """
    Query a SPARQL endpoint and return results in JSON format.

    Parameters:
    - endpoint_url: the URL of the SPARQL endpoint
    - query: the SPARQL query string
    - databus_key: Optional API key for authentication

    Returns:
    - Dictionary containing the query results
    """
    sparql = SPARQLWrapper(endpoint_url)
    sparql.method = "POST"
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    if databus_key is not None:
        sparql.setCustomHttpHeaders({"X-API-KEY": databus_key})
    results = sparql.query().convert()
    return results


def _get_file_download_urls_from_sparql_query(
    endpoint_url, query, databus_key=None
) -> List[str]:
    """
    Execute a SPARQL query to get databus file download URLs.

    Parameters:
    - endpoint_url: the URL of the SPARQL endpoint
    - query: the SPARQL query string
    - databus_key: Optional API key for authentication

    Returns:
    - List of file download URLs
    """
    result_dict = _query_sparql_endpoint(endpoint_url, query, databus_key=databus_key)

    bindings = result_dict.get("results", {}).get("bindings")
    if not isinstance(bindings, list):
        raise ValueError("Invalid SPARQL response: 'bindings' missing or not a list")

    urls: List[str] = []

    for binding in bindings:
        if not isinstance(binding, dict) or len(binding) != 1:
            raise ValueError(f"Invalid SPARQL binding structure: {binding}")

        value_dict = next(iter(binding.values()))
        value = value_dict.get("value")

        if not isinstance(value, str):
            raise ValueError(f"Invalid SPARQL value field: {value_dict}")

        urls.append(value)

    return urls


def __get_vault_access__(
    download_url: str, token_file: str, auth_url: str, client_id: str
) -> str:
    """
    Get Vault access token for a protected databus download.
    """
    # 1. Load refresh token
    refresh_token = os.environ.get("REFRESH_TOKEN")
    if not refresh_token:
        if not os.path.exists(token_file):
            raise FileNotFoundError(f"Vault token file not found: {token_file}")
        with open(token_file, "r") as f:
            refresh_token = f.read().strip()
    if len(refresh_token) < 80:
        print(f"Warning: token from {token_file} is short (<80 chars)")

    # 2. Refresh token -> access token
    resp = requests.post(
        auth_url,
        data={
            "client_id": client_id,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        timeout=30,
    )
    resp.raise_for_status()
    access_token = resp.json()["access_token"]

    # 3. Extract host as audience
    # Remove protocol prefix
    if download_url.startswith("https://"):
        host_part = download_url[len("https://") :]
    elif download_url.startswith("http://"):
        host_part = download_url[len("http://") :]
    else:
        host_part = download_url
    audience = host_part.split("/")[0]  # host is before first "/"

    # 4. Access token -> Vault token
    resp = requests.post(
        auth_url,
        data={
            "client_id": client_id,
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": access_token,
            "audience": audience,
        },
        timeout=30,
    )
    resp.raise_for_status()
    vault_token = resp.json()["access_token"]

    print(f"Using Vault access token for {download_url}")
    return vault_token


def _download_collection(
    uri: str,
    endpoint: str,
    localDir: str,
    vault_token: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
    validate_checksum: bool = False
) -> None:
    """
    Download all files in a databus collection.

    Parameters:
    - uri: The full databus collection URI
    - endpoint: the databus SPARQL endpoint URL
    - localDir: Local directory to download files to. If None, the databus folder structure is created in the current working directory.
    - vault_token: Path to Vault refresh token file for protected downloads
    - databus_key: Databus API key for protected downloads
    - auth_url: Keycloak token endpoint URL
    - client_id: Client ID for token exchange
    """
    query = _get_sparql_query_of_collection(uri, databus_key=databus_key)
    file_urls = _get_file_download_urls_from_sparql_query(
        endpoint, query, databus_key=databus_key
    )
    _download_files(
        list(file_urls),
        localDir,
        vault_token_file=vault_token,
        databus_key=databus_key,
        auth_url=auth_url,
        client_id=client_id,
        validate_checksum=validate_checksum,
    )


def _download_version(
    uri: str,
    localDir: str,
    vault_token_file: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
    validate_checksum: bool = False,
) -> None:
    """
    Download all files in a databus artifact version.

    Parameters:
    - uri: The full databus artifact version URI
    - localDir: Local directory to download files to. If None, the databus folder structure is created in the current working directory.
    - vault_token_file: Path to Vault refresh token file for protected downloads
    - databus_key: Databus API key for protected downloads
    - auth_url: Keycloak token endpoint URL
    - client_id: Client ID for token exchange
    """
    json_str = fetch_databus_jsonld(uri, databus_key=databus_key)
    file_urls = _get_file_download_urls_from_artifact_jsonld(json_str)
    # build url -> checksum mapping from JSON-LD when available
    checksums: dict = {}
    try:
        json_dict = json.loads(json_str)
        graph = json_dict.get("@graph", [])
        for node in graph:
            if node.get("@type") == "Part":
                file_uri = node.get("file")
                if not isinstance(file_uri, str):
                    continue
                expected = _extract_checksum_from_node(node)
                if expected:
                    checksums[file_uri] = expected
    except Exception:
        checksums = {}

    _download_files(
        file_urls,
        localDir,
        vault_token_file=vault_token_file,
        databus_key=databus_key,
        auth_url=auth_url,
        client_id=client_id,
        validate_checksum=validate_checksum,
        checksums=checksums,
    )


def _download_artifact(
    uri: str,
    localDir: str,
    all_versions: bool = False,
    vault_token_file: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
    validate_checksum: bool = False,
) -> None:
    """
    Download files in a databus artifact.

    Parameters:
    - uri: The full databus artifact URI
    - localDir: Local directory to download files to. If None, the databus folder structure is created in the current working directory.
    - all_versions: If True, download all versions of the artifact; otherwise, only download the latest version
    - vault_token_file: Path to Vault refresh token file for protected downloads
    - databus_key: Databus API key for protected downloads
    - auth_url: Keycloak token endpoint URL
    - client_id: Client ID for token exchange
    """
    json_str = fetch_databus_jsonld(uri, databus_key=databus_key)
    versions = _get_databus_versions_of_artifact(json_str, all_versions=all_versions)
    if isinstance(versions, str):
        versions = [versions]
    for version_uri in versions:
        print(f"Downloading version: {version_uri}")
        json_str = fetch_databus_jsonld(version_uri, databus_key=databus_key)
        file_urls = _get_file_download_urls_from_artifact_jsonld(json_str)
        # extract checksums for this version
        checksums: dict = {}
        try:
            jd = json.loads(json_str)
            graph = jd.get("@graph", [])
            for node in graph:
                if node.get("@type") == "Part":
                    file_uri = node.get("file")
                    if not isinstance(file_uri, str):
                        continue
                    expected = _extract_checksum_from_node(node)
                    if expected:
                        checksums[file_uri] = expected
        except Exception:
            checksums = {}

        _download_files(
            file_urls,
            localDir,
            vault_token_file=vault_token_file,
            databus_key=databus_key,
            auth_url=auth_url,
            client_id=client_id,
            validate_checksum=validate_checksum,
            checksums=checksums,
        )


def _get_databus_versions_of_artifact(
    json_str: str, all_versions: bool
) -> str | List[str]:
    """
    Parse the JSON-LD of a databus artifact to extract URLs of its versions.

    Parameters:
    - json_str: JSON-LD string of the databus artifact
    - all_versions: If True, return all version URLs; otherwise, return only the latest version URL

    Returns:
    - If all_versions is True: List of all version URLs
    - If all_versions is False: URL of the latest version
    """
    json_dict = json.loads(json_str)
    versions = json_dict.get("databus:hasVersion")

    if versions is None:
        raise ValueError("No 'databus:hasVersion' field in artifact JSON-LD")

    if isinstance(versions, dict):
        versions = [versions]
    elif not isinstance(versions, list):
        raise ValueError(
            f"Unexpected type for 'databus:hasVersion': {type(versions).__name__}"
        )

    version_urls = [v["@id"] for v in versions if isinstance(v, dict) and "@id" in v]

    if not version_urls:
        raise ValueError("No versions found in artifact JSON-LD")

    version_urls.sort(reverse=True)  # Sort versions in descending order

    if all_versions:
        return version_urls
    return version_urls[0]


def _get_file_download_urls_from_artifact_jsonld(json_str: str) -> List[str]:
    """
    Parse the JSON-LD of a databus artifact version to extract download URLs.
    Don't get downloadURLs directly from the JSON-LD, but follow the "file" links to count access to databus accurately.

    Parameters:
    - json_str: JSON-LD string of the databus artifact version

    Returns:
    List of all file download URLs in the artifact version.
    """

    databusIdUrl: List[str] = []

    json_dict = json.loads(json_str)
    graph = json_dict.get("@graph", [])
    for node in graph:
        if node.get("@type") == "Part":
            file_uri = node.get("file")
            if not isinstance(file_uri, str):
                continue
            databusIdUrl.append(file_uri)
    return databusIdUrl


def _download_group(
    uri: str,
    localDir: str,
    all_versions: bool = False,
    vault_token_file: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
    validate_checksum: bool = False,
) -> None:
    """
    Download files in a databus group.

    Parameters:
    - uri: The full databus group URI
    - localDir: Local directory to download files to. If None, the databus folder structure is created in the current working directory.
    - all_versions: If True, download all versions of each artifact in the group; otherwise, only download the latest version
    - vault_token_file: Path to Vault refresh token file for protected downloads
    - databus_key: Databus API key for protected downloads
    - auth_url: Keycloak token endpoint URL
    - client_id: Client ID for token exchange
    """
    json_str = fetch_databus_jsonld(uri, databus_key=databus_key)
    artifacts = _get_databus_artifacts_of_group(json_str)
    for artifact_uri in artifacts:
        print(f"Download artifact: {artifact_uri}")
        _download_artifact(
            artifact_uri,
            localDir,
            all_versions=all_versions,
            vault_token_file=vault_token_file,
            databus_key=databus_key,
            auth_url=auth_url,
            client_id=client_id,
            validate_checksum=validate_checksum,
        )


def _get_databus_artifacts_of_group(json_str: str) -> List[str]:
    """
    Parse the JSON-LD of a databus group to extract URLs of all artifacts.

    Returns a list of artifact URLs.
    """
    json_dict = json.loads(json_str)
    artifacts = json_dict.get("databus:hasArtifact")

    if artifacts is None:
        return []

    if isinstance(artifacts, dict):
        artifacts_iter = [artifacts]
    elif isinstance(artifacts, list):
        artifacts_iter = artifacts
    else:
        raise ValueError(
            f"Unexpected type for 'databus:hasArtifact': {type(artifacts).__name__}"
        )

    result: List[str] = []
    for item in artifacts_iter:
        if not isinstance(item, dict):
            continue
        uri = item.get("@id")
        if not uri:
            continue
        _, _, _, _, version, _ = get_databus_id_parts_from_file_url(uri)
        if version is None:
            result.append(uri)
    return result


def download(
    localDir: str,
    endpoint: str,
    databusURIs: List[str],
    token=None,
    databus_key=None,
    all_versions=None,
    auth_url="https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token",
    client_id="vault-token-exchange",
    validate_checksum: bool = False
) -> None:
    """
    Download datasets from databus.

    Download of files, versions, artifacts, groups or databus collections via their databus URIs or user-defined SPARQL queries that return file download URLs.

    Parameters:
    - localDir: Local directory to download datasets to. If None, the databus folder structure is created in the current working directory.
    - endpoint: the databus endpoint URL. If None, inferred from databusURI. Required for user-defined SPARQL queries.
    - databusURIs: databus identifiers to specify datasets to download.
    - token: Path to Vault refresh token file for protected downloads
    - databus_key: Databus API key for protected downloads
    - auth_url: Keycloak token endpoint URL. Default is "https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token".
    - client_id: Client ID for token exchange. Default is "vault-token-exchange".
    """
    for databusURI in databusURIs:
        host, account, group, artifact, version, file = (
            get_databus_id_parts_from_file_url(databusURI)
        )

        # Determine endpoint per-URI if not explicitly provided
        uri_endpoint = endpoint

        # dataID or databus collection
        if databusURI.startswith("http://") or databusURI.startswith("https://"):
            # Auto-detect sparql endpoint from host if not given
            if uri_endpoint is None:
                uri_endpoint = f"https://{host}/sparql"
            print(f"SPARQL endpoint {uri_endpoint}")

            if group == "collections" and artifact is not None:
                print(f"Downloading collection: {databusURI}")
                _download_collection(
                    databusURI,
                    uri_endpoint,
                    localDir,
                    token,
                    databus_key,
                    auth_url,
                    client_id,
                    validate_checksum=validate_checksum,
                )
            elif file is not None:
                print(f"Downloading file: {databusURI}")
                # Try to fetch expected checksum from the parent Version metadata
                expected = None
                if validate_checksum:
                    try:
                        version_uri = f"https://{host}/{account}/{group}/{artifact}/{version}"
                        json_str = fetch_databus_jsonld(version_uri, databus_key=databus_key)
                        json_dict = json.loads(json_str)
                        graph = json_dict.get("@graph", [])
                        for node in graph:
                            if node.get("file") == databusURI or node.get("@id") == databusURI:
                                expected = _extract_checksum_from_node(node)
                                if expected:
                                    break
                    except Exception as e:
                        print(f"WARNING: Could not fetch checksum for single file: {e}")

                # Call the worker to download the single file (passes expected checksum)
                _download_file(
                    databusURI,
                    localDir,
                    vault_token_file=token,
                    databus_key=databus_key,
                    auth_url=auth_url,
                    client_id=client_id,
                    validate_checksum=validate_checksum,
                    expected_checksum=expected,
                )
            elif version is not None:
                print(f"Downloading version: {databusURI}")
                _download_version(
                    databusURI,
                    localDir,
                    vault_token_file=token,
                    databus_key=databus_key,
                    auth_url=auth_url,
                    client_id=client_id,
                    validate_checksum=validate_checksum,
                )
            elif artifact is not None:
                print(
                    f"Downloading {'all' if all_versions else 'latest'} version(s) of artifact: {databusURI}"
                )
                _download_artifact(
                    databusURI,
                    localDir,
                    all_versions=all_versions,
                    vault_token_file=token,
                    databus_key=databus_key,
                    auth_url=auth_url,
                    client_id=client_id,
                    validate_checksum=validate_checksum,
                )
            elif group is not None and group != "collections":
                print(
                    f"Downloading group and all its artifacts and versions: {databusURI}"
                )
                _download_group(
                    databusURI,
                    localDir,
                    all_versions=all_versions,
                    vault_token_file=token,
                    databus_key=databus_key,
                    auth_url=auth_url,
                    client_id=client_id,
                    validate_checksum=validate_checksum,
                )
            elif account is not None:
                print("accountId not supported yet")  # TODO
            else:
                print(
                    "dataId not supported yet"
                )  # TODO add support for other DatabusIds
        # query in local file
        elif databusURI.startswith("file://"):
            print("query in file not supported yet")
        # query as argument
        else:
            print("QUERY {}", databusURI.replace("\n", " "))
            if uri_endpoint is None:  # endpoint is required for queries (--databus)
                raise ValueError("No endpoint given for query")
            res = _get_file_download_urls_from_sparql_query(
                uri_endpoint, databusURI, databus_key=databus_key
            )
            _download_files(
                res,
                localDir,
                vault_token_file=token,
                databus_key=databus_key,
                auth_url=auth_url,
                client_id=client_id,
                validate_checksum=validate_checksum,
            )
