import json
import os
from typing import List

import requests
from SPARQLWrapper import JSON, SPARQLWrapper
from tqdm import tqdm

from databusclient.api.utils import fetch_databus_jsonld, get_databus_id_parts_from_uri


def _download_file(
    url,
    localDir,
    vault_token_file=None,
    databus_key=None,
    auth_url=None,
    client_id=None,
) -> None:
    """
    Download a file from the internet with a progress bar using tqdm.

    Parameters:
    - url: the URL of the file to download
    - localDir: Local directory to download file to. If None, the databus folder structure is created in the current working directory.
    - vault_token_file: Path to Vault refresh token file
    - auth_url: Keycloak token endpoint URL
    - client_id: Client ID for token exchange

    Steps:
    1. Try direct GET without Authorization header.
    2. If server responds with WWW-Authenticate: Bearer, 401 Unauthorized), then fetch Vault access token and retry with Authorization header.
    """
    if localDir is None:
        _host, account, group, artifact, version, file = get_databus_id_parts_from_uri(
            url
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
    response = requests.head(url, stream=True)
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

    # --- 2. Try direct GET ---
    response = requests.get(url, stream=True, allow_redirects=True, timeout=30)
    www = response.headers.get(
        "WWW-Authenticate", ""
    )  # get WWW-Authenticate header if present to check for Bearer auth

    # Vault token required if 401 Unauthorized with Bearer challenge
    if response.status_code == 401 and "bearer" in www.lower():
        print(f"Authentication required for {url}")
        if not (vault_token_file):
            raise ValueError("Vault token file not given for protected download")

        # --- 3. Fetch Vault token ---
        # TODO: cache token
        vault_token = __get_vault_access__(url, vault_token_file, auth_url, client_id)
        headers = {"Authorization": f"Bearer {vault_token}"}

        # --- 4. Retry with token ---
        response = requests.get(url, headers=headers, stream=True, timeout=30)

    # Databus API key required if only 401 Unauthorized
    elif response.status_code == 401:
        print(f"API key required for {url}")
        if not databus_key:
            raise ValueError("Databus API key not given for protected download")

        headers = {"X-API-KEY": databus_key}
        response = requests.get(url, headers=headers, stream=True, timeout=30)

    try:
        response.raise_for_status()  # Raise if still failing
    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            print(f"WARNING: Skipping file {url} because it was not found (404).")
            return
        else:
            raise e

    total_size_in_bytes = int(response.headers.get("content-length", 0))
    block_size = 1024  # 1 KiB

    progress_bar = tqdm(total=total_size_in_bytes, unit="iB", unit_scale=True)
    with open(filename, "wb") as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()

    # TODO: could be a problem of github raw / openflaas
    # if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
    #     raise IOError("Downloaded size does not match Content-Length header")


def _download_files(
    urls: List[str],
    localDir: str,
    vault_token_file: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
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
        _download_file(
            url=url,
            localDir=localDir,
            vault_token_file=vault_token_file,
            databus_key=databus_key,
            auth_url=auth_url,
            client_id=client_id,
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

    return requests.get(uri, headers=headers, timeout=30).text


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
    )


def _download_version(
    uri: str,
    localDir: str,
    vault_token_file: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
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
    _download_files(
        file_urls,
        localDir,
        vault_token_file=vault_token_file,
        databus_key=databus_key,
        auth_url=auth_url,
        client_id=client_id,
    )


def _download_artifact(
    uri: str,
    localDir: str,
    all_versions: bool = False,
    vault_token_file: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
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
        _download_files(
            file_urls,
            localDir,
            vault_token_file=vault_token_file,
            databus_key=databus_key,
            auth_url=auth_url,
            client_id=client_id,
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

    # Single version case {}
    if isinstance(versions, dict):
        versions = [versions]
    # Multiple versions case [{}, {}]

    version_urls = [v["@id"] for v in versions if "@id" in v]
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

    databusIdUrl = []
    json_dict = json.loads(json_str)
    graph = json_dict.get("@graph", [])
    for node in graph:
        if node.get("@type") == "Part":
            id = node.get("file")
            databusIdUrl.append(id)
    return databusIdUrl


def _download_group(
    uri: str,
    localDir: str,
    all_versions: bool = False,
    vault_token_file: str = None,
    databus_key: str = None,
    auth_url: str = None,
    client_id: str = None,
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
        )


def _get_databus_artifacts_of_group(json_str: str) -> List[str]:
    """
    Parse the JSON-LD of a databus group to extract URLs of all artifacts.

    Returns a list of artifact URLs.
    """
    json_dict = json.loads(json_str)
    artifacts = json_dict.get("databus:hasArtifact", [])

    result = []
    for item in artifacts:
        uri = item.get("@id")
        if not uri:
            continue
        _, _, _, _, version, _ = get_databus_id_parts_from_uri(uri)
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
    auth_url=None,
    client_id=None,
) -> None:
    """
    Download datasets from databus.

    Download of files, versions, artifacts, groups or databus collections by ther databus URIs or user-defined SPARQL queries that return file download URLs.

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
        host, account, group, artifact, version, file = get_databus_id_parts_from_uri(
            databusURI
        )

        # dataID or databus collection
        if databusURI.startswith("http://") or databusURI.startswith("https://"):
            # Auto-detect sparql endpoint from host if not given
            if endpoint is None:
                endpoint = f"https://{host}/sparql"
            print(f"SPARQL endpoint {endpoint}")

            if group == "collections" and artifact is not None:
                print(f"Downloading collection: {databusURI}")
                _download_collection(
                    databusURI,
                    endpoint,
                    localDir,
                    token,
                    databus_key,
                    auth_url,
                    client_id,
                )
            elif file is not None:
                print(f"Downloading file: {databusURI}")
                _download_file(
                    databusURI,
                    localDir,
                    vault_token_file=token,
                    databus_key=databus_key,
                    auth_url=auth_url,
                    client_id=client_id,
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
            if endpoint is None:  # endpoint is required for queries (--databus)
                raise ValueError("No endpoint given for query")
            res = _get_file_download_urls_from_sparql_query(
                endpoint, databusURI, databus_key=databus_key
            )
            _download_files(
                res,
                localDir,
                vault_token_file=token,
                databus_key=databus_key,
                auth_url=auth_url,
                client_id=client_id,
            )
