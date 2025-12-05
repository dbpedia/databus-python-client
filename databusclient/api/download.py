from typing import List
import requests
import os
from tqdm import tqdm
import json
from SPARQLWrapper import SPARQLWrapper, JSON

from databusclient.api.utils import get_databus_id_parts_from_uri, get_json_ld_from_databus

def __handle_databus_collection__(uri: str, databus_key: str | None = None) -> str:
    headers = {"Accept": "text/sparql"}
    if databus_key is not None:
        headers["X-API-KEY"] = databus_key

    return requests.get(uri, headers=headers, timeout=30).text

def __get_vault_access__(download_url: str,
                         token_file: str,
                         auth_url: str,
                         client_id: str) -> str:
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
    resp = requests.post(auth_url, data={
        "client_id": client_id,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    })
    resp.raise_for_status()
    access_token = resp.json()["access_token"]

    # 3. Extract host as audience
    # Remove protocol prefix
    if download_url.startswith("https://"):
        host_part = download_url[len("https://"):]
    elif download_url.startswith("http://"):
        host_part = download_url[len("http://"):]
    else:
        host_part = download_url
    audience = host_part.split("/")[0]  # host is before first "/"

    # 4. Access token -> Vault token
    resp = requests.post(auth_url, data={
        "client_id": client_id,
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "subject_token": access_token,
        "audience": audience
    })
    resp.raise_for_status()
    vault_token = resp.json()["access_token"]

    print(f"Using Vault access token for {download_url}")
    return vault_token

def __download_file__(url, filename, vault_token_file=None, databus_key=None, auth_url=None, client_id=None) -> None:
    """
    Download a file from the internet with a progress bar using tqdm.

    Parameters:
    - url: the URL of the file to download
    - filename: the local file path where the file should be saved
    - vault_token_file: Path to Vault refresh token file
    - auth_url: Keycloak token endpoint URL
    - client_id: Client ID for token exchange

    Steps:
    1. Try direct GET without Authorization header.
    2. If server responds with WWW-Authenticate: Bearer, 401 Unauthorized) or url starts with "https://data.dbpedia.io/databus.dbpedia.org",
       then fetch Vault access token and retry with Authorization header.
    """

    print(f"Download file: {url}")
    dirpath = os.path.dirname(filename)
    if dirpath:
        os.makedirs(dirpath, exist_ok=True)  # Create the necessary directories
    # --- 1. Get redirect URL by requesting HEAD ---
    response = requests.head(url, stream=True)
    # Check for redirect and update URL if necessary
    if response.headers.get("Location") and response.status_code in [301, 302, 303, 307, 308]:
        url = response.headers.get("Location")
        print("Redirects url: ", url)

    # --- 2. Try direct GET ---
    response = requests.get(url, stream=True, allow_redirects=True, timeout=30)
    www = response.headers.get('WWW-Authenticate', '')  # get WWW-Authenticate header if present to check for Bearer auth

    # Vault token required if 401 Unauthorized with Bearer challenge
    if (response.status_code == 401 and "bearer" in www.lower()):
        print(f"Authentication required for {url}")
        if not (vault_token_file):
            raise ValueError("Vault token file not given for protected download")

        # --- 3. Fetch Vault token ---
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

    total_size_in_bytes = int(response.headers.get('content-length', 0))
    block_size = 1024  # 1 KiB

    progress_bar = tqdm(total=total_size_in_bytes, unit='iB', unit_scale=True)
    with open(filename, 'wb') as file:
        for data in response.iter_content(block_size):
            progress_bar.update(len(data))
            file.write(data)
    progress_bar.close()

    # TODO: could be a problem of github raw / openflaas
    if total_size_in_bytes != 0 and progress_bar.n != total_size_in_bytes:
        raise IOError("Downloaded size does not match Content-Length header")

def __download_list__(urls: List[str],
                      localDir: str,
                      vault_token_file: str = None,
                      databus_key: str = None,
                      auth_url: str = None,
                      client_id: str = None) -> None:
    fileLocalDir = localDir
    for url in urls:
        if localDir is None:
            _host, account, group, artifact, version, file = get_databus_id_parts_from_uri(url)
            fileLocalDir = os.path.join(os.getcwd(), account, group, artifact, version if version is not None else "latest")
            print(f"Local directory not given, using {fileLocalDir}")

        file = url.split("/")[-1]
        filename = os.path.join(fileLocalDir, file)
        print("\n")
        __download_file__(url=url, filename=filename, vault_token_file=vault_token_file, databus_key=databus_key, auth_url=auth_url, client_id=client_id)
        print("\n")

def __query_sparql__(endpoint_url, query, databus_key=None) -> dict:
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
    sparql.method = 'POST'
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    if databus_key is not None:
        sparql.setCustomHttpHeaders({"X-API-KEY": databus_key})
    results = sparql.query().convert()
    return results

def __handle_databus_file_query__(endpoint_url, query, databus_key=None) -> List[str]:
    result_dict = __query_sparql__(endpoint_url, query, databus_key=databus_key)
    for binding in result_dict['results']['bindings']:
        if len(binding.keys()) > 1:
            print("Error multiple bindings in query response")
            break
        else:
            value = binding[next(iter(binding.keys()))]['value']
        yield value

def __get_databus_latest_version_of_artifact__(json_str: str) -> str:
    """
    Parse the JSON-LD of a databus artifact to extract URLs of the latest version.

    Returns download URL of latest version of the artifact.
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
    return version_urls[0]  # Return the latest version URL

def __handle_databus_artifact_version__(json_str: str) -> List[str]:
    """
    Parse the JSON-LD of a databus artifact version to extract download URLs.
    Don't get downloadURLs directly from the JSON-LD, but follow the "file" links to count access to databus accurately.

    Returns a list of download URLs.
    """

    databusIdUrl = []
    json_dict = json.loads(json_str)
    graph = json_dict.get("@graph", [])
    for node in graph:
        if node.get("@type") == "Part":
            id = node.get("file")
            databusIdUrl.append(id)
    return databusIdUrl

def __get_databus_artifacts_of_group__(json_str: str) -> List[str]:
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
    auth_url=None,
    client_id=None
) -> None:
    """
    Download datasets to local storage from databus registry. If download is on vault, vault token will be used for downloading protected files.
    ------
    localDir: the local directory
    endpoint: the databus endpoint URL
    databusURIs: identifiers to access databus registered datasets
    token: Path to Vault refresh token file
    databus_key: Databus API key for protected downloads
    auth_url: Keycloak token endpoint URL
    client_id: Client ID for token exchange
    """

    # TODO: make pretty
    for databusURI in databusURIs:
        host, account, group, artifact, version, file = get_databus_id_parts_from_uri(databusURI)

        # dataID or databus collection
        if databusURI.startswith("http://") or databusURI.startswith("https://"):
            # Auto-detect sparql endpoint from databusURI if not given -> no need to specify endpoint (--databus)
            if endpoint is None:
                endpoint = f"https://{host}/sparql"
            print(f"SPARQL endpoint {endpoint}")

            # databus collection
            if group == "collections":
                query = __handle_databus_collection__(databusURI, databus_key=databus_key)
                res = __handle_databus_file_query__(endpoint, query)
                __download_list__(res, localDir, vault_token_file=token, databus_key=databus_key, auth_url=auth_url, client_id=client_id)
            # databus file
            elif file is not None:
                __download_list__([databusURI], localDir, vault_token_file=token, databus_key=databus_key, auth_url=auth_url, client_id=client_id)
            # databus artifact version
            elif version is not None:
                json_str = get_json_ld_from_databus(databusURI, databus_key=databus_key)
                res = __handle_databus_artifact_version__(json_str)
                __download_list__(res, localDir, vault_token_file=token, databus_key=databus_key, auth_url=auth_url, client_id=client_id)
            # databus artifact
            elif artifact is not None:
                json_str = get_json_ld_from_databus(databusURI, databus_key=databus_key)
                latest = __get_databus_latest_version_of_artifact__(json_str)
                print(f"No version given, using latest version: {latest}")
                json_str = get_json_ld_from_databus(latest, databus_key=databus_key)
                res = __handle_databus_artifact_version__(json_str)
                __download_list__(res, localDir, vault_token_file=token, databus_key=databus_key, auth_url=auth_url, client_id=client_id)

            # databus group
            elif group is not None:
                json_str = get_json_ld_from_databus(databusURI, databus_key=databus_key)
                artifacts = __get_databus_artifacts_of_group__(json_str)
                for artifact_uri in artifacts:
                    print(f"Processing artifact {artifact_uri}")
                    json_str = get_json_ld_from_databus(artifact_uri, databus_key=databus_key)
                    latest = __get_databus_latest_version_of_artifact__(json_str)
                    print(f"No version given, using latest version: {latest}")
                    json_str = get_json_ld_from_databus(latest, databus_key=databus_key)
                    res = __handle_databus_artifact_version__(json_str)
                    __download_list__(res, localDir, vault_token_file=token, databus_key=databus_key, auth_url=auth_url, client_id=client_id)

            # databus account
            elif account is not None:
                print("accountId not supported yet")  # TODO
            else:
                print("dataId not supported yet")  # TODO add support for other DatabusIds
        # query in local file
        elif databusURI.startswith("file://"):
            print("query in file not supported yet")
        # query as argument
        else:
            print("QUERY {}", databusURI.replace("\n", " "))
            if endpoint is None:  # endpoint is required for queries (--databus)
                raise ValueError("No endpoint given for query")
            res = __handle_databus_file_query__(endpoint, databusURI, databus_key=databus_key)
            __download_list__(res, localDir, vault_token_file=token, databus_key=databus_key, auth_url=auth_url, client_id=client_id)