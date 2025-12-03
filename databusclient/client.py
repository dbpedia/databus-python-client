from enum import Enum
from typing import List, Dict, Tuple, Optional, Union
import requests
import hashlib
import json
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON
from hashlib import sha256
import os

from databusclient.api.utils import get_databus_id_parts_from_uri, get_json_ld_from_databus

__debug = False


class DeployError(Exception):
    """Raised if deploy fails"""


class BadArgumentException(Exception):
    """Raised if an argument does not fit its requirements"""


class DeployLogLevel(Enum):
    """Logging levels for the Databus deploy"""

    error = 0
    info = 1
    debug = 2


def __get_content_variants(distribution_str: str) -> Optional[Dict[str, str]]:
    args = distribution_str.split("|")

    # cv string is ALWAYS at position 1 after the URL
    # if not return empty dict and handle it separately
    if len(args) < 2 or args[1].strip() == "":
        return {}

    cv_str = args[1].strip("_")

    cvs = {}
    for kv in cv_str.split("_"):
        key, value = kv.split("=")
        cvs[key] = value

    return cvs


def __get_filetype_definition(
    distribution_str: str,
) -> Tuple[Optional[str], Optional[str]]:
    file_ext = None
    compression = None

    # take everything except URL
    metadata_list = distribution_str.split("|")[1:]

    if len(metadata_list) == 4:
        # every parameter is set
        file_ext = metadata_list[-3]
        compression = metadata_list[-2]
    elif len(metadata_list) == 3:
        # when last item is shasum:length -> only file_ext set
        if ":" in metadata_list[-1]:
            file_ext = metadata_list[-2]
        else:
            # compression and format are set
            file_ext = metadata_list[-2]
            compression = metadata_list[-1]
    elif len(metadata_list) == 2:
        # if last argument is shasum:length -> both none
        if ":" in metadata_list[-1]:
            pass
        else:
            # only format -> compression is None
            file_ext = metadata_list[-1]
            compression = None
    elif len(metadata_list) == 1:
        # let them be None to be later inferred from URL path
        pass
    else:
        # in this case only URI is given, let all be later inferred
        pass

    return file_ext, compression


def __get_extensions(distribution_str: str) -> Tuple[str, str, str]:
    extension_part = ""
    format_extension, compression = __get_filetype_definition(distribution_str)

    if format_extension is not None:
        # build the format extension (only append compression if not none)
        extension_part = f".{format_extension}"
        if compression is not None:
            extension_part += f".{compression}"
        else:
            compression = "none"
        return extension_part, format_extension, compression

    # here we go if format not explicitly set: infer it from the path

    # first set default values
    format_extension = "file"
    compression = "none"

    # get the last segment of the URL
    last_segment = str(distribution_str).split("|")[0].split("/")[-1]

    # cut of fragments and split by dots
    dot_splits = last_segment.split("#")[0].rsplit(".", 2)

    if len(dot_splits) > 1:
        # if only format is given (no compression)
        format_extension = dot_splits[-1]
        extension_part = f".{format_extension}"

    if len(dot_splits) > 2:
        # if format and compression is in the filename
        compression = dot_splits[-1]
        format_extension = dot_splits[-2]
        extension_part = f".{format_extension}.{compression}"

    return extension_part, format_extension, compression


def __get_file_stats(distribution_str: str) -> Tuple[Optional[str], Optional[int]]:
    metadata_list = distribution_str.split("|")[1:]
    # check whether there is the shasum:length tuple separated by :
    if len(metadata_list) == 0 or ":" not in metadata_list[-1]:
        return None, None

    last_arg_split = metadata_list[-1].split(":")

    if len(last_arg_split) != 2:
        raise ValueError(
            f"Can't parse Argument {metadata_list[-1]}. Too many values, submit shasum and "
            f"content_length in the form of shasum:length"
        )

    sha256sum = last_arg_split[0]
    content_length = int(last_arg_split[1])

    return sha256sum, content_length


def __load_file_stats(url: str) -> Tuple[str, int]:
    resp = requests.get(url)
    if resp.status_code > 400:
        raise requests.exceptions.RequestException(response=resp)

    sha256sum = hashlib.sha256(bytes(resp.content)).hexdigest()
    content_length = len(resp.content)
    return sha256sum, content_length


def __get_file_info(distribution_str: str) -> Tuple[Dict[str, str], str, str, str, int]:
    cvs = __get_content_variants(distribution_str)
    extension_part, format_extension, compression = __get_extensions(distribution_str)

    content_variant_part = "_".join([f"{key}={value}" for key, value in cvs.items()])

    if __debug:
        print("DEBUG", distribution_str, extension_part)

    sha256sum, content_length = __get_file_stats(distribution_str)

    if sha256sum is None or content_length is None:
        __url = str(distribution_str).split("|")[0]
        sha256sum, content_length = __load_file_stats(__url)

    return cvs, format_extension, compression, sha256sum, content_length


def create_distribution(
    url: str,
    cvs: Dict[str, str],
    file_format: str = None,
    compression: str = None,
    sha256_length_tuple: Tuple[str, int] = None,
) -> str:
    """Creates the identifier-string for a distribution used as downloadURLs in the createDataset function.
    url: is the URL of the dataset
    cvs: dict of content variants identifying a certain distribution (needs to be unique for each distribution in the dataset)
    file_format: identifier for the file format (e.g. json). If set to None client tries to infer it from the path
    compression: identifier for the compression format (e.g. gzip). If set to None client tries to infer it from the path
    sha256_length_tuple: sha256sum and content_length of the file in the form of Tuple[shasum, length].
    If left out file will be downloaded extra and calculated.
    """

    meta_string = "_".join([f"{key}={value}" for key, value in cvs.items()])

    # check whether to add the custom file format
    if file_format is not None:
        meta_string += f"|{file_format}"

    # check whether to add the custom compression string
    if compression is not None:
        meta_string += f"|{compression}"

    # add shasum and length if present
    if sha256_length_tuple is not None:
        sha256sum, content_length = sha256_length_tuple
        meta_string += f"|{sha256sum}:{content_length}"

    return f"{url}|{meta_string}"

def create_distributions_from_metadata(metadata: List[Dict[str, Union[str, int]]]) -> List[str]:
    """
    Create distributions from metadata entries.

    Parameters
    ----------
    metadata : List[Dict[str, Union[str, int]]]
        List of metadata entries, each containing:
        - checksum: str - SHA-256 hex digest (64 characters)
        - size: int - File size in bytes (positive integer)
        - url: str - Download URL for the file
        - file_format: str - File format of the file [optional]
        - compression: str - Compression format of the file [optional]

    Returns
    -------
    List[str]
        List of distribution identifier strings for use with create_dataset
    """
    distributions = []
    counter = 0

    for entry in metadata:
        # Validate required keys
        required_keys = ["checksum", "size", "url"]
        missing_keys = [key for key in required_keys if key not in entry]
        if missing_keys:
            raise ValueError(f"Metadata entry missing required keys: {missing_keys}")

        checksum = entry["checksum"]
        size = entry["size"]
        url = entry["url"]
        if not isinstance(size, int) or size <= 0:
            raise ValueError(f"Invalid size for {url}: expected positive integer, got {size}")
        # Validate SHA-256 hex digest (64 hex chars)
        if not isinstance(checksum, str) or len(checksum) != 64 or not all(
            c in '0123456789abcdefABCDEF' for c in checksum):
                raise ValueError(f"Invalid checksum for {url}")

        distributions.append(
            create_distribution(
                url=url,
                cvs={"count": f"{counter}"},
                file_format=entry.get("file_format"),
                compression=entry.get("compression"),
                sha256_length_tuple=(checksum, size)
            )
        )
        counter += 1
    return distributions

def create_dataset(
    version_id: str,
    title: str,
    abstract: str,
    description: str,
    license_url: str,
    distributions: List[str],
    attribution: str = None,
    derived_from: str = None,
    group_title: str = None,
    group_abstract: str = None,
    group_description: str = None,
) -> Dict[str, Union[List[Dict[str, Union[bool, str, int, float, List]]], str]]:
    """
    Creates a Databus Dataset as a python dict from distributions and submitted metadata. WARNING: If file stats (sha256sum, content length)
    were not submitted, the client loads the files and calculates them. This can potentially take a lot of time, depending on the file size.
    The result can be transformed to a JSON-LD by calling json.dumps(dataset).

    Parameters
    ----------
    version_id: str
        The version ID representing the Dataset. Needs to be in the form of $DATABUS_BASE/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION
    title: str
        The title text of the dataset
    abstract: str
        A short (one or two sentences) description of the dataset
    description: str
        A long description of the dataset. Markdown syntax is supported
    license_url: str
        The license of the dataset as a URI.
    distributions: str
        Distribution information string as it is in the CLI. Can be created by running the create_distribution function
    attribution: str
        OPTIONAL! The attribution information for the Dataset
    derived_from: str
        OPTIONAL! Short text explain what the dataset was
    group_title: str
        OPTIONAL! Metadata for the Group: Title. NOTE: Is only used if all group metadata is set
    group_abstract: str
        OPTIONAL! Metadata for the Group: Abstract. NOTE: Is only used if all group metadata is set
    group_description: str
        OPTIONAL! Metadata for the Group: Description. NOTE: Is only used if all group metadata is set
    """

    _versionId = str(version_id).strip("/")
    _, account_name, group_name, artifact_name, version = _versionId.rsplit("/", 4)

    # could be build from stuff above,
    # was not sure if there are edge cases BASE=http://databus.example.org/"base"/...
    group_id = _versionId.rsplit("/", 2)[0]

    artifact_id = _versionId.rsplit("/", 1)[0]

    distribution_list = []
    for dst_string in distributions:
        __url = str(dst_string).split("|")[0]
        (
            cvs,
            formatExtension,
            compression,
            sha256sum,
            content_length,
        ) = __get_file_info(dst_string)

        if not cvs and len(distributions) > 1:
            raise BadArgumentException(
                "If there are more than one file in the dataset, the files must be annotated "
                "with content variants"
            )

        entity = {
            "@type": "Part",
            "formatExtension": formatExtension,
            "compression": compression,
            "downloadURL": __url,
            "byteSize": content_length,
            "sha256sum": sha256sum,
        }
        # set content variants
        for key, value in cvs.items():
            entity[f"dcv:{key}"] = value

        distribution_list.append(entity)

    graphs = []

    # only add the group graph if the necessary group properties are set
    if None not in [group_title, group_description, group_abstract]:
        group_dict = {
            "@id": group_id,
            "@type": "Group",
        }

        # add group metadata if set, else it can be left out
        for k, val in [
            ("title", group_title),
            ("abstract", group_abstract),
            ("description", group_description),
        ]:
            group_dict[k] = val

        graphs.append(group_dict)

    # add the artifact graph

    artifact_graph = {
        "@id": artifact_id,
        "@type": "Artifact",
        "title": title,
        "abstract": abstract,
        "description": description
    }
    graphs.append(artifact_graph)

    # add the dataset graph

    dataset_graph = {
        "@type": ["Version", "Dataset"],
        "@id": _versionId,
        "hasVersion": version,
        "title": title,
        "abstract": abstract,
        "description": description,
        "license": license_url,
        "distribution": distribution_list,
    }

    def append_to_dataset_graph_if_existent(add_key: str, add_value: str):
        if add_value is not None:
            dataset_graph[add_key] = add_value

    append_to_dataset_graph_if_existent("attribution", attribution)
    append_to_dataset_graph_if_existent("wasDerivedFrom", derived_from)

    graphs.append(dataset_graph)

    dataset = {
        "@context": "https://downloads.dbpedia.org/databus/context.jsonld",
        "@graph": graphs,
    }
    return dataset


def deploy(
    dataid: Dict[str, Union[List[Dict[str, Union[bool, str, int, float, List]]], str]],
    api_key: str,
    verify_parts: bool = False,
    log_level: DeployLogLevel = DeployLogLevel.debug,
    debug: bool = False,
) -> None:
    """Deploys a dataset to the databus. The endpoint is inferred from the DataID identifier.
    Parameters
    ----------
    dataid: Dict[str, Union[List[Dict[str, Union[bool, str, int, float, List]]], str]]
        The dataid represented as a python dict. Preferably created by the creaateDataset function
    api_key: str
        the API key of the user noted in the Dataset identifier
    verify_parts: bool
        flag of the publish POST request, prevents the databus from checking shasum and content length (is already handled by the client, reduces load on the Databus). Default is False
    log_level: DeployLogLevel
        log level of the deploy output
    debug: bool
        controls whether output shold be printed to the console (stdout)
    """

    headers = {"X-API-KEY": f"{api_key}", "Content-Type": "application/json"}
    data = json.dumps(dataid)
    base = "/".join(dataid["@graph"][0]["@id"].split("/")[0:3])
    api_uri = (
        base
        + f"/api/publish?verify-parts={str(verify_parts).lower()}&log-level={log_level.name}"
    )
    resp = requests.post(api_uri, data=data, headers=headers)

    if debug or __debug:
        dataset_uri = dataid["@graph"][0]["@id"]
        print(f"Trying submitting data to {dataset_uri}:")
        print(data)

    if resp.status_code != 200:
        raise DeployError(f"Could not deploy dataset to databus. Reason: '{resp.text}'")

    if debug or __debug:
        print("---------")
        print(resp.text)


def deploy_from_metadata(
    metadata: List[Dict[str, Union[str, int]]],
    version_id: str,
    title: str,
    abstract: str,
    description: str,
    license_url: str,
    apikey: str
) -> None:
    """
    Deploy a dataset from metadata entries.

    Parameters
    ----------
    metadata : List[Dict[str, Union[str, int]]]
        List of file metadata entries (see create_distributions_from_metadata)
    version_id : str
        Dataset version ID in the form $DATABUS_BASE/$ACCOUNT/$GROUP/$ARTIFACT/$VERSION
    title : str
        Dataset title
    abstract : str
        Short description of the dataset
    description : str
        Long description (Markdown supported)
    license_url : str
        License URI
    apikey : str
        API key for authentication
    """
    distributions = create_distributions_from_metadata(metadata)

    dataset = create_dataset(
        version_id=version_id,
        title=title,
        abstract=abstract,
        description=description,
        license_url=license_url,
        distributions=distributions
    )

    print(f"Deploying dataset version: {version_id}")
    deploy(dataset, apikey)

    print(f"Successfully deployed to {version_id}")
    print(f"Deployed {len(metadata)} file(s):")
    for entry in metadata:
        print(f"  - {entry['url']}")


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
    response = requests.get(url, stream=True, allow_redirects=True)
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
        response = requests.get(url, headers=headers, stream=True)
    
    # Databus API key required if only 401 Unauthorized
    elif response.status_code == 401:
        print(f"API key required for {url}")
        if not databus_key:
            raise ValueError("Databus API key not given for protected download")

        headers = {"X-API-KEY": databus_key}
        response = requests.get(url, headers=headers, stream=True)

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
        # raise IOError("Downloaded size does not match Content-Length header")
        print(f"Warning: Downloaded size does not match Content-Length header:\nExpected {total_size_in_bytes}, got {progress_bar.n}")


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


def __query_sparql__(endpoint_url, query) -> dict:
    """
    Query a SPARQL endpoint and return results in JSON format.

    Parameters:
    - endpoint_url: the URL of the SPARQL endpoint
    - query: the SPARQL query string

    Returns:
    - Dictionary containing the query results
    """
    sparql = SPARQLWrapper(endpoint_url)
    sparql.method = 'POST'
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    results = sparql.query().convert()
    return results


def __handle_databus_file_query__(endpoint_url, query) -> List[str]:
    result_dict = __query_sparql__(endpoint_url, query)
    for binding in result_dict['results']['bindings']:
        if len(binding.keys()) > 1:
            print("Error multiple bindings in query response")
            break
        else:
            value = binding[next(iter(binding.keys()))]['value']
        yield value


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


def wsha256(raw: str):
    return sha256(raw.encode('utf-8')).hexdigest()


def __handle_databus_collection__(uri: str, databus_key: str = None) -> str:
    headers = {"Accept": "text/sparql"}
    if databus_key is not None:
        headers["X-API-KEY"] = databus_key

    return requests.get(uri, headers=headers).text


def __download_list__(urls: List[str],
                      localDir: str,
                      vault_token_file: str = None,
                      databus_key: str = None,
                      auth_url: str = None,
                      client_id: str = None) -> None:
    fileLocalDir = localDir
    for url in urls:
        if localDir is None:
            host, account, group, artifact, version, file = get_databus_id_parts_from_uri(url)
            fileLocalDir = os.path.join(os.getcwd(), account, group, artifact, version if version is not None else "latest")
            print(f"Local directory not given, using {fileLocalDir}")

        file = url.split("/")[-1]
        filename = os.path.join(fileLocalDir, file)
        print("\n")
        __download_file__(url=url, filename=filename, vault_token_file=vault_token_file, databus_key=databus_key, auth_url=auth_url, client_id=client_id)
        print("\n")


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
            res = __handle_databus_file_query__(endpoint, databusURI)
            __download_list__(res, localDir, vault_token_file=token, databus_key=databus_key, auth_url=auth_url, client_id=client_id)
