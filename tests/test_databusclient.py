"""Client tests"""
import pytest
from databusclient.client import create_dataset, create_distribution, __get_file_info
from collections import OrderedDict


EXAMPLE_URL = "https://raw.githubusercontent.com/dbpedia/databus/608482875276ef5df00f2360a2f81005e62b58bd/server/app/api/swagger.yml"


@pytest.mark.skip(reason="temporarily disabled since code needs fixing")
def test_distribution_cases():

    metadata_args_with_filler = OrderedDict()

    metadata_args_with_filler["type=config_source=databus"] = ""
    metadata_args_with_filler["yml"] = None
    metadata_args_with_filler["none"] = None
    metadata_args_with_filler[
        "79582a2a7712c0ce78a74bb55b253dc2064931364cf9c17c827370edf9b7e4f1:56737"
    ] = None

    # test by leaving out an argument each

    artifact_name = "databusclient-pytest"
    uri = "https://raw.githubusercontent.com/dbpedia/databus/master/server/app/api/swagger.yml"
    parameters = list(metadata_args_with_filler.keys())

    for i in range(0, len(metadata_args_with_filler.keys())):

        if i == 1:
            continue

        dst_string = f"{uri}"
        for j in range(0, len(metadata_args_with_filler.keys())):
            if j == i:
                replacement = metadata_args_with_filler[parameters[j]]
                if replacement is None:
                    pass
                else:
                    dst_string += f"|{replacement}"
            else:
                dst_string += f"|{parameters[j]}"

        print(f"{dst_string=}")
        (
            _name,
            cvs,
            formatExtension,
            compression,
            sha256sum,
            content_length,
        ) = __get_file_info(artifact_name, dst_string)

        created_dst_str = create_distribution(
            uri, cvs, formatExtension, compression, (sha256sum, content_length)
        )

        assert dst_string == created_dst_str


@pytest.mark.skip(reason="temporarily disabled since code needs fixing")
def test_empty_cvs():

    dst = [create_distribution(url=EXAMPLE_URL, cvs={})]

    dataset = create_dataset(
        version_id="https://dev.databus.dbpedia.org/user/group/artifact/1970.01.01/",
        title="Test Title",
        abstract="Test abstract blabla",
        description="Test description blabla",
        license_url="https://license.url/test/",
        distributions=dst,
    )

    correct_dataset = {
        "@context": "https://downloads.dbpedia.org/databus/context.jsonld",
        "@graph": [
            {
                "@type": "Dataset",
                "@id": "https://dev.databus.dbpedia.org/user/group/artifact/1970.01.01#Dataset",
                "hasVersion": "1970.01.01",
                "title": "Test Title",
                "abstract": "Test abstract blabla",
                "description": "Test description blabla",
                "license": "https://license.url/test/",
                "distribution": [
                    {
                        "@id": "https://dev.databus.dbpedia.org/user/group/artifact/1970.01.01#artifact.yml",
                        "@type": "Part",
                        "file": "https://dev.databus.dbpedia.org/user/group/artifact/1970.01.01/artifact.yml",
                        "formatExtension": "yml",
                        "compression": "none",
                        "downloadURL": EXAMPLE_URL,
                        "byteSize": 59986,
                        "sha256sum": "088e6161bf8b4861bdd4e9f517be4441b35a15346cb9d2d3c6d2e3d6cd412030",
                    }
                ],
            }
        ],
    }

    assert dataset == correct_dataset


# ============================================================================
# New Tests for Enhanced Download Capabilities
# ============================================================================
from unittest.mock import Mock, patch, mock_open, MagicMock, call
import json
import os
import tempfile
from databusclient.client import (
    __get_vault_access__,
    __handle_databus_artifact_version__,
    __get_databus_latest_version_of_artifact__,
    __get_databus_artifacts_of_group__,
    __get_databus_id_parts__,
    __get_json_ld_from_databus__,
    __handle_databus_collection__,
    download,
)

TEST_LOCAL_DIR = os.path.join(tempfile.gettempdir(), "databusclient_test")


class TestVaultAuthentication:
    """Tests for Vault OAuth token exchange functionality"""

    def test_get_vault_access_with_file_token(self):
        """Test getting vault access token from file"""
        mock_token = "mock_refresh_token_" + "x" * 70
        mock_access_val = "mock_access_token_123"
        mock_vault_val = "mock_vault_token_456"

        with patch("builtins.open", mock_open(read_data=mock_token)):
            with patch("os.path.exists", return_value=True):
                with patch("requests.post") as mock_post:
                    # Setup mock responses for token exchange
                    mock_resp1 = Mock()
                    mock_resp1.raise_for_status = Mock()
                    mock_resp1.json.return_value = {"access_token": mock_access_val}

                    mock_resp2 = Mock()
                    mock_resp2.raise_for_status = Mock()
                    mock_resp2.json.return_value = {"access_token": mock_vault_val}

                    mock_post.side_effect = [mock_resp1, mock_resp2]

                    result = __get_vault_access__(
                        "https://example.com/file.txt",
                        "token.txt",
                        "https://auth.example.com/token",
                        "test-client",
                    )

                    assert result == mock_vault_val
                    assert mock_post.call_count == 2

    def test_get_vault_access_with_env_token(self):
        """Test getting vault access token from environment variable"""
        mock_token = "env_refresh_token_" + "x" * 70
        mock_access_val = "mock_access_token_123"
        mock_vault_val = "mock_vault_token_456"

        with patch.dict(os.environ, {"REFRESH_TOKEN": mock_token}):
            with patch("requests.post") as mock_post:
                mock_resp1 = Mock()
                mock_resp1.raise_for_status = Mock()
                mock_resp1.json.return_value = {"access_token": mock_access_val}

                mock_resp2 = Mock()
                mock_resp2.raise_for_status = Mock()
                mock_resp2.json.return_value = {"access_token": mock_vault_val}

                mock_post.side_effect = [mock_resp1, mock_resp2]

                result = __get_vault_access__(
                    "https://data.example.com/file.txt",
                    "token.txt",
                    "https://auth.example.com/token",
                    "test-client",
                )

                assert result == mock_vault_val

    def test_get_vault_access_missing_token_file(self):
        """Test error when token file is missing"""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists", return_value=False):
                with pytest.raises(FileNotFoundError):
                    __get_vault_access__(
                        "https://example.com/file.txt",
                        "nonexistent.txt",
                        "https://auth.example.com/token",
                        "test-client",
                    )

    def test_get_vault_access_token_refresh_fails(self):
        """Test error handling when token refresh fails"""
        mock_token = "mock_refresh_token_" + "x" * 70

        with patch("builtins.open", mock_open(read_data=mock_token)):
            with patch("os.path.exists", return_value=True):
                with patch("requests.post") as mock_post:
                    mock_resp = Mock()
                    mock_resp.raise_for_status.side_effect = Exception("Token refresh failed")
                    mock_post.return_value = mock_resp

                    with pytest.raises(Exception, match="Token refresh failed"):
                        __get_vault_access__(
                            "https://example.com/file.txt",
                            "token.txt",
                            "https://auth.example.com/token",
                            "test-client",
                        )

    def test_get_vault_access_audience_extraction_https(self):
        """Test correct audience extraction from HTTPS URL"""
        mock_token = "mock_token_" + "x" * 70

        with patch("builtins.open", mock_open(read_data=mock_token)):
            with patch("os.path.exists", return_value=True):
                with patch("requests.post") as mock_post:
                    mock_resp1 = Mock()
                    mock_resp1.raise_for_status = Mock()
                    mock_resp1.json.return_value = {"access_token": "access_token"}

                    mock_resp2 = Mock()
                    mock_resp2.raise_for_status = Mock()
                    mock_resp2.json.return_value = {"access_token": "vault_token"}

                    mock_post.side_effect = [mock_resp1, mock_resp2]

                    __get_vault_access__(
                        "https://data.dbpedia.io/path/to/file.txt",
                        "token.txt",
                        "https://auth.example.com/token",
                        "test-client",
                    )

                    # Check that audience is correctly extracted
                    second_call_data = mock_post.call_args_list[1][1]["data"]
                    assert second_call_data["audience"] == "data.dbpedia.io"

    def test_get_vault_access_audience_extraction_http(self):
        """Test correct audience extraction from HTTP URL"""
        mock_token = "mock_token_" + "x" * 70

        with patch("builtins.open", mock_open(read_data=mock_token)):
            with patch("os.path.exists", return_value=True):
                with patch("requests.post") as mock_post:
                    mock_resp1 = Mock()
                    mock_resp1.raise_for_status = Mock()
                    mock_resp1.json.return_value = {"access_token": "access_token"}

                    mock_resp2 = Mock()
                    mock_resp2.raise_for_status = Mock()
                    mock_resp2.json.return_value = {"access_token": "vault_token"}

                    mock_post.side_effect = [mock_resp1, mock_resp2]

                    __get_vault_access__(
                        "http://localhost:8080/file.txt",
                        "token.txt",
                        "https://auth.example.com/token",
                        "test-client",
                    )

                    second_call_data = mock_post.call_args_list[1][1]["data"]
                    assert second_call_data["audience"] == "localhost:8080"


class TestJSONLDParsing:
    """Tests for JSON-LD parsing functions"""

    def test_handle_databus_artifact_version_single_part(self):
        """Test parsing artifact version JSON-LD with single part"""
        json_str = json.dumps(
            {
                "@graph": [
                    {
                        "@type": "Part",
                        "file": "https://databus.dbpedia.org/account/group/artifact/version/file1.txt",
                    }
                ]
            }
        )

        result = __handle_databus_artifact_version__(json_str)

        assert len(result) == 1
        assert result[0] == "https://databus.dbpedia.org/account/group/artifact/version/file1.txt"

    def test_handle_databus_artifact_version_multiple_parts(self):
        """Test parsing artifact version JSON-LD with multiple parts"""
        json_str = json.dumps(
            {
                "@graph": [
                    {
                        "@type": "Part",
                        "file": "https://databus.dbpedia.org/account/group/artifact/version/file1.txt",
                    },
                    {
                        "@type": "Part",
                        "file": "https://databus.dbpedia.org/account/group/artifact/version/file2.txt",
                    },
                    {"@type": "Dataset", "title": "Test Dataset"},
                ]
            }
        )

        result = __handle_databus_artifact_version__(json_str)

        assert len(result) == 2
        assert "file1.txt" in result[0]
        assert "file2.txt" in result[1]

    def test_handle_databus_artifact_version_empty_graph(self):
        """Test parsing artifact version JSON-LD with empty graph"""
        json_str = json.dumps({"@graph": []})

        result = __handle_databus_artifact_version__(json_str)

        assert len(result) == 0

    def test_handle_databus_artifact_version_no_parts(self):
        """Test parsing artifact version JSON-LD with no parts"""
        json_str = json.dumps(
            {
                "@graph": [
                    {"@type": "Dataset", "title": "Test Dataset"},
                ]
            }
        )

        result = __handle_databus_artifact_version__(json_str)

        assert len(result) == 0

    def test_get_databus_latest_version_single_version(self):
        """Test getting latest version when only one version exists"""
        json_str = json.dumps(
            {"databus:hasVersion": {"@id": "https://databus.dbpedia.org/account/group/artifact/2023.01.01"}}
        )

        result = __get_databus_latest_version_of_artifact__(json_str)

        assert result == "https://databus.dbpedia.org/account/group/artifact/2023.01.01"

    def test_get_databus_latest_version_multiple_versions(self):
        """Test getting latest version from multiple versions"""
        json_str = json.dumps(
            {
                "databus:hasVersion": [
                    {"@id": "https://databus.dbpedia.org/account/group/artifact/2023.01.01"},
                    {"@id": "https://databus.dbpedia.org/account/group/artifact/2023.12.31"},
                    {"@id": "https://databus.dbpedia.org/account/group/artifact/2023.06.15"},
                ]
            }
        )

        result = __get_databus_latest_version_of_artifact__(json_str)

        # Latest version when sorted in descending order
        assert result == "https://databus.dbpedia.org/account/group/artifact/2023.12.31"

    def test_get_databus_latest_version_no_versions(self):
        """Test error when no versions exist"""
        json_str = json.dumps({"databus:hasVersion": []})

        with pytest.raises(ValueError, match="No versions found"):
            __get_databus_latest_version_of_artifact__(json_str)

    def test_get_databus_artifacts_of_group_single_artifact(self):
        """Test getting artifacts from group with single artifact"""
        json_str = json.dumps({"databus:hasArtifact": [{"@id": "https://databus.dbpedia.org/account/group/artifact1"}]})

        result = __get_databus_artifacts_of_group__(json_str)

        assert len(result) == 1
        assert result[0] == "https://databus.dbpedia.org/account/group/artifact1"

    def test_get_databus_artifacts_of_group_multiple_artifacts(self):
        """Test getting artifacts from group with multiple artifacts"""
        json_str = json.dumps(
            {
                "databus:hasArtifact": [
                    {"@id": "https://databus.dbpedia.org/account/group/artifact1"},
                    {"@id": "https://databus.dbpedia.org/account/group/artifact2"},
                    {"@id": "https://databus.dbpedia.org/account/group/artifact3"},
                ]
            }
        )

        result = __get_databus_artifacts_of_group__(json_str)

        assert len(result) == 3
        assert "artifact1" in result[0]
        assert "artifact2" in result[1]
        assert "artifact3" in result[2]

    def test_get_databus_artifacts_of_group_filters_versions(self):
        """Test that artifacts with versions are filtered out"""
        json_str = json.dumps(
            {
                "databus:hasArtifact": [
                    {"@id": "https://databus.dbpedia.org/account/group/artifact1"},
                    {"@id": "https://databus.dbpedia.org/account/group/artifact2/2023.01.01"},
                    {"@id": "https://databus.dbpedia.org/account/group/artifact3"},
                ]
            }
        )

        result = __get_databus_artifacts_of_group__(json_str)

        # Should only include artifacts without versions
        assert len(result) == 2
        assert any("artifact1" in uri for uri in result)
        assert any("artifact3" in uri for uri in result)
        assert not any("2023.01.01" in uri for uri in result)

    def test_get_databus_artifacts_of_group_empty(self):
        """Test getting artifacts from group with no artifacts"""
        json_str = json.dumps({"databus:hasArtifact": []})

        result = __get_databus_artifacts_of_group__(json_str)

        assert len(result) == 0


class TestDatabusIDParsing:
    """Tests for databus ID parsing functionality"""

    def test_get_databus_id_parts_full_uri(self):
        """Test parsing complete databus URI"""
        uri = "https://databus.dbpedia.org/account/group/artifact/version/file.txt"

        host, account, group, artifact, version, file = __get_databus_id_parts__(uri)

        assert host == "databus.dbpedia.org"
        assert account == "account"
        assert group == "group"
        assert artifact == "artifact"
        assert version == "version"
        assert file == "file.txt"

    def test_get_databus_id_parts_version_uri(self):
        """Test parsing databus URI without file"""
        uri = "https://databus.dbpedia.org/account/group/artifact/version"

        host, account, group, artifact, version, file = __get_databus_id_parts__(uri)

        assert host == "databus.dbpedia.org"
        assert account == "account"
        assert group == "group"
        assert artifact == "artifact"
        assert version == "version"
        assert file is None

    def test_get_databus_id_parts_artifact_uri(self):
        """Test parsing databus URI to artifact level"""
        uri = "https://databus.dbpedia.org/account/group/artifact"

        host, account, group, artifact, version, file = __get_databus_id_parts__(uri)

        assert host == "databus.dbpedia.org"
        assert account == "account"
        assert group == "group"
        assert artifact == "artifact"
        assert version is None
        assert file is None

    def test_get_databus_id_parts_group_uri(self):
        """Test parsing databus URI to group level"""
        uri = "https://databus.dbpedia.org/account/group"

        host, account, group, artifact, version, file = __get_databus_id_parts__(uri)

        assert host == "databus.dbpedia.org"
        assert account == "account"
        assert group == "group"
        assert artifact is None
        assert version is None
        assert file is None

    def test_get_databus_id_parts_account_uri(self):
        """Test parsing databus URI to account level"""
        uri = "https://databus.dbpedia.org/account"

        host, account, group, artifact, version, file = __get_databus_id_parts__(uri)

        assert host == "databus.dbpedia.org"
        assert account == "account"
        assert group is None
        assert artifact is None
        assert version is None
        assert file is None

    def test_get_databus_id_parts_http_uri(self):
        """Test parsing HTTP (non-HTTPS) URI"""
        uri = "http://databus.dbpedia.org/account/group"

        host, account, group, _artifact, _version, _file = __get_databus_id_parts__(uri)

        assert host == "databus.dbpedia.org"
        assert account == "account"
        assert group == "group"

    def test_get_databus_id_parts_trailing_slash(self):
        """Test parsing URI with trailing slash"""
        uri = "https://databus.dbpedia.org/account/group/artifact/"

        host, account, group, artifact, _version, _file = __get_databus_id_parts__(uri)

        assert host == "databus.dbpedia.org"
        assert account == "account"
        assert group == "group"
        assert artifact == "artifact"


class TestDownloadFunction:
    """Tests for enhanced download function"""

    def test_download_with_query(self):
        """Test downloading with SPARQL query"""
        query = "SELECT ?file WHERE { ?s <downloadURL> ?file } LIMIT 5"

        with patch("databusclient.client.__handle_databus_file_query__") as mock_query:
            with patch("databusclient.client.__download_list__") as mock_download:
                mock_query.return_value = iter(["https://example.com/file1.txt"])

                download(localDir=TEST_LOCAL_DIR, endpoint="https://databus.dbpedia.org/sparql", databusURIs=[query])

                mock_query.assert_called_once()
                mock_download.assert_called_once()

    def test_download_query_requires_endpoint(self):
        """Test that query download requires endpoint parameter"""
        query = "SELECT ?file WHERE { ?s <downloadURL> ?file }"

        with pytest.raises(ValueError, match="No endpoint given for query"):
            download(localDir=TEST_LOCAL_DIR, endpoint=None, databusURIs=[query])

    def test_download_with_collection(self):
        """Test downloading from databus collection"""
        collection_uri = "https://databus.dbpedia.org/account/collections/test-collection"

        with patch("databusclient.client.__handle_databus_collection__") as mock_collection:
            with patch("databusclient.client.__handle_databus_file_query__") as mock_query:
                with patch("databusclient.client.__download_list__") as mock_download:
                    mock_collection.return_value = "SELECT ?file WHERE { ?s <downloadURL> ?file }"
                    mock_query.return_value = iter(["https://example.com/file1.txt"])

                    download(localDir=TEST_LOCAL_DIR, endpoint="https://databus.dbpedia.org/sparql", databusURIs=[collection_uri])

                    mock_collection.assert_called_once()
                    mock_download.assert_called_once()

    def test_download_auto_detects_endpoint(self):
        """Test that endpoint is auto-detected from URI"""
        uri = "https://databus.dbpedia.org/account/group/artifact/version/file.txt"

        with patch("databusclient.client.__download_list__") as mock_download:
            download(localDir=TEST_LOCAL_DIR, endpoint=None, databusURIs=[uri])

            # Verify endpoint was auto-detected (download_list was called)
            mock_download.assert_called_once()

    def test_download_file_with_vault_params(self):
        """Test downloading file with vault authentication parameters"""
        uri = "https://databus.dbpedia.org/account/group/artifact/version/file.txt"

        with patch("databusclient.client.__download_list__") as mock_download:
            vault_filename = "vault_token.txt"
            download(
                localDir=TEST_LOCAL_DIR,
                endpoint="https://databus.dbpedia.org/sparql",
                databusURIs=[uri],
                token=vault_filename,
                auth_url="https://auth.example.com/token",
                client_id="test-client",
            )

            # Verify vault params were passed to download_list
            mock_download.assert_called_once()
            call_kwargs = mock_download.call_args[1]
            assert call_kwargs.get("vault_token_file") == vault_filename
            assert call_kwargs.get("auth_url") == "https://auth.example.com/token"
            assert call_kwargs.get("client_id") == "test-client"

    def test_download_artifact_version(self):
        """Test downloading from artifact version URI"""
        uri = "https://databus.dbpedia.org/account/group/artifact/2023.01.01"
        json_ld = json.dumps({"@graph": [{"@type": "Part", "file": "https://example.com/file1.txt"}]})

        with patch("databusclient.client.__get_json_ld_from_databus__") as mock_get_json:
            with patch("databusclient.client.__handle_databus_artifact_version__") as mock_handle:
                with patch("databusclient.client.__download_list__") as mock_download:
                    mock_get_json.return_value = json_ld
                    mock_handle.return_value = ["https://example.com/file1.txt"]

                    download(localDir=TEST_LOCAL_DIR, endpoint="https://databus.dbpedia.org/sparql", databusURIs=[uri])

                    mock_get_json.assert_called_once()
                    mock_handle.assert_called_once()
                    mock_download.assert_called_once()

    def test_download_artifact_gets_latest_version(self):
        """Test downloading from artifact URI gets latest version"""
        uri = "https://databus.dbpedia.org/account/group/artifact"
        artifact_json = json.dumps({"databus:hasVersion": [{"@id": "https://databus.dbpedia.org/account/group/artifact/2023.01.01"}]})
        version_json = json.dumps({"@graph": [{"@type": "Part", "file": "https://example.com/file1.txt"}]})

        with patch("databusclient.client.__get_json_ld_from_databus__") as mock_get_json:
            with patch("databusclient.client.__get_databus_latest_version_of_artifact__") as mock_latest:
                with patch("databusclient.client.__handle_databus_artifact_version__") as mock_handle:
                    with patch("databusclient.client.__download_list__") as _mock_download:
                        mock_get_json.side_effect = [artifact_json, version_json]
                        mock_latest.return_value = "https://databus.dbpedia.org/account/group/artifact/2023.01.01"
                        mock_handle.return_value = ["https://example.com/file1.txt"]

                        download(localDir=TEST_LOCAL_DIR, endpoint="https://databus.dbpedia.org/sparql", databusURIs=[uri])

                        mock_latest.assert_called_once()
                        assert mock_get_json.call_count == 2

    def test_download_group_processes_all_artifacts(self):
        """Test downloading from group URI processes all artifacts"""
        uri = "https://databus.dbpedia.org/account/group"
        group_json = json.dumps(
            {"databus:hasArtifact": [{"@id": "https://databus.dbpedia.org/account/group/artifact1"}, {"@id": "https://databus.dbpedia.org/account/group/artifact2"}]}
        )

        with patch("databusclient.client.__get_json_ld_from_databus__") as mock_get_json:
            with patch("databusclient.client.__get_databus_artifacts_of_group__") as mock_artifacts:
                with patch("databusclient.client.__get_databus_latest_version_of_artifact__") as mock_latest:
                    with patch("databusclient.client.__handle_databus_artifact_version__") as mock_handle:
                        with patch("databusclient.client.__download_list__") as mock_download:
                            mock_get_json.return_value = group_json
                            mock_artifacts.return_value = [
                                "https://databus.dbpedia.org/account/group/artifact1",
                                "https://databus.dbpedia.org/account/group/artifact2",
                            ]
                            mock_latest.return_value = "https://databus.dbpedia.org/account/group/artifact1/2023.01.01"
                            mock_handle.return_value = ["https://example.com/file1.txt"]

                            download(localDir=TEST_LOCAL_DIR, endpoint="https://databus.dbpedia.org/sparql", databusURIs=[uri])

                            # Should process both artifacts
                            assert mock_latest.call_count == 2
                            assert mock_download.call_count == 2


class TestHelperFunctions:
    """Tests for helper functions"""

    def test_get_json_ld_from_databus(self):
        """Test fetching JSON-LD from databus"""
        uri = "https://databus.dbpedia.org/account/group/artifact"
        expected_json = '{"@context": "test"}'

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.text = expected_json
            mock_get.return_value = mock_response

            result = __get_json_ld_from_databus__(uri)

            assert result == expected_json
            mock_get.assert_called_once_with(uri, headers={"Accept": "application/ld+json"})

    def test_handle_databus_collection(self):
        """Test fetching SPARQL query from collection"""
        uri = "https://databus.dbpedia.org/account/collections/test"
        expected_query = "SELECT ?file WHERE { ?s <downloadURL> ?file }"

        with patch("requests.get") as mock_get:
            mock_response = Mock()
            mock_response.text = expected_query
            mock_get.return_value = mock_response

            result = __handle_databus_collection__(uri)

            assert result == expected_query
            mock_get.assert_called_once_with(uri, headers={"Accept": "text/sparql"})


class TestDownloadFileWithAuthentication:
    """Tests for __download_file__ with vault authentication"""

    def test_download_file_direct_success(self):
        """Test successful file download without authentication"""
        url = "https://example.com/file.txt"
        filename = os.path.join(TEST_LOCAL_DIR, "file.txt")

        with patch("requests.head") as mock_head:
            with patch("requests.get") as mock_get:
                with patch("builtins.open", mock_open()) as _mock_file:
                    with patch("os.makedirs"):
                        with patch("tqdm.tqdm"):
                            # Setup HEAD response (no redirect)
                            mock_head_response = Mock()
                            mock_head_response.status_code = 200
                            mock_head_response.headers = {}
                            mock_head.return_value = mock_head_response

                            # Setup GET response
                            mock_get_response = Mock()
                            mock_get_response.status_code = 200
                            mock_get_response.headers = {"content-length": "100", "WWW-Authenticate": ""}
                            mock_get_response.iter_content = Mock(return_value=[b"test data"])
                            mock_get_response.raise_for_status = Mock()
                            mock_get.return_value = mock_get_response

                            from databusclient.client import __download_file__

                            __download_file__(url, filename)

                            mock_head.assert_called_once()
                            mock_get.assert_called()

    def test_download_file_with_redirect(self):
        """Test file download following redirect"""
        url = "https://example.com/file.txt"
        redirect_url = "https://cdn.example.com/file.txt"
        filename = os.path.join(TEST_LOCAL_DIR, "file.txt")

        with patch("requests.head") as mock_head:
            with patch("requests.get") as mock_get:
                with patch("builtins.open", mock_open()):
                    with patch("os.makedirs"):
                        with patch("tqdm.tqdm"):
                            # Setup HEAD response with redirect
                            mock_head_response = Mock()
                            mock_head_response.status_code = 302
                            mock_head_response.headers = {"Location": redirect_url}
                            mock_head.return_value = mock_head_response

                            # Setup GET response
                            mock_get_response = Mock()
                            mock_get_response.status_code = 200
                            mock_get_response.headers = {"content-length": "100", "WWW-Authenticate": ""}
                            mock_get_response.iter_content = Mock(return_value=[b"test"])
                            mock_get_response.raise_for_status = Mock()
                            mock_get.return_value = mock_get_response

                            from databusclient.client import __download_file__

                            __download_file__(url, filename)

                            # Should use redirected URL
                            assert any(redirect_url in str(call) for call in mock_get.call_args_list)

    def test_download_file_requires_authentication(self):
        """Test file download with authentication requirement"""
        url = "https://protected.example.com/file.txt"
        filename = os.path.join(TEST_LOCAL_DIR, "file.txt")

        with patch("requests.head") as mock_head:
            with patch("requests.get") as mock_get:
                with patch("builtins.open", mock_open()):
                    with patch("os.makedirs"):
                        with patch("tqdm.tqdm"):
                            with patch("databusclient.client.__get_vault_access__") as mock_vault:
                                # Setup HEAD response
                                mock_head_response = Mock()
                                mock_head_response.status_code = 200
                                mock_head_response.headers = {}
                                mock_head.return_value = mock_head_response

                                # First GET returns 401
                                mock_get_401 = Mock()
                                mock_get_401.status_code = 401
                                mock_get_401.headers = {"WWW-Authenticate": "Bearer"}

                                # Second GET with token succeeds
                                mock_get_200 = Mock()
                                mock_get_200.status_code = 200
                                mock_get_200.headers = {"content-length": "100"}
                                mock_get_200.iter_content = Mock(return_value=[b"test"])
                                mock_get_200.raise_for_status = Mock()

                                mock_get.side_effect = [mock_get_401, mock_get_200]
                                mock_vault.return_value = "vault_token_123"

                                from databusclient.client import __download_file__

                                __download_file__(
                                    url,
                                    filename,
                                    vault_token_file="token.txt",
                                    auth_url="https://auth.example.com",
                                    client_id="test-client",
                                )

                                mock_vault.assert_called_once()

    def test_download_file_auth_without_vault_token_fails(self):
        """Test that authentication fails if vault token not provided"""
        url = "https://protected.example.com/file.txt"
        filename = os.path.join(TEST_LOCAL_DIR, "file.txt")

        with patch("requests.head") as mock_head:
            with patch("requests.get") as mock_get:
                # Setup HEAD response
                mock_head_response = Mock()
                mock_head_response.status_code = 200
                mock_head_response.headers = {}
                mock_head.return_value = mock_head_response

                # GET returns 401
                mock_get_response = Mock()
                mock_get_response.status_code = 401
                mock_get_response.headers = {"WWW-Authenticate": "Bearer"}
                mock_get.return_value = mock_get_response

                from databusclient.client import __download_file__
                with pytest.raises(ValueError, match="Vault token file not given"):
                    __download_file__(url, filename)


class TestExtensionParsing:
    """Tests for file extension and compression parsing"""

    def test_get_extensions_with_format_and_compression(self):
        """Test parsing extensions when both format and compression are specified"""
        from databusclient.client import __get_extensions

        dist_str = "https://example.com/file.txt|type=test|json|gz|sha256:1000"
        ext, fmt, comp = __get_extensions(dist_str)

        assert ext == ".json.gz"
        assert fmt == "json"
        assert comp == "gz"

    def test_get_extensions_with_format_only(self):
        """Test parsing extensions when only format is specified"""
        from databusclient.client import __get_extensions

        dist_str = "https://example.com/file.txt|type=test|json|sha256:1000"
        ext, fmt, comp = __get_extensions(dist_str)

        assert ext == ".json"
        assert fmt == "json"
        assert comp == "none"

    def test_get_extensions_inferred_from_url(self):
        """Test inferring extensions from URL when not specified"""
        from databusclient.client import __get_extensions

        dist_str = "https://example.com/file.json.gz|type=test"
        ext, fmt, comp = __get_extensions(dist_str)

        assert ext == ".json.gz"
        assert fmt == "json"
        assert comp == "gz"