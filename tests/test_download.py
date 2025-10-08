"""Download Tests"""
import pytest
import databusclient.client as cl

DEFAULT_ENDPOINT="https://databus.dbpedia.org/sparql"
TEST_QUERY="""
PREFIX dcat: <http://www.w3.org/ns/dcat#>
SELECT ?x WHERE {
  ?sub dcat:downloadURL ?x .
} LIMIT 10
"""
TEST_COLLECTION="https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2022-12"

def test_with_query():
  cl.download("tmp",DEFAULT_ENDPOINT,[TEST_QUERY]

)

def test_with_collection():
  cl.download("tmp",DEFAULT_ENDPOINT,[TEST_COLLECTION])

# ============================================================================
# Tests for new vault authentication and download capabilities
# ============================================================================
import json
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock, mock_open, call
from io import BytesIO


class TestGetDatabusIdParts:
    """Test suite for __get_databus_id_parts__ function"""
    
    def test_full_file_uri_with_https(self):
        """Test parsing a complete file URI with https"""
        uri = "https://databus.dbpedia.org/dbpedia/mappings/artifact/2022.12.01/file.ttl.bz2"
        host, account, group, artifact, version, file = cl.__get_databus_id_parts__(uri)
        assert host == "databus.dbpedia.org"
        assert account == "dbpedia"
        assert group == "mappings"
        assert artifact == "artifact"
        assert version == "2022.12.01"
        assert file == "file.ttl.bz2"
    
    def test_full_file_uri_with_http(self):
        """Test parsing a complete file URI with http"""
        uri = "http://databus.dbpedia.org/dbpedia/mappings/artifact/2022.12.01/file.ttl.bz2"
        host, account, _group, _artifact, _version, _file = cl.__get_databus_id_parts__(uri)
        assert host == "databus.dbpedia.org"
        assert account == "dbpedia"
    
    def test_version_uri(self):
        """Test parsing a version URI (no file)"""
        uri = "https://databus.dbpedia.org/dbpedia/mappings/artifact/2022.12.01"
        host, account, group, artifact, version, file = cl.__get_databus_id_parts__(uri)
        assert host == "databus.dbpedia.org"
        assert account == "dbpedia"
        assert group == "mappings"
        assert artifact == "artifact"
        assert version == "2022.12.01"
        assert file is None
    
    def test_artifact_uri(self):
        """Test parsing an artifact URI (no version)"""
        uri = "https://databus.dbpedia.org/dbpedia/mappings/artifact"
        host, account, group, artifact, version, file = cl.__get_databus_id_parts__(uri)
        assert host == "databus.dbpedia.org"
        assert account == "dbpedia"
        assert group == "mappings"
        assert artifact == "artifact"
        assert version is None
        assert file is None
    
    def test_group_uri(self):
        """Test parsing a group URI (no artifact)"""
        uri = "https://databus.dbpedia.org/dbpedia/mappings"
        host, account, group, artifact, version, file = cl.__get_databus_id_parts__(uri)
        assert host == "databus.dbpedia.org"
        assert account == "dbpedia"
        assert group == "mappings"
        assert artifact is None
        assert version is None
        assert file is None
    
    def test_account_uri(self):
        """Test parsing an account URI (no group)"""
        uri = "https://databus.dbpedia.org/dbpedia"
        host, account, group, artifact, version, file = cl.__get_databus_id_parts__(uri)
        assert host == "databus.dbpedia.org"
        assert account == "dbpedia"
        assert group is None
        assert artifact is None
        assert version is None
        assert file is None
    
    def test_uri_with_trailing_slash(self):
        """Test parsing URI with trailing slash"""
        uri = "https://databus.dbpedia.org/dbpedia/mappings/"
        host, account, group, artifact, _version, _file = cl.__get_databus_id_parts__(uri)
        assert host == "databus.dbpedia.org"
        assert account == "dbpedia"
        assert group == "mappings"
        assert artifact is None
    
    def test_uri_without_protocol(self):
        """Test parsing URI without protocol"""
        uri = "databus.dbpedia.org/dbpedia/mappings/artifact"
        host, account, group, artifact, _version, _file = cl.__get_databus_id_parts__(uri)
        assert host == "databus.dbpedia.org"
        assert account == "dbpedia"
        assert group == "mappings"
        assert artifact == "artifact"


class TestHandleDatabusArtifactVersion:
    """Test suite for __handle_databus_artifact_version__ function"""
    
    def test_parse_single_file_version(self):
        """Test parsing JSON-LD with a single file"""
        json_data = json.dumps({
            "@graph": [
                {
                    "@type": "Part",
                    "file": "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file1.ttl.bz2"
                }
            ]
        })
        result = cl.__handle_databus_artifact_version__(json_data)
        assert len(result) == 1
        assert result[0] == "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file1.ttl.bz2"
    
    def test_parse_multiple_files_version(self):
        """Test parsing JSON-LD with multiple files"""
        json_data = json.dumps({
            "@graph": [
                {
                    "@type": "Part",
                    "file": "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file1.ttl.bz2"
                },
                {
                    "@type": "Part",
                    "file": "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file2.ttl.bz2"
                },
                {
                    "@type": "Dataset",
                    "title": "Some Dataset"
                }
            ]
        })
        result = cl.__handle_databus_artifact_version__(json_data)
        assert len(result) == 2
        assert "file1.ttl.bz2" in result[0]
        assert "file2.ttl.bz2" in result[1]
    
    def test_parse_empty_graph(self):
        """Test parsing JSON-LD with empty graph"""
        json_data = json.dumps({"@graph": []})
        result = cl.__handle_databus_artifact_version__(json_data)
        assert len(result) == 0
    
    def test_parse_no_parts(self):
        """Test parsing JSON-LD with no Part types"""
        json_data = json.dumps({
            "@graph": [
                {"@type": "Dataset", "title": "Test"},
                {"@type": "Distribution", "format": "ttl"}
            ]
        })
        result = cl.__handle_databus_artifact_version__(json_data)
        assert len(result) == 0
    
    def test_parse_part_without_file(self):
        """Test parsing Part node without file attribute"""
        json_data = json.dumps({
            "@graph": [
                {"@type": "Part", "format": "ttl"},
                {"@type": "Part", "file": "https://example.com/file.ttl"}
            ]
        })
        result = cl.__handle_databus_artifact_version__(json_data)
        assert len(result) == 2
        assert result[0] is None  # Part without file
        assert result[1] == "https://example.com/file.ttl"


class TestGetDatabusLatestVersionOfArtifact:
    """Test suite for __get_databus_latest_version_of_artifact__ function"""
    
    def test_single_version(self):
        """Test extracting latest version when only one version exists"""
        json_data = json.dumps({
            "databus:hasVersion": {
                "@id": "https://databus.dbpedia.org/account/group/artifact/2022.12.01"
            }
        })
        result = cl.__get_databus_latest_version_of_artifact__(json_data)
        assert result == "https://databus.dbpedia.org/account/group/artifact/2022.12.01"
    
    def test_multiple_versions_sorted(self):
        """Test extracting latest version from multiple versions"""
        json_data = json.dumps({
            "databus:hasVersion": [
                {"@id": "https://databus.dbpedia.org/account/group/artifact/2022.12.01"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact/2023.01.01"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact/2022.11.01"}
            ]
        })
        result = cl.__get_databus_latest_version_of_artifact__(json_data)
        # Latest should be 2023.01.01 after sorting
        assert "2023.01.01" in result
    
    def test_multiple_versions_numeric_sort(self):
        """Test that version sorting works correctly with different date formats"""
        json_data = json.dumps({
            "databus:hasVersion": [
                {"@id": "https://databus.dbpedia.org/account/group/artifact/2020.01.01"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact/2023.12.31"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact/2023.01.01"}
            ]
        })
        result = cl.__get_databus_latest_version_of_artifact__(json_data)
        assert "2023.12.31" in result
    
    def test_no_versions_raises_error(self):
        """Test that missing versions raises ValueError"""
        json_data = json.dumps({"databus:hasVersion": []})
        with pytest.raises(ValueError, match="No versions found"):
            cl.__get_databus_latest_version_of_artifact__(json_data)
    
    def test_versions_without_id(self):
        """Test handling versions without @id field"""
        json_data = json.dumps({
            "databus:hasVersion": [
                {"name": "version1"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact/2022.12.01"}
            ]
        })
        result = cl.__get_databus_latest_version_of_artifact__(json_data)
        assert result == "https://databus.dbpedia.org/account/group/artifact/2022.12.01"


class TestGetDatabusArtifactsOfGroup:
    """Test suite for __get_databus_artifacts_of_group__ function"""
    
    def test_single_artifact(self):
        """Test extracting single artifact from group"""
        json_data = json.dumps({
            "databus:hasArtifact": [
                {"@id": "https://databus.dbpedia.org/account/group/artifact1"}
            ]
        })
        result = cl.__get_databus_artifacts_of_group__(json_data)
        assert len(result) == 1
        assert result[0] == "https://databus.dbpedia.org/account/group/artifact1"
    
    def test_multiple_artifacts(self):
        """Test extracting multiple artifacts from group"""
        json_data = json.dumps({
            "databus:hasArtifact": [
                {"@id": "https://databus.dbpedia.org/account/group/artifact1"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact2"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact3"}
            ]
        })
        result = cl.__get_databus_artifacts_of_group__(json_data)
        assert len(result) == 3
        assert "artifact1" in result[0]
        assert "artifact2" in result[1]
        assert "artifact3" in result[2]
    
    def test_filter_versioned_artifacts(self):
        """Test that artifacts with version are filtered out"""
        json_data = json.dumps({
            "databus:hasArtifact": [
                {"@id": "https://databus.dbpedia.org/account/group/artifact1"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact2/2022.12.01"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact3"}
            ]
        })
        result = cl.__get_databus_artifacts_of_group__(json_data)
        # Should exclude the versioned artifact
        assert len(result) == 2
        assert any("artifact1" in r for r in result)
        assert any("artifact3" in r for r in result)
        assert not any("2022.12.01" in r for r in result)
    
    def test_empty_artifacts(self):
        """Test handling empty artifact list"""
        json_data = json.dumps({"databus:hasArtifact": []})
        result = cl.__get_databus_artifacts_of_group__(json_data)
        assert len(result) == 0
    
    def test_artifacts_without_id(self):
        """Test handling artifacts without @id field"""
        json_data = json.dumps({
            "databus:hasArtifact": [
                {"name": "artifact1"},
                {"@id": "https://databus.dbpedia.org/account/group/artifact2"}
            ]
        })
        result = cl.__get_databus_artifacts_of_group__(json_data)
        assert len(result) == 1
        assert "artifact2" in result[0]


class TestGetVaultAccess:
    """Test suite for __get_vault_access__ function"""
    
    @patch('databusclient.client.requests.post')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data="test_refresh_token_" + "x" * 80)
    def test_vault_access_with_token_file(self, _mock_file, mock_exists, mock_post):
        """Test getting vault access with token from file"""
        mock_exists.return_value = True
        
        # Mock the two POST requests
        mock_post.side_effect = [
            Mock(json=lambda: {"access_token": "test_access_token"}, raise_for_status=lambda: None),
            Mock(json=lambda: {"access_token": "test_vault_token"}, raise_for_status=lambda: None)
        ]
        
        result = cl.__get_vault_access__(
            "https://data.dbpedia.io/databus.dbpedia.org/file.ttl",
            "token.dat",
            "https://auth.example.com/token",
            "test-client"
        )
        
        assert result == "test_vault_token"
        assert mock_post.call_count == 2
        
        # Verify first call (refresh token exchange)
        first_call = mock_post.call_args_list[0]
        assert first_call[1]["data"]["grant_type"] == "refresh_token"
        assert "test_refresh_token_" in first_call[1]["data"]["refresh_token"]
        
        # Verify second call (token exchange for vault)
        second_call = mock_post.call_args_list[1]
        assert second_call[1]["data"]["grant_type"] == "urn:ietf:params:oauth:grant-type:token-exchange"
        assert second_call[1]["data"]["audience"] == "data.dbpedia.io"
    
    @patch.dict(os.environ, {'REFRESH_TOKEN': 'env_refresh_token_' + 'x' * 80})
    @patch('databusclient.client.requests.post')
    def test_vault_access_with_env_token(self, mock_post):
        """Test getting vault access with token from environment variable"""
        mock_post.side_effect = [
            Mock(json=lambda: {"access_token": "test_access_token"}, raise_for_status=lambda: None),
            Mock(json=lambda: {"access_token": "test_vault_token"}, raise_for_status=lambda: None)
        ]
        
        result = cl.__get_vault_access__(
            "https://data.dbpedia.io/file.ttl",
            "token.dat",
            "https://auth.example.com/token",
            "test-client"
        )
        
        assert result == "test_vault_token"
        first_call = mock_post.call_args_list[0]
        assert "env_refresh_token_" in first_call[1]["data"]["refresh_token"]
    
    @patch('os.path.exists')
    def test_vault_access_missing_token_file(self, mock_exists):
        """Test that missing token file raises FileNotFoundError"""
        mock_exists.return_value = False
        
        with pytest.raises(FileNotFoundError, match="Vault token file not found"):
            cl.__get_vault_access__(
                "https://data.dbpedia.io/file.ttl",
                "nonexistent.dat",
                "https://auth.example.com/token",
                "test-client"
            )
    
    @patch('databusclient.client.requests.post')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data="test_refresh_token_" + "x" * 80)
    def test_vault_access_http_url(self, _mock_file, mock_exists, mock_post):
        """Test audience extraction from HTTP URL"""
        mock_exists.return_value = True
        mock_post.side_effect = [
            Mock(json=lambda: {"access_token": "test_access_token"}, raise_for_status=lambda: None),
            Mock(json=lambda: {"access_token": "test_vault_token"}, raise_for_status=lambda: None)
        ]
        
        _result = cl.__get_vault_access__(
            "http://data.dbpedia.io/databus.dbpedia.org/file.ttl",
            "token.dat",
            "https://auth.example.com/token",
            "test-client"
        )
        
        second_call = mock_post.call_args_list[1]
        assert second_call[1]["data"]["audience"] == "data.dbpedia.io"
    
    @patch('databusclient.client.requests.post')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data="test_refresh_token_" + "x" * 80)
    def test_vault_access_no_protocol(self, _mock_file, mock_exists, mock_post):
        """Test audience extraction from URL without protocol"""
        mock_exists.return_value = True
        mock_post.side_effect = [
            Mock(json=lambda: {"access_token": "test_access_token"}, raise_for_status=lambda: None),
            Mock(json=lambda: {"access_token": "test_vault_token"}, raise_for_status=lambda: None)
        ]
        
        _result = cl.__get_vault_access__(
            "data.dbpedia.io/databus.dbpedia.org/file.ttl",
            "token.dat",
            "https://auth.example.com/token",
            "test-client"
        )
        
        second_call = mock_post.call_args_list[1]
        assert second_call[1]["data"]["audience"] == "data.dbpedia.io"
    
    @patch('databusclient.client.requests.post')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open, read_data="test_refresh_token_" + "x" * 80)
    def test_vault_access_auth_error(self, _mock_file, mock_exists, mock_post):
        """Test handling of authentication errors"""
        mock_exists.return_value = True
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("Authentication failed")
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception, match="Authentication failed"):
            cl.__get_vault_access__(
                "https://data.dbpedia.io/file.ttl",
                "token.dat",
                "https://auth.example.com/token",
                "test-client"
            )


class TestDownloadFile:
    """Test suite for __download_file__ function"""
    
    @patch('databusclient.client.requests.head')
    @patch('databusclient.client.requests.get')
    @patch('databusclient.client.tqdm')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_download_file_no_auth(self, mock_makedirs, mock_file, mock_tqdm, mock_get, mock_head):
        """Test downloading file without authentication"""
        # Mock HEAD request (no redirect)
        mock_head.return_value = Mock(
            headers={},
            status_code=200
        )
        
        # Mock GET request
        mock_response = Mock()
        mock_response.headers = {'content-length': '1024'}
        mock_response.iter_content = lambda _block_size: [b'data' * 256]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        # Mock progress bar
        mock_progress = Mock()
        mock_progress.n = 1024
        mock_tqdm.return_value = mock_progress
        
        tmp_file = os.path.join(tempfile.gettempdir(), "file.txt")
        cl.__download_file__("https://example.com/file.txt", tmp_file)
        
        mock_get.assert_called()
        mock_file.assert_called_once_with(tmp_file, 'wb')
        mock_makedirs.assert_called_once()
    
    @patch('databusclient.client.requests.head')
    @patch('databusclient.client.requests.get')
    def test_download_file_with_redirect(self, mock_get, mock_head):
        """Test downloading file with redirect"""
        # Mock HEAD request with redirect
        mock_head.return_value = Mock(
            headers={'Location': 'https://redirected.com/file.txt'},
            status_code=302
        )
        
        # Mock GET request
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {'content-length': '1024'}
        mock_response.iter_content = lambda _block_size: [b'data' * 256]
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        
        with patch('databusclient.client.tqdm'), \
             patch('builtins.open', mock_open()), \
             patch('os.makedirs'):
            tmp_file = os.path.join(tempfile.gettempdir(), "file.txt")
            cl.__download_file__("https://example.com/file.txt", tmp_file)
        
        # Verify GET was called with redirected URL
        get_call_url = mock_get.call_args_list[0][0][0]
        assert "redirected.com" in get_call_url
    
    @patch('databusclient.client.requests.head')
    @patch('databusclient.client.requests.get')
    @patch('databusclient.client.__get_vault_access__')
    def test_download_file_with_auth_401(self, mock_vault, mock_get, mock_head):
        """Test downloading file with 401 authentication required"""
        mock_head.return_value = Mock(headers={}, status_code=200)
        
        # First GET returns 401
        mock_response_401 = Mock()
        mock_response_401.status_code = 401
        mock_response_401.headers = {'WWW-Authenticate': 'Bearer realm="vault"'}
        
        # Second GET with token succeeds
        mock_response_ok = Mock()
        mock_response_ok.status_code = 200
        mock_response_ok.headers = {'content-length': '1024'}
        mock_response_ok.iter_content = lambda _block_size: [b'data' * 256]
        mock_response_ok.raise_for_status = Mock()
        
        mock_get.side_effect = [mock_response_401, mock_response_ok]
        mock_vault.return_value = "test_vault_token"
        
        with patch('databusclient.client.tqdm'), \
             patch('builtins.open', mock_open()), \
             patch('os.makedirs'):
            tmp_file = os.path.join(tempfile.gettempdir(), "file.txt")
            token_file = os.path.join(tempfile.gettempdir(), "token.dat")
            cl.__download_file__(
                "https://example.com/file.txt",
                tmp_file,
                vault_token_file=token_file,
                auth_url="https://auth.example.com",
                client_id="test-client"
            )
        
        # Verify vault access was called
        mock_vault.assert_called_once()
        
        # Verify second GET had Authorization header
        second_get_call = mock_get.call_args_list[1]
        assert 'headers' in second_get_call[1]
        assert second_get_call[1]['headers']['Authorization'] == 'Bearer test_vault_token'
    
    @patch('databusclient.client.requests.head')
    @patch('databusclient.client.requests.get')
    def test_download_file_auth_required_no_token(self, mock_get, mock_head):
        """Test that auth required without token raises ValueError"""
        mock_head.return_value = Mock(headers={}, status_code=200)
        
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.headers = {'WWW-Authenticate': 'Bearer'}
        mock_get.return_value = mock_response
        
        with pytest.raises(ValueError, match="Vault token file not given"):
            tmp_file = os.path.join(tempfile.gettempdir(), "file.txt")
            cl.__download_file__("https://example.com/file.txt", tmp_file)
    
    @patch('databusclient.client.requests.head')
    @patch('databusclient.client.requests.get')
    @patch('databusclient.client.tqdm')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_download_file_http_error(self, _mock_makedirs, _mock_file, _mock_tqdm, mock_get, mock_head):
        """Test handling of HTTP errors during download"""
        mock_head.return_value = Mock(headers={}, status_code=200)
        
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = Exception("HTTP 404 Not Found")
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception, match="HTTP 404"):
            tmp_file = os.path.join(tempfile.gettempdir(), "file.txt")
            cl.__download_file__("https://example.com/file.txt", tmp_file)


class TestDownloadFunction:
    """Test suite for download function with vault support"""
    
    @patch('databusclient.client.__download_file__')
    def test_download_file_uri(self, mock_download):
        """Test downloading a single file URI"""
        file_uri = "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl.bz2"
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        cl.download(
            localDir=local_dir,
            endpoint=None,
            databusURIs=[file_uri]
        )
        
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        assert file_uri in call_args[1]['url']
        assert "file.ttl.bz2" in call_args[1]['filename']
    
    @patch('databusclient.client.__download_file__')
    def test_download_file_uri_with_vault(self, mock_download):
        """Test downloading a file with vault authentication"""
        file_uri = "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl.bz2"
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        token_file = os.path.join(tempfile.gettempdir(), "vault-token.dat")
        cl.download(
            localDir=local_dir,
            endpoint=None,
            databusURIs=[file_uri],
            token=token_file,
            auth_url="https://auth.example.com",
            client_id="test-client"
        )
        
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        assert call_args[1]['vault_token_file'] == token_file
        assert call_args[1]['auth_url'] == "https://auth.example.com"
        assert call_args[1]['client_id'] == "test-client"
    
    @patch('databusclient.client.__get_json_ld_from_databus__')
    @patch('databusclient.client.__handle_databus_artifact_version__')
    @patch('databusclient.client.__download_file__')
    def test_download_version_uri(self, mock_download, mock_handle_version, mock_get_json):
        """Test downloading all files from a version URI"""
        version_uri = "https://databus.dbpedia.org/account/group/artifact/2022.12.01"
        
        mock_get_json.return_value = '{"@graph": []}'
        mock_handle_version.return_value = [
            "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file1.ttl",
            "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file2.ttl"
        ]
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        cl.download(
            localDir=local_dir,
            endpoint=None,
            databusURIs=[version_uri]
        )
        
        assert mock_download.call_count == 2
    
    @patch('databusclient.client.__get_json_ld_from_databus__')
    @patch('databusclient.client.__get_databus_latest_version_of_artifact__')
    @patch('databusclient.client.__handle_databus_artifact_version__')
    @patch('databusclient.client.__download_file__')
    def test_download_artifact_uri(self, mock_download, mock_handle_version, mock_latest, mock_get_json):
        """Test downloading latest version of an artifact"""
        artifact_uri = "https://databus.dbpedia.org/account/group/artifact"
        
        mock_get_json.return_value = '{"@graph": []}'
        mock_latest.return_value = "https://databus.dbpedia.org/account/group/artifact/2023.01.01"
        mock_handle_version.return_value = [
            "https://databus.dbpedia.org/account/group/artifact/2023.01.01/file.ttl"
        ]
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        cl.download(
            localDir=local_dir,
            endpoint=None,
            databusURIs=[artifact_uri]
        )
        
        mock_latest.assert_called_once()
        mock_download.assert_called_once()
    
    @patch('databusclient.client.__handle_databus_collection__')
    @patch('databusclient.client.__handle_databus_file_query__')
    @patch('databusclient.client.__download_file__')
    def test_download_collection(self, mock_download, mock_query, mock_collection):
        """Test downloading files from a collection"""
        collection_uri = "https://databus.dbpedia.org/account/collections/test-collection"
        
        mock_collection.return_value = "SELECT ?file WHERE { ?s ?p ?file }"
        mock_query.return_value = iter([
            "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file1.ttl"
        ])
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        cl.download(
            localDir=local_dir,
            endpoint="https://databus.dbpedia.org/sparql",
            databusURIs=[collection_uri]
        )
        
        mock_collection.assert_called_once_with(collection_uri)
        mock_download.assert_called_once()
    
    @patch('databusclient.client.__handle_databus_file_query__')
    @patch('databusclient.client.__download_file__')
    def test_download_query(self, mock_download, mock_query):
        """Test downloading files from a SPARQL query"""
        query = "SELECT ?x WHERE { ?s ?p ?x } LIMIT 5"
        
        mock_query.return_value = iter([
            "https://databus.dbpedia.org/file1.ttl",
            "https://databus.dbpedia.org/file2.ttl"
        ])
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        cl.download(
            localDir=local_dir,
            endpoint="https://databus.dbpedia.org/sparql",
            databusURIs=[query]
        )
        
        mock_query.assert_called_once()
        assert mock_download.call_count == 2
    
    def test_download_query_without_endpoint_raises_error(self):
        """Test that query without endpoint raises ValueError"""
        query = "SELECT ?x WHERE { ?s ?p ?x }"
        
        with pytest.raises(ValueError, match="No endpoint given"):
            local_dir = os.path.join(tempfile.gettempdir(), "test")
            cl.download(
                localDir=local_dir,
                endpoint=None,
                databusURIs=[query]
            )
    
    @patch('databusclient.client.__download_file__')
    def test_download_without_localdir_creates_structure(self, mock_download):
        """Test that download without localDir creates proper directory structure"""
        file_uri = "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl.bz2"
        
        with patch('os.getcwd', return_value='/home/user'):
            cl.download(
                localDir=None,
                endpoint=None,
                databusURIs=[file_uri]
            )
        
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        filename = call_args[1]['filename']
        # Should create directory structure based on databus URI
        assert 'account' in filename
        assert 'group' in filename
        assert 'artifact' in filename


class TestWsha256Function:
    """Test suite for wsha256 function"""
    
    def test_wsha256_empty_string(self):
        """Test SHA256 hash of empty string"""
        result = cl.wsha256("")
        # SHA256 of empty string
        assert result == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    
    def test_wsha256_simple_string(self):
        """Test SHA256 hash of simple string"""
        result = cl.wsha256("test")
        expected = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        assert result == expected
    
    def test_wsha256_unicode_string(self):
        """Test SHA256 hash of unicode string"""
        result = cl.wsha256("Hello 世界")
        # Should handle unicode correctly
        assert len(result) == 64  # SHA256 produces 64 hex characters
        assert all(c in '0123456789abcdef' for c in result)
    
    def test_wsha256_special_characters(self):
        """Test SHA256 hash with special characters"""
        result = cl.wsha256("\!@#$%^&*()")
        assert len(result) == 64
    
    def test_wsha256_consistency(self):
        """Test that same input produces same hash"""
        input_str = "consistent_input"
        result1 = cl.wsha256(input_str)
        result2 = cl.wsha256(input_str)
        assert result1 == result2


class TestHandleDatabusCollection:
    """Test suite for __handle_databus_collection__ function"""
    
    @patch('databusclient.client.requests.get')
    def test_handle_collection_returns_query(self, mock_get):
        """Test that collection handler returns SPARQL query"""
        mock_response = Mock()
        mock_response.text = "SELECT ?file WHERE { ?s <http://www.w3.org/ns/dcat#downloadURL> ?file }"
        mock_get.return_value = mock_response
        
        result = cl.__handle_databus_collection__("https://databus.dbpedia.org/collections/test")
        
        assert "SELECT" in result
        assert "downloadURL" in result
        mock_get.assert_called_once()
        # Verify Accept header for SPARQL
        call_args = mock_get.call_args
        assert call_args[1]['headers']['Accept'] == 'text/sparql'
    
    @patch('databusclient.client.requests.get')
    def test_handle_collection_with_different_uri(self, mock_get):
        """Test collection handler with different URI"""
        mock_response = Mock()
        mock_response.text = "SELECT ?x WHERE { ?sub ?pred ?x }"
        mock_get.return_value = mock_response
        
        collection_uri = "https://databus.dbpedia.org/account/collections/my-collection"
        result = cl.__handle_databus_collection__(collection_uri)
        
        mock_get.assert_called_once_with(collection_uri, headers={"Accept": "text/sparql"})
        assert result == "SELECT ?x WHERE { ?sub ?pred ?x }"


class TestGetJsonLdFromDatabus:
    """Test suite for __get_json_ld_from_databus__ function"""
    
    @patch('databusclient.client.requests.get')
    def test_get_json_ld(self, mock_get):
        """Test fetching JSON-LD from databus"""
        mock_response = Mock()
        mock_response.text = '{"@context": "test", "@graph": []}'
        mock_get.return_value = mock_response
        
        result = cl.__get_json_ld_from_databus__("https://databus.dbpedia.org/account/artifact")
        
        assert result == '{"@context": "test", "@graph": []}'
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert call_args[1]['headers']['Accept'] == 'application/ld+json'
    
    @patch('databusclient.client.requests.get')
    def test_get_json_ld_complex_structure(self, mock_get):
        """Test fetching complex JSON-LD structure"""
        complex_json = json.dumps({
            "@context": "https://downloads.dbpedia.org/databus/context.jsonld",
            "@graph": [
                {"@type": "Dataset", "@id": "test1"},
                {"@type": "Part", "file": "file1.ttl"}
            ]
        })
        mock_response = Mock()
        mock_response.text = complex_json
        mock_get.return_value = mock_response
        
        result = cl.__get_json_ld_from_databus__("https://databus.dbpedia.org/account/artifact/version")
        
        assert "@context" in result
        assert "@graph" in result


# ============================================================================
# Integration-style tests for complex scenarios
# ============================================================================
class TestDownloadComplexScenarios:
    """Integration-style tests for complex download scenarios"""
    
    @patch('databusclient.client.__get_json_ld_from_databus__')
    @patch('databusclient.client.__get_databus_artifacts_of_group__')
    @patch('databusclient.client.__get_databus_latest_version_of_artifact__')
    @patch('databusclient.client.__handle_databus_artifact_version__')
    @patch('databusclient.client.__download_file__')
    def test_download_group_with_multiple_artifacts(self, mock_download, mock_version_files, 
                                                      mock_latest, mock_artifacts, mock_json):
        """Test downloading entire group with multiple artifacts"""
        group_uri = "https://databus.dbpedia.org/account/group"
        
        # Mock group JSON
        mock_json.side_effect = [
            '{"databus:hasArtifact": []}',  # Group JSON
            '{"databus:hasVersion": []}',   # Artifact 1 JSON
            '{"@graph": []}',                # Version 1 JSON
            '{"databus:hasVersion": []}',   # Artifact 2 JSON
            '{"@graph": []}'                 # Version 2 JSON
        ]
        
        mock_artifacts.return_value = [
            "https://databus.dbpedia.org/account/group/artifact1",
            "https://databus.dbpedia.org/account/group/artifact2"
        ]
        
        mock_latest.side_effect = [
            "https://databus.dbpedia.org/account/group/artifact1/2023.01.01",
            "https://databus.dbpedia.org/account/group/artifact2/2023.01.01"
        ]
        
        mock_version_files.side_effect = [
            ["https://databus.dbpedia.org/account/group/artifact1/2023.01.01/file1.ttl"],
            ["https://databus.dbpedia.org/account/group/artifact2/2023.01.01/file2.ttl"]
        ]
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        cl.download(
            localDir=local_dir,
            endpoint=None,
            databusURIs=[group_uri]
        )
        
        # Should download one file from each artifact
        assert mock_download.call_count == 2
    
    @patch('databusclient.client.__download_file__')
    def test_download_multiple_file_uris(self, mock_download):
        """Test downloading multiple file URIs in one call"""
        file_uris = [
            "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file1.ttl",
            "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file2.ttl",
            "https://databus.dbpedia.org/account/group/artifact/2022.12.01/file3.ttl"
        ]
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        cl.download(
            localDir=local_dir,
            endpoint=None,
            databusURIs=file_uris
        )
        
        assert mock_download.call_count == 3
    
    @patch('databusclient.client.__handle_databus_file_query__')
    @patch('databusclient.client.__download_file__')
    def test_download_mixed_uris_and_queries(self, mock_download, mock_query):
        """Test downloading mix of file URIs and queries"""
        mock_query.return_value = iter([
            "https://databus.dbpedia.org/query/file1.ttl"
        ])
        
        mixed_inputs = [
            "https://databus.dbpedia.org/account/group/artifact/2022.12.01/direct.ttl",
            "SELECT ?x WHERE { ?s ?p ?x }"
        ]
        
        local_dir = os.path.join(tempfile.gettempdir(), "test")
        cl.download(
            localDir=local_dir,
            endpoint="https://databus.dbpedia.org/sparql",
            databusURIs=mixed_inputs
        )
        
        # Should download direct file + query result
        assert mock_download.call_count == 2