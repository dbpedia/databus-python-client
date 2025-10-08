"""CLI Tests for click-based commands"""
import pytest
from click.testing import CliRunner
from unittest.mock import patch, Mock
from databusclient.cli import app, deploy, download


class TestDeployCommand:
    """Test suite for deploy command"""

    def setup_method(self):
        """Setup test runner"""
        self.runner = CliRunner()

    @patch('databusclient.client.create_dataset')
    @patch('databusclient.client.deploy')
    def test_deploy_command_basic(self, mock_deploy_func, mock_create_dataset):
        """Test basic deploy command execution"""
        mock_create_dataset.return_value = {"test": "dataid"}
        mock_deploy_func.return_value = None

        result = self.runner.invoke(app, [
            'deploy',
            '--versionid', 'https://databus.dbpedia.org/user/group/artifact/1.0.0',
            '--title', 'Test Dataset',
            '--abstract', 'Test abstract',
            '--description', 'Test description',
            '--license', 'https://example.com/license',
            '--apikey', 'test_api_key',
            'https://example.com/file1.ttl',
            'https://example.com/file2.ttl'
        ])

        assert result.exit_code == 0
        mock_create_dataset.assert_called_once()
        mock_deploy_func.assert_called_once()

    def test_deploy_command_missing_versionid(self):
        """Test deploy command fails without versionid"""
        result = self.runner.invoke(app, [
            'deploy',
            '--title', 'Test Dataset',
            '--abstract', 'Test abstract',
            '--description', 'Test description',
            '--license', 'https://example.com/license',
            '--apikey', 'test_api_key',
            'https://example.com/file.ttl'
        ])

        assert result.exit_code != 0
        assert 'Missing option' in result.output or 'required' in result.output.lower()

    def test_deploy_command_missing_title(self):
        """Test deploy command fails without title"""
        result = self.runner.invoke(app, [
            'deploy',
            '--versionid', 'https://databus.dbpedia.org/user/group/artifact/1.0.0',
            '--abstract', 'Test abstract',
            '--description', 'Test description',
            '--license', 'https://example.com/license',
            '--apikey', 'test_api_key',
            'https://example.com/file.ttl'
        ])

        assert result.exit_code != 0

    def test_deploy_command_missing_distributions(self):
        """Test deploy command fails without distributions"""
        result = self.runner.invoke(app, [
            'deploy',
            '--versionid', 'https://databus.dbpedia.org/user/group/artifact/1.0.0',
            '--title', 'Test Dataset',
            '--abstract', 'Test abstract',
            '--description', 'Test description',
            '--license', 'https://example.com/license',
            '--apikey', 'test_api_key'
        ])

        assert result.exit_code != 0

    @patch('databusclient.client.create_dataset')
    @patch('databusclient.client.deploy')
    def test_deploy_command_with_multiple_distributions(self, mock_deploy_func, mock_create_dataset):
        """Test deploy with multiple distribution arguments"""
        mock_create_dataset.return_value = {"test": "dataid"}
        mock_deploy_func.return_value = None

        result = self.runner.invoke(app, [
            'deploy',
            '--versionid', 'https://databus.dbpedia.org/user/group/artifact/1.0.0',
            '--title', 'Test Dataset',
            '--abstract', 'Short abstract',
            '--description', 'Detailed description',
            '--license', 'https://example.com/license',
            '--apikey', 'test_api_key',
            'https://example.com/file1.ttl',
            'https://example.com/file2.ttl',
            'https://example.com/file3.ttl'
        ])

        assert result.exit_code == 0
        # Verify distributions were passed correctly
        call_args = mock_create_dataset.call_args
        distributions = call_args[0][4]  # 5th positional argument
        assert len(distributions) == 3

    @patch('databusclient.client.create_dataset')
    @patch('databusclient.client.deploy')
    def test_deploy_command_output_message(self, mock_deploy_func, mock_create_dataset):
        """Test that deploy command outputs deployment message"""
        mock_create_dataset.return_value = {"test": "dataid"}
        mock_deploy_func.return_value = None

        result = self.runner.invoke(app, [
            'deploy',
            '--versionid', 'https://databus.dbpedia.org/user/group/artifact/1.0.0',
            '--title', 'Test Dataset',
            '--abstract', 'Test abstract',
            '--description', 'Test description',
            '--license', 'https://example.com/license',
            '--apikey', 'test_api_key',
            'https://example.com/file.ttl'
        ])

        assert result.exit_code == 0
        assert 'Deploying' in result.output or 'databus.dbpedia.org' in result.output


class TestDownloadCommand:
    """Test suite for download command"""

    def setup_method(self):
        """Setup test runner"""
        self.runner = CliRunner()

    @patch('databusclient.client.download')
    def test_download_command_single_uri(self, mock_download):
        """Test download command with single URI"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code == 0
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        assert len(call_args[1]['databusURIs']) == 1

    @patch('databusclient.client.download')
    def test_download_command_multiple_uris(self, mock_download):
        """Test download command with multiple URIs"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact1/2022.12.01/file1.ttl',
            'https://databus.dbpedia.org/account/group/artifact2/2022.12.01/file2.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert len(call_args[1]['databusURIs']) == 2

    @patch('databusclient.client.download')
    def test_download_command_with_localdir(self, mock_download):
        """Test download command with custom local directory"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            '--localdir', '/custom/path',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['localDir'] == '/custom/path'

    @patch('databusclient.client.download')
    def test_download_command_with_databus_endpoint(self, mock_download):
        """Test download command with custom databus endpoint"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            '--databus', 'https://custom.databus.org/sparql',
            'SELECT ?x WHERE { ?s ?p ?x }'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['endpoint'] == 'https://custom.databus.org/sparql'

    @patch('databusclient.client.download')
    def test_download_command_with_vault_token(self, mock_download):
        """Test download command with vault token file"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            '--token', 'vault-token.dat',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['token'] == 'vault-token.dat'

    @patch('databusclient.client.download')
    def test_download_command_with_custom_auth_url(self, mock_download):
        """Test download command with custom auth URL"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            '--authurl', 'https://custom-auth.example.com/token',
            '--token', 'vault-token.dat',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['auth_url'] == 'https://custom-auth.example.com/token'

    @patch('databusclient.client.download')
    def test_download_command_with_custom_client_id(self, mock_download):
        """Test download command with custom client ID"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            '--clientid', 'custom-client',
            '--token', 'vault-token.dat',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['client_id'] == 'custom-client'

    @patch('databusclient.client.download')
    def test_download_command_default_auth_values(self, mock_download):
        """Test that default auth values are used when not specified"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        # Check default values are passed
        assert 'auth.dbpedia.org' in call_args[1]['auth_url']
        assert call_args[1]['client_id'] == 'vault-token-exchange'

    @patch('databusclient.client.download')
    def test_download_command_with_query(self, mock_download):
        """Test download command with SPARQL query"""
        mock_download.return_value = None

        query = 'PREFIX dcat: <http://www.w3.org/ns/dcat#> SELECT ?x WHERE { ?sub dcat:downloadURL ?x . } LIMIT 10'
        result = self.runner.invoke(app, [
            'download',
            '--databus', 'https://databus.dbpedia.org/sparql',
            query
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert query in call_args[1]['databusURIs']

    def test_download_command_missing_uri(self):
        """Test download command fails without URI"""
        result = self.runner.invoke(app, ['download'])

        assert result.exit_code != 0

    @patch('databusclient.client.download')
    def test_download_command_all_options_combined(self, mock_download):
        """Test download command with all options specified"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            '--localdir', '/custom/path',
            '--databus', 'https://custom.databus.org/sparql',
            '--token', 'vault-token.dat',
            '--authurl', 'https://custom-auth.example.com/token',
            '--clientid', 'custom-client',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['localDir'] == '/custom/path'
        assert call_args[1]['endpoint'] == 'https://custom.databus.org/sparql'
        assert call_args[1]['token'] == 'vault-token.dat'
        assert call_args[1]['auth_url'] == 'https://custom-auth.example.com/token'
        assert call_args[1]['client_id'] == 'custom-client'


class TestCLIHelp:
    """Test suite for CLI help messages"""

    def setup_method(self):
        """Setup test runner"""
        self.runner = CliRunner()

    def test_main_help(self):
        """Test main CLI help message"""
        result = self.runner.invoke(app, ['--help'])

        assert result.exit_code == 0
        assert 'Databus Client CLI' in result.output or 'Commands' in result.output
        assert 'deploy' in result.output
        assert 'download' in result.output

    def test_deploy_help(self):
        """Test deploy command help message"""
        result = self.runner.invoke(app, ['deploy', '--help'])

        assert result.exit_code == 0
        assert 'versionid' in result.output or 'version' in result.output.lower()
        assert 'title' in result.output.lower()
        assert 'abstract' in result.output.lower()
        assert 'description' in result.output.lower()
        assert 'license' in result.output.lower()
        assert 'apikey' in result.output.lower()

    def test_download_help(self):
        """Test download command help message"""
        result = self.runner.invoke(app, ['download', '--help'])

        assert result.exit_code == 0
        assert 'databusuris' in result.output.lower()
        assert 'localdir' in result.output.lower()
        assert 'databus' in result.output.lower()
        assert 'token' in result.output.lower()
        assert 'vault' in result.output.lower() or 'authentication' in result.output.lower()


class TestCLIEdgeCases:
    """Test suite for CLI edge cases and error handling"""

    def setup_method(self):
        """Setup test runner"""
        self.runner = CliRunner()

    @patch('databusclient.client.create_dataset')
    @patch('databusclient.client.deploy')
    def test_deploy_with_special_characters(self, mock_deploy_func, mock_create_dataset):
        """Test deploy command handles special characters in arguments"""
        mock_create_dataset.return_value = {"test": "dataid"}
        mock_deploy_func.return_value = None

        result = self.runner.invoke(app, [
            'deploy',
            '--versionid', 'https://databus.dbpedia.org/user/group/artifact/1.0.0',
            '--title', 'Test Dataset with "quotes" and \'apostrophes\'',
            '--abstract', 'Abstract with special chars: & < > "',
            '--description', 'Description with unicode: 世界',
            '--license', 'https://example.com/license',
            '--apikey', 'test_api_key',
            'https://example.com/file.ttl'
        ])

        assert result.exit_code == 0

    @patch('databusclient.client.download')
    def test_download_with_spaces_in_path(self, mock_download):
        """Test download command handles spaces in paths"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            '--localdir', '/path with spaces/data',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['localDir'] == '/path with spaces/data'

    @patch('databusclient.client.download')
    def test_download_with_unicode_uri(self, mock_download):
        """Test download command handles unicode in URIs"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file_世界.ttl'
        ])

        # Should handle unicode in URIs
        assert result.exit_code == 0 or 'error' in result.output.lower()

    @patch('databusclient.client.create_dataset', side_effect=Exception("Deploy failed"))
    def test_deploy_handles_exceptions(self, mock_create_dataset):
        """Test deploy command handles exceptions gracefully"""
        result = self.runner.invoke(app, [
            'deploy',
            '--versionid', 'https://databus.dbpedia.org/user/group/artifact/1.0.0',
            '--title', 'Test Dataset',
            '--abstract', 'Test abstract',
            '--description', 'Test description',
            '--license', 'https://example.com/license',
            '--apikey', 'test_api_key',
            'https://example.com/file.ttl'
        ])

        assert result.exit_code != 0

    @patch('databusclient.client.download', side_effect=Exception("Download failed"))
    def test_download_handles_exceptions(self, mock_download):
        """Test download command handles exceptions gracefully"""
        result = self.runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01/file.ttl'
        ])

        assert result.exit_code != 0


class TestCLIIntegration:
    """Integration tests for CLI commands"""

    def setup_method(self):
        """Setup test runner"""
        self.runner = CliRunner()

    @patch('databusclient.client.download')
    def test_download_collection_via_cli(self, mock_download):
        """Test downloading a collection via CLI"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/dbpedia/collections/test-collection'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert 'collections' in call_args[1]['databusURIs'][0]

    @patch('databusclient.client.download')
    def test_download_version_via_cli(self, mock_download):
        """Test downloading a version via CLI"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/2022.12.01'
        ])

        assert result.exit_code == 0

    @patch('databusclient.client.download')
    def test_download_artifact_via_cli(self, mock_download):
        """Test downloading an artifact (latest version) via CLI"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact'
        ])

        assert result.exit_code == 0

    @patch('databusclient.client.download')
    def test_download_with_vault_full_workflow(self, mock_download):
        """Test complete vault authentication workflow via CLI"""
        mock_download.return_value = None

        result = self.runner.invoke(app, [
            'download',
            '--token', 'vault-token.dat',
            '--authurl', 'https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token',
            '--clientid', 'vault-token-exchange',
            'https://databus.dbpedia.org/dbpedia-enterprise/live-fusion/fusion/2025-08-23/file.ttl.gz'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['token'] == 'vault-token.dat'
        assert 'auth.dbpedia.org' in call_args[1]['auth_url']