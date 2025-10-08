"""CLI Tests for databusclient"""
import pytest
from click.testing import CliRunner
from databusclient.cli import app, deploy, download
from unittest.mock import patch, MagicMock


@pytest.fixture
def runner():
    """Create a Click CLI test runner"""
    return CliRunner()


# ============================================================================
# Deploy Command Tests
# ============================================================================


def test_deploy_command_basic(runner):
    """Test deploy command with all required options"""
    with patch('databusclient.client.create_dataset') as mock_create:
        with patch('databusclient.client.deploy') as mock_deploy:
            mock_create.return_value = {"@context": "test"}

            result = runner.invoke(app, [
                'deploy',
                '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
                '--title', 'Test Dataset',
                '--abstract', 'Test abstract',
                '--description', 'Test description',
                '--license', 'https://license.url',
                '--apikey', 'test-api-key',
                'https://example.com/file1.ttl|type=data',
                'https://example.com/file2.ttl|type=metadata'
            ])

            assert result.exit_code == 0
            assert 'Deploying dataset version' in result.output
            mock_create.assert_called_once()
            mock_deploy.assert_called_once()


def test_deploy_command_missing_required_option(runner):
    """Test deploy command fails when required option is missing"""
    result = runner.invoke(app, [
        'deploy',
        '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
        '--title', 'Test Dataset',
        # Missing --abstract
        '--description', 'Test description',
        '--license', 'https://license.url',
        '--apikey', 'test-api-key',
        'https://example.com/file1.ttl'
    ])

    assert result.exit_code != 0
    assert 'abstract' in result.output.lower() or 'Missing option' in result.output


def test_deploy_command_no_distributions(runner):
    """Test deploy command fails when no distributions provided"""
    result = runner.invoke(app, [
        'deploy',
        '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
        '--title', 'Test Dataset',
        '--abstract', 'Test abstract',
        '--description', 'Test description',
        '--license', 'https://license.url',
        '--apikey', 'test-api-key'
    ])

    assert result.exit_code != 0


def test_deploy_command_single_distribution(runner):
    """Test deploy command with single distribution"""
    with patch('databusclient.client.create_dataset') as mock_create:
        with patch('databusclient.client.deploy') as mock_deploy:
            mock_create.return_value = {"@context": "test"}

            result = runner.invoke(app, [
                'deploy',
                '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
                '--title', 'Test Dataset',
                '--abstract', 'Test abstract',
                '--description', 'Test description',
                '--license', 'https://license.url',
                '--apikey', 'test-api-key',
                'https://example.com/file1.ttl'
            ])

            assert result.exit_code == 0
            # Verify create_dataset was called with one distribution
            call_args = mock_create.call_args
            assert len(call_args[0][5]) == 1  # distributions is 6th positional arg (index 5)


def test_deploy_command_multiple_distributions(runner):
    """Test deploy command with multiple distributions"""
    with patch('databusclient.client.create_dataset') as mock_create:
        with patch('databusclient.client.deploy') as mock_deploy:
            mock_create.return_value = {"@context": "test"}

            result = runner.invoke(app, [
                'deploy',
                '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
                '--title', 'Test Dataset',
                '--abstract', 'Test abstract',
                '--description', 'Test description',
                '--license', 'https://license.url',
                '--apikey', 'test-api-key',
                'https://example.com/file1.ttl|type=data',
                'https://example.com/file2.ttl|type=metadata',
                'https://example.com/file3.ttl|type=ontology'
            ])

            assert result.exit_code == 0
            call_args = mock_create.call_args
            assert len(call_args[0][5]) == 3


def test_deploy_command_passes_correct_parameters(runner):
    """Test that deploy command passes correct parameters to client functions"""
    with patch('databusclient.client.create_dataset') as mock_create:
        with patch('databusclient.client.deploy') as mock_deploy:
            mock_dataid = {"@context": "test", "@graph": []}
            mock_create.return_value = mock_dataid

            version_id = 'https://databus.dbpedia.org/account/group/artifact/1.0.0'
            title = 'Test Dataset'
            abstract = 'Test abstract'
            description = 'Test description'
            license_uri = 'https://license.url'
            apikey = 'test-api-key'

            runner.invoke(app, [
                'deploy',
                '--versionid', version_id,
                '--title', title,
                '--abstract', abstract,
                '--description', description,
                '--license', license_uri,
                '--apikey', apikey,
                'https://example.com/file1.ttl'
            ])

            # Verify create_dataset was called with correct args
            mock_create.assert_called_once_with(
                version_id, title, abstract, description, license_uri,
                ('https://example.com/file1.ttl',)
            )

            # Verify deploy was called with correct args
            mock_deploy.assert_called_once_with(dataid=mock_dataid, api_key=apikey)


# ============================================================================
# Download Command Tests
# ============================================================================


def test_download_command_basic_uri(runner):
    """Test download command with basic databus URI"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/1.0.0'
        ])

        assert result.exit_code == 0
        mock_download.assert_called_once()
        call_args = mock_download.call_args
        assert 'https://databus.dbpedia.org/account/group/artifact/1.0.0' in call_args[1]['databusURIs']


def test_download_command_with_localdir(runner):
    """Test download command with custom local directory"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            '--localdir', '/tmp/test-download',
            'https://databus.dbpedia.org/account/group/artifact/1.0.0'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['localDir'] == '/tmp/test-download'


def test_download_command_with_databus_endpoint(runner):
    """Test download command with custom databus endpoint"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            '--databus', 'https://custom.databus.org/sparql',
            'https://databus.dbpedia.org/account/group/artifact/1.0.0'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['endpoint'] == 'https://custom.databus.org/sparql'


def test_download_command_with_vault_token(runner):
    """Test download command with vault token file"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            '--token', '/path/to/token.txt',
            'https://data.dbpedia.io/protected/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['token'] == '/path/to/token.txt'


def test_download_command_with_all_vault_options(runner):
    """Test download command with all vault authentication options"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            '--token', '/path/to/token.txt',
            '--authurl', 'https://custom-auth.example.com/token',
            '--clientid', 'custom-client-id',
            'https://data.dbpedia.io/protected/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['token'] == '/path/to/token.txt'
        assert call_args[1]['auth_url'] == 'https://custom-auth.example.com/token'
        assert call_args[1]['client_id'] == 'custom-client-id'


def test_download_command_default_auth_values(runner):
    """Test that download command uses default values for auth URL and client ID"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/1.0.0'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        # Default values should be passed
        assert call_args[1]['auth_url'] == 'https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token'
        assert call_args[1]['client_id'] == 'vault-token-exchange'


def test_download_command_multiple_uris(runner):
    """Test download command with multiple databus URIs"""
    with patch('databusclient.client.download') as mock_download:
        uri1 = 'https://databus.dbpedia.org/account/group/artifact1/1.0.0'
        uri2 = 'https://databus.dbpedia.org/account/group/artifact2/1.0.0'
        uri3 = 'https://databus.dbpedia.org/account/group/artifact3/1.0.0'

        result = runner.invoke(app, [
            'download',
            uri1,
            uri2,
            uri3
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        databusURIs = call_args[1]['databusURIs']
        assert len(databusURIs) == 3
        assert uri1 in databusURIs
        assert uri2 in databusURIs
        assert uri3 in databusURIs


def test_download_command_with_query(runner):
    """Test download command with SPARQL query as argument"""
    with patch('databusclient.client.download') as mock_download:
        query = 'SELECT ?file WHERE { ?s <http://www.w3.org/ns/dcat#downloadURL> ?file } LIMIT 10'

        result = runner.invoke(app, [
            'download',
            '--databus', 'https://databus.dbpedia.org/sparql',
            query
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert query in call_args[1]['databusURIs']


def test_download_command_no_uris_provided(runner):
    """Test that download command fails when no URIs provided"""
    result = runner.invoke(app, ['download'])

    assert result.exit_code != 0
    assert 'databusuris' in result.output.lower() or 'Missing argument' in result.output


def test_download_command_with_collection(runner):
    """Test download command with databus collection URI"""
    with patch('databusclient.client.download') as mock_download:
        collection_uri = 'https://databus.dbpedia.org/dbpedia/collections/dbpedia-snapshot-2023-06'

        result = runner.invoke(app, [
            'download',
            collection_uri
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert collection_uri in call_args[1]['databusURIs']


def test_download_command_mixed_uri_types(runner):
    """Test download command with mixed URI types (artifact, collection, file)"""
    with patch('databusclient.client.download') as mock_download:
        artifact_uri = 'https://databus.dbpedia.org/account/group/artifact/1.0.0'
        collection_uri = 'https://databus.dbpedia.org/dbpedia/collections/test-collection'
        file_uri = 'https://databus.dbpedia.org/account/group/artifact/1.0.0/file.ttl'

        result = runner.invoke(app, [
            'download',
            artifact_uri,
            collection_uri,
            file_uri
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        databusURIs = call_args[1]['databusURIs']
        assert artifact_uri in databusURIs
        assert collection_uri in databusURIs
        assert file_uri in databusURIs


def test_download_command_with_all_options(runner):
    """Test download command with all available options"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            '--localdir', '/tmp/downloads',
            '--databus', 'https://custom.databus.org/sparql',
            '--token', '/path/to/token.txt',
            '--authurl', 'https://custom-auth.example.com/token',
            '--clientid', 'custom-client-id',
            'https://databus.dbpedia.org/account/group/artifact/1.0.0',
            'https://databus.dbpedia.org/account/group/artifact2/2.0.0'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        assert call_args[1]['localDir'] == '/tmp/downloads'
        assert call_args[1]['endpoint'] == 'https://custom.databus.org/sparql'
        assert call_args[1]['token'] == '/path/to/token.txt'
        assert call_args[1]['auth_url'] == 'https://custom-auth.example.com/token'
        assert call_args[1]['client_id'] == 'custom-client-id'
        assert len(call_args[1]['databusURIs']) == 2


# ============================================================================
# CLI App Structure Tests
# ============================================================================


def test_app_has_commands(runner):
    """Test that the CLI app has the expected commands"""
    result = runner.invoke(app, ['--help'])

    assert result.exit_code == 0
    assert 'deploy' in result.output
    assert 'download' in result.output


def test_deploy_command_help(runner):
    """Test deploy command help text"""
    result = runner.invoke(app, ['deploy', '--help'])

    assert result.exit_code == 0
    assert 'versionid' in result.output.lower()
    assert 'title' in result.output.lower()
    assert 'abstract' in result.output.lower()
    assert 'description' in result.output.lower()
    assert 'license' in result.output.lower()
    assert 'apikey' in result.output.lower()


def test_download_command_help(runner):
    """Test download command help text"""
    result = runner.invoke(app, ['download', '--help'])

    assert result.exit_code == 0
    assert 'localdir' in result.output.lower()
    assert 'databus' in result.output.lower()
    assert 'token' in result.output.lower()
    assert 'authurl' in result.output.lower()
    assert 'clientid' in result.output.lower()


def test_app_help_shows_description(runner):
    """Test that app help shows the CLI description"""
    result = runner.invoke(app, ['--help'])

    assert result.exit_code == 0
    assert 'Databus Client CLI' in result.output


# ============================================================================
# Parameter Validation Tests
# ============================================================================


def test_deploy_command_with_special_characters_in_params(runner):
    """Test deploy command handles special characters in parameters"""
    with patch('databusclient.client.create_dataset') as mock_create:
        with patch('databusclient.client.deploy') as mock_deploy:
            mock_create.return_value = {"@context": "test"}

            result = runner.invoke(app, [
                'deploy',
                '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
                '--title', 'Test & Dataset <with> "special" chars',
                '--abstract', 'Abstract with special chars: @#$%',
                '--description', 'Description with\nnewlines\nand\ttabs',
                '--license', 'https://license.url',
                '--apikey', 'test-api-key',
                'https://example.com/file1.ttl'
            ])

            assert result.exit_code == 0


def test_download_command_with_empty_optional_params(runner):
    """Test download command when optional parameters are not provided"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/1.0.0'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        # localDir and endpoint should be None when not provided
        assert call_args[1]['localDir'] is None
        assert call_args[1]['endpoint'] is None


def test_download_command_token_without_auth_params(runner):
    """Test that token can be provided without auth URL and client ID (uses defaults)"""
    with patch('databusclient.client.download') as mock_download:
        result = runner.invoke(app, [
            'download',
            '--token', '/path/to/token.txt',
            'https://data.dbpedia.io/protected/file.ttl'
        ])

        assert result.exit_code == 0
        call_args = mock_download.call_args
        # Should use default values
        assert call_args[1]['auth_url'] is not None
        assert call_args[1]['client_id'] is not None


# ============================================================================
# Error Handling Tests
# ============================================================================


def test_deploy_command_handles_client_exception(runner):
    """Test that deploy command handles exceptions from client"""
    with patch('databusclient.client.create_dataset') as mock_create:
        mock_create.side_effect = Exception("Test error")

        result = runner.invoke(app, [
            'deploy',
            '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
            '--title', 'Test Dataset',
            '--abstract', 'Test abstract',
            '--description', 'Test description',
            '--license', 'https://license.url',
            '--apikey', 'test-api-key',
            'https://example.com/file1.ttl'
        ])

        assert result.exit_code != 0


def test_download_command_handles_client_exception(runner):
    """Test that download command handles exceptions from client"""
    with patch('databusclient.client.download') as mock_download:
        mock_download.side_effect = Exception("Test download error")

        result = runner.invoke(app, [
            'download',
            'https://databus.dbpedia.org/account/group/artifact/1.0.0'
        ])

        assert result.exit_code != 0


# ============================================================================
# Integration-like Tests
# ============================================================================


def test_deploy_then_download_workflow(runner):
    """Test a workflow where we deploy then download"""
    with patch('databusclient.client.create_dataset') as mock_create:
        with patch('databusclient.client.deploy') as mock_deploy:
            with patch('databusclient.client.download') as mock_download:
                mock_create.return_value = {"@context": "test"}

                # First deploy
                deploy_result = runner.invoke(app, [
                    'deploy',
                    '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
                    '--title', 'Test Dataset',
                    '--abstract', 'Test abstract',
                    '--description', 'Test description',
                    '--license', 'https://license.url',
                    '--apikey', 'test-api-key',
                    'https://example.com/file1.ttl'
                ])

                assert deploy_result.exit_code == 0

                # Then download the same artifact
                download_result = runner.invoke(app, [
                    'download',
                    'https://databus.dbpedia.org/account/group/artifact/1.0.0'
                ])

                assert download_result.exit_code == 0


def test_command_isolation(runner):
    """Test that deploy and download commands are independent"""
    with patch('databusclient.client.create_dataset') as mock_create:
        with patch('databusclient.client.deploy') as mock_deploy:
            with patch('databusclient.client.download') as mock_download:
                mock_create.return_value = {"@context": "test"}

                # Run deploy
                runner.invoke(app, [
                    'deploy',
                    '--versionid', 'https://databus.dbpedia.org/account/group/artifact/1.0.0',
                    '--title', 'Test Dataset',
                    '--abstract', 'Test abstract',
                    '--description', 'Test description',
                    '--license', 'https://license.url',
                    '--apikey', 'test-api-key',
                    'https://example.com/file1.ttl'
                ])

                # Run download
                runner.invoke(app, [
                    'download',
                    'https://databus.dbpedia.org/account/group/artifact/1.0.0'
                ])

                # Verify both were called independently
                assert mock_create.called
                assert mock_deploy.called
                assert mock_download.called