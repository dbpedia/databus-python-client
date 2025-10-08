"""CLI tests for databusclient"""
import pytest
from click.testing import CliRunner
from unittest.mock import patch, Mock
from databusclient.cli import app, deploy, download


class TestDeployCommand:
    """Tests for the deploy command"""

    def test_deploy_command_success(self):
        """Test successful deploy command execution"""
        runner = CliRunner()
        
        with patch("databusclient.client.create_dataset") as mock_create:
            with patch("databusclient.client.deploy") as mock_deploy:
                mock_dataid = {"@graph": [{"@type": "Dataset"}]}
                mock_create.return_value = mock_dataid
                
                result = runner.invoke(app, [
                    "deploy",
                    "--versionid", "https://databus.dbpedia.org/test/group/artifact/1.0.0",
                    "--title", "Test Dataset",
                    "--abstract", "Test abstract",
                    "--description", "Test description",
                    "--license", "https://license.example.com/",
                    "--apikey", "test-api-key",
                    "https://example.com/file1.txt|type=test|json|none|sha256:1000",
                    "https://example.com/file2.txt|type=test|csv|gz|sha256:2000"
                ])
                
                assert result.exit_code == 0
                assert "Deploying dataset version" in result.output
                mock_create.assert_called_once()
                mock_deploy.assert_called_once()

    def test_deploy_command_missing_required_options(self):
        """Test deploy command fails without required options"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["deploy"])
        
        assert result.exit_code != 0
        assert "Missing option" in result.output or "Error" in result.output

    def test_deploy_command_with_single_distribution(self):
        """Test deploy command with single distribution"""
        runner = CliRunner()
        
        with patch("databusclient.client.create_dataset") as mock_create:
            with patch("databusclient.client.deploy") as mock_deploy:
                mock_create.return_value = {"@graph": []}
                
                result = runner.invoke(app, [
                    "deploy",
                    "--versionid", "https://databus.dbpedia.org/test/group/artifact/1.0.0",
                    "--title", "Test Dataset",
                    "--abstract", "Test abstract",
                    "--description", "Test description",
                    "--license", "https://license.example.com/",
                    "--apikey", "test-api-key",
                    "https://example.com/file.txt"
                ])
                
                assert result.exit_code == 0
                # Verify create_dataset was called with one distribution
                call_args = mock_create.call_args
                distributions = call_args[0][5]
                assert len(distributions) == 1

    def test_deploy_command_version_id_format(self):
        """Test deploy command validates version ID format"""
        runner = CliRunner()
        
        with patch("databusclient.client.create_dataset") as mock_create:
            with patch("databusclient.client.deploy") as mock_deploy:
                mock_create.return_value = {"@graph": []}
                
                result = runner.invoke(app, [
                    "deploy",
                    "--versionid", "https://databus.dbpedia.org/account/group/artifact/2023.01.01",
                    "--title", "Test",
                    "--abstract", "Abstract",
                    "--description", "Description",
                    "--license", "https://license.url/",
                    "--apikey", "key123",
                    "https://example.com/file.txt"
                ])
                
                # Should accept valid version ID format
                assert result.exit_code == 0


class TestDownloadCommand:
    """Tests for the download command"""

    def test_download_command_with_uri(self):
        """Test download command with databus URI"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "https://databus.dbpedia.org/account/group/artifact/version"
            ])
            
            assert result.exit_code == 0
            mock_download.assert_called_once()
            call_kwargs = mock_download.call_args[1]
            assert call_kwargs["databusURIs"] == ("https://databus.dbpedia.org/account/group/artifact/version",)

    def test_download_command_with_multiple_uris(self):
        """Test download command with multiple URIs"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "https://databus.dbpedia.org/account/group/artifact1",
                "https://databus.dbpedia.org/account/group/artifact2"
            ])
            
            assert result.exit_code == 0
            call_kwargs = mock_download.call_args[1]
            assert len(call_kwargs["databusURIs"]) == 2

    def test_download_command_with_localdir(self):
        """Test download command with local directory option"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "--localdir", "/tmp/custom-dir",
                "https://databus.dbpedia.org/account/group/artifact"
            ])
            
            assert result.exit_code == 0
            call_kwargs = mock_download.call_args[1]
            assert call_kwargs["localDir"] == "/tmp/custom-dir"

    def test_download_command_with_databus_endpoint(self):
        """Test download command with custom databus endpoint"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "--databus", "https://custom.databus.org/sparql",
                "https://databus.dbpedia.org/account/group/artifact"
            ])
            
            assert result.exit_code == 0
            call_kwargs = mock_download.call_args[1]
            assert call_kwargs["endpoint"] == "https://custom.databus.org/sparql"

    def test_download_command_with_vault_options(self):
        """Test download command with vault authentication options"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "--token", "vault_token.txt",
                "--authurl", "https://auth.example.com/token",
                "--clientid", "test-client",
                "https://databus.dbpedia.org/account/group/artifact"
            ])
            
            assert result.exit_code == 0
            call_kwargs = mock_download.call_args[1]
            assert call_kwargs["token"] == "vault_token.txt"
            assert call_kwargs["auth_url"] == "https://auth.example.com/token"
            assert call_kwargs["client_id"] == "test-client"

    def test_download_command_with_default_authurl(self):
        """Test download command uses default auth URL"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "https://databus.dbpedia.org/account/group/artifact"
            ])
            
            assert result.exit_code == 0
            call_kwargs = mock_download.call_args[1]
            # Default authurl should be used
            assert call_kwargs["auth_url"] == "https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token"

    def test_download_command_with_default_clientid(self):
        """Test download command uses default client ID"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "https://databus.dbpedia.org/account/group/artifact"
            ])
            
            assert result.exit_code == 0
            call_kwargs = mock_download.call_args[1]
            # Default clientid should be used
            assert call_kwargs["client_id"] == "vault-token-exchange"

    def test_download_command_with_sparql_query(self):
        """Test download command with SPARQL query"""
        runner = CliRunner()
        query = "SELECT ?file WHERE { ?s <downloadURL> ?file } LIMIT 10"
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "--databus", "https://databus.dbpedia.org/sparql",
                query
            ])
            
            assert result.exit_code == 0
            call_kwargs = mock_download.call_args[1]
            assert query in call_kwargs["databusURIs"]

    def test_download_command_missing_required_argument(self):
        """Test download command fails without URI argument"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["download"])
        
        assert result.exit_code != 0
        assert "Missing argument" in result.output or "Error" in result.output

    def test_download_command_with_collection_uri(self):
        """Test download command with collection URI"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "https://databus.dbpedia.org/account/collections/test-collection"
            ])
            
            assert result.exit_code == 0
            mock_download.assert_called_once()


class TestCLIIntegration:
    """Integration tests for CLI commands"""

    def test_app_has_both_commands(self):
        """Test that app has both deploy and download commands"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["--help"])
        
        assert result.exit_code == 0
        assert "deploy" in result.output
        assert "download" in result.output

    def test_deploy_help_text(self):
        """Test deploy command help text"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["deploy", "--help"])
        
        assert result.exit_code == 0
        assert "Deploy a dataset version" in result.output
        assert "--versionid" in result.output
        assert "--title" in result.output
        assert "--abstract" in result.output
        assert "--description" in result.output
        assert "--license" in result.output
        assert "--apikey" in result.output

    def test_download_help_text(self):
        """Test download command help text"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["download", "--help"])
        
        assert result.exit_code == 0
        assert "Download datasets from databus" in result.output
        assert "--localdir" in result.output
        assert "--databus" in result.output
        assert "--token" in result.output
        assert "--authurl" in result.output
        assert "--clientid" in result.output

    def test_cli_group_docstring(self):
        """Test CLI app has proper docstring"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["--help"])
        
        assert result.exit_code == 0
        assert "Databus Client CLI" in result.output


class TestClickMigration:
    """Tests for migration from typer to click"""

    def test_deploy_uses_click_options(self):
        """Test that deploy command uses click options instead of typer"""
        runner = CliRunner()
        
        # Test that option names follow click convention (lowercase, dashes)
        result = runner.invoke(app, ["deploy", "--help"])
        
        assert result.exit_code == 0
        # Should have click-style options
        assert "--versionid" in result.output
        assert "--apikey" in result.output

    def test_download_uses_click_options(self):
        """Test that download command uses click options instead of typer"""
        runner = CliRunner()
        
        result = runner.invoke(app, ["download", "--help"])
        
        assert result.exit_code == 0
        # Should have click-style options
        assert "--localdir" in result.output
        assert "--databus" in result.output

    def test_app_is_click_group(self):
        """Test that app is a click Group"""
        from click import Group
        assert isinstance(app, Group)


class TestErrorHandling:
    """Tests for error handling in CLI"""

    def test_deploy_handles_client_error(self):
        """Test deploy command handles client errors gracefully"""
        runner = CliRunner()
        
        with patch("databusclient.client.create_dataset") as mock_create:
            with patch("databusclient.client.deploy") as mock_deploy:
                mock_create.return_value = {"@graph": []}
                mock_deploy.side_effect = Exception("Deploy failed")
                
                result = runner.invoke(app, [
                    "deploy",
                    "--versionid", "https://databus.dbpedia.org/test/group/artifact/1.0.0",
                    "--title", "Test",
                    "--abstract", "Abstract",
                    "--description", "Description",
                    "--license", "https://license.url/",
                    "--apikey", "key",
                    "https://example.com/file.txt"
                ])
                
                assert result.exit_code != 0
                assert "Deploy failed" in str(result.exception) or result.exception is not None

    def test_download_handles_client_error(self):
        """Test download command handles client errors gracefully"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            mock_download.side_effect = ValueError("Invalid URI")
            
            result = runner.invoke(app, [
                "download",
                "invalid-uri"
            ])
            
            assert result.exit_code != 0


class TestParameterPassing:
    """Tests for correct parameter passing between CLI and client"""

    def test_deploy_passes_all_parameters(self):
        """Test that deploy command passes all parameters correctly"""
        runner = CliRunner()
        
        with patch("databusclient.client.create_dataset") as mock_create:
            with patch("databusclient.client.deploy") as mock_deploy:
                mock_create.return_value = {"@graph": []}
                
                version_id = "https://databus.dbpedia.org/test/group/artifact/1.0.0"
                title = "Test Dataset"
                abstract = "Test abstract"
                description = "Test description"
                license_uri = "https://license.example.com/"
                dist1 = "https://example.com/file1.txt"
                dist2 = "https://example.com/file2.txt"
                
                result = runner.invoke(app, [
                    "deploy",
                    "--versionid", version_id,
                    "--title", title,
                    "--abstract", abstract,
                    "--description", description,
                    "--license", license_uri,
                    "--apikey", "test-key",
                    dist1,
                    dist2
                ])
                
                assert result.exit_code == 0
                
                # Verify create_dataset called with correct params
                mock_create.assert_called_once()
                args = mock_create.call_args[0]
                assert args[0] == version_id
                assert args[1] == title
                assert args[2] == abstract
                assert args[3] == description
                assert args[4] == license_uri
                assert dist1 in args[5]
                assert dist2 in args[5]

    def test_download_passes_all_parameters(self):
        """Test that download command passes all parameters correctly"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            localdir = "/tmp/test"
            databus = "https://custom.databus.org/sparql"
            token = "token.txt"
            authurl = "https://auth.example.com"
            clientid = "custom-client"
            uri = "https://databus.dbpedia.org/test/group/artifact"
            
            result = runner.invoke(app, [
                "download",
                "--localdir", localdir,
                "--databus", databus,
                "--token", token,
                "--authurl", authurl,
                "--clientid", clientid,
                uri
            ])
            
            assert result.exit_code == 0
            
            # Verify download called with correct params
            mock_download.assert_called_once()
            kwargs = mock_download.call_args[1]
            assert kwargs["localDir"] == localdir
            assert kwargs["endpoint"] == databus
            assert kwargs["token"] == token
            assert kwargs["auth_url"] == authurl
            assert kwargs["client_id"] == clientid
            assert uri in kwargs["databusURIs"]


class TestOptionalParameters:
    """Tests for optional parameters in CLI commands"""

    def test_download_without_optional_params(self):
        """Test download command works without optional parameters"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "https://databus.dbpedia.org/test/group/artifact"
            ])
            
            assert result.exit_code == 0
            kwargs = mock_download.call_args[1]
            # Optional params should be None or defaults
            assert kwargs["localDir"] is None
            assert kwargs["endpoint"] is None
            assert kwargs["token"] is None

    def test_download_with_partial_vault_params(self):
        """Test download command with only some vault parameters"""
        runner = CliRunner()
        
        with patch("databusclient.client.download") as mock_download:
            result = runner.invoke(app, [
                "download",
                "--token", "token.txt",
                "https://databus.dbpedia.org/test/group/artifact"
            ])
            
            assert result.exit_code == 0
            kwargs = mock_download.call_args[1]
            assert kwargs["token"] == "token.txt"
            # Other vault params should have defaults
            assert kwargs["auth_url"] is not None
            assert kwargs["client_id"] is not None