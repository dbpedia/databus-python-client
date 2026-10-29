"""Tests for downloading all datasets of an account from Databus."""

from unittest.mock import MagicMock
from click.testing import CliRunner

from databusclient.api.download import (
    _get_groups_of_account,
    _download_account,
    download as api_download,
)
from databusclient.cli import app


def test_get_groups_of_account_v2(monkeypatch):
    fake_results = {
        "results": {
            "bindings": [
                {"group": {"value": "https://databus.dbpedia.org/myuser/group1"}},
                {"group": {"value": "https://databus.dbpedia.org/myuser/group2"}},
                {"group": {"value": "https://databus.dbpedia.org/otheruser/groupX"}},  # should filter out
            ]
        }
    }

    monkeypatch.setattr(
        "databusclient.api.download._query_sparql_endpoint",
        lambda endpoint, query, databus_key=None: fake_results,
    )

    groups = _get_groups_of_account(
        "https://databus.dbpedia.org/myuser",
        "https://databus.dbpedia.org/sparql",
    )
    assert groups == [
        "https://databus.dbpedia.org/myuser/group1",
        "https://databus.dbpedia.org/myuser/group2",
    ]


def test_get_groups_of_account_legacy_fallback(monkeypatch):
    call_count = 0

    def fake_query(endpoint, query, databus_key=None):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # v2 query returns empty
            return {"results": {"bindings": []}}
        else:
            # legacy query returns result
            return {
                "results": {
                    "bindings": [
                        {"group": {"value": "https://databus.dbpedia.org/legacyuser/group1"}},
                    ]
                }
            }

    monkeypatch.setattr("databusclient.api.download._query_sparql_endpoint", fake_query)

    groups = _get_groups_of_account(
        "https://databus.dbpedia.org/legacyuser",
        "https://databus.dbpedia.org/sparql",
    )
    assert call_count == 2
    assert groups == ["https://databus.dbpedia.org/legacyuser/group1"]


def test_get_groups_of_account_empty_and_error_handling(monkeypatch):
    def fake_query_err(endpoint, query, databus_key=None):
        raise RuntimeError("SPARQL error")

    monkeypatch.setattr("databusclient.api.download._query_sparql_endpoint", fake_query_err)

    groups = _get_groups_of_account(
        "https://databus.dbpedia.org/brokenuser",
        "https://databus.dbpedia.org/sparql",
    )
    assert groups == []


def test_download_account_downloads_each_group(monkeypatch):
    mock_download_group = MagicMock()
    monkeypatch.setattr("databusclient.api.download._download_group", mock_download_group)
    monkeypatch.setattr(
        "databusclient.api.download._get_groups_of_account",
        lambda account, endpoint, databus_key=None: [
            "https://databus.dbpedia.org/testuser/group1",
            "https://databus.dbpedia.org/testuser/group2",
        ],
    )

    _download_account(
        "https://databus.dbpedia.org/testuser",
        endpoint="https://databus.dbpedia.org/sparql",
        localDir="/tmp/test",
        all_versions=True,
    )

    assert mock_download_group.call_count == 2
    mock_download_group.assert_any_call(
        "https://databus.dbpedia.org/testuser/group1",
        "/tmp/test",
        all_versions=True,
        vault_token_file=None,
        databus_key=None,
        auth_url=None,
        client_id=None,
        compression=None,
        convert_format=None,
        graph_name=None,
        base_uri=None,
        graph_mode=None,
        manifest_context=None,
        validate_checksum=False,
    )


def test_download_account_no_groups(monkeypatch):
    mock_download_group = MagicMock()
    monkeypatch.setattr("databusclient.api.download._download_group", mock_download_group)
    monkeypatch.setattr(
        "databusclient.api.download._get_groups_of_account",
        lambda account, endpoint, databus_key=None: [],
    )

    _download_account(
        "https://databus.dbpedia.org/emptyuser",
        endpoint="https://databus.dbpedia.org/sparql",
        localDir="/tmp/test",
    )

    mock_download_group.assert_not_called()


def test_download_dispatch_detects_account(monkeypatch):
    mock_download_account = MagicMock()
    monkeypatch.setattr("databusclient.api.download._download_account", mock_download_account)

    api_download(
        localDir="/tmp/out",
        endpoint="https://databus.dbpedia.org/sparql",
        databusURIs=["https://databus.dbpedia.org/myorg"],
        all_versions=False,
    )

    mock_download_account.assert_called_once_with(
        "https://databus.dbpedia.org/myorg",
        "https://databus.dbpedia.org/sparql",
        "/tmp/out",
        all_versions=False,
        vault_token_file=None,
        databus_key=None,
        auth_url="https://auth.dbpedia.org/realms/dbpedia/protocol/openid-connect/token",
        client_id="vault-token-exchange",
        compression=None,
        convert_format=None,
        graph_name=None,
        base_uri=None,
        graph_mode=None,
        manifest_context=None,
        validate_checksum=False,
    )


def test_cli_download_account(monkeypatch):
    mock_download = MagicMock()
    monkeypatch.setattr("databusclient.cli.api_download", mock_download)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["download", "https://databus.dbpedia.org/myuser", "--localdir", "/data"],
    )

    assert result.exit_code == 0
    mock_download.assert_called_once()
    assert "https://databus.dbpedia.org/myuser" in mock_download.call_args[1]["databusURIs"]
