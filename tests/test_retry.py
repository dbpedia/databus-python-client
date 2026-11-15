"""Tests for HTTP retry session helper and CLI options."""

import requests
from click.testing import CliRunner
from requests.adapters import HTTPAdapter

from databusclient.api.utils import get_http_session, fetch_databus_jsonld
from databusclient.cli import app


def test_get_http_session_defaults():
    session = get_http_session()
    assert isinstance(session, requests.Session)

    http_adapter = session.adapters.get("http://")
    https_adapter = session.adapters.get("https://")

    assert isinstance(http_adapter, HTTPAdapter)
    assert isinstance(https_adapter, HTTPAdapter)
    assert http_adapter.max_retries.total == 3


def test_get_http_session_custom_retries():
    session = get_http_session(retries=5, backoff_factor=1.0)
    http_adapter = session.adapters.get("https://")

    assert http_adapter.max_retries.total == 5
    assert http_adapter.max_retries.backoff_factor == 1.0


def test_get_http_session_with_jitter():
    session = get_http_session(retries=4, backoff_factor=0.5, backoff_jitter=0.2)
    http_adapter = session.adapters.get("https://")
    assert http_adapter.max_retries.total == 4
    if hasattr(http_adapter.max_retries, "backoff_jitter"):
        assert http_adapter.max_retries.backoff_jitter == 0.2


def test_fetch_databus_jsonld_with_custom_session():
    class MockResponse:
        status_code = 200
        text = '{"@context": "https://databus.dbpedia.org/context.jsonld"}'

        def raise_for_status(self):
            pass

    class MockSession(requests.Session):
        def get(self, url, headers=None, timeout=None):
            assert timeout == 15
            assert headers["Accept"] == "application/ld+json"
            assert headers["X-API-KEY"] == "test_key"
            return MockResponse()

    session = MockSession()
    result = fetch_databus_jsonld(
        "https://databus.dbpedia.org/test",
        databus_key="test_key",
        session=session,
        timeout=15,
    )
    assert result == '{"@context": "https://databus.dbpedia.org/context.jsonld"}'


def test_cli_delete_force_and_retry_options(monkeypatch):
    captured = {}

    def fake_api_delete(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.cli.api_delete", fake_api_delete)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "delete",
            "https://databus.dbpedia.org/user/group/artifact/version",
            "--databus-key",
            "test-key",
            "--force",
            "--retries",
            "5",
            "--request-timeout",
            "45",
        ],
    )

    assert result.exit_code == 0
    assert captured["force"] is True
    assert captured["timeout"] == 45
    assert captured["session"].adapters["https://"].max_retries.total == 5


def test_cli_delete_max_retries_alias(monkeypatch):
    captured = {}

    def fake_api_delete(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("databusclient.cli.api_delete", fake_api_delete)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "delete",
            "https://databus.dbpedia.org/user/group/artifact/version",
            "--databus-key",
            "test-key",
            "--force",
            "--max-retries",
            "7",
        ],
    )

    assert result.exit_code == 0
    assert captured["session"].adapters["https://"].max_retries.total == 7
