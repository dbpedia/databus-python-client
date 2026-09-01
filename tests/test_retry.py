"""Tests for HTTP retry session helper."""

import requests
from requests.adapters import HTTPAdapter

from databusclient.api.utils import get_http_session, fetch_databus_jsonld


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
    result = fetch_databus_jsonld("https://databus.dbpedia.org/test", databus_key="test_key", session=session, timeout=15)
    assert result == '{"@context": "https://databus.dbpedia.org/context.jsonld"}'
