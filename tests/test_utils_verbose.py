from unittest.mock import Mock, patch

import databusclient.api.utils as utils
import databusclient.api.download as dl

import requests
import logging




def make_response(status=200, headers=None, text=''):
    headers = headers or {}
    mock = Mock()
    mock.status_code = status
    mock.headers = headers
    mock.text = text
    def raise_for_status():
        if mock.status_code >= 400:
            raise requests.exceptions.HTTPError()
    mock.raise_for_status = raise_for_status
    return mock


def test_fetch_databus_jsonld_verbose_redacts_api_key(caplog):
    caplog.set_level(logging.DEBUG, logger='databusclient')
    url = "https://databus.example/resource"
    resp = make_response(status=200, headers={"content-type": "application/ld+json"}, text='{}')
    with patch("databusclient.api.utils.requests.get", return_value=resp):
        txt = utils.fetch_databus_jsonld(url, databus_key="SECRET", verbose=True)
        assert "[HTTP] GET" in caplog.text
        assert "REDACTED" in caplog.text
        assert "SECRET" not in caplog.text
        assert txt == '{}'



def test_get_sparql_query_of_collection_verbose(caplog):
    caplog.set_level(logging.DEBUG, logger='databusclient')
    url = "https://databus.example/collections/col"
    resp = make_response(status=200, headers={"content-type": "text/sparql"}, text='SELECT *')
    with patch("databusclient.api.download.requests.get", return_value=resp):
        txt = dl._get_sparql_query_of_collection(url, databus_key="SECRET", verbose=True)
        assert "[HTTP] GET" in caplog.text
        assert "REDACTED" in caplog.text
        assert "SECRET" not in caplog.text
        assert txt == 'SELECT *'



def test_query_sparql_endpoint_verbose(caplog):
    caplog.set_level(logging.DEBUG, logger='databusclient')
    endpoint = "https://dbpedia.org/sparql"
    sample = {"results": {"bindings": []}}
    class MockSPARQL:
        def __init__(self, url):
            self.url = url
            self.method = None
            self._query = None
            self._headers = None
        def setQuery(self, q):
            self._query = q
        def setReturnFormat(self, fmt):
            pass
        def setCustomHttpHeaders(self, headers):
            self._headers = headers
        def query(self):
            mock = Mock()
            mock.convert.return_value = sample
            return mock
    with patch("databusclient.api.download.SPARQLWrapper", new=MockSPARQL):
        res = dl._query_sparql_endpoint(endpoint, "SELECT ?s WHERE { ?s ?p ?o }", databus_key="SECRET", verbose=True)
        assert "[HTTP] POST" in caplog.text
        assert "REDACTED" in caplog.text
        assert "SECRET" not in caplog.text
        assert res == sample
