import pytest
import requests
from databusclient.api import utils
from databusclient.api.utils import (
    get_databus_id_parts_from_file_url,
    compute_sha256_and_length,
    fetch_databus_jsonld,
)


def test_split_full_url():
    result = get_databus_id_parts_from_file_url(
        "https://databus.dbpedia.org/dbpedia/mappings/geo-coordinates/2022.12.01/file.ttl"
    )
    assert result == (
        "databus.dbpedia.org",
        "dbpedia",
        "mappings",
        "geo-coordinates",
        "2022.12.01",
        "file.ttl",
    )


def test_split_short_url_fills_none():
    result = get_databus_id_parts_from_file_url(
        "https://databus.dbpedia.org/dbpedia/mappings"
    )
    assert result == (
        "databus.dbpedia.org",
        "dbpedia",
        "mappings",
        None,
        None,
        None,
    )


def test_split_http_same_as_https():
    result = get_databus_id_parts_from_file_url(
        "http://databus.dbpedia.org/dbpedia/mappings/geo-coordinates/2022.12.01/file.ttl"
    )
    assert result == (
        "databus.dbpedia.org",
        "dbpedia",
        "mappings",
        "geo-coordinates",
        "2022.12.01",
        "file.ttl",
    )


def test_split_trailing_slash_ignored():
    result = get_databus_id_parts_from_file_url(
        "https://databus.dbpedia.org/dbpedia/mappings/geo-coordinates/2022.12.01/file.ttl/"
    )
    assert result == (
        "databus.dbpedia.org",
        "dbpedia",
        "mappings",
        "geo-coordinates",
        "2022.12.01",
        "file.ttl",
    )


def test_split_empty_string():
    result = get_databus_id_parts_from_file_url("")
    assert result == (
        "",
        None,
        None,
        None,
        None,
        None,
    )


def test_split_too_long_keeps_first_six():
    result = get_databus_id_parts_from_file_url(
        "https://databus.dbpedia.org/a/b/c/d/e/f/g/h"
    )
    assert result == (
        "databus.dbpedia.org",
        "a",
        "b",
        "c",
        "d",
        "e",
    )


def test_sha256_hello_file(tmp_path):
    f = tmp_path / "hello.txt"
    f.write_text("hello")
    result = compute_sha256_and_length(f)

    assert result == (
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        5,
    )


def test_sha256_empty_file(tmp_path):
    f = tmp_path / "empty.txt"
    f.write_text("")
    result = compute_sha256_and_length(f)

    assert result == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        0,
    )


def test_sha256_missing_file_raises(tmp_path):
    missing = tmp_path / "does_not_exist.txt"
    with pytest.raises(FileNotFoundError):
        compute_sha256_and_length(missing)


class FakeResponse:
    def __init__(self, text="", ok=True):
        self.text = text
        self.ok = ok

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError("404 Not Found")


def test_fetch_sends_accept_header_without_key(monkeypatch):
    calls = {}

    def fake_get(uri, headers, timeout):
        calls["uri"] = uri
        calls["headers"] = headers
        return FakeResponse(text='{"ok": true}')

    monkeypatch.setattr(utils.requests, "get", fake_get)

    result = fetch_databus_jsonld("https://databus.dbpedia.org/dbpedia/mappings")

    assert result == '{"ok": true}'
    assert calls["headers"] == {"Accept": "application/ld+json"}


def test_fetch_adds_api_key_header(monkeypatch):
    calls = {}

    def fake_get(uri, headers, timeout):
        calls["uri"] = uri
        calls["headers"] = headers
        return FakeResponse(text='{"ok": true}')

    monkeypatch.setattr(utils.requests, "get", fake_get)

    result = fetch_databus_jsonld(
        "https://databus.dbpedia.org/dbpedia/mappings", databus_key="secret123"
    )

    assert result == '{"ok": true}'
    assert calls["headers"] == {
        "Accept": "application/ld+json",
        "X-API-KEY": "secret123",
    }


def test_fetch_raises_on_server_error(monkeypatch):
    def fake_get(uri, headers, timeout):
        return FakeResponse(ok=False)

    monkeypatch.setattr(utils.requests, "get", fake_get)

    with pytest.raises(requests.HTTPError):
        fetch_databus_jsonld("https://databus.dbpedia.org/dbpedia/mappings")
