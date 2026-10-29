"""Offline tests for the checksum helpers behind --validate-checksum."""

import json

import pytest

import databusclient.api.download as dl


VERSION = "https://databus.dbpedia.org/dbpedia/generic/geo-coordinates/2022.12.01"
FILE_A = f"{VERSION}/geo-coordinates_lang=min.ttl.bz2"
FILE_B = f"{VERSION}/geo-coordinates_lang=yo.ttl.bz2"
SHA_A = "45692701126f3a1df25ccb98d1716aebe7ba19dd41797bee3c7ba891d5c27198"
SHA_B = "b" * 64


def part(file_url, **fields):
    # Same shape as a real Part node in a Databus version JSON-LD
    return {
        "@id": file_url.replace(f"{VERSION}/", f"{VERSION}#"),
        "@type": "Part",
        "file": file_url,
        **fields,
    }


def version_jsonld(*nodes):
    return json.dumps({"@graph": [{"@id": VERSION, "@type": "Version"}, *nodes]})


# --- _extract_checksum_from_node ---


@pytest.mark.parametrize("key", ["checksum", "sha256sum", "sha256", "databus:checksum"])
def test_node_plain_string_under_each_allowed_key(key):
    assert dl._extract_checksum_from_node({key: SHA_A}) == SHA_A


@pytest.mark.parametrize(
    "value",
    [
        {"@value": SHA_A},
        [SHA_A],
        ["not-a-checksum", {"@value": SHA_A}],
        {"nested": {"deeper": SHA_A}},
        f"  {SHA_A}\n",
    ],
)
def test_node_value_shapes(value):
    assert dl._extract_checksum_from_node({"sha256sum": value}) == SHA_A


def test_node_keeps_uppercase_hex_as_given():
    assert dl._extract_checksum_from_node({"sha256sum": SHA_A.upper()}) == SHA_A.upper()


@pytest.mark.parametrize(
    "node",
    [
        {},
        {"sha256sum": SHA_A[:-1]},  # 63 chars
        {"sha256sum": SHA_A + "0"},  # 65 chars
        {"sha256sum": "z" * 64},  # not hex
        {"sha256sum": 12345},
        {"md5": SHA_A},  # key not in the allowed list
        {"description": SHA_A},
    ],
)
def test_node_without_valid_checksum_returns_none(node):
    assert dl._extract_checksum_from_node(node) is None


def test_node_skips_invalid_key_and_uses_next_allowed_one():
    node = {"checksum": "invalid", "sha256sum": SHA_A}
    assert dl._extract_checksum_from_node(node) == SHA_A


# --- _extract_checksums_from_jsonld ---


def test_jsonld_maps_both_file_and_id_to_checksum():
    node = part(FILE_A, sha256sum=SHA_A)
    result = dl._extract_checksums_from_jsonld(version_jsonld(node))
    assert result == {FILE_A: SHA_A, node["@id"]: SHA_A}


def test_jsonld_multiple_parts():
    result = dl._extract_checksums_from_jsonld(
        version_jsonld(part(FILE_A, sha256sum=SHA_A), part(FILE_B, sha256sum=SHA_B))
    )
    assert result[FILE_A] == SHA_A
    assert result[FILE_B] == SHA_B


def test_jsonld_top_level_list_is_accepted():
    data = json.dumps([part(FILE_A, sha256sum=SHA_A)])
    assert dl._extract_checksums_from_jsonld(data)[FILE_A] == SHA_A


def test_jsonld_ignores_non_part_nodes_and_parts_without_checksum():
    data = version_jsonld(
        {"@id": "x", "@type": "Artifact", "sha256sum": SHA_A},
        part(FILE_B),
    )
    assert dl._extract_checksums_from_jsonld(data) == {}


@pytest.mark.parametrize("text", ["", "not json", '{"@graph": [', "42", '"text"'])
def test_jsonld_invalid_or_unexpected_input_returns_empty(text):
    assert dl._extract_checksums_from_jsonld(text) == {}


# --- _resolve_checksums_for_urls ---


def test_resolve_fetches_each_version_once(monkeypatch):
    fetched = []

    def fake_fetch(uri, databus_key=None):
        fetched.append((uri, databus_key))
        return version_jsonld(part(FILE_A, sha256sum=SHA_A), part(FILE_B, sha256sum=SHA_B))

    monkeypatch.setattr(dl, "fetch_databus_jsonld", fake_fetch)
    result = dl._resolve_checksums_for_urls([FILE_A, FILE_B], "key")
    assert result == {FILE_A: SHA_A, FILE_B: SHA_B}
    assert fetched == [(VERSION, "key")]


def test_resolve_skips_urls_without_version(monkeypatch):
    def fail_fetch(uri, databus_key=None):
        raise AssertionError(f"unexpected fetch: {uri}")

    monkeypatch.setattr(dl, "fetch_databus_jsonld", fail_fetch)
    urls = [
        "https://databus.dbpedia.org/dbpedia/generic/geo-coordinates",
        "https://databus.dbpedia.org/dbpedia",
    ]
    assert dl._resolve_checksums_for_urls(urls, None) == {}


def test_resolve_skips_a_failing_version_and_keeps_others(monkeypatch):
    other_version = "https://databus.dbpedia.org/acc/grp/art/2020"
    other_file = f"{other_version}/data.nt"

    def fake_fetch(uri, databus_key=None):
        if uri == other_version:
            raise ConnectionError("boom")
        return version_jsonld(part(FILE_A, sha256sum=SHA_A))

    monkeypatch.setattr(dl, "fetch_databus_jsonld", fake_fetch)
    assert dl._resolve_checksums_for_urls([other_file, FILE_A], None) == {FILE_A: SHA_A}


def test_resolve_only_returns_requested_urls(monkeypatch):
    monkeypatch.setattr(
        dl,
        "fetch_databus_jsonld",
        lambda uri, databus_key=None: version_jsonld(
            part(FILE_A, sha256sum=SHA_A), part(FILE_B, sha256sum=SHA_B)
        ),
    )
    assert dl._resolve_checksums_for_urls([FILE_A], None) == {FILE_A: SHA_A}


# --- get_expected_checksum ---


def test_expected_checksum_fetches_parent_version(monkeypatch):
    fetched = []

    def fake_fetch(uri, databus_key=None):
        fetched.append((uri, databus_key))
        return version_jsonld(part(FILE_A, sha256sum=SHA_A))

    monkeypatch.setattr(dl, "fetch_databus_jsonld", fake_fetch)
    assert dl.get_expected_checksum(FILE_A, "key") == SHA_A
    assert fetched == [(VERSION, "key")]


def test_expected_checksum_http_uri_matches_https_metadata(monkeypatch):
    monkeypatch.setattr(
        dl,
        "fetch_databus_jsonld",
        lambda uri, databus_key=None: version_jsonld(part(FILE_A, sha256sum=SHA_A)),
    )
    http_uri = FILE_A.replace("https://", "http://")
    assert dl.get_expected_checksum(http_uri, None) == SHA_A


def test_expected_checksum_missing_file_returns_none(monkeypatch):
    monkeypatch.setattr(
        dl,
        "fetch_databus_jsonld",
        lambda uri, databus_key=None: version_jsonld(part(FILE_B, sha256sum=SHA_B)),
    )
    assert dl.get_expected_checksum(FILE_A, None) is None


def test_expected_checksum_fetch_error_warns_and_returns_none(monkeypatch, capsys):
    def fail_fetch(uri, databus_key=None):
        raise ConnectionError("network down")

    monkeypatch.setattr(dl, "fetch_databus_jsonld", fail_fetch)
    assert dl.get_expected_checksum(FILE_A, None) is None
    assert "WARNING: Could not fetch checksum" in capsys.readouterr().out
