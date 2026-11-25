"""Offline tests for the JSON-LD parsers in databusclient.api.download."""

import json

import pytest

import databusclient.api.download as dl

BASE = "https://databus.dbpedia.org/acc/grp"


def test_file_urls_come_from_part_nodes_only():
    jsonld = json.dumps(
        {
            "@graph": [
                {"@type": "Version", "@id": f"{BASE}/art/2024.01.01"},
                {"@type": "Part", "file": f"{BASE}/art/2024.01.01/a.ttl"},
                {"@type": "Dataset", "file": f"{BASE}/art/not-a-part.ttl"},
                {"@type": "Part", "file": f"{BASE}/art/2024.01.01/b.nt.gz"},
            ]
        }
    )

    assert dl._get_file_download_urls_from_artifact_jsonld(jsonld) == [
        f"{BASE}/art/2024.01.01/a.ttl",
        f"{BASE}/art/2024.01.01/b.nt.gz",
    ]


def test_file_urls_skip_parts_without_a_string_file():
    jsonld = json.dumps(
        {
            "@graph": [
                {"@type": "Part"},
                {"@type": "Part", "file": {"@id": f"{BASE}/art/v1/as-object.ttl"}},
                {"@type": "Part", "file": f"{BASE}/art/v1/ok.ttl"},
            ]
        }
    )

    assert dl._get_file_download_urls_from_artifact_jsonld(jsonld) == [
        f"{BASE}/art/v1/ok.ttl"
    ]


def test_file_urls_without_graph_are_empty():
    assert dl._get_file_download_urls_from_artifact_jsonld("{}") == []


def test_group_artifacts_keep_artifact_uris_and_drop_version_uris():
    jsonld = json.dumps(
        {
            "databus:hasArtifact": [
                {"@id": f"{BASE}/art-a"},
                {"@id": f"{BASE}/art-a/2024.01.01"},
                {"@id": f"{BASE}/art-b"},
            ]
        }
    )

    assert dl._get_databus_artifacts_of_group(jsonld) == [
        f"{BASE}/art-a",
        f"{BASE}/art-b",
    ]


def test_group_artifacts_accept_a_single_artifact_object():
    jsonld = json.dumps({"databus:hasArtifact": {"@id": f"{BASE}/art"}})

    assert dl._get_databus_artifacts_of_group(jsonld) == [f"{BASE}/art"]


def test_group_artifacts_skip_entries_without_an_id():
    jsonld = json.dumps(
        {"databus:hasArtifact": [f"{BASE}/as-string", {"label": "no id"}, {"@id": ""}]}
    )

    assert dl._get_databus_artifacts_of_group(jsonld) == []


def test_group_without_artifacts_is_empty():
    assert dl._get_databus_artifacts_of_group("{}") == []


def test_group_artifacts_reject_an_unexpected_type():
    jsonld = json.dumps({"databus:hasArtifact": "not-a-list"})

    with pytest.raises(ValueError, match="databus:hasArtifact"):
        dl._get_databus_artifacts_of_group(jsonld)
