import json

import pytest

from databusclient.api import delete as delmod


def _group_json(artifacts):
    return json.dumps({"databus:hasArtifact": artifacts})


@pytest.fixture(autouse=True)
def _mock_helpers(monkeypatch):
    monkeypatch.setattr(delmod, "fetch_databus_jsonld", lambda *a, **k: "")
    monkeypatch.setattr(delmod, "_delete_artifact", lambda *a, **k: None)
    monkeypatch.setattr(delmod, "_delete_resource", lambda *a, **k: None)


def _run(artifacts_payload, monkeypatch):
    monkeypatch.setattr(
        delmod, "fetch_databus_jsonld", lambda *a, **k: _group_json(artifacts_payload)
    )
    captured = {"artifacts": []}

    def fake_delete_artifact(uri, *a, **k):
        captured["artifacts"].append(uri)

    monkeypatch.setattr(delmod, "_delete_artifact", fake_delete_artifact)
    delmod._delete_group("https://example.org/g", "key", dry_run=True)
    return captured


def test_single_artifact_dict(monkeypatch):
    payload = {"@id": "https://example.org/g/a"}
    captured = _run(payload, monkeypatch)
    assert captured["artifacts"] == ["https://example.org/g/a"]


def test_multiple_artifact_list(monkeypatch):
    payload = [
        {"@id": "https://example.org/g/a1"},
        {"@id": "https://example.org/g/a2"},
    ]
    captured = _run(payload, monkeypatch)
    assert captured["artifacts"] == [
        "https://example.org/g/a1",
        "https://example.org/g/a2",
    ]


def test_missing_key(monkeypatch):
    monkeypatch.setattr(
        delmod, "fetch_databus_jsonld", lambda *a, **k: json.dumps({})
    )
    called = {"n": 0}

    def fake_delete_artifact(*a, **k):
        called["n"] += 1

    monkeypatch.setattr(delmod, "_delete_artifact", fake_delete_artifact)
    delmod._delete_group("https://example.org/g", "key", dry_run=True)
    assert called["n"] == 0


def test_none_value(monkeypatch):
    monkeypatch.setattr(
        delmod, "fetch_databus_jsonld", lambda *a, **k: json.dumps({"databus:hasArtifact": None})
    )
    called = {"n": 0}

    def fake_delete_artifact(*a, **k):
        called["n"] += 1

    monkeypatch.setattr(delmod, "_delete_artifact", fake_delete_artifact)
    delmod._delete_group("https://example.org/g", "key", dry_run=True)
    assert called["n"] == 0


def test_unexpected_type(monkeypatch):
    monkeypatch.setattr(
        delmod, "fetch_databus_jsonld", lambda *a, **k: json.dumps({"databus:hasArtifact": "not-a-list"})
    )
    called = {"n": 0}

    def fake_delete_artifact(*a, **k):
        called["n"] += 1

    monkeypatch.setattr(delmod, "_delete_artifact", fake_delete_artifact)
    delmod._delete_group("https://example.org/g", "key", dry_run=True)
    assert called["n"] == 0


def test_non_dict_and_missing_id_skipped(monkeypatch):
    payload = [
        "not-a-dict",
        {"foo": "bar"},  # no @id
        {"@id": "https://example.org/g/a"},
    ]
    captured = _run(payload, monkeypatch)
    assert captured["artifacts"] == ["https://example.org/g/a"]


def test_non_string_id_skipped(monkeypatch):
    payload = [
        {"@id": 123},
        {"@id": {"x": 1}},
        {"@id": "https://example.org/g/a"},
    ]
    captured = _run(payload, monkeypatch)
    assert captured["artifacts"] == ["https://example.org/g/a"]
