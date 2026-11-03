from databusclient.api.delete import DeleteQueue, _confirm_delete
from databusclient.api import delete


def test_new_queue_is_empty():
    q = DeleteQueue("fake-key")
    assert q.is_empty() is True
    assert q.is_not_empty() is False


def test_add_uri_makes_queue_not_empty():
    q = DeleteQueue("fake-key")
    q.add_uri("https://databus.dbpedia.org/acc/group")
    assert q.is_empty() is False


def test_add_uris_adds_all():
    q = DeleteQueue("fake-key")
    q.add_uris(
        [
            "https://databus.dbpedia.org/acc/group",
            "https://databus.dbpedia.org/acc/group/artifact",
            "https://databus.dbpedia.org/acc/group/artifact/2024.01.01",
        ]
    )
    assert len(q.queue) == 3


def test_duplicate_uri_stored_once():
    q = DeleteQueue("fake-key")
    q.add_uri("https://databus.dbpedia.org/acc/group")
    q.add_uri("https://databus.dbpedia.org/acc/group")
    assert len(q.queue) == 1


def test_sorted_queue_deepest_first():
    q = DeleteQueue("fake-key")
    q.add_uri("https://databus.dbpedia.org/acc/group")
    q.add_uri("https://databus.dbpedia.org/acc/group/artifact/2024.01.01")
    q.add_uri("https://databus.dbpedia.org/acc/group/artifact")
    assert q.sorted_queue() == [
        "https://databus.dbpedia.org/acc/group/artifact/2024.01.01",
        "https://databus.dbpedia.org/acc/group/artifact",
        "https://databus.dbpedia.org/acc/group",
    ]


def test_path_depth_counts_segments():
    assert DeleteQueue._path_depth("https://databus.dbpedia.org/acc/group") == 2
    assert (
        DeleteQueue._path_depth(
            "https://databus.dbpedia.org/acc/group/artifact/2024.01.01"
        )
        == 4
    )


def test_path_depth_ignores_trailing_slash():
    assert DeleteQueue._path_depth("https://databus.dbpedia.org/acc/group/") == 2


def test_execute_deletes_deepest_first(monkeypatch):
    calls = {}

    def fake_delete_list(databusURIs, databus_key, force=False, manifest_context=None):
        calls["uris"] = databusURIs
        calls["key"] = databus_key
        calls["force"] = force

    monkeypatch.setattr(delete, "_delete_list", fake_delete_list)

    q = DeleteQueue("fake-key")
    q.add_uris(
        [
            "https://databus.dbpedia.org/acc/group",
            "https://databus.dbpedia.org/acc/group/artifact/2024.01.01",
            "https://databus.dbpedia.org/acc/group/artifact",
        ]
    )
    q.execute()

    assert calls["uris"] == [
        "https://databus.dbpedia.org/acc/group/artifact/2024.01.01",
        "https://databus.dbpedia.org/acc/group/artifact",
        "https://databus.dbpedia.org/acc/group",
    ]
    assert calls["key"] == "fake-key"
    assert calls["force"] is True


def test_confirm_y_returns_confirm(monkeypatch):
    def fake_input(prompt):
        return "y"

    monkeypatch.setattr("builtins.input", fake_input)

    assert _confirm_delete("https://databus.dbpedia.org/acc/group") == "confirm"


def test_confirm_messy_yes_returns_confirm(monkeypatch):
    def fake_input(prompt):
        return " YES "

    monkeypatch.setattr("builtins.input", fake_input)

    assert _confirm_delete("https://databus.dbpedia.org/acc/group") == "confirm"


def test_confirm_s_returns_skip(monkeypatch):
    def fake_input(prompt):
        return "s"

    monkeypatch.setattr("builtins.input", fake_input)

    assert _confirm_delete("https://databus.dbpedia.org/acc/group") == "skip"


def test_confirm_c_returns_cancel(monkeypatch):
    def fake_input(prompt):
        return "c"

    monkeypatch.setattr("builtins.input", fake_input)

    assert _confirm_delete("https://databus.dbpedia.org/acc/group") == "cancel"


def test_confirm_invalid_then_valid_asks_again(monkeypatch):
    answers = ["maybe", "s"]

    def fake_input(prompt):
        return answers.pop(0)

    monkeypatch.setattr("builtins.input", fake_input)

    assert _confirm_delete("https://databus.dbpedia.org/acc/group") == "skip"
    assert answers == []
