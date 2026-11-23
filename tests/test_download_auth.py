from unittest.mock import Mock, patch

import pytest

import requests

import databusclient.api.download as dl

from databusclient.api.download import VAULT_REQUIRED_HOSTS, DownloadAuthError


def make_response(status=200, headers=None, content=b""):
    headers = headers or {}
    mock = Mock()
    mock.status_code = status
    mock.headers = headers
    mock.content = content

    def iter_content(chunk_size):
        if content:
            yield content
        else:
            return

    mock.iter_content = lambda chunk: iter(iter_content(chunk))

    def raise_for_status():
        if mock.status_code >= 400:
            raise requests.exceptions.HTTPError()

    mock.raise_for_status = raise_for_status
    return mock


def test_vault_host_no_token_raises():
    vault_host = next(iter(VAULT_REQUIRED_HOSTS))
    url = f"https://{vault_host}/some/protected/file.ttl"

    resp_head = make_response(status=401, headers={})

    with patch("requests.head", return_value=resp_head):
        with pytest.raises(DownloadAuthError) as exc:
            dl._download_file(url, localDir=".", vault_token_file=None)

    assert "Vault token required" in str(exc.value)


def test_non_vault_host_no_token_allows_download(tmp_path):
    url = "https://example.com/public/file.txt"

    resp_head = make_response(status=200, headers={})
    resp_get = make_response(status=200, headers={"content-length": "0"}, content=b"")

    with (
        patch("requests.head", return_value=resp_head),
        patch("requests.get", return_value=resp_get),
    ):
        # should not raise
        dl._download_file(url, localDir=str(tmp_path), vault_token_file=None)

    assert (tmp_path / "file.txt").exists()


def test_download_file_uses_databus_layout_under_localdir(tmp_path):
    url = "https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2"

    resp_head = make_response(status=200, headers={})
    resp_get = make_response(
        status=200, headers={"content-length": "4"}, content=b"data"
    )

    with (
        patch("requests.head", return_value=resp_head),
        patch("requests.get", return_value=resp_get),
    ):
        dl._download_file(url, localDir=str(tmp_path), vault_token_file=None)

    expected = (
        tmp_path
        / "dbpedia"
        / "mappings"
        / "mappingbased-literals"
        / "2022.12.01"
        / "mappingbased-literals_lang=az.ttl.bz2"
    )
    assert expected.read_bytes() == b"data"
    assert not (tmp_path / "mappingbased-literals_lang=az.ttl.bz2").exists()


def test_download_file_uses_databus_layout_under_cwd_when_localdir_missing(
    tmp_path, monkeypatch
):
    url = "https://databus.dbpedia.org/dbpedia/mappings/mappingbased-literals/2022.12.01/mappingbased-literals_lang=az.ttl.bz2"

    monkeypatch.chdir(tmp_path)
    resp_head = make_response(status=200, headers={})
    resp_get = make_response(
        status=200, headers={"content-length": "4"}, content=b"data"
    )

    with (
        patch("requests.head", return_value=resp_head),
        patch("requests.get", return_value=resp_get),
    ):
        dl._download_file(url, localDir=None, vault_token_file=None)

    expected = (
        tmp_path
        / "dbpedia"
        / "mappings"
        / "mappingbased-literals"
        / "2022.12.01"
        / "mappingbased-literals_lang=az.ttl.bz2"
    )
    assert expected.read_bytes() == b"data"


def test_401_after_token_exchange_reports_invalid_token(monkeypatch):
    vault_host = next(iter(VAULT_REQUIRED_HOSTS))
    url = f"https://{vault_host}/protected/file.ttl"

    # initial head and get -> 401 with Bearer
    resp_head = make_response(status=200, headers={})
    resp_401 = make_response(
        status=401, headers={"WWW-Authenticate": 'Bearer realm="auth"'}
    )

    # after retry with token -> still 401
    resp_401_retry = make_response(status=401, headers={})

    # Mock requests.get side effects: first 401 (challenge), then 401 after token
    get_side_effects = [resp_401, resp_401_retry]

    # Mock token exchange responses
    post_resp_1 = Mock()
    post_resp_1.json.return_value = {"access_token": "ACCESS"}
    post_resp_2 = Mock()
    post_resp_2.json.return_value = {"access_token": "VAULT"}

    with (
        patch("requests.head", return_value=resp_head),
        patch("requests.get", side_effect=get_side_effects),
        patch("requests.post", side_effect=[post_resp_1, post_resp_2]),
    ):
        # set REFRESH_TOKEN so __get_vault_access__ doesn't try to open a file
        monkeypatch.setenv("REFRESH_TOKEN", "x" * 90)

        with pytest.raises(DownloadAuthError) as exc:
            dl._download_file(url, localDir=".", vault_token_file="/does/not/matter")

        assert "invalid or expired" in str(exc.value)


def test_403_reports_insufficient_permissions():
    vault_host = next(iter(VAULT_REQUIRED_HOSTS))
    url = f"https://{vault_host}/protected/file.ttl"

    resp_head = make_response(status=200, headers={})
    resp_403 = make_response(status=403, headers={})

    with (
        patch("requests.head", return_value=resp_head),
        patch("requests.get", return_value=resp_403),
    ):
        # provide a token path so early check does not block
        with pytest.raises(DownloadAuthError) as exc:
            dl._download_file(url, localDir=".", vault_token_file="/some/token/file")

        assert "permission" in str(exc.value) or "forbidden" in str(exc.value)
