from click.testing import CliRunner
from unittest.mock import Mock, patch

import databusclient.cli as cli


# CLI-level integration test for -v flag
def test_cli_download_verbose_logs_redacted(caplog):
    caplog.set_level("DEBUG", logger="databusclient")
    runner = CliRunner()

    # Prepare mocked HTTP responses
    resp_head_401 = Mock()
    resp_head_401.status_code = 401
    resp_head_401.headers = {}

    resp_head_200 = Mock()
    resp_head_200.status_code = 200
    resp_head_200.headers = {}

    resp_get = Mock()
    resp_get.status_code = 200
    resp_get.headers = {"content-length": "0"}
    resp_get.iter_content = lambda chunk: iter([])

    # Initial HEAD returns 401 so client uses --databus-key header on retry
    with patch("requests.head", side_effect=[resp_head_401, resp_head_200]), patch(
        "requests.get", return_value=resp_get
    ):
        # Run CLI with verbose flag and databus key (so X-API-KEY will be redacted in logs)
        target = "https://example.com/account/group/artifact/1/file.txt"
        res = runner.invoke(cli.app, ["-v", "download", target, "--localdir", ".", "--databus-key", "SECRET"]) 

    assert res.exit_code == 0, res.output
    # Should log HTTP activity and redact secret (captured by caplog)
    assert "[HTTP]" in caplog.text
    assert "REDACTED" in caplog.text
    assert "SECRET" not in caplog.text
