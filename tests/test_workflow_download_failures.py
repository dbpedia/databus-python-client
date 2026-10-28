"""A recorded download failure must reach workflow error handling."""

from unittest.mock import Mock

import pytest
import requests

from databusclient.manifest.context import ManifestContext
from databusclient.workflow.engine import WorkflowEngine, WorkflowExecutionError


@pytest.mark.parametrize("with_manifest", [False, True])
@pytest.mark.parametrize("policy", ["fail", "continue", "retry"])
def test_download_404_obeys_workflow_error_policy(
    monkeypatch, tmp_path, with_manifest, policy
):
    missing = requests.Response()
    missing.status_code = 404
    success = requests.Response()
    success.status_code = 200
    success.headers["Content-Length"] = "4"
    success.iter_content = lambda chunk_size: iter([b"data"])
    head = requests.Response()
    head.status_code = 200
    monkeypatch.setattr("requests.head", lambda *args, **kwargs: head)
    get = Mock(side_effect=[missing, success])
    monkeypatch.setattr("requests.get", get)

    manifest = ManifestContext(command="workflow") if with_manifest else None
    engine = WorkflowEngine(manifest_context=manifest)
    step = {
        "name": "fetch",
        "command": "download",
        "uri": "https://example.org/account/group/artifact/version/data.ttl",
        "localdir": str(tmp_path),
        "on_error": policy,
    }
    if policy == "retry":
        step["retry"] = {"max_attempts": 2, "delay_seconds": 0}

    if policy == "fail":
        with pytest.raises(WorkflowExecutionError, match="download failed"):
            engine.run([step])
    else:
        result = engine.run([step])[0]
        if policy == "continue":
            assert result.status == "skipped_error"
            assert get.call_count == 1
        else:
            assert result.status == "success"
            assert result.attempts == 2
            assert get.call_count == 2
            output = tmp_path / "account/group/artifact/version/data.ttl"
            assert output.read_bytes() == b"data"
