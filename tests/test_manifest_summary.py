from databusclient.manifest.summary import format_summary


def test_summary_full_download_manifest():
    manifest = {
        "dbus:command": "download",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:endpoint": "https://databus.dbpedia.org",
        "dbus:authMethod": "vault_token",
        "dbus:executionResult": {
            "dbus:succeeded": 1,
            "dbus:failed": 0,
            "dbus:totalBytes": 104857600,
        },
    }
    output = format_summary(manifest)
    assert "Command  : download" in output
    assert "Executed : 2024-03-24T10:00:00Z" in output
    assert "Endpoint : https://databus.dbpedia.org" in output
    assert "Auth     : vault_token" in output
    assert "Files    : 1 succeeded \u00b7 0 failed" in output
    assert "Total    : 100.0 MB" in output
    assert "Status   : completed" in output


def test_summary_omits_missing_optional_fields():
    """Delete/deploy manifests without endpoint/authMethod omit those lines."""
    manifest = {
        "dbus:command": "delete",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {
            "dbus:succeeded": 1,
            "dbus:failed": 0,
            "dbus:totalBytes": 0,
        },
    }
    output = format_summary(manifest)
    assert "Endpoint" not in output
    assert "Auth" not in output
    assert "Total" not in output
    assert "Command  : delete" in output
    assert "Status   : completed" in output


def test_summary_shows_failed_files():
    manifest = {
        "dbus:command": "download",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {
            "dbus:succeeded": 2,
            "dbus:failed": 1,
            "dbus:totalBytes": 2048,
        },
    }
    output = format_summary(manifest)
    assert "Files    : 2 succeeded \u00b7 1 failed" in output
    assert "Status   : completed with errors" in output


def test_summary_shows_operation_error_as_failed_status():
    manifest = {
        "dbus:command": "deploy",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {
            "dbus:succeeded": 0,
            "dbus:failed": 0,
            "dbus:totalBytes": 0,
        },
        "dbus:operationError": {
            "dbus:errorType": "DeployError",
            "dbus:errorMessage": "bad API key",
        },
    }
    output = format_summary(manifest)
    assert "Status   : failed" in output


def test_summary_total_omitted_for_non_download_command():
    manifest = {
        "dbus:command": "deploy",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {
            "dbus:succeeded": 1,
            "dbus:failed": 0,
            "dbus:totalBytes": 12345,
        },
    }
    output = format_summary(manifest)
    assert "Total" not in output

def test_summary_includes_error_message_when_operation_failed():
    manifest = {
        "dbus:command": "deploy",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {
            "dbus:succeeded": 0,
            "dbus:failed": 0,
            "dbus:totalBytes": 0,
        },
        "dbus:operationError": {
            "dbus:errorType": "DeployError",
            "dbus:errorMessage": "Could not deploy dataset to databus. Reason: 'Invalid API key'",
        },
    }
    output = format_summary(manifest)
    assert "Status   : failed" in output
    assert "Error    : DeployError: Could not deploy dataset to databus. Reason: 'Invalid API key'" in output


def test_summary_no_error_line_when_no_operation_error():
    manifest = {
        "dbus:command": "download",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {"dbus:succeeded": 1, "dbus:failed": 0, "dbus:totalBytes": 100},
    }
    output = format_summary(manifest)
    assert "Error" not in output

def test_summary_ignores_step_name_field_gracefully():
    """dbus:stepName is a per-file field, not surfaced in the top-level
    summary -- confirms it doesn't break formatting."""
    manifest = {
        "dbus:command": "workflow",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {"dbus:succeeded": 1, "dbus:failed": 0, "dbus:totalBytes": 0},
        "dataid:distribution": {"dataid:file": [{"dbus:stepName": "fetch"}]},
    }
    output = format_summary(manifest)
    assert "Command  : workflow" in output

def test_summary_lists_failed_file_details_with_step_name():
    manifest = {
        "dbus:command": "workflow",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {"dbus:succeeded": 1, "dbus:failed": 1, "dbus:totalBytes": 0},
        "dataid:distribution": {
            "dataid:file": [
                {"dcat:downloadURL": "https://a.org/x", "dbus:status": "success"},
                {
                    "dcat:downloadURL": "step:deploy_with_bad_key",
                    "dbus:status": "failed",
                    "dbus:stepName": "deploy_with_bad_key",
                    "dbus:errorMessage": "Authentication failed.",
                },
            ]
        },
    }
    output = format_summary(manifest)
    assert "Failures:" in output
    assert "[deploy_with_bad_key] step:deploy_with_bad_key: Authentication failed." in output


def test_summary_lists_failed_file_details_without_step_name():
    """Single-command manifests (not workflows) have no dbus:stepName --
    confirm the failed-file line still renders cleanly without it."""
    manifest = {
        "dbus:command": "download",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {"dbus:succeeded": 0, "dbus:failed": 1, "dbus:totalBytes": 0},
        "dataid:distribution": {
            "dataid:file": [
                {
                    "dcat:downloadURL": "https://databus.dbpedia.org/account/notexisting",
                    "dbus:status": "failed",
                    "dbus:errorMessage": "404 Not Found",
                },
            ]
        },
    }
    output = format_summary(manifest)
    assert "Failures:" in output
    assert "https://databus.dbpedia.org/account/notexisting: 404 Not Found" in output


def test_summary_no_failed_files_section_when_all_succeeded():
    manifest = {
        "dbus:command": "download",
        "dcterms:issued": {"@value": "2024-03-24T10:00:00Z"},
        "dbus:executionResult": {"dbus:succeeded": 1, "dbus:failed": 0, "dbus:totalBytes": 100},
        "dataid:distribution": {
            "dataid:file": [{"dcat:downloadURL": "https://a.org/x", "dbus:status": "success"}]
        },
    }
    output = format_summary(manifest)
    assert "Failures:" not in output