import pytest
import requests_mock
import os
import hashlib
from unittest.mock import patch

# Import the functions and classes from your client.py file
# This assumes test_client.py is in a parent folder of databusclient
# Adjust the import if your directory structure is different
from databusclient.client import download, ShaValidationMode, __get_json_ld_from_databus__

# --- Mock Data ---

# This is the fake content we will "download"
MOCK_FILE_CONTENT = b"This is the actual file content."
# This is the CORRECT hash for the content above
CORRECT_SHA256 = hashlib.sha256(MOCK_FILE_CONTENT).hexdigest()
# This is a FAKE hash that we will use to trigger a mismatch
INCORRECT_SHA256 = "this_is_a_fake_hash_that_will_not_match"

# The Databus Artifact URL we will be "querying"
ARTIFACT_URL = "https://example.databus.com/my-account/my-group/my-artifact/2025-10-31"
# The "file" URL that the artifact metadata points to
FILE_URL = "https://example.databus.com/my-account/my-group/my-artifact/2025-10-31/my-file.ttl"


def get_mock_jsonld(sha_hash_to_use):
    """Helper to generate mock JSON-LD with a specific hash."""
    return {
        "@context": "https://downloads.dbpedia.org/databus/context.jsonld",
        "@graph": [
            {
                "@type": "Part",
                "file": FILE_URL,
                "sha256sum": sha_hash_to_use
            }
        ]
    }


# --- Pytest Tests ---

@pytest.fixture
def mock_file_download(requests_mock, tmp_path):
    """
    A pytest fixture to set up ONLY the file download mock.
    The metadata mock (which differs for each test) will be set up by the test itself.
    """

    # 1. Mock the file download itself (this is the same for all tests)
    requests_mock.head(FILE_URL, headers={"Content-Length": str(len(MOCK_FILE_CONTENT))})
    requests_mock.get(FILE_URL, content=MOCK_FILE_CONTENT)

    # Provide the temporary path to the test
    return tmp_path


# We patch 'builtins.print' to capture the console output
@patch('builtins.print')
def test_sha_mismatch_error(mock_print, mock_file_download, requests_mock):
    """
    Tests that validation_mode=ERROR stops execution (raises ValueError) on mismatch.
    """
    print("\n--- Testing SHA Mismatch with Mode: ERROR ---")
    local_dir = mock_file_download

    # Set up the *specific* metadata mock for THIS test
    requests_mock.get(
        ARTIFACT_URL,
        json=get_mock_jsonld(INCORRECT_SHA256),  # Use INCORRECT hash
        headers={"Accept": "application/ld+json"}
    )

    # We expect this to fail with a ValueError
    with pytest.raises(ValueError) as e:
        download(
            localDir=str(local_dir),
            endpoint=None,  # Will be auto-detected
            databusURIs=[ARTIFACT_URL],
            validation_mode=ShaValidationMode.ERROR
        )

    # Check that the error message is correct
    assert "SHA256 mismatch" in str(e.value)


@patch('builtins.print')
def test_sha_mismatch_warning(mock_print, mock_file_download, requests_mock):
    """
    Tests that validation_mode=WARNING prints a warning but does NOT stop execution.
    """
    print("\n--- Testing SHA Mismatch with Mode: WARNING ---")
    local_dir = mock_file_download

    # Set up the *specific* metadata mock for THIS test
    requests_mock.get(
        ARTIFACT_URL,
        json=get_mock_jsonld(INCORRECT_SHA256),  # Use INCORRECT hash
        headers={"Accept": "application/ld+json"}
    )

    # We expect this to run without raising an error
    try:
        download(
            localDir=str(local_dir),
            endpoint=None,
            databusURIs=[ARTIFACT_URL],
            validation_mode=ShaValidationMode.WARNING
        )
    except ValueError:
        pytest.fail("ValidationMode.WARNING raised a ValueError when it should not have.")

    # Check that the warning was printed to the console
    printed_output = "\n".join([call.args[0] for call in mock_print.call_args_list if call.args])
    assert "WARNING: SHA256 mismatch" in printed_output


@patch('builtins.print')
def test_sha_mismatch_off(mock_print, mock_file_download, requests_mock):
    """
    Tests that validation_mode=OFF skips validation entirely.
    """
    print("\n--- Testing SHA Mismatch with Mode: OFF ---")
    local_dir = mock_file_download

    # Set up the *specific* metadata mock for THIS test
    requests_mock.get(
        ARTIFACT_URL,
        json=get_mock_jsonld(INCORRECT_SHA256),  # Use INCORRECT hash
        headers={"Accept": "application/ld+json"}
    )

    # We expect this to run without raising an error
    try:
        download(
            localDir=str(local_dir),
            endpoint=None,
            databusURIs=[ARTIFACT_URL],
            validation_mode=ShaValidationMode.OFF
        )
    except ValueError:
        pytest.fail("ValidationMode.OFF raised a ValueError when it should not have.")

    # Check that the "skipping" message was printed
    printed_output = "\n".join([call.args[0] for call in mock_print.call_args_list if call.args])
    assert "Skipping SHA256 validation" in printed_output
    assert "WARNING: SHA256 mismatch" not in printed_output  # Ensure no warning was printed


@patch('builtins.print')
def test_sha_match_success(mock_print, mock_file_download, requests_mock):
    """
    Tests that a correct SHA256 hash passes validation.
    """
    print("\n--- Testing SHA Match (Success) ---")
    local_dir = mock_file_download

    # Set up the *specific* metadata mock for THIS test
    requests_mock.get(
        ARTIFACT_URL,
        json=get_mock_jsonld(CORRECT_SHA256),  # Use CORRECT hash
        headers={"Accept": "application/ld+json"}
    )

    # This test uses the metadata with the CORRECT hash
    # We expect this to run without raising an error
    try:
        download(
            localDir=str(local_dir),
            endpoint=None,
            databusURIs=[ARTIFACT_URL],
            validation_mode=ShaValidationMode.WARNING  # Mode doesn't matter, it should pass
        )
    except ValueError:
        pytest.fail("Validation failed when SHA hashes matched.")

    # Check that the "validated" message was printed
    printed_output = "\n".join([call.args[0] for call in mock_print.call_args_list if call.args])
    assert "SHA256 validated" in printed_output

