import pytest
import requests
# Removed bad import: from databusclient import client

def test_mock_databus_connection(mock_databus):
    """
    Sanity Check: Ensure the Mock Databus is running and reachable.
    'mock_databus' is the URL string (e.g., http://localhost:54321) passed from conftest.py
    """
    # Try to fetch the test file we defined in the handler
    print(f"\n[TEST] Connecting to {mock_databus}/test-file.txt")
    response = requests.get(f"{mock_databus}/test-file.txt")
    
    # Assertions
    assert response.status_code == 200
    assert response.text == "test content"
    print(f"[SUCCESS] Downloaded from {mock_databus}/test-file.txt")

def test_mock_download_404(mock_databus):
    """
    Ensure the Mock Databus correctly returns 404 for missing files.
    """
    response = requests.get(f"{mock_databus}/non-existent-file.txt")
    assert response.status_code == 404