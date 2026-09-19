import sys
import types
import pytest
import threading
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer

# --- 1. Original SPARQL Patch---
if "SPARQLWrapper" not in sys.modules:
    mod = types.ModuleType("SPARQLWrapper")
    mod.JSON = None

    class DummySPARQL:
        def __init__(self, *args, **kwargs):
            pass

        def setQuery(self, q):
            self._q = q

        def setReturnFormat(self, f):
            self._fmt = f

        def setCustomHttpHeaders(self, h):
            self._headers = h

        def query(self):
            class R:
                def convert(self):
                    return {"results": {"bindings": []}}
            return R()

    mod.SPARQLWrapper = DummySPARQL
    sys.modules["SPARQLWrapper"] = mod

# --- 2. New Mock Databus Server  ---

class MockDatabusHandler(BaseHTTPRequestHandler):
    """
    A custom RequestHandler that acts like the real Databus.
    It serves specific files that our tests will try to download.
    """
    def do_GET(self):
        # Route 1: A simple text file
        if self.path.endswith("/test-file.txt"):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.send_header("Content-Length", "12")
            self.end_headers()
            self.wfile.write(b"test content") # SHA256: 6ae8a755...
            return

        # Route 2: 404 for anything else
        self.send_error(404, "File not found on Mock Databus")

    def log_message(self, format, *args):
        # Silence server logs to keep test output clean
        pass

@pytest.fixture(scope="session")
def mock_databus():
    """
    Fixture that starts a local HTTP server in a background thread.
    Returns the base URL (e.g., http://localhost:54321).
    """
    # 1. Find a free port automatically
    # We bind to port 0, and the OS assigns a free one.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(('localhost', 0))
    port = sock.getsockname()[1]
    sock.close()

    # 2. Start the server
    server = HTTPServer(('localhost', port), MockDatabusHandler)
    
    # Run server in a separate thread so it doesn't block the tests
    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()

    base_url = f"http://localhost:{port}"
    print(f"\n[MockDatabus] Server started at {base_url}")

    yield base_url

    # 3. Cleanup after tests
    server.shutdown()
    print("\n[MockDatabus] Server stopped.")