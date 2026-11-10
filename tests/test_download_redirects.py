"""Exercise download redirects against a local HTTP server."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest

from databusclient.api.download import _download_file


@pytest.mark.parametrize(
    ("location", "expected_path"),
    [
        ("/data.ttl", "/data.ttl"),
        ("../data.ttl", "/data.ttl"),
        ("data.ttl", "/entry/data.ttl"),
        ("{base}/data.ttl", "/data.ttl"),
        ("//{host}/data.ttl", "/data.ttl"),
    ],
)
def test_download_resolves_redirect_location(tmp_path, location, expected_path):
    content = b"<urn:s> <urn:p> <urn:o> .\n"
    requested_paths = []

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            if self.path == "/entry/file.ttl":
                self.send_response(302)
                self.send_header("Location", redirect_location)
            else:
                self.send_response(200)
            self.end_headers()

        def do_GET(self):
            requested_paths.append(self.path)
            self.send_response(200)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, format, *args):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        host = f"127.0.0.1:{server.server_port}"
        base = f"http://{host}"
        redirect_location = location.format(base=base, host=host)
        thread = Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            _download_file(f"{base}/entry/file.ttl", localDir=str(tmp_path))
        finally:
            server.shutdown()
            thread.join()

    assert requested_paths == [expected_path]
    assert (tmp_path / "file.ttl").read_bytes() == content
