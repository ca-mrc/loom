"""Exercise the installed CLI command through a disposable HTTP server."""

import json
import os
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlsplit

import pytest


@pytest.mark.parametrize("forbidden", [False, True])
def test_watch_cli_http_replays_terminal_history_and_enforces_auth(tmp_path, forbidden):
    cursors = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            path = urlsplit(self.path)
            status = 403 if forbidden else 200
            assert self.headers["Authorization"] == "Bearer disposable-test-token"
            if forbidden:
                body = {"detail": "forbidden"}
            elif path.path.endswith("/events"):
                query = parse_qs(path.query)
                cursor = int(query["after_seq"][0])
                cursors.append(cursor)
                events = [{"seq": seq, "type": "message"} for seq in range(cursor + 1, 3)][:2]
                body = {"events": events, "next_after_seq": events[-1]["seq"] if events else None}
            else:
                body = {
                    "id": "trial-test", "state": "succeeded", "reward": 0,
                    "progress": {"stage": "terminal", "label": "Succeeded", "timeline": []},
                }
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    config = tmp_path / "loom" / "config.toml"
    config.parent.mkdir()
    config.write_text(
        f'server_url = "http://127.0.0.1:{server.server_port}"\n'
        'auth_token = "disposable-test-token"\n'
    )
    config.chmod(0o600)
    try:
        result = subprocess.run(
            [sys.executable, "-m", "loom_cli", "eval", "trial", "watch", "trial-test",
             "--format", "json", "--limit", "2", "--poll-interval", "0.1"],
            env={**os.environ, "XDG_CONFIG_HOME": str(tmp_path)},
            capture_output=True, text=True, timeout=20,
        )
        if forbidden:
            assert result.returncode != 0
            assert not cursors
            assert "token rejected" in result.stderr and "forbidden" in result.stderr
        else:
            assert result.returncode == 0, result.stderr
            rows = [json.loads(line) for line in result.stdout.splitlines()]
            assert [row["event"]["seq"] for row in rows if row["kind"] == "event"] == [0, 1, 2]
            assert cursors == [-1, 1, 2]
            assert rows[-1]["kind"] == "result"
            assert rows[-1]["trial"]["reward"] == 0
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
