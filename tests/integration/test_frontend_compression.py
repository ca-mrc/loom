"""Exercise the deployed nginx config, including a TLS proxy's Via header."""

from __future__ import annotations

import gzip
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path

import pytest

pytestmark = [pytest.mark.docker, pytest.mark.timeout(60)]
ROOT = Path(__file__).resolve().parents[2]
BODY = b'console.log("Loom compression regression");\n' * 1000


@pytest.fixture(scope="module")
def web_origin(tmp_path_factory):
    root = tmp_path_factory.mktemp("frontend-compression")
    root.chmod(0o755)
    (root / "assets").mkdir()
    (root / "assets/probe.js").write_bytes(BODY)
    (root / "index.html").write_text("Loom compression fixture")
    name = "loom-compression-" + uuid.uuid4().hex[:10]
    try:
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                name,
                "-p",
                "127.0.0.1::8080",
                "-v",
                f"{root}:/usr/share/nginx/html:ro",
                "-v",
                f"{ROOT}/deploy/nginx-spa.conf:/etc/nginx/conf.d/default.conf:ro",
                "-v",
                f"{ROOT}/deploy/nginx-spa-security-headers.conf:"
                "/etc/nginx/loom-spa-security-headers.conf:ro",
                "nginxinc/nginx-unprivileged:1.27-alpine",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        binding = subprocess.check_output(
            ["docker", "port", name, "8080"],
            text=True,
        ).strip()
        origin = "http://" + binding
        for _ in range(50):
            try:
                with urllib.request.urlopen(origin, timeout=1):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            pytest.fail("Disposable nginx did not become ready")
        yield origin
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)


@pytest.mark.parametrize("prefix", ["", "/dev", "/prod", "/staging"])
@pytest.mark.parametrize("via", [False, True])
@pytest.mark.parametrize("encoding", ["gzip", "gzip;q=0, identity", "br"])
def test_static_asset_compression_negotiation(web_origin, prefix, via, encoding):
    headers = {"Accept-Encoding": encoding}
    if via:
        headers["Via"] = "2.0 Caddy"
    request = urllib.request.Request(web_origin + prefix + "/assets/probe.js", headers=headers)
    with urllib.request.urlopen(request, timeout=5) as response:
        body = response.read()
        assert response.status == 200
        assert response.headers["Content-Type"] == "application/javascript"
        assert "Accept-Encoding" in response.headers["Vary"]
        assert "immutable" in ",".join(response.headers.get_all("Cache-Control"))
        assert response.headers["X-Content-Type-Options"] == "nosniff"
        if encoding == "gzip":
            assert response.headers.get("Content-Encoding") == "gzip"
            assert gzip.decompress(body) == BODY
            assert len(body) < len(BODY)
        else:
            assert response.headers.get("Content-Encoding") is None
            assert body == BODY
