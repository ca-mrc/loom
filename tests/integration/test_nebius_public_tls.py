"""Opt-in, disposable TLS-ALPN migration/renewal smoke against the built web image.

LOOM_NEBIUS_WEB_IMAGE=loom-web-tls:test uv run --extra dev pytest -q -s \
    tests/integration/test_nebius_public_tls.py
"""

from __future__ import annotations

import gzip
import http.client
import json
import os
import re
import socket
import ssl
import subprocess
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from loom.nebius_platform_render import public_tls_config

pytestmark = [pytest.mark.docker, pytest.mark.timeout(240)]


def _certificate(path: Path, name: str, lifetime: int) -> int:
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, name)])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(seconds=60))
        .not_valid_after(now + timedelta(seconds=lifetime))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(name)]), False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
        .sign(key, hashes.SHA256())
    )
    path.with_suffix(".crt").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    path.with_suffix(".key").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return cert.serial_number


def _wait(check, timeout: float = 90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(0.5)
    raise AssertionError("TLS smoke condition did not converge")


def test_built_web_tls_bootstrap_renewal_restart_and_proxy(tmp_path: Path) -> None:
    image = os.environ.get("LOOM_NEBIUS_WEB_IMAGE")
    if not image:
        pytest.skip("set LOOM_NEBIUS_WEB_IMAGE to the locally built candidate web image")
    prefix = "loom-tls-" + uuid.uuid4().hex[:10]
    names: list[str] = []
    network_created = False

    def docker(*args: str) -> str:
        return subprocess.check_output(
            ["docker", *args], text=True, stderr=subprocess.STDOUT
        ).strip()

    def run(name: str, *args: str) -> None:
        names.append(prefix + "-" + name)
        docker("run", "-d", "--name", names[-1], *args)

    # Containers run as the production UID. These are generated test keys only.
    tmp_path.chmod(0o755)
    data = tmp_path / "data"
    data.mkdir()
    data.chmod(0o777)
    bootstrap = _certificate(tmp_path / "tls", "loom.example", 300)
    _certificate(tmp_path / "pebble", "localhost", 3600)
    config = public_tls_config(
        {"public_host": "loom.example", "namespace": "test", "public_tls_bootstrap": True}
    )
    tls = config["apps"]["tls"]
    tls["certificates"]["load_files"] = [{"certificate": "/test/tls.crt", "key": "/test/tls.key"}]
    tls["automation"]["renew_interval"] = "1s"
    issuer = tls["automation"]["policies"][0]["issuers"][0]
    issuer.update(ca="https://localhost:14000/dir", trusted_roots_pem_files=["/test/pebble.crt"])
    (tmp_path / "caddy.json").write_text(json.dumps(config))
    (tmp_path / "pebble.json").write_text(
        json.dumps(
            {
                "pebble": {
                    "listenAddress": "0.0.0.0:14000",
                    "managementListenAddress": "0.0.0.0:15000",
                    "certificate": "/test/pebble.crt",
                    "privateKey": "/test/pebble.key",
                    "httpPort": 5002,
                    "tlsPort": 8443,
                    "profiles": {
                        "default": {"description": "short local smoke", "validityPeriod": 90}
                    },
                }
            }
        )
    )
    (tmp_path / "backend.py").write_text("""
from http.server import BaseHTTPRequestHandler, HTTPServer
import json, time
class Handler(BaseHTTPRequestHandler):
 def log_message(self, *args): pass
 def do_GET(self):
  if self.path.startswith('/api/v1/events'):
   self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.end_headers()
   self.wfile.write(b'data: first\\n\\n'); self.wfile.flush(); time.sleep(2)
   self.wfile.write(b'data: second\\n\\n'); self.wfile.flush(); return
  self.do_POST()
 def do_POST(self):
  body=self.rfile.read(int(self.headers.get('Content-Length',0)))
  payload=json.dumps({'path':self.path,'headers':dict(self.headers),'size':len(body)}).encode()
  self.send_response(200); self.send_header('Content-Length',str(len(payload))); self.end_headers()
  self.wfile.write(payload)
HTTPServer(('0.0.0.0',8090),Handler).serve_forever()
""")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    context = ssl._create_unverified_context()  # Kubelet HTTPS-probe semantics.

    def request(path="/", body=None, headers=None):
        conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=10, context=context)
        conn.request(
            "GET" if body is None else "POST",
            path,
            body,
            {"Host": "loom.example", **(headers or {})},
        )
        response = conn.getresponse()
        payload = response.read()
        result = response.status, dict(response.getheaders()), payload
        conn.close()
        return result

    def peer_serial():
        with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
            with context.wrap_socket(sock, server_hostname="loom.example") as conn:
                cert = x509.load_der_x509_certificate(conn.getpeercert(binary_form=True))
                assert cert.not_valid_after_utc > datetime.now(UTC), "served an expired certificate"
                return cert.serial_number

    def managed_certificate():
        # Caddy creates private storage directories as the production UID. Read
        # only the test's public certificate as that same UID inside the container,
        # so an unprivileged host runner need not traverse or relax those directories.
        pem = docker(
            "exec", prefix + "-tls", "sh", "-c",
            'for cert in /data/certificates/*/loom.example/*.crt; do '
            'if [ -f "$cert" ]; then cat "$cert"; exit; fi; done',
        )
        return x509.load_pem_x509_certificate(pem.encode()) if pem else None

    try:
        docker("network", "create", prefix)
        network_created = True
        run(
            "backend",
            "--network",
            prefix,
            "--network-alias",
            "loom.example",
            "--network-alias",
            "loom-service.test.svc",
            "-p",
            f"127.0.0.1:{port}:8443",
            "-v",
            f"{tmp_path}:/test:ro",
            "python:3.11-alpine",
            "python",
            "/test/backend.py",
        )
        network = "container:" + prefix + "-backend"
        run("web", "--network", network, image)
        run(
            "pebble",
            "--network",
            network,
            "-v",
            f"{tmp_path}:/test:ro",
            "-e",
            "PEBBLE_VA_NOSLEEP=1",
            "ghcr.io/letsencrypt/pebble:2.10.1",
            "-config",
            "/test/pebble.json",
        )
        run(
            "tls",
            "--network",
            network,
            "-v",
            f"{tmp_path}:/test:ro",
            "-v",
            f"{data}:/data",
            "-e",
            "XDG_CONFIG_HOME=/data/config",
            "--entrypoint",
            "/usr/bin/caddy",
            image,
            "run",
            "--config",
            "/test/caddy.json",
        )
        _wait(lambda: request()[0] == 200)
        assert peer_serial() == bootstrap  # The valid bootstrap serves the initial transition.
        status, headers, _ = request("/nested/spa/path")
        assert status == 200 and headers["Content-Security-Policy"].startswith("default-src 'self'")
        assert headers["X-Content-Type-Options"] == "nosniff"
        assert headers["Strict-Transport-Security"] == "max-age=31536000"
        # Follow the real published entry through Caddy and nginx. Caddy adds
        # Via; nginx's default gzip_proxied=off used to disable compression here.
        _, _, shell = request()
        entry = re.search(rb'<script[^>]+src="([^"]+)"', shell)
        assert entry is not None
        asset = urljoin("/", entry.group(1).decode())
        status, _, plain = request(asset, headers={"Accept-Encoding": "identity"})
        assert status == 200
        status, compressed_headers, compressed = request(asset, headers={"Accept-Encoding": "gzip"})
        assert status == 200 and compressed_headers.get("Content-Encoding") == "gzip"
        assert gzip.decompress(compressed) == plain and len(compressed) < len(plain)
        assert "Accept-Encoding" in compressed_headers["Vary"]
        status, _, payload = request(
            "/api/v1/echo?key=a%2Fb", b"hello", {"X-Forwarded-For": "spoofed"}
        )
        echo = json.loads(payload)
        assert status == 200 and echo["path"] == "/api/v1/echo?key=a%2Fb" and echo["size"] == 5
        assert echo["headers"]["Host"] == "loom.example"
        assert echo["headers"]["X-Forwarded-Proto"] == "https"
        assert echo["headers"]["X-Forwarded-For"] != "spoofed"
        conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=15, context=context)
        try:
            conn.request(
                "POST", "/api/v1/echo", b"x" * (100 * 1024 * 1024 + 1), {"Host": "loom.example"}
            )
        except (BrokenPipeError, ConnectionResetError):
            pass  # A proxy may close the upload immediately after the size limit.
        assert conn.getresponse().status == 413
        conn.close()
        conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=5, context=context)
        started = time.monotonic()
        conn.request("GET", "/api/v1/events", headers={"Host": "loom.example"})
        response = conn.getresponse()
        assert response.readline() == b"data: first\n" and time.monotonic() - started < 1.5
        response.read()
        conn.close()
        first = _wait(managed_certificate).serial_number
        # This mirrors the supported second render/deploy with bootstrap=false.
        # Remove the bootstrap promptly, avoiding CertMagic's inclusive final
        # NotAfter second, which some TLS clients already regard as expired.
        docker("pause", prefix + "-pebble")
        config["apps"]["tls"].pop("certificates")
        _certificate(tmp_path / "tls", "loom.example", -2)  # Expired files must stay unused.
        (tmp_path / "caddy.json").write_text(json.dumps(config))
        docker("restart", prefix + "-tls")
        _wait(lambda: request()[0] == 200)
        assert peer_serial() == first
        docker("unpause", prefix + "-pebble")

        def renewed():
            cert = managed_certificate()
            assert cert is not None
            assert peer_serial() != bootstrap
            return cert.serial_number if cert.serial_number != first else None

        renewed_serial = _wait(renewed, timeout=100)
        docker("pause", prefix + "-pebble")
        docker("restart", prefix + "-tls")
        _wait(lambda: request()[0] == 200)
        assert peer_serial() == renewed_serial  # No ACME service is available on restart.
        print(
            f"TLS smoke: bootstrap={bootstrap:x}, issued={first:x}, renewed={renewed_serial:x}; "
            "no-SNI readiness, SPA security, API headers/query/body limit, SSE, offline restart passed"
        )
    finally:
        for name in reversed(names):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        try:
            remaining = docker(
                "ps", "-a", "--filter", f"name={prefix}-", "--format", "{{.Names}}",
            )
            assert not remaining, f"Refusing TLS storage cleanup with remaining containers: {remaining}"
            # Private directories also prevent the host runner from cleaning its
            # temporary directory. Remove only this fixture's data as its owner,
            # after stopping every writer; keep the storage permissions intact.
            docker(
                "run", "--rm", "--name", prefix + "-cleanup", "--network", "none",
                "-v", f"{data}:/data", "--entrypoint", "sh", image,
                "-c", "rm -rf /data/*",
            )
        finally:
            if network_created:
                docker("network", "rm", prefix)
