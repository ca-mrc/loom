"""One bounded, fixed read-only probe inside the already-running dev API Pod."""
from __future__ import annotations

import base64
import json
import os
import re
import ssl
import stat
import tempfile
from pathlib import Path

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_certificate_gateway import run_private
from scripts.ops.nebius_development_install import DevelopmentInstallError
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

# The installed image need not contain this installer module. Execute only this
# fixed stdlib source; read its own mounted admin key, never pass one on argv.
SERVICE_PROBE = r'''
import http.client, json, sys, tomllib
from pathlib import Path

def get(path, token=None):
    connection = http.client.HTTPConnection("127.0.0.1", 8090, timeout=5)
    try:
        connection.request("GET", path, headers={"Accept-Encoding": "identity",
            **({"Authorization": "Bearer " + token} if token else {})})
        response = connection.getresponse()
        raw = response.read(8193)
        if len(raw) > 8192 or response.getheader("Content-Encoding", "identity") != "identity":
            raise ValueError()
        return response.status, raw
    finally:
        connection.close()

try:
    for token in (None, "loom-dev-invalid-probe-token"):
        if get("/api/v1/health/ready", token)[0] not in (401, 403):
            raise ValueError()
    token = tomllib.loads(Path("/var/run/loom/admin/secrets.toml").read_text())["admin"]["token"]
    status, raw = get("/api/v1/health/ready", token)
    if status != 200 or json.loads(raw) != {"status": "ready", "mode": "api_only",
            "postgres": "ready", "object_store": "ready", "blockers": []}:
        raise ValueError()
    status, raw = get("/api/v1/version")
    if status != 200 or json.loads(raw).get("buildRevision") != sys.argv[1]:
        raise ValueError()
except Exception:
    raise SystemExit(1) from None
print(json.dumps({"status": "development_dependencies_verified", "candidate": sys.argv[1]}))
'''


def probe_private_service(*, kubectl: Path, api_server: str, ssl_context: ssl.SSLContext,
                          token: str, pod_name: str, candidate: str) -> None:
    """Explicit trust and token; no ambient kubeconfig, SSH, proxies or plugins."""
    try:
        if (not kubectl.is_absolute() or kubectl != kubectl.resolve()
                or re.fullmatch(r"loom-service-[a-z0-9-]{1,200}", pod_name) is None
                or re.fullmatch(r"[0-9a-f]{40}", candidate) is None):
            raise ValueError()
        info = kubectl.stat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid not in {0, os.getuid()}
                or info.st_mode & 0o022 or not os.access(kubectl, os.X_OK)):
            raise ValueError()
        with ManagementKubernetesTransport(api_server=api_server, ssl_context=ssl_context, token=token):
            pass  # Validate explicit endpoint, verification mode and bearer syntax.
        ca = "".join(ssl.DER_cert_to_PEM_cert(value) for value in ssl_context.get_ca_certs(binary_form=True))
        if not ca:
            raise ValueError()
        config = {"apiVersion": "v1", "kind": "Config", "current-context": "dev-proof",
            "clusters": [{"name": "qualified", "cluster": {"server": api_server,
                "certificate-authority-data": base64.b64encode(ca.encode()).decode()}}],
            "users": [{"name": "operator", "user": {"token": token}}],
            "contexts": [{"name": "dev-proof", "context": {"cluster": "qualified", "user": "operator",
                "namespace": "loom-dev"}}]}
        with tempfile.TemporaryDirectory(prefix="loom-dev-probe-") as directory:
            path = Path(directory) / "kubeconfig.json"
            private_state._atomic_json(path, config)
            raw = run_private([str(kubectl), "--kubeconfig", str(path), "--context=dev-proof",
                "--request-timeout=10s", "--namespace=loom-dev", "exec", "--pod-running-timeout=10s",
                pod_name, "--container=loom-service", "--", "python", "-c", SERVICE_PROBE, candidate], timeout=45)
        if len(raw) > 4096 or json.loads(raw) != {"status": "development_dependencies_verified", "candidate": candidate}:
            raise ValueError()
    except Exception:
        raise DevelopmentInstallError("development private API dependencies unavailable") from None
