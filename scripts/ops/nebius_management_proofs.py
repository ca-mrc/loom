"""Read-only management authentication and off-node PostgreSQL dump proof.

The protected adapter must bind the route and backup Job/Pod identities before
calling these probes. Dump readback is not a substitute for a restore exercise.
"""
from __future__ import annotations

import hashlib
import json
import re
import ssl
from typing import Any, Literal
from uuid import UUID, uuid4

import httpx
from scripts.ops.nebius_management_install import ManagementInstallError
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_environment_contract import _hostname


class ManagementPublicProbe(ManagementKubernetesTransport):
    error_type = ManagementInstallError

    def __init__(self, *, host: str, runtime: Literal['legacy', 'applications'] = 'legacy'):
        if runtime not in {'legacy', 'applications'}:
            raise ManagementInstallError('management public runtime differs')
        self.runtime = runtime
        super().__init__(api_server="https://" + _hostname(host), ssl_context=ssl.create_default_context())

    def _get(self, path: str, *, token: str | None = None) -> tuple[int, bytes]:
        # Standalone requests never inherit cookies/default auth from a prior
        # successful request. No redirects or environment proxy/auth fallback.
        request = httpx.Request("GET", self.api_server + path, headers={
            "Accept-Encoding": "identity", **({"Authorization": "Bearer " + token} if token else {}),
        }, extensions={"timeout": {"connect": 10.0, "read": 30.0, "write": 10.0, "pool": 10.0}})
        response = self.client.send(request, stream=True, auth=None, follow_redirects=False)
        try:
            if response.headers.get("content-encoding", "identity").lower() != "identity":
                raise ManagementInstallError("management public response encoding differs")
            payload = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                if len(payload) + len(chunk) > 65536:
                    raise ManagementInstallError("management public response exceeds bound")
                payload.extend(chunk)
            return response.status_code, bytes(payload)
        finally:
            response.close()

    def verify(self, *, admin_token: str) -> None:
        try:
            if not admin_token or len(admin_token) > 16384 or re.fullmatch(r"[A-Za-z0-9._~-]+", admin_token) is None:
                raise ValueError()
            code, payload = self._get("/api/v1/health/ready")
            if code != 200 or json.loads(payload) != {
                "status": "ready", "mode": "management", "postgres": "ready",
                "application_provisioner" if self.runtime == 'applications' else "provisioner": "ready",
            }:
                raise ValueError()
            if self._get("/api/v1/environments")[0] not in {401, 403}:
                raise ValueError()
            if self.runtime == 'applications':
                for path in ('/api/v1/applications', '/api/v1/application-operations/' + str(uuid4())):
                    if self._get(path)[0] not in {401, 403}:
                        raise ValueError()
            path = "/api/v1/admin/audit-events?limit=1"
            if self._get(path)[0] not in {401, 403} or self._get(path, token="invalid-" + uuid4().hex)[0] not in {401, 403}:
                raise ValueError()
            if self._get("/api/v1/tasks")[0] != 404:
                raise ValueError()
            code, payload = self._get(path, token=admin_token)
            result = json.loads(payload) if code == 200 else None
            if not isinstance(result, dict) or not isinstance(result.get("items"), list):
                raise ValueError()
        except Exception:
            raise ManagementInstallError("management public readiness or authentication proof failed") from None


def verify_backup_object(*, client: Any, bucket: str, namespace: str, job_uid: str,
                         report: dict[str, Any], max_bytes: int) -> dict[str, Any]:
    """Verify the exact Job's reported dump bytes, bounded by database allowance.

The caller owns an explicit-credential, HTTPS, no-retry S3 client for the separate
backup bucket. Only HEAD/GET are used here; no upload, deletion or list fallback.
"""
    try:
        if str(UUID(job_uid)) != job_uid or UUID(job_uid).int == 0:
            raise ValueError()
        if (set(report) != {"backup_key", "sha256", "bytes"} or type(report["bytes"]) is not int
                or type(max_bytes) is not int or not 0 < report["bytes"] <= max_bytes
                or not isinstance(report["sha256"], str) or not re.fullmatch(r"[0-9a-f]{64}", report["sha256"])
                or not isinstance(report["backup_key"], str)
                or not re.fullmatch(re.escape(namespace) + r"/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{6}-"
                                    + report["sha256"][:12] + r"\.dump", report["backup_key"])):
            raise ValueError()
        args = {"Bucket": bucket, "Key": report["backup_key"]}
        head = client.head_object(**args)
        checksums = [value for key, value in head.get("Metadata", {}).items() if key.lower() == "sha256"]
        if (head.get("ContentLength") != report["bytes"] or checksums != [report["sha256"]]
                or not isinstance(head.get("ETag"), str) or not head["ETag"]):
            raise ValueError()
        result = client.get_object(**args, IfMatch=head["ETag"])
        stream = result["Body"]
        try:
            if result.get("ContentLength") != report["bytes"] or result.get("ETag") != head["ETag"]:
                raise ValueError()
            checksum, size, magic = hashlib.sha256(), 0, b""
            while chunk := stream.read(65536):
                if not isinstance(chunk, bytes) or size + len(chunk) > report["bytes"]:
                    raise ValueError()
                magic = (magic + chunk)[:5] if len(magic) < 5 else magic
                size += len(chunk)
                checksum.update(chunk)
            if size != report["bytes"] or magic != b"PGDMP" or checksum.hexdigest() != report["sha256"]:
                raise ValueError()
        finally:
            stream.close()
        after = client.head_object(**args)
        if any(after.get(key) != head.get(key) for key in ("ETag", "VersionId", "ContentLength", "Metadata")):
            raise ValueError()
        return {"job_uid": job_uid, "key": report["backup_key"], "sha256": report["sha256"], "bytes": report["bytes"]}
    except Exception:
        raise ManagementInstallError("management off-node backup readback failed", stage="backup_object") from None
