"""Installed proof needs authenticated management and actual off-node dump bytes."""
from __future__ import annotations

import asyncio
import hashlib
import io
import json
from uuid import UUID, uuid4

import httpx
import pytest


def test_public_probe_checks_management_readiness_and_auth_without_workload_routes():
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe

    requests = []

    def handle(request):
        requests.append(request)
        if request.url.path.endswith("/health/ready"):
            assert "authorization" not in request.headers
            return httpx.Response(200, json={"status": "ready", "mode": "management", "postgres": "ready", "provisioner": "ready"})
        if request.url.path == "/api/v1/tasks":
            return httpx.Response(404)
        if request.headers.get("authorization") == "Bearer private-admin":
            return httpx.Response(200, json={"items": [], "next_cursor": None})
        return httpx.Response(401, json={"detail": "unauthorized"})

    with ManagementPublicProbe(host="manage.example.com") as probe:
        probe.client.close()
        probe.client = httpx.Client(transport=httpx.MockTransport(handle))
        probe.verify(admin_token="private-admin")
    assert len([request for request in requests if request.headers.get("authorization") == "Bearer private-admin"]) == 1
    assert all(request.method == "GET" for request in requests)


@pytest.mark.parametrize("failure", ["wrong_mode", "worker_missing", "anonymous_allowed", "redirect"])
def test_public_probe_rejects_wrong_service_auth_bypass_or_redirect(failure):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe

    sent_admin = []

    def handle(request):
        sent_admin.append(request.headers.get("authorization") == "Bearer private-admin")
        if failure == "redirect":
            return httpx.Response(302, headers={"Location": "https://foreign.example/token"})
        if request.url.path.endswith("/health/ready"):
            value = {"status": "ready", "mode": "management", "postgres": "ready", "provisioner": "ready"}
            if failure == "wrong_mode":
                value["mode"] = "service"
            if failure == "worker_missing":
                del value["provisioner"]
            return httpx.Response(200, json=value)
        return httpx.Response(200, json={"items": []})

    with ManagementPublicProbe(host="manage.example.com") as probe:
        probe.client.close()
        probe.client = httpx.Client(transport=httpx.MockTransport(handle))
        with pytest.raises(ManagementInstallError):
            probe.verify(admin_token="private-admin")
    assert not any(sent_admin)


@pytest.mark.parametrize('worker', ['application_provisioner', 'provisioner', 'missing', 'not-ready'])
def test_application_upgrade_requires_new_worker_health_and_protected_application_routes(worker):
    from fastapi import FastAPI, HTTPException
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe

    from loom_service.routes.applications import router
    from loom_service.routes.environments import management_principal

    requests = []
    app = FastAPI()
    app.include_router(router, prefix='/api/v1')

    async def unauthenticated():
        raise HTTPException(status_code=401)

    app.dependency_overrides[management_principal] = unauthenticated

    async def application_request(path):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://fixture') as client:
            return await client.get(path)

    def handle(request):
        requests.append(request)
        if request.url.path.endswith('/health/ready'):
            health = {'status': 'ready', 'mode': 'management', 'postgres': 'ready'}
            if worker != 'missing':
                health['application_provisioner' if worker == 'not-ready' else worker] = (
                    'not-ready' if worker == 'not-ready' else 'ready')
            return httpx.Response(200, json=health)
        if request.url.path == '/api/v1/tasks':
            return httpx.Response(404)
        if request.url.path.startswith(('/api/v1/applications', '/api/v1/application-operations')):
            response = asyncio.run(application_request(request.url.path))
            return httpx.Response(response.status_code, content=response.content)
        if request.headers.get('authorization') == 'Bearer private-admin':
            return httpx.Response(200, json={'items': []})
        return httpx.Response(401)

    with ManagementPublicProbe(host='manage.example.com', runtime='applications') as probe:
        probe.client.close()
        probe.client = httpx.Client(transport=httpx.MockTransport(handle))
        if worker == 'application_provisioner':
            probe.verify(admin_token='private-admin')
            assert any(request.url.path == '/api/v1/applications' and 'authorization' not in request.headers
                for request in requests)
            operation, = [request for request in requests if request.url.path.startswith('/api/v1/application-operations/')]
            assert UUID(operation.url.path.rsplit('/', 1)[1]).int and 'authorization' not in operation.headers
        else:
            with pytest.raises(ManagementInstallError):
                probe.verify(admin_token='private-admin')
            assert not any('authorization' in request.headers for request in requests)


class Objects:
    def __init__(self, payload, checksum):
        self.payload = payload
        self.checksum = checksum
        self.calls = []
        self.body = io.BytesIO(payload)

    def head_object(self, **kwargs):
        self.calls.append(("head", kwargs))
        return {"ContentLength": len(self.payload), "ETag": '"object-etag"', "Metadata": {"Sha256": self.checksum}}

    def get_object(self, **kwargs):
        self.calls.append(("get", kwargs))
        return {"ContentLength": len(self.payload), "ETag": '"object-etag"', "Body": self.body}


@pytest.fixture
def backup():
    payload = b"PGDMP" + b"real-streamed-test-content" * 100
    checksum = hashlib.sha256(payload).hexdigest()
    return Objects(payload, checksum), {"backup_key": "loom-nebius-management/2026/09/24/180000-" + checksum[:12] + ".dump",
                                       "sha256": checksum, "bytes": len(payload)}


def test_backup_proof_hashes_streamed_object_and_pins_object_version(backup):
    from scripts.ops.nebius_management_proofs import verify_backup_object

    objects, report = backup
    job = str(uuid4())
    proof = verify_backup_object(client=objects, bucket="management-backup", namespace="loom-nebius-management",
                                 job_uid=job, report=report, max_bytes=10000)
    assert proof == {"key": report["backup_key"], "sha256": report["sha256"], "bytes": report["bytes"], "job_uid": job}
    assert objects.calls[1] == ("get", {"Bucket": "management-backup", "Key": report["backup_key"], "IfMatch": '"object-etag"'})
    assert objects.body.closed


@pytest.mark.parametrize("failure", ["foreign_key", "oversized", "digest", "metadata", "not_dump"])
def test_backup_metadata_or_successful_job_alone_cannot_prove_dump(backup, failure):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_proofs import verify_backup_object

    objects, report = backup
    if failure == "foreign_key":
        report["backup_key"] = report["backup_key"].replace("loom-nebius-management/", "foreign/")
    elif failure == "oversized":
        report["bytes"] = 10001
    elif failure == "digest":
        objects.body = io.BytesIO(b"PGDMP" + b"x" * (report["bytes"] - 5))
    elif failure == "metadata":
        objects.checksum = "0" * 64
    else:
        objects.payload = b"not-a-custom-postgres-dump"
        objects.body = io.BytesIO(objects.payload)
        objects.checksum = hashlib.sha256(objects.payload).hexdigest()
        report.update(sha256=objects.checksum, bytes=len(objects.payload))
        report["backup_key"] = "loom-nebius-management/2026/09/24/180000-" + objects.checksum[:12] + ".dump"
    with pytest.raises(ManagementInstallError) as error:
        verify_backup_object(client=objects, bucket="management-backup", namespace="loom-nebius-management",
                             job_uid=str(uuid4()), report=report, max_bytes=10000)
    assert report["backup_key"] not in str(error.value)
    assert error.value.stage == "backup_object"
    if failure in {"foreign_key", "oversized"}:
        assert not objects.calls
    assert "private-admin" not in json.dumps(objects.calls)
