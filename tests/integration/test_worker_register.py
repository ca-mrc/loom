"""Bug 5 regression: POST /workers/register validates `capabilities`
against the Capabilities Pydantic model (extra=forbid), so garbage like
typo'd OS or non-list payload is rejected at the boundary."""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token, Worker
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def worker_token(postgres_url: str) -> Iterator[str]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    raw = f"w_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:report"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        yield raw
    finally:
        with session_factory() as s:
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str, worker_token: str):
    for k, v in {
        "LOOM_LOCAL_EXECUTION": "1",
        "LOOM_ENV": "development",
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


_VALID_CAP = {
    "os": "linux",
    "gpu_vendor": "none",
    "network_policies": ["public"],
    "dynamic_network_policy": False,
    "mounted_fs": False,
    "resource_modes": ["auto"],
}


def test_register_with_valid_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "host-1",
                "version": "0.1",
                "capabilities": [_VALID_CAP],
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert "worker_id" in body
        assert body["heartbeat_interval_sec"] > 0


def test_register_persists_worker_capacity_and_pool(
    app,
    worker_token,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "local-worker-7",
                "version": "0.1",
                "capabilities": [_VALID_CAP],
                "max_concurrent": 10,
                "pool_name": "local",
            },
        )
        assert r.status_code == 200, r.text
        worker_id = r.json()["worker_id"]

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT max_concurrent, pool_name "
                    "FROM workers WHERE id = :worker_id"
                ),
                {"worker_id": worker_id},
            ).one()
    finally:
        engine.dispose()

    assert row[0] == 10
    assert row[1] == "local"


@pytest.mark.parametrize("case", ["x86_64", "arm64", "no-reader", "wrong-digest"])
def test_ordinary_trial_only_registration_keeps_measured_signed_reader_identity(app, worker_token, case):
    from loom.models.worker_capabilities import WorkerCapabilitySnapshotV1

    arch = "arm64" if case == "arm64" else "x86_64"
    snapshot = WorkerCapabilitySnapshotV1.model_validate(dict(schema_version="loom.worker-capabilities.v1",
        cpu_arch=arch, cpu_cores=2, memory_bytes=4096, scratch_bytes=4096,
        network_profiles=["gateway", "none"], container_runtime_features=[] if case == "no-reader" else ["task-image-execution-v2"],
        gpu_devices=[], input_cache_capacity_bytes=0, input_cache_reserved_bytes=0, input_cache_ready_bytes=0))
    with TestClient(app) as client:
        response = client.post("/workers/register", headers={"Authorization": f"Bearer {worker_token}"}, json=dict(
            hostname="ordinary-worker", version="test", capabilities=[dict(_VALID_CAP, cpu_arch=arch, network_policies=["public", "allowlist", "no-network"])],
            supported_work_kinds=["trial"], pool_name="local", max_concurrent=2,
            capability_snapshot=snapshot.model_dump(mode="json"), capability_snapshot_digest="sha256:" + "0" * 64 if case == "wrong-digest" else snapshot.digest))
    if case in {"no-reader", "wrong-digest"}:
        assert response.status_code in {400, 409}
    else:
        assert response.status_code == (400 if case == "arm64" else 200), response.text
        if case == "arm64":
            assert "x86_64 only" in response.json()["detail"]
            return
        assert response.json()["supported_work_kinds"] == ["trial"]
        assert response.json()["capability_snapshot_digest"] == snapshot.digest


def test_register_persists_closed_input_cache_capacity_snapshot(
    app,
    worker_token,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        response = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "cache-worker",
                "version": "0.1",
                "capabilities": [_VALID_CAP],
                "supported_work_kinds": ["trial", "execution_attempt"],
                "input_cache_capacity_bytes": 1_649_267_441_664,
                "input_cache_reserved_bytes": 4096,
                "input_cache_ready_bytes": 8192,
            },
        )
        assert response.status_code == 200, response.text
        worker_id = response.json()["worker_id"]

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT input_cache_capacity_bytes,input_cache_reserved_bytes,"
                    "input_cache_ready_bytes FROM workers WHERE id=:worker_id"
                ),
                {"worker_id": worker_id},
            ).one()
    finally:
        engine.dispose()

    assert tuple(row) == (1_649_267_441_664, 4096, 8192)


def test_register_rejects_partial_or_overcommitted_cache_snapshot(
    app, worker_token
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        partial = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "capabilities": [_VALID_CAP],
                "input_cache_capacity_bytes": 100,
            },
        )
        overcommitted = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "capabilities": [_VALID_CAP],
                "input_cache_capacity_bytes": 100,
                "input_cache_reserved_bytes": 101,
                "input_cache_ready_bytes": 0,
            },
        )

    assert partial.status_code == 400
    assert overcommitted.status_code == 400


def test_local_gpu_registration_persists_capabilities_without_cluster_allocation(
    app,
    worker_token,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    snapshot = {
        "schema_version": "loom.worker-capabilities.v1",
        "cpu_arch": "x86_64",
        "cpu_cores": 20,
        "memory_bytes": 128 << 30,
        "scratch_bytes": 200 << 30,
        "network_profiles": ["gateway", "none"],
        "container_runtime_features": [
            "egl",
            "loom-secret-tmpfs-v1",
            "nvidia-container-runtime",
        ],
        "gpu_devices": [
            {
                "allocation_id": "local:gpu-0",
                "device_uuid": "GPU-LOCAL",
                "vendor": "nvidia",
                "model": "NVIDIA test GPU",
                "memory_kind": "unified",
                "memory_mb": None,
                "unified_memory_mb": 124_000,
                "nvidia_driver_version": "580.12.0",
                "mig_mode": "disabled",
            }
        ],
        "input_cache_capacity_bytes": 1_649_267_441_664,
        "input_cache_reserved_bytes": 0,
        "input_cache_ready_bytes": 0,
    }
    payload = {
        "hostname": "local-gpu-worker",
        "version": "0.1",
        "capabilities": [
            {
                **_VALID_CAP,
                "cpu_arch": "x86_64",
                "gpu_vendor": "nvidia",
                "network_policies": ["allowlist", "no-network"],
            }
        ],
        "supported_work_kinds": ["trial", "execution_attempt"],
        "max_concurrent": 1,
        "pool_name": "local-gpu",
        "capability_snapshot": snapshot,
    }
    with TestClient(app) as client:
        response = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json=payload,
        )
        assert response.status_code == 200, response.text
        worker_id = response.json()["worker_id"]

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT capability_snapshot_json->>'cpu_arch',"
                    "capability_snapshot_json->'gpu_devices'->0->>'device_uuid',"
                    "capability_snapshot_digest "
                    "FROM workers WHERE id=:worker_id"
                ),
                {"worker_id": worker_id},
            ).one()
    finally:
        engine.dispose()
    assert row[0] == "x86_64"
    assert row[1] == "GPU-LOCAL"
    assert row[2].startswith("sha256:")


def test_register_rejects_missing_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "host-2", "version": "0.1"},
        )
        assert r.status_code == 400
        assert "capabilities" in r.json()["detail"]


def test_register_rejects_empty_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": []},
        )
        assert r.status_code == 400


def test_register_rejects_non_list_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "h", "version": "v",
                "capabilities": "linux",
            },
        )
        assert r.status_code == 400


def test_register_rejects_typo_os(app, worker_token):  # type: ignore[no-untyped-def]
    """Bug 5 regression: a typo'd OS value would silently never match any
    DRF claim queries — must be caught at the boundary."""
    bad = dict(_VALID_CAP, os="lunix")
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": [bad]},
        )
        assert r.status_code == 400
        assert "invalid capabilities" in r.json()["detail"]


def test_register_rejects_extra_keys(app, worker_token):  # type: ignore[no-untyped-def]
    """Bug 5 regression: Capabilities is extra='forbid', so unknown keys
    are caught — protects against future fields drifting silently."""
    bad = dict(_VALID_CAP, hax="yes")
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": [bad]},
        )
        assert r.status_code == 400


def test_register_rejects_arm_without_creating_worker(app, worker_token, postgres_url):
    engine = create_engine(postgres_url)
    with TestClient(app) as client:
        r = client.post("/workers/register", headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "arm-rejected", "version": "0.1",
                  "capabilities": [{**_VALID_CAP, "cpu_arch": "arm64"}]})
    assert r.status_code == 400, r.text
    assert "x86_64 only" in r.text
    with engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM workers WHERE hostname='arm-rejected'")).scalar() == 0
    engine.dispose()


@pytest.mark.parametrize("mainland_proxy", [None, False, True])
def test_register_preserves_optional_mainland_capability(app, worker_token, postgres_url, mainland_proxy):
    capability = dict(_VALID_CAP)
    if mainland_proxy is not None:
        capability["mainland_proxy"] = mainland_proxy
    with TestClient(app) as client:
        response = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "mainland-worker", "version": "test", "capabilities": [capability]},
        )
        assert response.status_code == 200, response.text
        worker_id = response.json()["worker_id"]
    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            capabilities = connection.execute(
                text("SELECT capabilities FROM workers WHERE id=:worker_id"),
                {"worker_id": worker_id},
            ).scalar_one()
    finally:
        engine.dispose()
    if mainland_proxy is True:
        assert capabilities[0]["mainland_proxy"] is True
    else:
        assert "mainland_proxy" not in capabilities[0]
