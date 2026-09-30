"""Participant HTTP uncertainty never retries a write or trusts another receipt."""
from __future__ import annotations

import asyncio
from uuid import uuid4

import httpx
import pytest

from loom.nebius_pool_contract import PoolActivationV1, PoolRequestActionV1
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.pipeline.keys import canonical_digest
from tests.unit.test_nebius_pool_task_image_render import build_inputs


def inputs():
    _, body, _ = build_inputs()
    request = PoolTaskImagePrepareV1.model_validate(body)
    receipt = {"schema_version": "loom.pool-receipt.v1", "reservation_id": str(uuid4()),
        "pool_id": str(request.pool_id), "request_key": request.key.model_dump(mode="json"),
        "request_sha256": canonical_digest(request.model_dump(mode="json")).removeprefix("sha256:"),
        "admission_epoch": request.admission_epoch, "phase": "reserved"}
    return request, receipt


def transport(tmp_path, handler, **changes):
    from loom_execution_actuator.pool_client import PoolClient

    token = tmp_path / "participant-token"
    token.write_text("private-test-credential")
    token.chmod(0o600)
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return PoolClient(origin="https://management.example", bearer_token_file=token,
        timeout_seconds=changes.get("timeout_seconds", 1), client=http), http


@pytest.mark.parametrize("damage", ["pool", "key", "epoch", "digest", "redirect", "timeout", "malformed", "oversize", "compressed"])
async def test_prepare_checks_reply_identity_and_never_retries_or_redirects(tmp_path, damage):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    request, receipt = inputs()
    calls = []

    def handler(incoming):
        calls.append(incoming)
        assert incoming.url.path == f"/internal/pools/v1/{request.pool_id}/prepare"
        assert incoming.headers["Authorization"] == "Bearer private-test-credential"
        assert incoming.content == request.model_dump_json().encode()
        if damage == "redirect":
            return httpx.Response(307, headers={"Location": "https://foreign.example/private-test-credential"})
        if damage == "timeout":
            raise httpx.ReadTimeout("private-test-credential")
        if damage == "malformed":
            return httpx.Response(200, content=b"private-test-credential: not-json")
        if damage == "oversize":
            return httpx.Response(200, content=b" " * (1024 * 1024 + 1))
        if damage == "compressed":
            return httpx.Response(200, content=b"", headers={"Content-Encoding": "gzip"})
        if damage == "pool":
            receipt["pool_id"] = str(uuid4())
        elif damage == "key":
            receipt["request_key"]["generation"] += 1
        elif damage == "epoch":
            receipt["admission_epoch"] += 1
        else:
            receipt["request_sha256"] = "d" * 64
        return httpx.Response(200, json=receipt)

    management, http = transport(tmp_path, handler)
    async with http:
        with pytest.raises(PoolRequestUnconfirmedError) as caught:
            await management.prepare(request)
    assert "private-test-credential" not in str(caught.value)
    assert len(calls) == 1


async def test_waiting_identity_is_checked_and_never_becomes_an_activation_receipt(tmp_path):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    request, receipt = inputs()
    waiting = {"schema_version": "loom.pool-waiting.v1", "phase": "waiting",
        "request_key": receipt["request_key"], "pool_id": receipt["pool_id"],
        "request_sha256": receipt["request_sha256"], "reason": "capacity_unavailable"}
    management, http = transport(tmp_path, lambda _: httpx.Response(200, json=waiting))
    async with http:
        assert (await management.prepare(request)).phase == "waiting"
        action = PoolRequestActionV1(pool_id=request.pool_id, request_key=request.key,
            admission_epoch=request.admission_epoch, request_sha256=receipt["request_sha256"])
        with pytest.raises(PoolRequestUnconfirmedError):
            await management.activate(PoolActivationV1(action=action, not_after=request.deadline_at))
        with pytest.raises(PoolRequestUnconfirmedError):
            await management.cancel_unstarted(action)
        waiting["request_sha256"] = "b" * 64
        with pytest.raises(PoolRequestUnconfirmedError):
            await management.prepare(request)


@pytest.mark.parametrize("kind", ["slow", "overflow"])
async def test_stream_read_has_total_timeout_and_incremental_byte_limit(tmp_path, kind):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    request, _ = inputs()
    visited = []

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            if kind == "slow":
                await asyncio.sleep(1)
            yield b" " * 524288
            yield b" " * 524289
            visited.append("past-limit")
            yield b"private-test-credential"

    management, http = transport(tmp_path, lambda _: httpx.Response(200, stream=Stream()), timeout_seconds=0.02)
    async with http:
        with pytest.raises(PoolRequestUnconfirmedError):
            await management.prepare(request)
    assert not visited


@pytest.mark.parametrize("origin", ["http://management.example", "https://u:p@management.example",
    "https://management.example/path", "https://management.example?x=1", "https://management.example#secret"])
def test_client_rejects_non_origin_urls_before_loading_credentials(tmp_path, origin):
    from loom_execution_actuator.pool_client import PoolClient

    with pytest.raises(ValueError):
        PoolClient(origin=origin, bearer_token_file=tmp_path / "absent", timeout_seconds=1)


@pytest.mark.parametrize("damage", ["pool", "key", "epoch", "digest", "name", "effect", "manifest", "malformed"])
async def test_native_runtime_rejects_cross_request_or_unbound_identity(tmp_path, damage):
    from loom_execution_actuator.pool_client import PoolRequestUnconfirmedError

    request, receipt = inputs()
    action = PoolRequestActionV1(pool_id=request.pool_id, request_key=request.key,
        admission_epoch=request.admission_epoch, request_sha256=receipt["request_sha256"])
    receipt.update(phase="observed", plan_sha256="a" * 64, job_uid=str(uuid4()))
    runtime = {"receipt": receipt, "target_id": request.target_id,
        "namespace": {"name": "loom-build", "uid": str(uuid4())},
        "job_name": "loom-pool-" + receipt["reservation_id"].replace("-", ""),
        "lease_epoch": request.build.expected_lease_epoch + 1,
        "deadline_at": request.deadline_at.isoformat(), "registry_repository": "registry.example/tasks",
        "job_effect_id": str(uuid4())}
    if damage == "pool":
        receipt["pool_id"] = str(uuid4())
    elif damage == "key":
        receipt["request_key"]["generation"] += 1
    elif damage == "epoch":
        receipt["admission_epoch"] += 1
    elif damage == "digest":
        receipt["request_sha256"] = "b" * 64
    elif damage == "name":
        runtime["job_name"] = "foreign-build"
    elif damage == "effect":
        runtime["job_effect_id"] = None
    elif damage == "manifest":
        runtime["job"] = {"spec": {"private-test-credential": "must not be returned"}}
    calls = []

    def handler(incoming):
        calls.append(incoming)
        assert incoming.url.path == f"/internal/pools/v1/{request.pool_id}/native-runtime"
        assert incoming.content == action.model_dump_json().encode()
        return (httpx.Response(200, content=b"private-test-credential: not-json") if damage == "malformed"
                else httpx.Response(200, json=runtime))

    management, http = transport(tmp_path, handler)
    async with http:
        with pytest.raises(PoolRequestUnconfirmedError) as caught:
            await management.native_runtime(action)
    assert "private-test-credential" not in str(caught.value)
    assert len(calls) == 1
