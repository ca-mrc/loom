"""Dedicated bounded participant HTTPS transport; never retry an uncertain write.

The caller persists its request before I/O and reconciles using the same identity.
No database session, credentials in bodies, raw manifest, or legacy fallback.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import urlsplit

import httpx
from pydantic import Field, TypeAdapter

from loom.nebius_pool_allocation import PoolNodeAllocationRequestV1, PoolNodeAllocationV1
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import (
    MAX_POOL_REQUEST_BYTES,
    PoolActivationV1,
    PoolReceiptV1,
    PoolRequestActionV1,
    PoolWaitingV1,
)
from loom.nebius_pool_execution_runtime import PoolExecutionRuntimeV1
from loom.nebius_pool_lifecycle import PoolDrainV1, PoolStopV1
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom.nebius_pool_workload import PoolExecutionPrepareV1
from loom.pipeline.keys import canonical_digest
from loom_execution_capacity_collector.control_plane import read_owner_only_secret

PoolResult = PoolReceiptV1 | PoolWaitingV1
PoolOperation = Literal["prepare", "status", "activate", "cancel-unstarted", "stop", "drain", "native-runtime", "execution-runtime", "node-allocation"]
_RESULT: TypeAdapter[PoolResult] = TypeAdapter(Annotated[PoolResult, Field(discriminator="schema_version")])


class PoolRequestUnconfirmedError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("pool request acceptance is unconfirmed; retain the original request")


class PoolClient:
    def __init__(self, *, origin: str, bearer_token_file: Path, timeout_seconds: float,
                 client: httpx.AsyncClient | None = None) -> None:
        parsed = urlsplit(origin)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.path not in {"", "/"} or parsed.query or parsed.fragment
                or type(timeout_seconds) not in {int, float} or not 0 < timeout_seconds <= 60):
            raise ValueError("pool management URL must be a credential-free HTTPS origin")
        self._origin = origin.rstrip("/")
        self._token = read_owner_only_secret(bearer_token_file)
        self._timeout = timeout_seconds
        self._owns_client, self._closed = client is None, False
        self._client = client or httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=False, trust_env=False)

    async def _exchange(self, action: PoolRequestActionV1 | PoolNodeAllocationRequestV1,
                        operation: PoolOperation, body: bytes) -> bytes:
        if self._closed or len(body) > MAX_POOL_REQUEST_BYTES:
            raise PoolRequestUnconfirmedError
        try:
            async with asyncio.timeout(self._timeout):
                async with self._client.stream("POST", f"{self._origin}/internal/pools/v1/{action.pool_id}/{operation}",
                    headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json",
                             "Accept-Encoding": "identity"}, content=body, follow_redirects=False,
                    timeout=self._timeout) as response:
                    if response.status_code != 200 or response.headers.get("content-encoding", "identity") != "identity":
                        raise PoolRequestUnconfirmedError
                    declared = response.headers.get("content-length")
                    if declared is not None and (not declared.isdigit() or int(declared) > MAX_POOL_REQUEST_BYTES):
                        raise PoolRequestUnconfirmedError
                    result = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(result) + len(chunk) > MAX_POOL_REQUEST_BYTES:
                            raise PoolRequestUnconfirmedError
                        result.extend(chunk)
            return bytes(result)
        except (httpx.HTTPError, TimeoutError, ValueError):
            raise PoolRequestUnconfirmedError from None

    @staticmethod
    def _identity(action: PoolRequestActionV1, receipt: PoolResult) -> None:
        if (receipt.pool_id != action.pool_id or receipt.request_key != action.request_key
                or receipt.request_sha256 != action.request_sha256
                or (isinstance(receipt, PoolReceiptV1) and receipt.admission_epoch != action.admission_epoch)):
            raise PoolRequestUnconfirmedError

    async def _post(self, action: PoolRequestActionV1, operation: PoolOperation, body: bytes) -> PoolResult:
        try:
            receipt = _RESULT.validate_json(await self._exchange(action, operation, body))
            self._identity(action, receipt)
            return receipt
        except ValueError:
            raise PoolRequestUnconfirmedError from None

    async def prepare(self, request: PoolExecutionPrepareV1 | PoolTaskImagePrepareV1 | PoolApplicationImagePrepareV1) -> PoolResult:
        parsed: PoolExecutionPrepareV1 | PoolTaskImagePrepareV1 | PoolApplicationImagePrepareV1
        if isinstance(request, PoolExecutionPrepareV1):
            parsed = PoolExecutionPrepareV1.model_validate_json(request.model_dump_json())
        elif isinstance(request, PoolTaskImagePrepareV1):
            parsed = PoolTaskImagePrepareV1.model_validate_json(request.model_dump_json())
        elif isinstance(request, PoolApplicationImagePrepareV1):
            parsed = PoolApplicationImagePrepareV1.model_validate_json(request.model_dump_json())
        else:
            raise ValueError("unsupported pool workload")
        action = PoolRequestActionV1(pool_id=parsed.pool_id, request_key=parsed.key,
            admission_epoch=parsed.admission_epoch,
            request_sha256=canonical_digest(parsed.model_dump(mode="json")).removeprefix("sha256:"))
        return await self._post(action, "prepare", parsed.model_dump_json().encode())

    async def status(self, action: PoolRequestActionV1) -> PoolResult:
        action = PoolRequestActionV1.model_validate_json(action.model_dump_json())
        return await self._post(action, "status", action.model_dump_json().encode())

    async def activate(self, activation: PoolActivationV1) -> PoolReceiptV1:
        activation = PoolActivationV1.model_validate_json(activation.model_dump_json())
        receipt = await self._post(activation.action, "activate", activation.model_dump_json().encode())
        if not isinstance(receipt, PoolReceiptV1) or receipt.phase in {"reserved", "cancelled_unstarted"}:
            raise PoolRequestUnconfirmedError
        return receipt

    async def cancel_unstarted(self, action: PoolRequestActionV1) -> PoolReceiptV1:
        action = PoolRequestActionV1.model_validate_json(action.model_dump_json())
        receipt = await self._post(action, "cancel-unstarted", action.model_dump_json().encode())
        if not isinstance(receipt, PoolReceiptV1) or receipt.phase != "cancelled_unstarted":
            raise PoolRequestUnconfirmedError
        return receipt

    async def close(self) -> None:
        self._closed, self._token = True, ""
        if self._owns_client:
            await self._client.aclose()

    async def _lifecycle(self, body: PoolStopV1 | PoolDrainV1) -> PoolReceiptV1:
        receipt = await self._post(body.action, "stop" if isinstance(body, PoolStopV1) else "drain", body.model_dump_json().encode())
        if (not isinstance(receipt, PoolReceiptV1) or receipt.phase not in {"cleanup_intent", "released"}
                or receipt.reservation_id != body.reservation_id or receipt.plan_sha256 != body.plan_sha256):
            raise PoolRequestUnconfirmedError
        return receipt

    async def stop(self, body: PoolStopV1) -> PoolReceiptV1:
        return await self._lifecycle(PoolStopV1.model_validate_json(body.model_dump_json()))

    async def drain(self, body: PoolDrainV1) -> PoolReceiptV1:
        return await self._lifecycle(PoolDrainV1.model_validate_json(body.model_dump_json()))

    async def native_runtime(self, action: PoolRequestActionV1) -> PoolNativeRuntimeV1:
        action = PoolRequestActionV1.model_validate_json(action.model_dump_json())
        try:
            runtime = PoolNativeRuntimeV1.model_validate_json(
                await self._exchange(action, "native-runtime", action.model_dump_json().encode()))
            self._identity(action, runtime.receipt)
            return runtime
        except ValueError:
            raise PoolRequestUnconfirmedError from None

    async def execution_runtime(self, action: PoolRequestActionV1) -> PoolExecutionRuntimeV1:
        action = PoolRequestActionV1.model_validate_json(action.model_dump_json())
        try:
            runtime = PoolExecutionRuntimeV1.model_validate_json(
                await self._exchange(action, "execution-runtime", action.model_dump_json().encode()))
            self._identity(action, runtime.receipt)
            return runtime
        except ValueError:
            raise PoolRequestUnconfirmedError from None

    async def node_allocation(self, scope: PoolNodeAllocationRequestV1) -> PoolNodeAllocationV1:
        scope = PoolNodeAllocationRequestV1.model_validate_json(scope.model_dump_json())
        try:
            result = PoolNodeAllocationV1.model_validate_json(
                await self._exchange(scope, "node-allocation", scope.model_dump_json().encode()))
            if result.scope != scope:
                raise PoolRequestUnconfirmedError
            return result
        except ValueError:
            raise PoolRequestUnconfirmedError from None
