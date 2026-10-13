"""HTTP client for the Control Plane.

Used by the worker to register, claim, heartbeat, PATCH state, and update the
trajectory index. All operations that need crash-safe ownership semantics
are fenced by `worker_id` in the Control Plane's UPDATE WHERE clause
(state PATCH, trajectory index). An explicit stale-worker conflict surfaces
as `False` so callers stop reporting against a trial they no longer own;
semantic 409 conflicts remain typed HTTP errors rather than being mislabeled
as claim fencing.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from math import ceil
from typing import Any, Literal, Protocol
from urllib.parse import quote
from uuid import UUID

import httpx
import jwt

from loom.models.resource_usage import TrialResourceUsageReport
from loom.pipeline.live_preview import LivePreviewRecordV1, validate_preview_jpeg
from loom_task_image_authority.execution_delivery import TaskImageExecutionDelivery
from loom_task_image_authority.execution_grant import _canonical_object
from loom_task_image_authority.execution_refresh import (
    MAX_EXECUTION_DELIVERY_BYTES,
    ExecutionRefreshRequest,
)
from loom_task_image_authority.execution_start import ExecutionStartReceipt, ExecutionStartRequest
from loom_worker.trial_cancellation_watchdog import TrialOwnershipSnapshot

EXECUTOR_WORKER_CREDENTIAL_HEADER = "X-Loom-Executor-Worker-Credential"
_MAX_STEP_TOKEN_TTL_SEC = 30_000
_STEP_TOKEN_DEADLINE_GRACE_SEC = 300


def validate_task_image_execution_origin(base_url: str) -> httpx.URL:
    """Require server-authenticated authority before any trusted-worker token use."""
    origin = httpx.URL(base_url)
    if (
        origin.scheme != "https" or not origin.host
        or origin.userinfo or origin.query or origin.fragment
    ):
        raise ValueError("online execution start requires an authenticated HTTPS origin")
    return origin


def _parse_wall_clock_timestamp(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an RFC 3339 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an RFC 3339 string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class StepTokenGrant:
    """Signed step credential plus its server-authoritative lifetime."""

    token: str
    expires_at: datetime
    attempt_deadline_wall_clock: datetime | None
    agent_attempt_id: UUID | None = None
    step_jwt_id: UUID | None = None
    # Only in-process CLI issuers may construct this; never read from HTTP JSON.
    local_only: bool = False

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> StepTokenGrant:
        token = payload.get("token")
        if not isinstance(token, str) or not token.startswith("loom_step_"):
            raise ValueError("step-token response did not contain a Loom step token")
        expires_at = _parse_wall_clock_timestamp(
            payload.get("expires_at"),
            field_name="expires_at",
        )
        raw_deadline = payload.get("attempt_deadline_wall_clock")
        deadline = (
            _parse_wall_clock_timestamp(
                raw_deadline,
                field_name="attempt_deadline_wall_clock",
            )
            if raw_deadline is not None
            else None
        )
        if deadline is not None and expires_at < deadline + timedelta(
            seconds=_STEP_TOKEN_DEADLINE_GRACE_SEC
        ):
            raise ValueError("step-token response does not cover the attempt deadline")
        raw_attempt_id = payload.get("agent_attempt_id")
        raw_grant_id = payload.get("step_jwt_id")
        attempt_id = UUID(str(raw_attempt_id)) if raw_attempt_id is not None else None
        grant_id = UUID(str(raw_grant_id)) if raw_grant_id is not None else None
        if attempt_id is not None:
            if deadline is None or grant_id is None:
                raise ValueError("step-token response has incomplete agent attempt identity")
            # Consistency check only: workers do not own the signing key. Gateway
            # verifies the signature before treating these claims as authority.
            try:
                claims = jwt.decode(token[len("loom_step_") :], options={"verify_signature": False})
                if (
                    claims.get("agent_attempt_id") != str(attempt_id)
                    or claims.get("jti") != str(grant_id)
                    or claims.get("subject_kind") != "trial"
                    or not claims.get("trial_id")
                    or claims.get("execution_attempt_id") is not None
                    or _parse_wall_clock_timestamp(
                        claims.get("attempt_deadline_wall_clock"),
                        field_name="signed deadline",
                    )
                    != deadline
                    or claims.get("exp") != int(expires_at.timestamp())
                ):
                    raise ValueError("step-token response metadata does not match token claims")
            except jwt.PyJWTError as exc:
                raise ValueError("step-token response has malformed identity token") from exc
        return cls(
            token=token,
            expires_at=expires_at,
            attempt_deadline_wall_clock=deadline,
            agent_attempt_id=attempt_id,
            step_jwt_id=grant_id,
        )


def _validate_step_token_deadline_request(
    *,
    attempt_deadline_wall_clock: datetime,
    ttl_sec: int,
    now: datetime | None = None,
) -> datetime:
    if not isinstance(attempt_deadline_wall_clock, datetime):
        raise ValueError("attempt deadline must be timezone-aware wall-clock time")
    if (
        attempt_deadline_wall_clock.tzinfo is None
        or attempt_deadline_wall_clock.utcoffset() is None
    ):
        raise ValueError("attempt deadline must be timezone-aware wall-clock time")
    deadline = attempt_deadline_wall_clock.astimezone(UTC)
    observed_at = datetime.now(UTC) if now is None else now.astimezone(UTC)
    remaining_sec = (deadline - observed_at).total_seconds()
    if remaining_sec <= 0:
        raise ValueError("attempt deadline has already elapsed")
    required_ttl_sec = ceil(remaining_sec) + _STEP_TOKEN_DEADLINE_GRACE_SEC
    if required_ttl_sec > _MAX_STEP_TOKEN_TTL_SEC:
        raise ValueError("attempt deadline requires a step-token TTL above 30000 seconds")
    if ttl_sec < required_ttl_sec:
        raise ValueError("step-token TTL does not cover attempt deadline plus 300 seconds")
    if ttl_sec > _MAX_STEP_TOKEN_TTL_SEC:
        raise ValueError("step-token TTL exceeds 30000 seconds")
    return deadline


class StepTokenClient(Protocol):
    async def mint_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
    ) -> str: ...

    async def mint_attempt_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
        attempt_deadline_wall_clock: datetime,
        agent_attempt_id: UUID | None = None,
    ) -> StepTokenGrant: ...


class ExecutionAttemptStepTokenClient(Protocol):
    async def mint_execution_attempt_step_token(
        self,
        *,
        team_id: UUID,
        execution_attempt_id: UUID,
        step_id: str,
        ttl_sec: int,
        claim: ExecutionAttemptClaimHeaders,
    ) -> str: ...


@dataclass(frozen=True)
class ExecutionAttemptClaimHeaders:
    """The bearer-independent fencing identity for one Attempt claim.

    The raw lease token is deliberately kept out of request bodies.  Mutation
    callers supply their own stable ``request_id`` so an HTTP retry reuses the
    server-side idempotency key rather than creating a second operation.
    """

    claim_id: UUID
    lease_epoch: int
    lease_token: str

    def __post_init__(self) -> None:
        if self.lease_epoch <= 0:
            raise ValueError("lease_epoch must be positive")
        if not self.lease_token:
            raise ValueError("lease_token must be non-empty")

    def as_headers(
        self,
        *,
        request_id: UUID | None = None,
        include_lease_token: bool = True,
    ) -> dict[str, str]:
        headers = {
            "X-Loom-Claim-Id": str(self.claim_id),
            "X-Loom-Lease-Epoch": str(self.lease_epoch),
        }
        if include_lease_token:
            headers["X-Loom-Lease-Token"] = self.lease_token
        if request_id is not None:
            headers["X-Loom-Request-Id"] = str(request_id)
        return headers


# Short compatibility name for callers that do not need the subject repeated.
AttemptClaimHeaders = ExecutionAttemptClaimHeaders


@dataclass(frozen=True)
class TaskImageBuildClaim:
    id: UUID
    materialization_key: str
    task_id: str
    task_checksum: str
    cpu_arch: Literal["x86_64", "arm64"]
    task_config: dict[str, Any]
    task_source: str | None
    task_source_provenance: dict[str, Any]
    attempt_count: int
    max_attempts: int
    lease_epoch: int
    lease_expires_at: str
    registry_images: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> TaskImageBuildClaim:
        cpu_arch = str(payload["cpu_arch"])
        if cpu_arch not in {"x86_64", "arm64"}:
            raise ValueError("task image claim cpu_arch must be native")
        lease_epoch = int(payload["lease_epoch"])
        if lease_epoch <= 0:
            raise ValueError("task image claim lease_epoch must be positive")
        return cls(
            id=UUID(str(payload["id"])),
            materialization_key=str(payload["materialization_key"]),
            task_id=str(payload["task_id"]),
            task_checksum=str(payload["task_checksum"]),
            cpu_arch=cpu_arch,  # type: ignore[arg-type]
            task_config=dict(payload["task_config"]),
            task_source=(
                str(payload["task_source"]) if payload.get("task_source") is not None else None
            ),
            task_source_provenance=dict(payload["task_source_provenance"]),
            attempt_count=int(payload["attempt_count"]),
            max_attempts=int(payload["max_attempts"]),
            lease_epoch=lease_epoch,
            lease_expires_at=str(payload["lease_expires_at"]),
            registry_images={
                str(component): str(image)
                for component, image in dict(payload.get("registry_images") or {}).items()
            },
        )


@dataclass
class HttpControlPlaneClient:
    """One-per-worker, long-lived. Inject `_client` in tests for ASGITransport."""

    base_url: str
    token: str
    timeout_sec: float = 30.0
    _client: httpx.AsyncClient | None = None
    executor_worker_credential: str | None = None

    def _http(self) -> tuple[httpx.AsyncClient, bool]:
        """Return (client, owned). owned=True means caller must close it."""
        if self._client is not None:
            return self._client, False
        return (
            httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout_sec,
            ),
            True,
        )

    @property
    def request_headers(self) -> dict[str, str]:
        """Return a fresh header set for this worker's authenticated session."""

        headers = {"Authorization": f"Bearer {self.token}"}
        if self.executor_worker_credential is not None:
            headers[EXECUTOR_WORKER_CREDENTIAL_HEADER] = self.executor_worker_credential
        return headers

    @property
    def _headers(self) -> dict[str, str]:
        return self.request_headers

    async def register(
        self,
        *,
        hostname: str,
        version: str,
        capabilities: list[dict[str, Any]],
        max_concurrent: int = 1,
        pool_name: str = "default",
        supported_work_kinds: Sequence[str] | None = None,
        capability_snapshot_digest: str | None = None,
        input_cache_capacity_bytes: int | None = None,
        input_cache_reserved_bytes: int | None = None,
        input_cache_ready_bytes: int | None = None,
        capability_snapshot: Mapping[str, Any] | None = None,
        executor_worker_credential: str | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "hostname": hostname,
            "version": version,
            "capabilities": capabilities,
            "max_concurrent": max_concurrent,
            "pool_name": pool_name,
        }
        # Omission is intentional rolling-upgrade compatibility: old workers
        # continue to register as Trial-only with the byte-compatible body.
        if supported_work_kinds is not None:
            payload["supported_work_kinds"] = list(supported_work_kinds)
        if capability_snapshot_digest is not None:
            payload["capability_snapshot_digest"] = capability_snapshot_digest
        if capability_snapshot is not None:
            payload["capability_snapshot"] = dict(capability_snapshot)
        cache_values = (
            input_cache_capacity_bytes,
            input_cache_reserved_bytes,
            input_cache_ready_bytes,
        )
        if any(value is not None for value in cache_values):
            if any(value is None for value in cache_values):
                raise ValueError("input cache registration fields must be supplied together")
            payload.update(
                input_cache_capacity_bytes=input_cache_capacity_bytes,
                input_cache_reserved_bytes=input_cache_reserved_bytes,
                input_cache_ready_bytes=input_cache_ready_bytes,
            )
        client, owned = self._http()
        try:
            if executor_worker_credential is not None:
                if (
                    self.executor_worker_credential is not None
                    and self.executor_worker_credential != executor_worker_credential
                ):
                    raise ValueError(
                        "executor worker credential cannot change after client creation"
                    )
                self.executor_worker_credential = executor_worker_credential
            r = await client.post(
                "/workers/register",
                headers=self._headers,
                json=payload,
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def claim(
        self,
        *,
        worker_id: UUID,
        caps: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        client, owned = self._http()
        try:
            r = await client.post(
                "/trials/claim",
                headers=self._headers,
                json={"worker_id": str(worker_id), "caps": caps},
            )
            if r.status_code == 204:
                return None
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def refresh_task_image_execution(self, request: ExecutionRefreshRequest) -> TaskImageExecutionDelivery:
        """Fetch fresh signed evidence, without consuming or retrying a start."""
        request = ExecutionRefreshRequest.model_validate(request.model_dump(mode="json", by_alias=True, exclude_none=True))
        origin = validate_task_image_execution_origin(self.base_url)
        client, owned = self._http()
        try:
            if str(client.base_url).rstrip("/") != str(origin).rstrip("/"):
                raise ValueError("execution refresh client origin differs from configuration")
            async with client.stream(
                "POST", f"/trials/{request.previous.claim.trial_id}/task-image/refresh",
                headers=self.request_headers, json=request.model_dump(mode="json", by_alias=True, exclude_none=True), follow_redirects=False,
            ) as response:
                response.raise_for_status()
                if response.status_code != 200:
                    raise ValueError("execution refresh did not return signed delivery")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body) + len(chunk) > MAX_EXECUTION_DELIVERY_BYTES:
                        raise ValueError("execution refresh delivery exceeds byte ceiling")
                    body.extend(chunk)
                _canonical_object(bytes(body), MAX_EXECUTION_DELIVERY_BYTES)
                return TaskImageExecutionDelivery.model_validate_json(bytes(body))
        finally:
            if owned:
                await client.aclose()

    async def consume_task_image_execution_start(
        self, request: ExecutionStartRequest,
    ) -> ExecutionStartReceipt:
        """Single authenticated attempt; a missing acknowledgement is not replayable.

        The caller owns the end-to-end start deadline, including local evidence
        verification. A receipt is returned only for a fresh committed consume,
        never a generic 200/idempotency replay. No redirect can select an issuer.
        """
        _ = request.digest
        origin = validate_task_image_execution_origin(self.base_url)
        client, owned = self._http()
        try:
            if str(client.base_url).rstrip("/") != str(origin).rstrip("/"):
                raise ValueError("online execution start client origin differs from configuration")
            async with client.stream(
                "POST", f"/trials/{request.claim.trial_id}/task-image/start",
                headers=self.request_headers,
                json=request.model_dump(mode="json", by_alias=True, exclude_none=True),
                follow_redirects=False,
            ) as response:
                response.raise_for_status()
                if response.status_code != 201:
                    raise ValueError("online start did not acknowledge a fresh consume")
                chunks = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(chunks) + len(chunk) > 8192:
                        raise ValueError("execution start receipt exceeds byte ceiling")
                    chunks.extend(chunk)
                return ExecutionStartReceipt.model_validate(_canonical_object(bytes(chunks), 8192))
        finally:
            if owned:
                await client.aclose()

    async def claim_work(
        self,
        *,
        worker_id: UUID,
        capability_snapshot_digest: str,
        free_slots: int,
        supported_work_kinds: Sequence[str] = ("trial", "execution_attempt"),
    ) -> dict[str, Any] | None:
        """Claim one item from the unified Trial/ExecutionAttempt scheduler.

        ``free_slots`` is only the worker's advisory view.  The Control Plane
        remains authoritative and may return 204 or reject a stale capability
        snapshot even when this process believes that it has capacity.
        """

        if free_slots < 1:
            raise ValueError("free_slots must be positive")
        client, owned = self._http()
        try:
            r = await client.post(
                "/work/claim",
                headers=self._headers,
                json={
                    "capability_snapshot_digest": capability_snapshot_digest,
                    "free_slots": free_slots,
                    "schema_version": "loom.work-claim-request.v1",
                    "supported_work_kinds": list(supported_work_kinds),
                    "worker_id": str(worker_id),
                },
            )
            if r.status_code == 204:
                return None
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def heartbeat_execution_attempt(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="heartbeats",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def get_execution_attempt_control(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        after_seq: int,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.get(
                f"/execution-attempts/{attempt_id}/control",
                headers={**self._headers, **claim.as_headers()},
                params={"after_seq": after_seq},
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def append_execution_attempt_events(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        events: Sequence[Mapping[str, Any]],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="events",
            claim=claim,
            request_id=request_id,
            payload={"events": [dict(event) for event in events]},
        )


    async def report_execution_attempt_started(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="started",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def complete_execution_attempt(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="complete",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def fail_execution_attempt(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="failed",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def acknowledge_execution_attempt_cancel(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_execution_attempt_report(
            attempt_id=attempt_id,
            operation="cancel-ack",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def acknowledge_worker_lost_cleanup(
        self,
        *,
        attempt_id: UUID,
        claim_id: UUID,
        lease_epoch: int,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Report positive cleanup proof after the Attempt lease expired.

        This is the one report that intentionally omits the now-expired raw
        lease token; the worker/node-reaper bearer plus claim id and epoch are
        the server-side fencing inputs.
        """

        if lease_epoch <= 0:
            raise ValueError("lease_epoch must be positive")
        client, owned = self._http()
        try:
            r = await client.post(
                f"/execution-attempts/{attempt_id}/worker-lost-cleanup-ack",
                headers={
                    **self._headers,
                    "X-Loom-Claim-Id": str(claim_id),
                    "X-Loom-Lease-Epoch": str(lease_epoch),
                    "X-Loom-Request-Id": str(request_id),
                },
                json=dict(payload),
            )
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def read_execution_attempt_input_manifest(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        manifest_sha256: str,
        claim: ExecutionAttemptClaimHeaders,
    ) -> httpx.Response:
        path = self._execution_attempt_input_path(
            attempt_id=attempt_id,
            binding_name=binding_name,
            item_key=item_key,
        )
        return await self._get_claim_bound_input(
            path=f"{path}/manifest",
            claim=claim,
            if_match_sha256=manifest_sha256,
        )

    async def read_execution_attempt_input_file(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        file_index: int,
        manifest_sha256: str,
        claim: ExecutionAttemptClaimHeaders,
        range_start: int | None = None,
    ) -> httpx.Response:
        if file_index < 0:
            raise ValueError("file_index must be non-negative")
        path = self._execution_attempt_input_path(
            attempt_id=attempt_id,
            binding_name=binding_name,
            item_key=item_key,
        )
        return await self._get_claim_bound_input(
            path=f"{path}/files/{file_index}",
            claim=claim,
            if_match_sha256=manifest_sha256,
            range_start=range_start,
        )

    @asynccontextmanager
    async def stream_execution_attempt_input_file(
        self,
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
        file_index: int,
        file_sha256: str,
        claim: ExecutionAttemptClaimHeaders,
        range_start: int | None = None,
    ) -> AsyncIterator[httpx.Response]:
        """Stream one claim-bound file with the fixed input timeout contract."""

        if file_index < 0 or (range_start is not None and range_start < 0):
            raise ValueError("file index and range start must be non-negative")
        path = self._execution_attempt_input_path(
            attempt_id=attempt_id,
            binding_name=binding_name,
            item_key=item_key,
        )
        headers = {
            **self._headers,
            **claim.as_headers(),
            "If-Match": self._quoted_etag(file_sha256),
        }
        if range_start is not None:
            headers["Range"] = f"bytes={range_start}-"
        client, owned = self._http()
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0)
        try:
            async with client.stream(
                "GET",
                f"{path}/files/{file_index}",
                headers=headers,
                timeout=timeout,
                follow_redirects=False,
            ) as response:
                response.raise_for_status()
                yield response
        finally:
            if owned:
                await client.aclose()

    async def prepare_final_output(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(f"/api/v1/internal/execution-attempts/{attempt_id}/final-output-sessions"),
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def prepare_checkpoint(
        self,
        *,
        attempt_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=f"/api/v1/internal/execution-attempts/{attempt_id}/checkpoint-sessions",
            claim=claim,
            request_id=request_id,
            payload=payload,
        )

    async def publish_live_preview_frame(
        self,
        *,
        attempt_id: UUID,
        sequence: int,
        step_idx: int,
        jpeg_sha256: str,
        jpeg: bytes,
        claim: ExecutionAttemptClaimHeaders,
    ) -> dict[str, Any]:
        """Publish one bounded frame over the existing Attempt-fenced channel."""

        record = LivePreviewRecordV1(
            schema_version="loom.behavior-stage1-live-preview.v1",
            sequence=sequence,
            step_idx=step_idx,
            jpeg_sha256=jpeg_sha256,
            jpeg_size_bytes=len(jpeg),
        )
        validate_preview_jpeg(jpeg)
        if record.jpeg_sha256 != "sha256:" + hashlib.sha256(jpeg).hexdigest():
            raise ValueError("preview digest does not match bytes")
        idempotency_key = f"live-preview:{attempt_id}:{sequence}"
        client, owned = self._http()
        try:
            response = await client.put(
                f"/api/v1/execution-attempts/{attempt_id}/live-preview/frames/{sequence}",
                headers={
                    **self._headers,
                    **claim.as_headers(),
                    "Idempotency-Key": idempotency_key,
                    "Content-Type": "image/jpeg",
                    "Content-Length": str(len(jpeg)),
                    "If-Match": self._quoted_etag(jpeg_sha256),
                    "X-Loom-Preview-Step": str(step_idx),
                },
                content=jpeg,
                follow_redirects=False,
            )
            response.raise_for_status()
            return self._response_json(response)
        finally:
            if owned:
                await client.aclose()

    async def commit_checkpoint_session(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/commit"
            ),
            claim=claim,
            request_id=request_id,
            payload={"schema_version": "loom.final-output-session-commit.v1"},
            extra_headers={"X-Loom-Upload-Token": upload_token},
        )

    async def renew_checkpoint_token(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/renew"
            ),
            claim=claim,
            payload={"schema_version": "loom.upload-token-renew.v1"},
        )

    async def upload_checkpoint_part(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        file_index: int,
        part_number: int,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
        content_sha256: str,
        content: bytes,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            response = await client.put(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/files/{file_index}/parts/{part_number}",
                headers={
                    **self._headers,
                    **claim.as_headers(request_id=request_id),
                    "X-Loom-Upload-Token": upload_token,
                    "X-Loom-Content-Sha256": content_sha256,
                    "Content-Length": str(len(content)),
                },
                content=content,
            )
            response.raise_for_status()
            return self._response_json(response)
        finally:
            if owned:
                await client.aclose()

    async def complete_checkpoint_file(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        file_index: int,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/files/{file_index}/complete"
            ),
            claim=claim,
            request_id=request_id,
            payload=payload,
            extra_headers={"X-Loom-Upload-Token": upload_token},
        )

    async def abort_checkpoint_session(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        reason: str,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"checkpoint-sessions/{session_id}/abort"
            ),
            claim=claim,
            request_id=request_id,
            payload={"schema_version": "loom.final-output-abort.v1", "reason": reason},
        )

    async def renew_final_output_token(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/renew"
            ),
            claim=claim,
            payload={"schema_version": "loom.upload-token-renew.v1"},
        )

    async def upload_final_output_part(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        file_index: int,
        part_number: int,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
        content_sha256: str,
        content: bytes,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.put(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/files/{file_index}/parts/{part_number}",
                headers={
                    **self._headers,
                    **claim.as_headers(request_id=request_id),
                    "X-Loom-Upload-Token": upload_token,
                    "X-Loom-Content-Sha256": content_sha256,
                    "Content-Length": str(len(content)),
                },
                content=content,
            )
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def complete_final_output_file(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        file_index: int,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/files/{file_index}/complete"
            ),
            claim=claim,
            request_id=request_id,
            payload=payload,
            extra_headers={"X-Loom-Upload-Token": upload_token},
        )

    async def commit_final_output_session(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        upload_token: str,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/commit"
            ),
            claim=claim,
            request_id=request_id,
            payload={"schema_version": "loom.final-output-session-commit.v1"},
            extra_headers={"X-Loom-Upload-Token": upload_token},
        )

    async def abort_final_output_session(
        self,
        *,
        attempt_id: UUID,
        session_id: UUID,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        reason: str,
    ) -> dict[str, Any]:
        return await self._post_internal_claim_bound(
            path=(
                f"/api/v1/internal/execution-attempts/{attempt_id}/"
                f"final-output-sessions/{session_id}/abort"
            ),
            claim=claim,
            request_id=request_id,
            payload={"schema_version": "loom.final-output-abort.v1", "reason": reason},
        )

    async def _post_internal_claim_bound(
        self,
        *,
        path: str,
        claim: ExecutionAttemptClaimHeaders,
        payload: Mapping[str, Any],
        request_id: UUID | None = None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.post(
                path,
                headers={
                    **self._headers,
                    **claim.as_headers(request_id=request_id),
                    **dict(extra_headers or {}),
                },
                json=dict(payload),
            )
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def _post_execution_attempt_report(
        self,
        *,
        attempt_id: UUID,
        operation: str,
        claim: ExecutionAttemptClaimHeaders,
        request_id: UUID,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.post(
                f"/execution-attempts/{attempt_id}/{operation}",
                headers={
                    **self._headers,
                    **claim.as_headers(request_id=request_id),
                },
                json=dict(payload),
            )
            r.raise_for_status()
            return self._response_json(r)
        finally:
            if owned:
                await client.aclose()

    async def _get_claim_bound_input(
        self,
        *,
        path: str,
        claim: ExecutionAttemptClaimHeaders,
        if_match_sha256: str,
        range_start: int | None = None,
    ) -> httpx.Response:
        headers = {
            **self._headers,
            **claim.as_headers(),
            "If-Match": self._quoted_etag(if_match_sha256),
        }
        if range_start is not None:
            if range_start < 0:
                raise ValueError("range_start must be non-negative")
            headers["Range"] = f"bytes={range_start}-"
        client, owned = self._http()
        try:
            # AsyncClient.get buffers the body before returning.  The Response
            # therefore remains readable after an owned one-shot client closes.
            r = await client.get(
                path,
                headers=headers,
                timeout=httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0),
                follow_redirects=False,
            )
            r.raise_for_status()
            return r
        finally:
            if owned:
                await client.aclose()

    @staticmethod
    def _execution_attempt_input_path(
        *,
        attempt_id: UUID,
        binding_name: str,
        item_key: str,
    ) -> str:
        binding = quote(binding_name, safe="")
        item = quote(item_key, safe="")
        return (
            f"/api/v1/internal/execution-attempts/{attempt_id}"
            f"/input-bindings/{binding}/items/{item}"
        )

    @staticmethod
    def _quoted_etag(value: str) -> str:
        if value.startswith('"') and value.endswith('"'):
            return value
        return f'"{value}"'

    @staticmethod
    def _response_json(response: httpx.Response) -> dict[str, Any]:
        if not response.content:
            return {}
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("Control Plane response must be a JSON object")
        return body

    async def heartbeat(self, worker_id: UUID, *, status: str | None = None) -> None:
        client, owned = self._http()
        try:
            kwargs: dict[str, Any] = {"headers": self._headers}
            if status is not None:
                kwargs["json"] = {"status": status}
            r = await client.post(
                f"/workers/{worker_id}/heartbeat",
                **kwargs,
            )
            r.raise_for_status()
        finally:
            if owned:
                await client.aclose()

    async def patch_state(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        state: str,
        failure_reason: str | None = None,
        failure_message: str | None = None,
    ) -> bool:
        """True on accepted, False if the trial is no longer ours (409)."""
        client, owned = self._http()
        try:
            payload: dict[str, Any] = {
                "worker_id": str(worker_id),
                "state": state,
            }
            if failure_reason is not None:
                payload["failure_reason"] = failure_reason
            if failure_message is not None:
                payload["failure_message"] = failure_message
            r = await client.patch(
                f"/trials/{trial_id}/state",
                headers=self._headers,
                json=payload,
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def report_resource_usage(
        self,
        report: TrialResourceUsageReport,
    ) -> bool:
        """Idempotently persist one staged/final resource usage report."""

        client, owned = self._http()
        try:
            response = await client.put(
                f"/trials/{report.trial_id}/resource-usage",
                headers=self._headers,
                json=report.model_dump(mode="json"),
            )
            if response.status_code == 409:
                return False
            response.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def requeue_trial_retry(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        failure_reason: str,
        failure_message: str | None,
        retry_after_sec: float,
    ) -> bool:
        """True on accepted, False if the trial is no longer ours (409)."""
        client, owned = self._http()
        try:
            payload: dict[str, Any] = {
                "worker_id": str(worker_id),
                "failure_reason": failure_reason,
                "retry_after_sec": retry_after_sec,
            }
            if failure_message is not None:
                payload["failure_message"] = failure_message
            r = await client.post(
                f"/trials/{trial_id}/retry",
                headers=self._headers,
                json=payload,
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def pre_start_heartbeat(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
    ) -> bool:
        """True on accepted, False if the trial is no longer in pre-start setup."""
        client, owned = self._http()
        try:
            r = await client.post(
                f"/trials/{trial_id}/pre-start-heartbeat",
                headers=self._headers,
                json={"worker_id": str(worker_id)},
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def get_trial_ownership(self, trial_id: UUID) -> TrialOwnershipSnapshot:
        """Fetch CP ownership fields for the live-worker revoke watchdog (#1491)."""
        client, owned = self._http()
        try:
            r = await client.get(
                f"/trials/{trial_id}",
                headers=self._headers,
            )
            r.raise_for_status()
            body = r.json()
            raw_worker_id = body.get("worker_id")
            worker_id: UUID | None
            if raw_worker_id is None or raw_worker_id == "":
                worker_id = None
            else:
                worker_id = UUID(str(raw_worker_id))
            raw_attempt = body.get("attempt_count")
            attempt_count = (
                int(raw_attempt) if isinstance(raw_attempt, int) else None
            )
            # Operator cancel may stamp cancellation_requested_at before the
            # row reaches state=cancelled; treat that as cancelled for revoke.
            if body.get("cancellation_requested_at") is not None:
                state = "cancelled"
            else:
                state = str(body["state"])
            return TrialOwnershipSnapshot(
                state=state,
                worker_id=worker_id,
                attempt_count=attempt_count,
            )
        finally:
            if owned:
                await client.aclose()

    async def get_trial_state(self, trial_id: UUID) -> str:
        """Fetch the current CP-side state for ``trial_id``.

        Prefer :meth:`get_trial_ownership` for new revoke logic; this helper
        remains for callers that only need the state string. A non-null
        ``cancellation_requested_at`` is reported as ``cancelled``.
        """
        return (await self.get_trial_ownership(trial_id)).state

    async def get_task_bundle(self, task_id: str) -> dict[str, Any]:
        """Fetch full TaskConfig + checksum + source by `task_id`."""
        client, owned = self._http()
        try:
            encoded_task_id = quote(task_id, safe="/")
            r = await client.get(
                f"/tasks/{encoded_task_id}/bundle",
                headers=self._headers,
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def mint_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
    ) -> str:
        """Mint a step-scoped JWT the agent presents to the Gateway as its
        bearer token (Plan 11 + Plan 9 Task 4). Returns the raw
        `loom_step_<jwt>` string."""
        payload = await self._request_step_token_payload(
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            ttl_sec=ttl_sec,
            attempt_deadline_wall_clock=None,
        )
        return str(payload["token"])

    async def mint_attempt_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
        attempt_deadline_wall_clock: datetime,
        agent_attempt_id: UUID | None = None,
    ) -> StepTokenGrant:
        """Mint a credential bound to an absolute attempt wall-clock deadline.

        The preflight rejects lifetimes that cannot fit under the 30,000-second
        token ceiling before any HTTP request is dispatched. Callers must
        translate a monotonic attempt budget to wall-clock time at the attempt
        boundary; process-local monotonic values are never sent on the wire.
        """
        deadline = _validate_step_token_deadline_request(
            attempt_deadline_wall_clock=attempt_deadline_wall_clock,
            ttl_sec=ttl_sec,
        )
        payload = await self._request_step_token_payload(
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            ttl_sec=ttl_sec,
            attempt_deadline_wall_clock=deadline,
            agent_attempt_id=agent_attempt_id,
        )
        grant = StepTokenGrant.from_payload(payload)
        if grant.attempt_deadline_wall_clock != deadline:
            raise ValueError("step-token response changed the attempt deadline")
        if grant.agent_attempt_id != agent_attempt_id:
            raise ValueError("step-token response changed the agent attempt identity")
        if agent_attempt_id is not None:
            claims = jwt.decode(
                grant.token[len("loom_step_") :], options={"verify_signature": False}
            )
            if (
                claims.get("team_id") != str(team_id)
                or claims.get("trial_id") != str(trial_id)
                or claims.get("step_id") != step_id
            ):
                raise ValueError("step-token response changed the requested subject")
        return grant

    async def _request_step_token_payload(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
        attempt_deadline_wall_clock: datetime | None,
        agent_attempt_id: UUID | None = None,
    ) -> Mapping[str, Any]:
        client, owned = self._http()
        try:
            body: dict[str, Any] = {
                "team_id": str(team_id),
                "trial_id": str(trial_id),
                "step_id": step_id,
                "ttl_sec": ttl_sec,
            }
            if attempt_deadline_wall_clock is not None:
                body["attempt_deadline_wall_clock"] = attempt_deadline_wall_clock.isoformat()
            if agent_attempt_id is not None:
                body["agent_attempt_id"] = str(agent_attempt_id)
            r = await client.post(
                "/admin/step-tokens",
                headers=self._headers,
                json=body,
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def mint_execution_attempt_step_token(
        self,
        *,
        team_id: UUID,
        execution_attempt_id: UUID,
        step_id: str,
        ttl_sec: int,
        claim: ExecutionAttemptClaimHeaders,
    ) -> str:
        """Mint the rotating JWT for one fenced Pipeline Attempt.

        The worker bearer remains outside the JSON body, and the lease token is
        confined to the existing claim headers.  The Control Plane verifies
        the exact Attempt, selected gateway network profile, and TTL contract.
        """

        client, owned = self._http()
        try:
            response = await client.post(
                "/admin/step-tokens",
                headers={**self._headers, **claim.as_headers()},
                json={
                    "team_id": str(team_id),
                    "execution_attempt_id": str(execution_attempt_id),
                    "step_id": step_id,
                    "ttl_sec": ttl_sec,
                },
            )
            response.raise_for_status()
            body = response.json()
            return str(body["token"])
        finally:
            if owned:
                await client.aclose()

    async def reclaim_terminus_execution(
        self,
        *,
        trial_id: UUID,
        step_id: str,
        worker_id: UUID | None = None,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            body: dict[str, Any] = {"step_id": step_id}
            if worker_id is not None:
                body["worker_id"] = str(worker_id)
            r = await client.post(
                f"/trials/{trial_id}/terminus/reclaim",
                headers=self._headers,
                json=body,
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def post_episode_checkpoint(
        self,
        *,
        trial_id: UUID,
        execution_id: UUID,
        run_attempt_id: UUID,
        episode: int,
        active_role: str,
        last_call_ordinal: int,
        last_seq: int,
        tmux_session_id: str | None = None,
        attempt_deadline_wall_clock: datetime | None = None,
    ) -> dict[str, Any]:
        client, owned = self._http()
        try:
            r = await client.post(
                f"/trials/{trial_id}/terminus/episode-checkpoints",
                headers=self._headers,
                json={
                    "execution_id": str(execution_id),
                    "run_attempt_id": str(run_attempt_id),
                    "episode": episode,
                    "active_role": active_role,
                    "last_call_ordinal": last_call_ordinal,
                    "last_seq": last_seq,
                    "tmux_session_id": tmux_session_id,
                    "attempt_deadline_wall_clock": (
                        attempt_deadline_wall_clock.isoformat()
                        if attempt_deadline_wall_clock is not None
                        else None
                    ),
                },
            )
            r.raise_for_status()
            return r.json()  # type: ignore[no-any-return]
        finally:
            if owned:
                await client.aclose()

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, Any]]:
        """Fetch every `llm_calls` row the Gateway recorded against this
        trial (Plan 11 amendment A11.1). Called by the worker at finalize
        to project LLMCallEvents into the trial's local trajectory JSONL
        before ATIF projection runs."""
        client, owned = self._http()
        try:
            r = await client.get(
                f"/trials/{trial_id}/llm-calls",
                headers=self._headers,
            )
            r.raise_for_status()
            body = r.json()
            items: list[dict[str, Any]] = body.get("items", [])
            return items
        finally:
            if owned:
                await client.aclose()

    async def patch_trajectory_index(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        **fields: Any,
    ) -> bool:
        return await self._patch_trajectory_index_payload(
            trial_id=trial_id,
            worker_id=worker_id,
            fields=fields,
        )

    async def _patch_trajectory_index_payload(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        fields: dict[str, Any],
    ) -> bool:
        client, owned = self._http()
        try:
            r = await client.patch(
                f"/trials/{trial_id}/trajectory_index",
                headers=self._headers,
                json={"worker_id": str(worker_id), **fields},
            )
            if r.status_code == 409:
                try:
                    conflict = r.json()
                except ValueError:
                    conflict = None
                if isinstance(conflict, Mapping) and conflict.get("detail") == "worker lost claim":
                    return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def patch_output_projection(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        result: dict[str, Any],
        trajectory_index: dict[str, Any],
    ) -> bool:
        return await self._patch_trajectory_index_payload(
            trial_id=trial_id,
            worker_id=worker_id,
            fields={"result": result, **trajectory_index},
        )

    async def append_events(
        self,
        *,
        trial_id: UUID,
        worker_id: UUID,
        events: list[dict[str, Any]],
    ) -> bool:
        """POST a batch of typed trajectory events to the CP
        `trial_events` table (#5 Slice 3a endpoint).

        Returns True on success, False on 409 (worker lost claim — the
        caller's writer should stop trying to flush events for this
        trial). Raises on other HTTP errors so the caller can decide
        whether to drop the batch (MinIO trajectory remains the
        authoritative copy in Slice 3b — Slice 3c flips the SSE
        reader to Postgres, at which point a persistent CP write
        failure starts mattering more).
        """
        if not events:
            return True
        client, owned = self._http()
        try:
            r = await client.post(
                f"/trials/{trial_id}/events",
                headers=self._headers,
                json={
                    "worker_id": str(worker_id),
                    "events": events,
                },
            )
            if r.status_code == 409:
                return False
            r.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    # ─── #317 trial-cache build coordination ────────────────────────

    async def claim_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
        *,
        ttl_sec: float,
    ) -> bool:
        """Atomic claim. Returns True if THIS worker is now the builder."""
        client, owned = self._http()
        try:
            r = await client.post(
                "/api/v1/internal/trial-cache/claim",
                headers=self._headers,
                json={
                    "cache_key": cache_key,
                    "worker_id": str(worker_id),
                    "ttl_sec": ttl_sec,
                },
            )
            r.raise_for_status()
            return bool(r.json().get("i_am_builder"))
        finally:
            if owned:
                await client.aclose()

    async def trial_cache_slot_exists(self, cache_key: str) -> bool:
        """Cheap probe for the waiter loop. True iff a non-expired slot exists."""
        client, owned = self._http()
        try:
            r = await client.get(
                f"/api/v1/internal/trial-cache/{cache_key}",
                headers=self._headers,
            )
            r.raise_for_status()
            return bool(r.json().get("exists"))
        finally:
            if owned:
                await client.aclose()

    async def release_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
    ) -> None:
        """Release our slot. Idempotent: a stolen-by-TTL slot deletes nothing."""
        client, owned = self._http()
        try:
            r = await client.delete(
                f"/api/v1/internal/trial-cache/{cache_key}",
                headers=self._headers,
                params={"worker_id": str(worker_id)},
            )
            r.raise_for_status()
        finally:
            if owned:
                await client.aclose()

    async def refresh_trial_cache_slot(
        self,
        cache_key: str,
        worker_id: UUID,
        *,
        ttl_sec: float,
    ) -> bool:
        """Heartbeat: extend our slot TTL. False = we no longer own the slot."""
        client, owned = self._http()
        try:
            r = await client.post(
                f"/api/v1/internal/trial-cache/{cache_key}/refresh",
                headers=self._headers,
                json={
                    "worker_id": str(worker_id),
                    "ttl_sec": ttl_sec,
                },
            )
            r.raise_for_status()
            return bool(r.json().get("refreshed"))
        finally:
            if owned:
                await client.aclose()

    # ─── durable task-image builder protocol ────────────────────────

    async def claim_task_image_materialization(
        self,
        *,
        builder_id: str,
        cpu_arch: Literal["x86_64", "arm64"],
    ) -> TaskImageBuildClaim | None:
        client, owned = self._http()
        try:
            response = await client.post(
                "/api/v1/internal/task-image-materializations/claim",
                headers=self._headers,
                json={"builder_id": builder_id, "cpu_arch": cpu_arch},
            )
            if response.status_code == 204:
                return None
            response.raise_for_status()
            return TaskImageBuildClaim.from_payload(response.json())
        finally:
            if owned:
                await client.aclose()

    async def _mutate_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        operation: str,
        payload: dict[str, Any],
    ) -> bool:
        client, owned = self._http()
        try:
            response = await client.post(
                f"/api/v1/internal/task-image-materializations/{materialization_id}/{operation}",
                headers=self._headers,
                json=payload,
            )
            if response.status_code == 409:
                return False
            response.raise_for_status()
            return True
        finally:
            if owned:
                await client.aclose()

    async def start_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="start",
            payload={"builder_id": builder_id, "lease_epoch": lease_epoch},
        )

    async def heartbeat_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="heartbeat",
            payload={"builder_id": builder_id, "lease_epoch": lease_epoch},
        )

    async def record_task_image_publication(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        attempt_count: int,
        lease_epoch: int,
        component: str,
        registry_image: str,
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="publication",
            payload={
                "builder_id": builder_id,
                "attempt_count": attempt_count,
                "lease_epoch": lease_epoch,
                "component": component,
                "registry_image": registry_image,
            },
        )

    async def complete_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
        registry_images: Mapping[str, str],
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="complete",
            payload={
                "builder_id": builder_id,
                "lease_epoch": lease_epoch,
                "registry_images": dict(registry_images),
            },
        )

    async def fail_task_image_materialization(
        self,
        *,
        materialization_id: UUID,
        builder_id: str,
        lease_epoch: int,
        retryable: bool,
        failure_reason: str,
        failure_message: str,
        registry_images: Mapping[str, str],
    ) -> bool:
        return await self._mutate_task_image_materialization(
            materialization_id=materialization_id,
            operation="fail",
            payload={
                "builder_id": builder_id,
                "lease_epoch": lease_epoch,
                "retryable": retryable,
                "failure_reason": failure_reason,
                "failure_message": failure_message,
                "registry_images": dict(registry_images),
            },
        )
