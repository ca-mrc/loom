"""Opt-in request routing; selection and durable dispatch admission are atomic.

The signed connection remains the logical route. Only this trusted hosted,
stateless Chat path may resolve it to another authorized physical connection.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID

import httpx
from fastapi import HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import (
    ProviderConnection,
    ProviderModelCache,
    ProviderRequestPool,
    ProviderRequestPoolMember,
    Trial,
)
from loom.provider_model_preflight import preflight_failure_kind
from loom_llm_gateway.attempt_deadline import GatewayAttemptDeadline
from loom_llm_gateway.dispatch_audit import DispatchAudit, request_dispatch_audit
from loom_llm_gateway.execution_attempt_dispatch import (
    admitted_service_execution_models,
    authorize_trial_execution_dispatch,
)
from loom_llm_gateway.retry import retry_after_seconds
from loom_llm_gateway.routes._facade_common import (
    decrypt_facade_api_key,
    resolve_facade_connection,
)

logger = logging.getLogger(__name__)

_OPENAI_TYPES = frozenset({"openai-compatible", "custom"})
_STATEFUL_FIELDS = frozenset(
    {
        "previous_response_id",
        "conversation",
        "conversation_id",
        "thread_id",
        "assistant_id",
        "file_id",
        "file_ids",
        "vector_store_ids",
        "attachments",
        "store",
        "background",
    }
)


def validate_pool_payload(payload: dict[str, Any]) -> None:
    """Complete client-owned history and ordinary client-side function tools only."""
    extra = payload.get("extra_body")
    if isinstance(extra, dict):
        if "extra_body" in extra:
            raise HTTPException(400, "request pool does not support nested extra_body")
        validate_pool_payload({**payload, **extra, "extra_body": None})
    if payload.get("stream") or _STATEFUL_FIELDS.intersection(payload):
        raise HTTPException(400, "request pool does not support streaming or server-side state")
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise HTTPException(400, "request pool requires complete messages")
    for message in messages:
        if not isinstance(message, dict) or message.get("role") not in {
            "system",
            "developer",
            "user",
            "assistant",
            "tool",
            "function",
        }:
            raise HTTPException(400, "request pool requires complete messages")
        content = message.get("content")
        if isinstance(content, list):
            for part in content:
                # Hosted files/tools can be scoped to the original account.
                if not isinstance(part, dict) or part.get("type") != "text":
                    raise HTTPException(400, "request pool does not support account-scoped content")
    tools = payload.get("tools", [])
    if not isinstance(tools, list) or any(
        not isinstance(tool, dict) or tool.get("type") != "function" for tool in tools
    ):
        raise HTTPException(400, "request pool supports client-side function tools only")


async def prepare_request_pool(
    request: Request,
    ctx: AuthContext,
    connection_id: UUID,
    payload: dict[str, Any],
) -> PoolDispatchAudit | None:
    async with request.app.state.session_factory() as session:
        enabled = await session.scalar(
            select(ProviderRequestPool.enabled).where(
                ProviderRequestPool.connection_id == connection_id,
            )
        )
        if not enabled:
            return None
        if ctx.service_execution_lease_id is None or ctx.trial_id is None:
            raise HTTPException(400, "request pool requires a hosted Terminus2 execution")
        config = await session.scalar(select(Trial.config).where(Trial.id == ctx.trial_id))
        if not isinstance(config, dict) or config.get("agent_name") != "terminus-2":
            raise HTTPException(400, "request pool requires a hosted Terminus2 execution")
        validate_pool_payload(payload)
        model_names = {name for _, name in admitted_service_execution_models(config)}
        if not model_names or payload["model"] not in model_names:
            raise HTTPException(403, "request pool model is not admitted for this execution")
    base = request_dispatch_audit(request, "facade_openai", connection_id)
    if base is None:
        raise HTTPException(403, "request pool requires authenticated dispatch auditing")
    return PoolDispatchAudit(base, model_names)


class PoolDispatchAudit(DispatchAudit):
    """A per-request selector; secrets and selected auth never live on shared clients."""

    def __init__(self, base: DispatchAudit, model_names: set[str]) -> None:
        super().__init__(
            base.state, base.ctx, base.dialect, base.provider_connection_id, base.purpose
        )
        assert base.provider_connection_id is not None
        self.pool_id = base.provider_connection_id
        self.model_names = model_names
        self.row: ProviderConnection | None = None
        self.api_key = ""
        self.attempt_timeout_sec = 20.0
        self.cooldown_sec = 30.0
        self.quota_group = ""
        self.last_attempt = 1
        self.failover_reason: str | None = None
        self.possible_duplicate_inference = False
        self._keys: list[str] = []
        self._failed_members: set[UUID] = set()
        self._failed_groups: set[str] = set()
        self._deadline: GatewayAttemptDeadline | None = None

    async def _prepare_dispatch(self, session: AsyncSession, values: dict[str, Any]) -> None:
        pool = await session.scalar(
            select(ProviderRequestPool)
            .where(
                ProviderRequestPool.connection_id == self.pool_id,
            )
            .with_for_update()
        )
        if pool is None or not pool.enabled:
            raise HTTPException(409, "request pool changed; retry the request")
        await authorize_trial_execution_dispatch(session, self.ctx)
        assert self.ctx.team_id is not None
        await resolve_facade_connection(
            session,
            self.pool_id,
            self.ctx.team_id,
            supported_types=_OPENAI_TYPES,
            dialect_label="request pool",
            allow_request_pool=True,
        )
        required = set(pool.required_models)
        if not self.model_names.issubset(required):
            raise HTTPException(409, "request pool does not support every admitted execution model")
        members = list(
            (
                await session.scalars(
                    select(ProviderRequestPoolMember)
                    .where(
                        ProviderRequestPoolMember.pool_connection_id == self.pool_id,
                    )
                    .order_by(ProviderRequestPoolMember.ordinal)
                )
            ).all()
        )
        now = datetime.now(UTC)
        for offset in range(len(members)):
            index = (pool.next_member + offset) % len(members)
            member = members[index]
            if (
                not member.enabled
                or member.connection_id in self._failed_members
                or member.quota_group in self._failed_groups
                or (member.cooldown_until and member.cooldown_until > now)
            ):
                continue
            try:
                row = await resolve_facade_connection(
                    session,
                    member.connection_id,
                    self.ctx.team_id,
                    supported_types=_OPENAI_TYPES,
                    dialect_label="request pool",
                    allow_request_pool=member.connection_id == self.pool_id,
                )
            except HTTPException:
                continue  # Revoked/deleted/dialect-incompatible members never receive traffic.
            if row.status != "valid" or (
                row.allowed_models and not required.issubset(row.allowed_models)
            ):
                continue
            cached = list(
                (
                    await session.scalars(
                        select(ProviderModelCache).where(
                            ProviderModelCache.provider_connection_id == row.id,
                            ProviderModelCache.visible.is_(True),
                            ProviderModelCache.upstream_present.is_(True),
                            ProviderModelCache.model_id.in_(required),
                        )
                    )
                ).all()
            )
            if {model.model_id for model in cached} != required or any(
                preflight_failure_kind(
                    model.last_preflight_status,
                    model.last_preflight_error_code,
                    model.last_preflight_http_status,
                )
                == "rejected"
                for model in cached
            ):
                continue
            key = await decrypt_facade_api_key(session, row)
            pool.next_member = (index + 1) % len(members)
            self.row, self.api_key = row, key
            self.quota_group = member.quota_group
            self.last_attempt = values["attempt"]
            self._keys.append(key)
            self.attempt_timeout_sec = pool.attempt_timeout_sec
            self.cooldown_sec = pool.cooldown_sec
            values.update(
                provider_connection_id=row.id,
                pool_connection_id=self.pool_id,
                failover_reason=self.failover_reason,
            )
            return
        raise HTTPException(
            429,
            "provider_request_pool_unavailable",
            headers={"Retry-After": "1"},
        )

    @staticmethod
    def invalid_credential(response: httpx.Response) -> bool:
        if response.status_code not in {401, 403}:
            return False
        try:
            body = response.json()
            error = body.get("error", {}) if isinstance(body, dict) else {}
            code = error.get("code") if isinstance(error, dict) else None
            return code in {
                "invalid_api_key",
                "invalid_token",
                "token_invalid",
                "invalid_credential",
            }
        except ValueError:
            return False

    def response_retryable(self, response: httpx.Response) -> bool:
        return response.status_code in {408, 429, 500, 502, 503, 504} or self.invalid_credential(
            response
        )

    async def _cool_down(self, *, reason: str, response: httpx.Response | None = None) -> None:
        assert self.row is not None and self.state.session_factory is not None
        self.failover_reason = reason
        if reason in {"upstream_timeout", "upstream_504"}:
            self.possible_duplicate_inference = True
        self._failed_members.add(self.row.id)
        if reason == "quota":
            self._failed_groups.add(self.quota_group)
        # A failed shared-state write must never make this request reuse a key.
        timeout = min(1.0, max(0.0, self._deadline.remaining())) if self._deadline else 1.0
        try:
            async with asyncio.timeout(timeout), self.state.session_factory() as session:
                # Same lock order as admission/config edits; shared by replicas.
                await session.scalar(
                    select(ProviderRequestPool)
                    .where(
                        ProviderRequestPool.connection_id == self.pool_id,
                    )
                    .with_for_update()
                )
                members = list(
                    (
                        await session.scalars(
                            select(ProviderRequestPoolMember).where(
                                ProviderRequestPoolMember.pool_connection_id == self.pool_id,
                            )
                        )
                    ).all()
                )
                failed = next((m for m in members if m.connection_id == self.row.id), None)
                if failed is None:
                    return
                if reason == "quota":
                    self._failed_groups.add(failed.quota_group)
                delay = max(
                    self.cooldown_sec, retry_after_seconds(response) if response is not None else 0
                )
                now = datetime.now(UTC)
                delay = min(delay, (datetime.max.replace(tzinfo=UTC) - now).total_seconds() - 1)
                until = now + timedelta(seconds=delay)
                for member in members:
                    if member.connection_id == failed.connection_id or (
                        reason == "quota" and member.quota_group == failed.quota_group
                    ):
                        member.cooldown_until = max(member.cooldown_until or until, until)
                        if (
                            reason == "invalid_credential"
                            and member.connection_id == failed.connection_id
                        ):
                            member.enabled = False
                await session.commit()
        except Exception:
            # Do not log driver text/binds, credentials, or response content.
            # Current-request exclusion remains effective if persistence fails.
            logger.warning("request_pool_cooldown_unavailable pool_connection_id=%s", self.pool_id)

    async def send(
        self,
        operation: Any,
        *,
        deadline: GatewayAttemptDeadline | None,
        attempt: int = 1,
    ) -> Any:
        self._deadline = deadline
        try:
            response = await super().send(operation, deadline=deadline, attempt=attempt)
        except httpx.TimeoutException:
            await self._cool_down(reason="upstream_timeout")
            raise httpx.ReadTimeout("request pool upstream timed out") from None
        except httpx.HTTPError:
            await self._cool_down(reason="transport_error")
            raise httpx.ConnectError("request pool upstream transport failure") from None
        if self.response_retryable(response):
            reason = (
                "invalid_credential"
                if self.invalid_credential(response)
                else "quota"
                if response.status_code == 429
                else f"upstream_{response.status_code}"
            )
            await self._cool_down(reason=reason, response=response)
        return response

    def redact(self, text: str) -> str:
        for key in self._keys:
            text = text.replace(key, "[REDACTED]")
        return text

    def attribution(self) -> dict[str, Any]:
        return {
            "pool_connection_id": str(self.pool_id),
            "provider_connection_id": str(self.row.id) if self.row else None,
            "failover_reason": self.failover_reason,
            "possible_duplicate_inference": self.possible_duplicate_inference,
        }

    def redact_log(self, log: dict[str, Any]) -> dict[str, Any]:
        return cast(dict[str, Any], json.loads(self.redact(json.dumps(log))))
