"""Native issuer -> real Gateway auth -> bounded provider TCP request."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import verify_step_jwt
from loom.db.schema import LlmCall, Trial
from loom_control_plane.service_execution_output import (
    ServiceExecutionBrokerError,
    mint_service_execution_peer_token,
)
from loom_llm_gateway.routes import facade_openai
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


async def test_native_phase_deadline_is_signed_clamped_and_cancels_provider_tcp(
    postgres_url: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    key = "disposable-native-deadline-key-" + "x" * 40
    now = datetime.now(UTC)
    trial_id = None
    provider_calls = 0
    disconnected = asyncio.Event()

    async def provider(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal provider_calls
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            size = next(int(line.split(b":", 1)[1]) for line in headers.split(b"\r\n")
                        if line.lower().startswith(b"content-length:"))
            await reader.readexactly(size)
            provider_calls += 1
            # No response bytes: an actual HTTP transport must be cancelled at
            # the native phase cutoff, rather than its much longer read timeout.
            assert await asyncio.wait_for(reader.read(), 10) == b""
            disconnected.set()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(provider, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    upstream = httpx.AsyncClient(trust_env=False)
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=trial_id, target=target, now=now)
            lease.observed_state = "running"
            trial = await session.get(Trial, trial_id)
            trial.state = "running"
            # The one model a native Trial is admitted with; the Gateway refuses others.
            trial.config = {**trial.config, "agent_model": {"provider": "openai", "name": "fixture", "source": "api"}}
            await session.commit()
            # New phase deadline can outlive a rotating short-lived token.
            phase_end = datetime.now(UTC) + timedelta(seconds=900)
            token, expires, _ = await mint_service_execution_peer_token(
                session, lease=lease, ttl_seconds=480, signing_key=key,
                attempt_deadline_wall_clock=phase_end,
            )
            ctx = verify_step_jwt(token, signing_key=key)
            assert ctx.attempt_deadline_wall_clock == phase_end
            assert expires < phase_end
            # A caller cannot extend the admitted lease or mint an expired cutoff.
            clamped, _, _ = await mint_service_execution_peer_token(
                session, lease=lease, ttl_seconds=480, signing_key=key,
                attempt_deadline_wall_clock=lease.deadline_at + timedelta(hours=1),
            )
            assert verify_step_jwt(clamped, signing_key=key).attempt_deadline_wall_clock == lease.deadline_at
            with pytest.raises(ServiceExecutionBrokerError, match="deadline_elapsed"):
                await mint_service_execution_peer_token(
                    session, lease=lease, ttl_seconds=480, signing_key=key,
                    attempt_deadline_wall_clock=now - timedelta(seconds=1),
                )
            # Old runtimes still receive a bounded signed lease cutoff during rollout.
            fallback, _, _ = await mint_service_execution_peer_token(
                session, lease=lease, ttl_seconds=480, signing_key=key,
            )
            assert verify_step_jwt(fallback, signing_key=key).attempt_deadline_wall_clock == lease.deadline_at
            await session.commit()

        app = FastAPI()
        app.state.session_factory = sessions
        app.state.settings = SimpleNamespace(
            step_jwt_signing_key=SimpleNamespace(get_secret_value=lambda: key),
            legacy_attempt_deadline_compat_sec=0,
            upstream_timeout_sec=200, llm_retry_max_attempts=5,
            llm_retry_base_backoff_sec=0.5, llm_retry_jitter_sec=0,
            llm_retry_max_backoff_sec=8, llm_retry_budget_sec=60,
        )
        app.state.egress_client_pool = SimpleNamespace(get=AsyncMock(return_value=upstream))
        app.include_router(facade_openai.router)
        # Only the external provider configuration/key is disposable. Native
        # signature, live DB authorization, deadline, retry and HTTP send are real.
        monkeypatch.setattr(facade_openai, "resolve_provider_connection_id", lambda *_: uuid4())
        monkeypatch.setattr(facade_openai, "resolve_facade_connection", AsyncMock(return_value=
                            SimpleNamespace(base_url=f"http://127.0.0.1:{port}", provider_type="openai-compatible")))
        monkeypatch.setattr(facade_openai, "decrypt_facade_api_key", AsyncMock(return_value="fixture"))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as client:
            # Start the signed cutoff after fixture construction. Give real DB
            # authorization/dispatch time to connect on a loaded runner, while
            # remaining far below the 200-second upstream read timeout.
            async with sessions() as session:
                end = datetime.now(UTC) + timedelta(seconds=5)
                bounded, _, _ = await mint_service_execution_peer_token(
                    session, lease=lease, ttl_seconds=480, signing_key=key,
                    attempt_deadline_wall_clock=end,
                )
                await session.commit()
            response = await client.post(
                "/openai/v1/chat/completions", headers={"Authorization": f"Bearer {bounded}"},
                json={"model": "fixture", "messages": [{"role": "user", "content": "fixture"}]},
            )
            assert response.status_code == 504, response.text
            assert response.json()["detail"]["reason"] == "attempt_deadline_reached"
            assert provider_calls == 1, "deadline elapsed before the provider connection started"
            await asyncio.wait_for(disconnected.wait(), 2)
            response = await client.post(
                "/openai/v1/chat/completions", headers={"Authorization": f"Bearer {bounded}"},
                json={"model": "fixture", "messages": [{"role": "user", "content": "fixture"}]},
            )
            assert response.status_code == 504
            assert provider_calls == 1  # No retry/replay crosses the phase boundary.
        async with sessions() as session:
            calls = list((await session.scalars(select(LlmCall).where(LlmCall.trial_id == trial_id))).all())
            assert len(calls) == 1
            assert calls[0].provider_extras["_loom_failure_category"] == "attempt_deadline_reached"
            assert calls[0].provider_extras["_loom_usage_status"] == "missing"
            assert calls[0].provider_extras["_loom_raw_provider_log"]["service_execution"] == {
                "lease_id": str(lease.id), "generation": lease.generation,
            }
    finally:
        server.close()
        await server.wait_closed()
        await upstream.aclose()
        if trial_id is not None:
            async with sessions() as session:
                await session.execute(delete(LlmCall).where(LlmCall.trial_id == trial_id))
                await session.commit()
        await engine.dispose()
