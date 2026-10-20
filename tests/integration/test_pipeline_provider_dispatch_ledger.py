from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from starlette.requests import Request

from loom.auth import AuthContext
from loom.db.schema import PipelineProviderDispatch
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.work_protocol import (
    ExecutionCancelAckV1,
    WorkerCleanupProofV1,
    WorkerLostCleanupAckV1,
)
from loom_control_plane.routes import execution_attempts
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.provider_dispatch import (
    ProviderDispatchError,
    ProviderDispatchGrant,
    mark_provider_dispatch_sent,
    release_provider_dispatch_unsent,
    reserve_provider_dispatch,
    settle_provider_dispatch,
    settle_stale_provider_dispatches,
)


@dataclass(frozen=True)
class _LedgerSeed:
    sessions: async_sessionmaker[AsyncSession]
    ids: dict[str, UUID]
    ctx: AuthContext
    now: datetime
    lease_token: str

    async def reserve(self) -> ProviderDispatchGrant:
        request_id = uuid4()
        async with self.sessions() as session:
            return await reserve_provider_dispatch(
                session,
                ctx=self.ctx,
                provider_request_id=request_id,
                request_digest=canonical_digest({"request": str(request_id)}),
                provider_connection_id=self.ids["connection"],
                provider="openai",
                model="gpt-test",
                wire_api="responses",
            )


@asynccontextmanager
async def _seed_ledger(postgres_url: str) -> AsyncIterator[_LedgerSeed]:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    ids = {name: uuid4() for name in ("team", "run", "stage", "attempt", "worker", "connection", "claim")}
    binding_digest = "sha256:" + "b" * 64
    authorization_digest = "sha256:" + "c" * 64
    step_jwt_id = uuid4()
    execution_spec = {
        "container_node": {"network_profile": "gateway"},
        "control_binding_snapshots": [{"snapshot_sha256": binding_digest}],
    }
    execution_spec_digest = canonical_digest(execution_spec)
    binding_snapshot = {
        "schema_version": "loom.recipe-provider-binding.v2",
        "provider_connection_id": str(ids["connection"]),
        "provider": "openai",
        "model": "gpt-test",
        "wire_api": "responses",
    }
    authorization = {"schema_version": "loom.terminalgen-authoring-grant.v1"}
    now = datetime.now(UTC)
    lease_token = "provider-ledger-lease-token" * 2
    try:
        async with sessions() as session, session.begin():
            await session.execute(
                text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
                {"id": ids["team"], "name": f"provider-ledger-{ids['team']}"},
            )
            await session.execute(
                text("""
                    INSERT INTO provider_connections (
                        id,team_id,provider_type,display_name,base_url,upstream_host,
                        encrypted_api_key_ref,status,created_by
                    ) VALUES (
                        :id,:team,'openai-compatible',:name,'https://provider.invalid/v1',
                        'provider.invalid','loom://test/provider','valid','test'
                    )
                """),
                {
                    "id": ids["connection"],
                    "team": ids["team"],
                    "name": str(ids["connection"]),
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_runs (
                        id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                        graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                        resolved_inputs_json,budget_json,request_digest,idempotency_key,state
                    ) VALUES (
                        :id,:team,'ordinary','terminalgen-authoring',1,:digest,
                        '{}'::jsonb,:digest,'{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,
                        :digest,:key,'running'
                    )
                """),
                {
                    "id": ids["run"],
                    "team": ids["team"],
                    "digest": "sha256:" + "a" * 64,
                    "key": f"provider-ledger-{ids['run']}",
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_stage_runs (
                        id,pipeline_run_id,node_key,shard_key,node_kind,state,
                        resolved_execution_spec_json,resolved_execution_spec_bytes,
                        execution_spec_digest,resource_profile_json,resource_profile_digest,
                        provider_connection_ref,resolved_input_bindings_json,
                        resolved_input_bindings_digest,failure_policy,attempt_count,
                        claimed_at,started_at
                    ) VALUES (
                        :id,:run,'generate_card_00','singleton','container','running',
                        CAST(:spec AS jsonb),:spec_bytes,:spec_digest,'{}'::jsonb,:digest,
                        :connection,'[]'::jsonb,:inputs_digest,'fail_run',1,:now,:now
                    )
                """),
                {
                    "id": ids["stage"],
                    "run": ids["run"],
                    "spec": json.dumps(execution_spec),
                    "spec_bytes": canonical_document(execution_spec),
                    "spec_digest": execution_spec_digest,
                    "digest": "sha256:" + "d" * 64,
                    "connection": ids["connection"],
                    "inputs_digest": canonical_digest([]),
                    "now": now,
                },
            )
            await session.execute(
                text("""
                    INSERT INTO workers (
                        id,hostname,version,capabilities,registered_at,last_seen_at,status,auth_token_hash
                    ) VALUES (:id,:hostname,'test','[]'::jsonb,:now,:now,'active',:token_hash)
                """),
                {
                    "id": ids["worker"],
                    "hostname": f"provider-ledger-{ids['worker']}",
                    "token_hash": b"pipeline-ledger-worker-token",
                    "now": now,
                },
            )
            await session.execute(
                text("""
                    INSERT INTO execution_attempts (
                        id,stage_run_id,attempt_number,state,worker_id,claim_id,lease_epoch,
                        lease_token_digest,lease_expires_at,step_jwt_id,
                        execution_authorization_json,execution_authorization_bytes,
                        execution_authorization_digest,queued_at,claimed_at,started_at
                    ) VALUES (
                        :id,:stage,1,'running',:worker,:claim,1,:lease_digest,:expires,:jti,
                        CAST(:authorization AS jsonb),:authorization_bytes,
                        :authorization_digest,:now,:now,:now
                    )
                """),
                {
                    "id": ids["attempt"],
                    "stage": ids["stage"],
                    "worker": ids["worker"],
                    "claim": ids["claim"],
                    "lease_digest": hashlib.sha256(lease_token.encode()).hexdigest(),
                    "expires": now + timedelta(minutes=5),
                    "jti": step_jwt_id,
                    "authorization": json.dumps(authorization),
                    "authorization_bytes": canonical_document(authorization),
                    "authorization_digest": authorization_digest,
                    "now": now,
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_run_control_bindings (
                        id,pipeline_run_id,logical_name,kind,node_key,source_object_id,
                        source_version,snapshot_json,snapshot_bytes,snapshot_sha256,
                        provider_connection_id,provider_request_limit,
                        provider_cost_limit_microusd,per_call_timeout_seconds
                    ) VALUES (
                        :id,:run,'terminalgen_generate_card_00','provider',
                        'generate_card_00',:source,1,CAST(:snapshot AS jsonb),
                        :snapshot_bytes,:digest,:connection,2,10,30
                    )
                """),
                {
                    "id": uuid4(),
                    "run": ids["run"],
                    "source": uuid4(),
                    "snapshot": json.dumps(binding_snapshot),
                    "snapshot_bytes": canonical_document(binding_snapshot),
                    "digest": binding_digest,
                    "connection": ids["connection"],
                },
            )
            await session.execute(
                text("""
                    INSERT INTO pipeline_budget_ledgers (
                        pipeline_run_id,provider_limit_microusd,gpu_limit_seconds,
                        artifact_limit_bytes,stage_run_limit,attempt_limit,wall_deadline_at
                    ) VALUES (:run,13,5,7,1,1,:deadline)
                """),
                {"run": ids["run"], "deadline": now + timedelta(hours=1)},
            )
            await session.execute(
                text("""
                    INSERT INTO execution_attempt_provider_budgets (
                        attempt_id,binding_snapshot_sha256,request_limit,
                        cost_limit_microusd,per_call_timeout_seconds
                    ) VALUES (:attempt,:binding,2,10,30)
                """),
                {"attempt": ids["attempt"], "binding": binding_digest},
            )

        ctx = AuthContext(
            token_hash=b"",
            type="step_session",
            scopes=["llm:call"],
            team_id=ids["team"],
            expires_at=now + timedelta(minutes=5),
            execution_attempt_id=ids["attempt"],
            step_id="generate_card_00",
            provider_connection_id=ids["connection"],
            provider_connection_id_bound=True,
            step_jwt_id=step_jwt_id,
            execution_attempt_lease_epoch=1,
            execution_spec_digest=execution_spec_digest,
            control_binding_snapshot_digest=binding_digest,
            execution_authorization_digest=authorization_digest,
        )
        yield _LedgerSeed(sessions, ids, ctx, now, lease_token)
    finally:
        async with sessions() as session, session.begin():
            await session.execute(
                text("DELETE FROM pipeline_provider_dispatches WHERE execution_attempt_id=:id"),
                {"id": ids["attempt"]},
            )
            await session.execute(
                text("DELETE FROM llm_calls WHERE execution_attempt_id=:id"),
                {"id": ids["attempt"]},
            )
            await session.execute(
                text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": ids["run"]}
            )
            await session.execute(
                text("DELETE FROM provider_connections WHERE id=:id"),
                {"id": ids["connection"]},
            )
            await session.execute(text("DELETE FROM workers WHERE id=:id"), {"id": ids["worker"]})
            await session.execute(text("DELETE FROM teams WHERE id=:id"), {"id": ids["team"]})
        await engine.dispose()


@pytest.fixture
async def ledger_seed(postgres_url: str) -> AsyncIterator[_LedgerSeed]:
    async with _seed_ledger(postgres_url) as seed:
        yield seed


async def test_pipeline_provider_dispatch_is_exactly_once_and_conservatively_settled(
    ledger_seed: _LedgerSeed,
) -> None:
    sessions, ids, ctx, now = (
        ledger_seed.sessions, ledger_seed.ids, ledger_seed.ctx, ledger_seed.now
    )
    unsent_request_id = uuid4()
    unsent_digest = canonical_digest({"ordinal": 0, "prompt": "secret-canary"})
    async with sessions() as session:
        unsent = await reserve_provider_dispatch(
            session,
            ctx=ctx,
            provider_request_id=unsent_request_id,
            request_digest=unsent_digest,
            provider_connection_id=ids["connection"],
            provider="openai",
            model="gpt-test",
            wire_api="responses",
        )
    async with sessions() as session:
        released = await release_provider_dispatch_unsent(
            session,
            ctx=ctx,
            dispatch_id=unsent.dispatch_id,
        )
    assert released.outcome == "not_dispatched"

    request_id = uuid4()
    request_digest = canonical_digest({"ordinal": 1, "prompt": "secret-canary"})
    async with sessions() as session:
        grant = await reserve_provider_dispatch(
            session,
            ctx=ctx,
            provider_request_id=request_id,
            request_digest=request_digest,
            provider_connection_id=ids["connection"],
            provider="openai",
            model="gpt-test",
            wire_api="responses",
        )
    assert grant.reserved_cost_microusd == 10
    async with sessions() as session:
        replay = await reserve_provider_dispatch(
            session,
            ctx=ctx,
            provider_request_id=request_id,
            request_digest=request_digest,
            provider_connection_id=ids["connection"],
            provider="openai",
            model="gpt-test",
            wire_api="responses",
        )
    assert replay == grant
    async with sessions() as session:
        with pytest.raises(ProviderDispatchError, match="request_id_conflict"):
            await reserve_provider_dispatch(
                session,
                ctx=ctx,
                provider_request_id=request_id,
                request_digest=canonical_digest({"ordinal": 999}),
                provider_connection_id=ids["connection"],
                provider="openai",
                model="gpt-test",
                wire_api="responses",
            )
    async with sessions() as session:
        await mark_provider_dispatch_sent(session, ctx=ctx, dispatch_id=grant.dispatch_id)
    response = {"id": "response-1", "usage": {"input_tokens": 3, "output_tokens": 2}}
    async with sessions() as session:
        settled = await settle_provider_dispatch(
            session,
            ctx=ctx,
            dispatch_id=grant.dispatch_id,
            outcome="succeeded",
            usage=TokenUsage(
                input_tokens=3,
                output_tokens=2,
                provider_extras={"reasoning_tokens": 1, "raw_response": "secret-canary"},
            ),
            actual_cost_microusd=4,
            rate_card_hash="test-rate-card",
            request_params={"temperature": 0.1, "prompt": "secret-canary"},
            response_digest=canonical_digest(response),
        )
    assert settled.outcome == "succeeded"
    assert settled.actual_cost_microusd == 4

    async with sessions() as session:
        rows = (
            await session.execute(
                text("""
                    SELECT d.state,d.outcome,d.actual_cost_microusd,d.response_digest,
                           b.requests_reserved,b.requests_settled,
                           b.cost_reserved_microusd,b.cost_settled_microusd,
                           l.provider_reserved_microusd,l.provider_settled_microusd,
                           c.provider_extras::text,c.request_params::text
                      FROM pipeline_provider_dispatches d
                      JOIN execution_attempt_provider_budgets b
                        ON b.attempt_id=d.execution_attempt_id
                      JOIN pipeline_budget_reservations r ON r.id=d.reservation_id
                      JOIN pipeline_budget_ledgers l ON l.pipeline_run_id=r.pipeline_run_id
                      JOIN llm_calls c ON c.id=d.llm_call_id
                     WHERE d.id=:id
                """),
                {"id": grant.dispatch_id},
            )
        ).one()
    assert rows[:10] == (
        "settled",
        "succeeded",
        4,
        canonical_digest(response),
        0,
        1,
        0,
        4,
        0,
        4,
    )
    assert "secret-canary" not in rows[10]
    assert "secret-canary" not in rows[11]
    assert "raw_response" not in rows[10]
    assert "reasoning_tokens" in rows[10]

    stale_request_id = uuid4()
    async with sessions() as session:
        stale = await reserve_provider_dispatch(
            session,
            ctx=ctx,
            provider_request_id=stale_request_id,
            request_digest=canonical_digest({"ordinal": 2}),
            provider_connection_id=ids["connection"],
            provider="openai",
            model="gpt-test",
            wire_api="responses",
        )
    assert stale.reserved_cost_microusd == 6
    async with sessions() as session:
        await mark_provider_dispatch_sent(session, ctx=ctx, dispatch_id=stale.dispatch_id)
    async with sessions() as session:
        recovered = await settle_stale_provider_dispatches(
            session,
            stale_before=now + timedelta(hours=1),
        )
    assert recovered == 1
    async with sessions() as session:
        recovered_row = (
            await session.execute(
                text("""
                    SELECT d.state,d.outcome,d.actual_cost_microusd,
                           d.reserved_cost_microusd,c.provider_extras::text
                    FROM pipeline_provider_dispatches d
                    JOIN llm_calls c ON c.id=d.llm_call_id
                    WHERE d.id=:id
                """),
                {"id": stale.dispatch_id},
            )
        ).one()
    assert recovered_row[:2] == ("settled", "uncertain")
    assert recovered_row.actual_cost_microusd == recovered_row.reserved_cost_microusd
    assert "gateway_crash_after_send" in recovered_row.provider_extras


async def _cutoff(seed: _LedgerSeed) -> datetime:
    async with seed.sessions() as session:
        return (await session.execute(text("SELECT clock_timestamp()"))).scalar_one()


async def _accounting(seed: _LedgerSeed, grant: ProviderDispatchGrant) -> tuple[Any, ...]:
    async with seed.sessions() as session:
        row = (
            await session.execute(
                text("""
                    SELECT d.state,d.outcome,d.actual_cost_microusd,
                           r.state,r.settled_amount,b.requests_reserved,b.requests_settled,
                           b.cost_reserved_microusd,b.cost_settled_microusd,
                           l.provider_reserved_microusd,l.provider_settled_microusd,
                           (SELECT count(*) FROM llm_calls c
                             WHERE c.execution_attempt_id=d.execution_attempt_id)
                      FROM pipeline_provider_dispatches d
                      JOIN pipeline_budget_reservations r ON r.id=d.reservation_id
                      JOIN execution_attempt_provider_budgets b
                        ON b.attempt_id=d.execution_attempt_id
                      JOIN pipeline_budget_ledgers l ON l.pipeline_run_id=r.pipeline_run_id
                     WHERE d.id=:id
                """),
                {"id": grant.dispatch_id},
            )
        ).one()
        return tuple(row)


async def test_stale_unsent_recovery_releases_allowance_and_fences_replays(
    ledger_seed: _LedgerSeed,
) -> None:
    seed = ledger_seed
    grant = await seed.reserve()
    reserved = ("reserved", None, None, "active", None, 1, 0, 10, 0, 10, 0, 0)
    async with seed.sessions() as session:
        assert await settle_stale_provider_dispatches(session, stale_before=seed.now) == 0
    assert await _accounting(seed, grant) == reserved
    stale_before = await _cutoff(seed)
    async with seed.sessions() as session:
        assert await settle_stale_provider_dispatches(session, stale_before=stale_before) == 1
    released = ("settled", "not_dispatched", 0, "released", None, 0, 0, 0, 0, 0, 0, 0)
    assert await _accounting(seed, grant) == released
    async with seed.sessions() as session:
        assert await settle_stale_provider_dispatches(session, stale_before=stale_before) == 0
    async with seed.sessions() as session:
        replay = await release_provider_dispatch_unsent(
            session, ctx=seed.ctx, dispatch_id=grant.dispatch_id
        )
    assert replay.outcome == "not_dispatched" and replay.llm_call_id is None
    async with seed.sessions() as session:
        with pytest.raises(ProviderDispatchError, match="replay_settled_not_dispatched"):
            await mark_provider_dispatch_sent(session, ctx=seed.ctx, dispatch_id=grant.dispatch_id)
    async with seed.sessions() as session:
        with pytest.raises(ProviderDispatchError, match="replay_settled_not_dispatched"):
            await reserve_provider_dispatch(
                session, ctx=seed.ctx, provider_request_id=grant.provider_request_id,
                request_digest=canonical_digest({"request": str(grant.provider_request_id)}),
                provider_connection_id=seed.ids["connection"], provider="openai",
                model="gpt-test", wire_api="responses",
            )
    assert await _accounting(seed, grant) == released
    assert (await seed.reserve()).reserved_cost_microusd == 10


async def test_stale_sweep_skips_sender_lock_and_uses_send_age(
    ledger_seed: _LedgerSeed, monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = ledger_seed
    grant = await seed.reserve()
    stale_before = await _cutoff(seed)
    locked, release = asyncio.Event(), asyncio.Event()
    original_get = AsyncSession.get

    async def gated_get(session: AsyncSession, entity: Any, ident: Any, **kwargs: Any) -> Any:
        result = await original_get(session, entity, ident, **kwargs)
        if entity is PipelineProviderDispatch and ident == grant.dispatch_id:
            locked.set()
            await release.wait()
        return result

    async def send() -> None:
        async with seed.sessions() as session:
            await mark_provider_dispatch_sent(session, ctx=seed.ctx, dispatch_id=grant.dispatch_id)

    monkeypatch.setattr(AsyncSession, "get", gated_get)
    sender = asyncio.create_task(send())
    try:
        await asyncio.wait_for(locked.wait(), timeout=5)
        async with seed.sessions() as session:
            assert await settle_stale_provider_dispatches(session, stale_before=stale_before) == 0
    finally:
        release.set()
        await asyncio.wait_for(sender, timeout=5)
    async with seed.sessions() as session:
        assert await settle_stale_provider_dispatches(session, stale_before=stale_before) == 0
    assert (await _accounting(seed, grant))[:3] == ("dispatched", None, None)
    stale_before = await _cutoff(seed)
    async with seed.sessions() as session:
        assert await settle_stale_provider_dispatches(session, stale_before=stale_before) == 1
    assert await _accounting(seed, grant) == (
        "settled", "uncertain", 10, "settled", 10, 0, 1, 0, 10, 0, 10, 1,
    )


@pytest.mark.parametrize("cleanup", ["cancel", "worker_lost"])
@pytest.mark.parametrize("sent", [False, True], ids=["unsent", "sent"])
async def test_worker_cleanup_preserves_dispatch_owner_until_accounting_converges(
    ledger_seed: _LedgerSeed, monkeypatch: pytest.MonkeyPatch, cleanup: str, sent: bool,
) -> None:
    seed = ledger_seed
    grant = await seed.reserve()
    if sent:
        async with seed.sessions() as session:
            await mark_provider_dispatch_sent(session, ctx=seed.ctx, dispatch_id=grant.dispatch_id)
    before = await _accounting(seed, grant)
    async with seed.sessions() as session, session.begin():
        # Other provider, GPU and artifact reservations retain generic cleanup ownership.
        for kind, amount in (("provider", 3), ("gpu", 5), ("artifact", 7)):
            await session.execute(
                text("""
                    INSERT INTO pipeline_budget_reservations (
                        id,pipeline_run_id,execution_attempt_id,kind,reservation_key,
                        request_digest,reserved_amount,state,metadata_json
                    ) VALUES (:id,:run,:attempt,:kind,:key,:digest,:amount,'active','{}'::jsonb)
                """),
                {"id": uuid4(), "run": seed.ids["run"], "attempt": seed.ids["attempt"],
                 "kind": kind, "key": {
                     "provider": f"provider:{seed.ids['attempt']}:{uuid4()}",
                     "gpu": f"gpu:{seed.ids['attempt']}",
                     "artifact": f"artifact:final:{seed.ids['attempt']}",
                 }[kind], "digest": "sha256:" + "f" * 64,
                 "amount": amount},
            )
        await session.execute(
            text("""
                UPDATE pipeline_budget_ledgers SET provider_reserved_microusd=13,
                    gpu_reserved_seconds=5,artifact_reserved_bytes=7,
                    terminal_cause=:cause,terminal_cause_at=:cancel_at WHERE pipeline_run_id=:run
            """),
            {"run": seed.ids["run"], "cause": "user_cancel" if cleanup == "cancel" else None,
             "cancel_at": seed.now if cleanup == "cancel" else None},
        )
        await session.execute(
            text("""
                UPDATE execution_attempts SET cancellation_requested_at=:cancel_at,
                    lease_expires_at=:expires WHERE id=:attempt
            """),
            {"attempt": seed.ids["attempt"], "cancel_at": seed.now if cleanup == "cancel" else None,
             "expires": seed.now + timedelta(minutes=5) if cleanup == "cancel"
             else seed.now - timedelta(seconds=1)},
        )
    worker_ctx = AuthContext(
        token_hash=b"pipeline-ledger-worker-token", type="worker",
        scopes=["worker:report", "worker:reap"], team_id=seed.ids["team"], expires_at=None,
    )

    async def verify_token(*args: Any, **kwargs: Any) -> AuthContext:
        return worker_ctx

    monkeypatch.setattr(execution_attempts, "verify_bearer_token", verify_token)
    app = FastAPI()
    app.state.session_factory = seed.sessions
    request = Request({"type": "http", "app": app})
    proof = WorkerCleanupProofV1(
        container_absent=True, cgroup_empty=True, network_absent=True, step_jwt_revoked=True,
        runtime_secret_mount_absent=True, scratch_absent=True, outputs_absent=True,
        input_views_absent=True, active_upload_session_ids=[],
    )
    observed_at = datetime.now(UTC)
    request_id = uuid4()

    async def cleanup_attempt(claim_id: UUID) -> dict[str, Any]:
        if cleanup == "cancel":
            return await execution_attempts.report_attempt_cancelled(
                attempt_id=seed.ids["attempt"],
                payload=ExecutionCancelAckV1(
                    outcome="forced", observed_at=observed_at,
                    last_committed_checkpoint_artifact_id=None,
                    teardown_observed=True, resources=proof,
                ),
                request=request, claim_id=claim_id, lease_epoch=1, lease_token=seed.lease_token,
                request_id=request_id, authorization="Bearer test",
            )
        return await execution_attempts.report_worker_lost_cleanup(
            attempt_id=seed.ids["attempt"],
            payload=WorkerLostCleanupAckV1(
                schema_version="loom.worker-lost-cleanup-ack.v1", observer_kind="worker_journal",
                observed_at=observed_at, resources=proof,
            ),
            request=request, claim_id=claim_id, lease_epoch=1,
            request_id=request_id, authorization="Bearer test",
        )

    with pytest.raises(HTTPException) as fenced:
        await cleanup_attempt(uuid4())
    assert fenced.value.detail == "claim_fenced"
    response = await cleanup_attempt(seed.ids["claim"])
    assert response["state"] == ("cancelled" if cleanup == "cancel" else "lost")
    assert await cleanup_attempt(seed.ids["claim"]) == response
    assert await _accounting(seed, grant) == before
    async with seed.sessions() as session:
        generic_rows = (
            await session.execute(
                text("""
                    SELECT r.state,l.gpu_reserved_seconds,l.artifact_reserved_bytes
                      FROM pipeline_budget_reservations r
                      JOIN pipeline_budget_ledgers l USING (pipeline_run_id)
                     WHERE r.execution_attempt_id=:attempt AND r.id<>:dispatch_reservation
                """),
                {"attempt": seed.ids["attempt"], "dispatch_reservation": grant.reservation_id},
            )
        ).all()
    assert [tuple(row) for row in generic_rows] == [("released", 0, 0)] * 3
    # Both ordinary gateway completion and bounded crash recovery must still work
    # after attempt termination. Neither re-authorizes the dead worker to send.
    if sent and cleanup == "cancel":
        async with seed.sessions() as session:
            settlement = await settle_provider_dispatch(
                session, ctx=seed.ctx, dispatch_id=grant.dispatch_id, outcome="succeeded",
                usage=TokenUsage(input_tokens=1, output_tokens=1), actual_cost_microusd=4,
                rate_card_hash="test", request_params={}, response_digest=canonical_digest({"ok": 1}),
            )
        assert settlement.actual_cost_microusd == 4
        expected = ("settled", "succeeded", 4, "settled", 4, 0, 1, 0, 4, 0, 4, 1)
    else:
        stale_before = await _cutoff(seed)
        async with seed.sessions() as session:
            assert await settle_stale_provider_dispatches(session, stale_before=stale_before) == 1
        expected = (
            ("settled", "uncertain", 10, "settled", 10, 0, 1, 0, 10, 0, 10, 1) if sent
            else ("settled", "not_dispatched", 0, "released", None, 0, 0, 0, 0, 0, 0, 0)
        )
    assert await _accounting(seed, grant) == expected
    stale_before = await _cutoff(seed)
    async with seed.sessions() as session:
        assert await settle_stale_provider_dispatches(session, stale_before=stale_before) == 0
    assert await cleanup_attempt(seed.ids["claim"]) == response
    assert await _accounting(seed, grant) == expected


@pytest.mark.parametrize("corruption", ["reservation", "counter"])
async def test_stale_sweep_isolates_corrupt_accounting_and_recovers_healthy_candidate(
    ledger_seed: _LedgerSeed, postgres_url: str, corruption: str, caplog: pytest.LogCaptureFixture,
) -> None:
    bad_seed = ledger_seed
    bad = await bad_seed.reserve()
    async with bad_seed.sessions() as session:
        await mark_provider_dispatch_sent(session, ctx=bad_seed.ctx, dispatch_id=bad.dispatch_id)
    async with bad_seed.sessions() as session, session.begin():
        if corruption == "reservation":
            await session.execute(
                text("UPDATE pipeline_budget_reservations SET state='released',settled_at=clock_timestamp() WHERE id=:id"),
                {"id": bad.reservation_id},
            )
        else:
            await session.execute(
                text("""UPDATE execution_attempt_provider_budgets
                        SET cost_reserved_microusd=0 WHERE attempt_id=:id"""),
                {"id": bad_seed.ids["attempt"]},
            )
    bad_before = await _accounting(bad_seed, bad)
    async with _seed_ledger(postgres_url) as healthy_seed:
        healthy = await healthy_seed.reserve()
        stale_before = await _cutoff(healthy_seed)
        async with healthy_seed.sessions() as session:
            assert await settle_stale_provider_dispatches(
                session, stale_before=stale_before, limit=2,
            ) == 1
        assert await _accounting(healthy_seed, healthy) == (
            "settled", "not_dispatched", 0, "released", None, 0, 0, 0, 0, 0, 0, 0,
        )
        assert await _accounting(bad_seed, bad) == bad_before
        assert str(bad.dispatch_id) in caplog.text
        assert "provider_dispatch_accounting_drift" in caplog.text


async def test_stale_sweep_yields_to_cleanup_attempt_lock_before_accounting(
    ledger_seed: _LedgerSeed, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cleanup ledger lock must not cycle with the LlmCall attempt FK lock."""
    from loom.db.schema import ExecutionAttempt, PipelineBudgetLedger, PipelineBudgetReservation
    from loom_llm_gateway import provider_dispatch

    seed = ledger_seed
    grant = await seed.reserve()
    async with seed.sessions() as session:
        await mark_provider_dispatch_sent(session, ctx=seed.ctx, dispatch_id=grant.dispatch_id)
    generic_id = uuid4()
    async with seed.sessions() as session, session.begin():
        session.add(PipelineBudgetReservation(
            id=generic_id, pipeline_run_id=seed.ids["run"],
            execution_attempt_id=seed.ids["attempt"], kind="gpu",
            reservation_key=f"gpu:{seed.ids['attempt']}", request_digest="sha256:" + "f" * 64,
            reserved_amount=5, state="active", metadata_json={},
        ))
        await session.execute(
            text("UPDATE pipeline_budget_ledgers SET gpu_reserved_seconds=5 WHERE pipeline_run_id=:id"),
            {"id": seed.ids["run"]},
        )
    stale_before = await _cutoff(seed)
    attempt_locked, accounting_locked, release_cleanup = (
        asyncio.Event(), asyncio.Event(), asyncio.Event()
    )
    lock_accounting = provider_dispatch._lock_accounting

    async def observe_accounting(*args: Any, **kwargs: Any) -> Any:
        result = await lock_accounting(*args, **kwargs)
        accounting_locked.set()
        release_cleanup.set()
        return result

    monkeypatch.setattr(provider_dispatch, "_lock_accounting", observe_accounting)

    async def cleanup() -> None:
        # The real cleanup route's lock order, including a generic sibling
        # reservation. A dispatch-owned reservation is never touched here.
        async with seed.sessions() as session, session.begin():
            await session.get(ExecutionAttempt, seed.ids["attempt"], with_for_update=True)
            attempt_locked.set()
            await release_cleanup.wait()
            reservation = await session.get(
                PipelineBudgetReservation, generic_id, with_for_update=True,
            )
            ledger = await session.get(PipelineBudgetLedger, seed.ids["run"], with_for_update=True)
            assert reservation is not None and ledger is not None
            ledger.gpu_reserved_seconds -= reservation.reserved_amount
            reservation.state = "released"
            reservation.settled_at = datetime.now(UTC)

    async def recover() -> int:
        await attempt_locked.wait()
        async with seed.sessions() as session:
            return await settle_stale_provider_dispatches(session, stale_before=stale_before)

    cleanup_task = asyncio.create_task(cleanup())
    recovery_task = asyncio.create_task(recover())
    accounting_task = asyncio.create_task(accounting_locked.wait())
    try:
        done, _ = await asyncio.wait(
            [recovery_task, accounting_task], timeout=5, return_when=asyncio.FIRST_COMPLETED,
        )
        assert done, "sweep neither skipped the live cleanup nor reached accounting"
        release_cleanup.set()
        _, recovered = await asyncio.wait_for(
            asyncio.gather(cleanup_task, recovery_task), timeout=5,
        )
        assert recovered == 0
    finally:
        release_cleanup.set()
        for task in (cleanup_task, recovery_task, accounting_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(cleanup_task, recovery_task, accounting_task, return_exceptions=True)
    async with seed.sessions() as session:
        assert await settle_stale_provider_dispatches(session, stale_before=stale_before) == 1
    assert await _accounting(seed, grant) == (
        "settled", "uncertain", 10, "settled", 10, 0, 1, 0, 10, 0, 10, 1,
    )


@pytest.mark.parametrize("accounting", ["release", "settlement", "sweep"])
async def test_dispatch_accounting_and_terminal_latch_do_not_deadlock(
    ledger_seed: _LedgerSeed, monkeypatch: pytest.MonkeyPatch, accounting: str,
) -> None:
    """Exercise the real run/ledger-before-Attempt cancellation writer."""
    from loom.db.schema import ExecutionAttempt
    from loom.pipeline.budget import TerminalCause
    from loom_pipeline_orchestrator.repository import PipelineRepository, RunLease

    seed = ledger_seed
    grant = await seed.reserve()
    if accounting != "release":
        async with seed.sessions() as session:
            await mark_provider_dispatch_sent(session, ctx=seed.ctx, dispatch_id=grant.dispatch_id)
    lease = RunLease(seed.ids["run"], "dispatch-accounting-test", 1, seed.now + timedelta(minutes=5))
    async with seed.sessions() as session, session.begin():
        await session.execute(
            text("""UPDATE pipeline_runs
                    SET claimed_by=:owner,lease_epoch=1,lease_expires_at=:expires WHERE id=:id"""),
            {"owner": lease.claimed_by, "expires": lease.lease_expires_at,
             "id": lease.pipeline_run_id},
        )
    stale_before = await _cutoff(seed)
    accounting_started, latch_has_ledger, sweep_selected = (
        asyncio.Event(), asyncio.Event(), asyncio.Event()
    )
    original_get, original_execute = AsyncSession.get, AsyncSession.execute

    async def gated_get(session: AsyncSession, entity: Any, ident: Any, **kwargs: Any) -> Any:
        result = await original_get(session, entity, ident, **kwargs)
        if accounting != "sweep" and not accounting_started.is_set() and entity in (
            ExecutionAttempt, PipelineProviderDispatch,
        ):
            accounting_started.set()
            await latch_has_ledger.wait()
        return result

    async def gated_execute(session: AsyncSession, statement: Any, *args: Any, **kwargs: Any) -> Any:
        result = await original_execute(session, statement, *args, **kwargs)
        descriptions = getattr(statement, "column_descriptions", [])
        if accounting == "sweep" and descriptions and (
            descriptions[0].get("entity") is PipelineProviderDispatch
        ):
            sweep_selected.set()
        return result

    class GatedRepository(PipelineRepository):
        async def _cancel_not_started_attempts(
            self, session: AsyncSession, lease: RunLease, cause: TerminalCause,
        ) -> None:
            latch_has_ledger.set()
            if accounting == "sweep":
                await sweep_selected.wait()
            await super()._cancel_not_started_attempts(session, lease, cause)

    async def account() -> Any:
        if accounting == "sweep":
            await latch_has_ledger.wait()
        async with seed.sessions() as session:
            if accounting == "release":
                return await release_provider_dispatch_unsent(
                    session, ctx=seed.ctx, dispatch_id=grant.dispatch_id,
                )
            if accounting == "settlement":
                return await settle_provider_dispatch(
                    session, ctx=seed.ctx, dispatch_id=grant.dispatch_id, outcome="succeeded",
                    usage=TokenUsage(input_tokens=1, output_tokens=1), actual_cost_microusd=4,
                    rate_card_hash="test", request_params={},
                    response_digest=canonical_digest({"ok": 1}),
                )
            return await settle_stale_provider_dispatches(session, stale_before=stale_before)

    async def cancel() -> TerminalCause:
        if accounting != "sweep":
            await accounting_started.wait()
        return await GatedRepository(seed.sessions).latch_terminal_cause(
            lease, TerminalCause.USER_CANCEL,
        )

    monkeypatch.setattr(AsyncSession, "get", gated_get)
    monkeypatch.setattr(AsyncSession, "execute", gated_execute)
    tasks = [asyncio.create_task(account()), asyncio.create_task(cancel())]
    try:
        settled, cause = await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
        assert cause is TerminalCause.USER_CANCEL
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    if accounting == "sweep":
        assert settled == 0
        assert await account() == 1
        expected = ("settled", "uncertain", 10, "settled", 10, 0, 1, 0, 10, 0, 10, 1)
    else:
        assert await account() == settled
        expected = (
            ("settled", "not_dispatched", 0, "released", None, 0, 0, 0, 0, 0, 0, 0)
            if accounting == "release"
            else ("settled", "succeeded", 4, "settled", 4, 0, 1, 0, 4, 0, 4, 1)
        )
    assert await _accounting(seed, grant) == expected
    async with seed.sessions() as session:
        row = (await session.execute(text("""
            SELECT l.terminal_cause,a.cancellation_requested_at,o.state
              FROM pipeline_budget_ledgers l
              JOIN pipeline_stage_runs s ON s.pipeline_run_id=l.pipeline_run_id
              JOIN execution_attempts a ON a.stage_run_id=s.id
              JOIN pipeline_cancellation_outbox o ON o.execution_attempt_id=a.id
             WHERE a.id=:id
        """), {"id": seed.ids["attempt"]})).one()
    assert row.terminal_cause == "user_cancel"
    assert row.cancellation_requested_at is not None
    assert row.state == "pending"


async def test_settlement_ledger_contention_is_bounded_and_retryable(
    ledger_seed: _LedgerSeed,
) -> None:
    from loom.db.schema import PipelineBudgetLedger

    seed = ledger_seed
    grant = await seed.reserve()
    async with seed.sessions() as session:
        await mark_provider_dispatch_sent(session, ctx=seed.ctx, dispatch_id=grant.dispatch_id)
    before = await _accounting(seed, grant)

    async def settle() -> Any:
        async with seed.sessions() as session:
            return await settle_provider_dispatch(
                session, ctx=seed.ctx, dispatch_id=grant.dispatch_id, outcome="succeeded",
                usage=TokenUsage(input_tokens=1, output_tokens=1), actual_cost_microusd=4,
                rate_card_hash="test", request_params={}, response_digest=canonical_digest({"ok": 1}),
            )

    async with seed.sessions() as blocker, blocker.begin():
        await blocker.get(PipelineBudgetLedger, seed.ids["run"], with_for_update=True)
        with pytest.raises(ProviderDispatchError, match="provider_dispatch_accounting_busy"):
            await asyncio.wait_for(settle(), timeout=4)
    assert await _accounting(seed, grant) == before
    assert (await settle()).actual_cost_microusd == 4
    assert await _accounting(seed, grant) == (
        "settled", "succeeded", 4, "settled", 4, 0, 1, 0, 4, 0, 4, 1,
    )
