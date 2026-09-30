"""Dedicated pool credentials cannot substitute for ordinary bearer authority."""
from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import insert, text, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.fixture
async def pool_sessions(isolated_migration_postgres_url):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


async def credential(sessions, *, role="participant", **token_changes):
    from loom.db.nebius_pool_schema import (
        NebiusPoolBinding,
        NebiusPoolMachine,
        NebiusPoolMachineCredential,
        NebiusPoolParticipant,
    )
    from loom.db.schema import Token

    pool_id, participant_id, machine_id = uuid4(), uuid4(), uuid4()
    raw = "loom_pool_" + uuid4().hex + uuid4().hex
    token_hash = hashlib.sha256(raw.encode()).digest()
    now = datetime.now(UTC)
    async with sessions.begin() as session:
        await session.execute(insert(NebiusPoolBinding).values(
            pool_id=pool_id, installation_id=uuid4(), cluster_id="cluster-" + uuid4().hex,
            node_group_id="group-1", policy_revision=1, admission_epoch=1, mode="global",
            binding_json={"protected": True}, binding_sha256="a" * 64))
        await session.execute(insert(NebiusPoolParticipant).values(
            participant_id=participant_id, pool_id=pool_id, environment_id=uuid4(),
            incarnation=uuid4(), binding_revision=1, admission_epoch=1, phase="active",
            binding_json={"protected": True}, binding_sha256="b" * 64))
        await session.execute(insert(NebiusPoolMachine).values(
            machine_id=machine_id, pool_id=pool_id,
            participant_id=participant_id if role == "participant" else None,
            role=role, credential_epoch=1, phase="active"))
        await session.execute(insert(Token).values({
            "token_hash": token_hash, "type": "pool_machine", "scopes": [],
            "issued_at": now, "expires_at": now + timedelta(hours=1),
        } | token_changes))
        await session.execute(insert(NebiusPoolMachineCredential).values(
            token_hash=token_hash, machine_id=machine_id, credential_epoch=1))
    return raw, pool_id, participant_id if role == "participant" else None, machine_id


@pytest.mark.parametrize("role", ["participant", "observer", "gateway"])
async def test_dedicated_machine_auth_is_bound_and_does_not_commit_or_touch_token(pool_sessions, role):
    from loom.auth import validate_bearer_token
    from loom.db.schema import Token
    from loom_service.pool_management.auth import authorize_pool_machine, resolve_pool_machine

    raw, pool_id, participant_id, _ = await credential(pool_sessions, role=role)
    token_hash = hashlib.sha256(raw.encode()).digest()
    async with pool_sessions() as session:
        # Authentication must not commit unrelated caller-owned changes.
        await session.execute(update(Token).where(Token.token_hash == token_hash).values(name="uncommitted"))
        principal = await resolve_pool_machine(session, "Bearer " + raw)
        assert principal is not None
        assert (principal.pool_id, principal.participant_id, principal.role) == (pool_id, participant_id, role)
        assert await authorize_pool_machine(session, principal, role=role,
            pool_id=pool_id, participant_id=participant_id) == principal
        row = await session.get(Token, token_hash)
        assert row.last_seen_at is None and row.last_used_at is None
        ordinary = await validate_bearer_token(session, "Bearer " + raw)
        assert ordinary.context is None
        await session.rollback()
    async with pool_sessions() as session:
        assert (await session.get(Token, token_hash)).name is None


@pytest.mark.parametrize("changes", [
    {"type": "team"}, {"type": "worker"}, {"type": "admin"},
    {"scopes": ["admin:tokens"]}, {"scopes": ["pool:dispatch"]},
    {"expires_at": None}, {"expires_at": datetime(2000, 1, 1, tzinfo=UTC)},
    {"revoked_at": datetime(2000, 1, 1, tzinfo=UTC)},
    {"issued_at": datetime(2100, 1, 1, tzinfo=UTC)},
])
async def test_token_row_without_current_dedicated_authority_is_denied(pool_sessions, changes):
    from loom_service.pool_management.auth import resolve_pool_machine

    raw, _, _, _ = await credential(pool_sessions, **changes)
    async with pool_sessions() as session:
        assert await resolve_pool_machine(session, "Bearer " + raw) is None


async def test_authentication_does_not_erase_or_flush_pending_credential_revocation(pool_sessions):
    from loom.db.schema import Token
    from loom_service.pool_management.auth import resolve_pool_machine

    raw, _, _, _ = await credential(pool_sessions)
    token_hash = hashlib.sha256(raw.encode()).digest()
    async with pool_sessions() as session:
        row = await session.get(Token, token_hash)
        pending_revocation = datetime.now(UTC)
        row.revoked_at = pending_revocation
        assert await resolve_pool_machine(session, "Bearer " + raw) is None
        assert row.revoked_at == pending_revocation
        assert row in session.dirty
        async with pool_sessions() as observer:
            assert (await observer.get(Token, token_hash)).revoked_at is None
        await session.rollback()


@pytest.mark.parametrize("damage", ["token_revoked", "machine_revoked", "rotation", "pool_epoch", "participant_epoch", "binding"])
async def test_admission_rechecks_current_authority_not_cached_orm_objects(pool_sessions, damage):
    from loom.db.nebius_pool_schema import (
        NebiusPoolBinding,
        NebiusPoolMachine,
        NebiusPoolParticipant,
    )
    from loom.db.schema import Token
    from loom_service.pool_management.auth import (
        PoolAuthenticationError,
        authorize_pool_machine,
        resolve_pool_machine,
    )

    raw, pool_id, participant_id, machine_id = await credential(pool_sessions)
    async with pool_sessions() as session:
        principal = await resolve_pool_machine(session, "Bearer " + raw)
        assert principal is not None
        await session.commit()
        async with pool_sessions.begin() as writer:
            if damage == "token_revoked":
                await writer.execute(update(Token).where(Token.token_hash == principal.token_hash).values(revoked_at=datetime.now(UTC)))
            elif damage in {"machine_revoked", "rotation"}:
                await writer.execute(update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == machine_id).values(
                    **({"phase": "revoked"} if damage == "machine_revoked" else {"credential_epoch": 2})))
            elif damage == "pool_epoch":
                await writer.execute(update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool_id).values(admission_epoch=2))
            else:
                await writer.execute(update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == participant_id).values(
                    **({"admission_epoch": 2} if damage == "participant_epoch" else {
                        "binding_revision": 2, "binding_json": {"protected": "changed"}, "binding_sha256": "c" * 64})))
        with pytest.raises(PoolAuthenticationError):
            await authorize_pool_machine(session, principal, role="participant", pool_id=pool_id, participant_id=participant_id)


@pytest.mark.parametrize("damage", ["role", "pool", "participant"])
async def test_scope_cannot_be_widened_by_a_request(pool_sessions, damage):
    from loom_service.pool_management.auth import (
        PoolAuthenticationError,
        authorize_pool_machine,
        resolve_pool_machine,
    )

    raw, pool_id, participant_id, _ = await credential(pool_sessions)
    async with pool_sessions() as session:
        principal = await resolve_pool_machine(session, "Bearer " + raw)
        assert principal is not None
        with pytest.raises(PoolAuthenticationError):
            await authorize_pool_machine(session, principal,
                role="gateway" if damage == "role" else "participant",
                pool_id=uuid4() if damage == "pool" else pool_id,
                participant_id=uuid4() if damage == "participant" else participant_id)


@pytest.mark.parametrize("header", [None, "", "Basic hidden", "Bearer", "Bearer too many", "Bearer " + "a" * 513])
async def test_missing_or_malformed_machine_credentials_fail_closed(pool_sessions, header):
    from loom_service.pool_management.auth import resolve_pool_machine

    async with pool_sessions() as session:
        assert await resolve_pool_machine(session, header) is None


async def test_unbound_token_never_gains_machine_authority(pool_sessions):
    from loom.db.schema import Token
    from loom_service.pool_management.auth import resolve_pool_machine

    raw = "loom_pool_" + uuid4().hex
    async with pool_sessions.begin() as session:
        await session.execute(insert(Token).values(token_hash=hashlib.sha256(raw.encode()).digest(),
            type="pool_machine", scopes=[], issued_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(hours=1)))
    async with pool_sessions() as session:
        assert await resolve_pool_machine(session, "Bearer " + raw) is None


@pytest.mark.parametrize("target", ["token", "machine", "pool", "participant"])
async def test_final_authorization_holds_current_rows_until_callers_transaction_finishes(pool_sessions, target):
    from loom.db.nebius_pool_schema import (
        NebiusPoolBinding,
        NebiusPoolMachine,
        NebiusPoolParticipant,
    )
    from loom.db.schema import Token
    from loom_service.pool_management.auth import authorize_pool_machine, resolve_pool_machine

    raw, pool_id, participant_id, machine_id = await credential(pool_sessions)
    statements = {
        "token": update(Token).where(Token.token_hash == hashlib.sha256(raw.encode()).digest()).values(revoked_at=datetime.now(UTC)),
        "machine": update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == machine_id).values(phase="revoked"),
        "pool": update(NebiusPoolBinding).where(NebiusPoolBinding.pool_id == pool_id).values(mode="closed"),
        "participant": update(NebiusPoolParticipant).where(NebiusPoolParticipant.participant_id == participant_id).values(phase="fenced"),
    }
    async with pool_sessions() as admission:
        principal = await resolve_pool_machine(admission, "Bearer " + raw)
        assert principal is not None
        await authorize_pool_machine(admission, principal, role="participant", pool_id=pool_id, participant_id=participant_id)
        with pytest.raises(DBAPIError) as blocked:
            async with pool_sessions.begin() as writer:
                await writer.execute(text("SET LOCAL lock_timeout = '100ms'"))
                await writer.execute(statements[target])
        assert blocked.value.orig.sqlstate == "55P03"
        await admission.rollback()
    async with pool_sessions.begin() as writer:
        await writer.execute(statements[target])


@pytest.mark.parametrize("target", ["machine_scope", "credential_epoch", "credential_delete"])
async def test_protected_machine_scope_and_historical_credential_binding_are_immutable(pool_sessions, target):
    from sqlalchemy import delete

    from loom.db.nebius_pool_schema import NebiusPoolMachine, NebiusPoolMachineCredential

    raw, _, _, machine_id = await credential(pool_sessions)
    token_hash = hashlib.sha256(raw.encode()).digest()
    statements = {
        "machine_scope": update(NebiusPoolMachine).where(NebiusPoolMachine.machine_id == machine_id).values(role="gateway", participant_id=None),
        "credential_epoch": update(NebiusPoolMachineCredential).where(NebiusPoolMachineCredential.token_hash == token_hash).values(credential_epoch=2),
        "credential_delete": delete(NebiusPoolMachineCredential).where(NebiusPoolMachineCredential.token_hash == token_hash),
    }
    with pytest.raises(DBAPIError):
        async with pool_sessions.begin() as writer:
            await writer.execute(statements[target])
