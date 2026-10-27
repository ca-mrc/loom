"""LocalEncryptedSecretStore — encrypted round-trip + edge cases against
a real Postgres.

Spec: docs/architecture/cluster-deploy.md §Secrets, SSRF, gateway hot path.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from testcontainers.postgres import PostgresContainer

from loom.security.secret_store import (
    DecryptError,
    InvalidRefError,
    LocalEncryptedSecretStore,
    SecretNotFoundError,
    SecretStoreError,
    parse_ref,
)


@pytest.fixture(scope="module")
def postgres_url() -> str:
    """Spin up a Postgres + alembic upgrade head once per module."""
    with PostgresContainer("postgres:16") as pg:
        sync_url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = sync_url
        repo_root = Path(__file__).resolve().parents[2]
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c",
             "database/migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root, check=True,
        )
        yield sync_url


@pytest_asyncio.fixture()
async def session(postgres_url: str) -> AsyncGenerator[AsyncSession]:
    """An AsyncSession bound to the test Postgres. The project uses
    psycopg3 (which supports both sync and async modes via the same
    package), not asyncpg — same `postgresql+psycopg://` URL works for
    both create_engine and create_async_engine.
    """
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as sess:
        try:
            yield sess
        finally:
            # Clean up rows this test wrote so module-scoped fixture
            # state stays consistent across tests.
            await sess.execute(text("DELETE FROM secrets"))
            await sess.commit()
    await engine.dispose()


# Test master key: deterministic for reproducibility. Real deployments
# get fresh random bytes from the operator.
_TEST_KEY = bytes(range(32))
_TEST_KEY_B = bytes(range(1, 33))


# ──────────────────────────────────────────────────────────────────────
# parse_ref — pure function, doesn't touch the DB
# ──────────────────────────────────────────────────────────────────────


def test_parse_ref_happy_path() -> None:
    ref = "loom://team:abc/12345678-1234-5678-1234-567812345678"
    parsed = parse_ref(ref)
    assert parsed.scheme == "loom"
    assert parsed.namespace == "team:abc"
    assert str(parsed.secret_id) == "12345678-1234-5678-1234-567812345678"
    assert parsed.as_string() == ref


def test_parse_ref_rejects_bad_scheme() -> None:
    with pytest.raises(InvalidRefError, match="unsupported ref scheme"):
        parse_ref("k8s://ns/00000000-0000-0000-0000-000000000000")


def test_parse_ref_rejects_missing_separator() -> None:
    with pytest.raises(InvalidRefError, match="missing scheme separator"):
        parse_ref("loom-not-a-url")


def test_parse_ref_rejects_empty_namespace() -> None:
    with pytest.raises(InvalidRefError, match="namespace is empty"):
        parse_ref("loom:///00000000-0000-0000-0000-000000000000")


def test_parse_ref_rejects_non_uuid() -> None:
    with pytest.raises(InvalidRefError, match="uuid does not parse"):
        parse_ref("loom://ns/not-a-uuid")


def test_parse_ref_handles_namespace_with_slash() -> None:
    """Namespaces may contain slashes (e.g., 'team/prod'); the rsplit
    on '/' picks the LAST segment as the uuid."""
    ref = "loom://team/prod/12345678-1234-5678-1234-567812345678"
    parsed = parse_ref(ref)
    assert parsed.namespace == "team/prod"


# ──────────────────────────────────────────────────────────────────────
# LocalEncryptedSecretStore
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_put_get_roundtrip(session: AsyncSession) -> None:
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    ref = await store.put(namespace="team:abc", value="sk-secret-XXXX")
    assert ref.startswith("loom://team:abc/")
    assert await store.get(ref) == "sk-secret-XXXX"


@pytest.mark.asyncio
async def test_put_generates_unique_refs(session: AsyncSession) -> None:
    """Two puts with the same namespace + value yield distinct refs
    (fresh UUID + fresh nonce per call). Important: nonces must NEVER
    be reused under the same key in AES-GCM."""
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    refs = [
        await store.put(namespace="ns", value="x") for _ in range(5)
    ]
    assert len(set(refs)) == 5


@pytest.mark.asyncio
async def test_get_unknown_ref_raises_not_found(
    session: AsyncSession,
) -> None:
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    with pytest.raises(SecretNotFoundError):
        await store.get(
            "loom://nope/00000000-0000-0000-0000-000000000000",
        )


@pytest.mark.asyncio
async def test_get_with_wrong_master_key_raises_decrypt_error(
    session: AsyncSession,
) -> None:
    """Encrypt with key A; create a new store with key B; decrypt MUST
    fail with DecryptError (NOT return junk bytes — that's the AEAD
    guarantee). Load-bearing: protects against master-key-rotation
    misconfiguration silently corrupting data."""
    store_a = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    ref = await store_a.put(namespace="ns", value="alpha")

    store_b = LocalEncryptedSecretStore(session, master_key=_TEST_KEY_B)
    with pytest.raises(DecryptError):
        await store_b.get(ref)


@pytest.mark.asyncio
async def test_get_with_tampered_ciphertext_raises_decrypt_error(
    session: AsyncSession,
) -> None:
    """Flip a bit in the stored ciphertext; AES-GCM tag verification
    must catch it. Round-trip with the original ciphertext still works
    (sanity check the test didn't break the store itself)."""
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    ref = await store.put(namespace="ns", value="omega")

    before = (await session.execute(text(
        "SELECT ciphertext FROM secrets WHERE ref = :r",
    ), {"r": ref})).scalar_one()

    # Flip the low bit of the first byte. Assigning a fixed byte can be a
    # probabilistic no-op when the random ciphertext already has that value.
    await session.execute(text(
        "UPDATE secrets SET ciphertext = set_byte("
        "ciphertext, 0, get_byte(ciphertext, 0) # 1"
        ") WHERE ref = :r",
    ), {"r": ref})
    await session.commit()
    after = (await session.execute(text(
        "SELECT ciphertext FROM secrets WHERE ref = :r",
    ), {"r": ref})).scalar_one()

    assert after != before

    with pytest.raises(DecryptError):
        await store.get(ref)


@pytest.mark.asyncio
async def test_get_with_aad_mismatch_raises_decrypt_error(
    session: AsyncSession,
) -> None:
    """The ref string is bound as AAD; reusing a ciphertext under a
    different ref must fail. Defense against a future bug shuffling
    ref-vs-row assignments."""
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    ref_a = await store.put(namespace="ns", value="a-value")
    # Manually copy ciphertext+nonce from ref_a to a new row under a
    # different ref. The decrypt call will use the new ref as AAD,
    # which mismatches what was encrypted.
    row = (await session.execute(text(
        "SELECT ciphertext, nonce, master_key_version FROM secrets "
        "WHERE ref = :r",
    ), {"r": ref_a})).one()
    bogus_ref = (
        "loom://ns/ffffffff-ffff-ffff-ffff-ffffffffffff"
    )
    await session.execute(text(
        "INSERT INTO secrets (ref, ciphertext, nonce, master_key_version) "
        "VALUES (:r, :c, :n, :v)",
    ), {"r": bogus_ref, "c": row[0], "n": row[1], "v": row[2]})
    await session.commit()

    with pytest.raises(DecryptError):
        await store.get(bogus_ref)


@pytest.mark.asyncio
async def test_delete_is_idempotent(session: AsyncSession) -> None:
    """Deleting a known ref succeeds; deleting the same ref again is
    also a success (no exception). Simplifies caller rollback paths."""
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    ref = await store.put(namespace="ns", value="del-me")
    await store.delete(ref)
    await store.delete(ref)  # no-op, no exception
    with pytest.raises(SecretNotFoundError):
        await store.get(ref)


@pytest.mark.asyncio
async def test_delete_rejects_malformed_ref(
    session: AsyncSession,
) -> None:
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    with pytest.raises(InvalidRefError):
        await store.delete("not-a-loom-ref")


@pytest.mark.asyncio
async def test_list_refs_filters_by_namespace(
    session: AsyncSession,
) -> None:
    """The count assertion (`== 3`) assumes the per-test cleanup runs
    before this test starts. pytest_asyncio serializes tests within a
    module by default; if a future config parallelizes within a module,
    this test would flake — switch to filtering by a test-unique
    namespace prefix instead."""
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    await store.put(namespace="team:a", value="x")
    await store.put(namespace="team:a", value="y")
    await store.put(namespace="team:b", value="z")

    all_refs = [r async for r in store.list_refs()]
    assert len(all_refs) == 3

    a_refs = [r async for r in store.list_refs(namespace="team:a")]
    assert len(a_refs) == 2
    assert all(r.startswith("loom://team:a/") for r in a_refs)


@pytest.mark.asyncio
async def test_rewrap_re_encrypts_in_place_with_new_key(
    session: AsyncSession,
) -> None:
    """rewrap decrypts with the current key, re-encrypts with new_key,
    persists. The ref is stable (returned unchanged); the row's
    ciphertext + nonce + master_key_version change. After rewrap, the
    OLD store can no longer decrypt; a store constructed with the NEW
    key can. Validates the rotation primitive Phase 5 will use."""
    store = LocalEncryptedSecretStore(
        session, master_key=_TEST_KEY, master_key_version=1,
    )
    ref = await store.put(namespace="ns", value="payload")

    new_ref = await store.rewrap(ref, new_master_key=_TEST_KEY_B)
    assert new_ref == ref, "rewrap MUST preserve the ref string"

    # Old store can no longer decrypt this row.
    with pytest.raises(DecryptError):
        await store.get(ref)

    # A store constructed with the new key + bumped version can.
    new_store = LocalEncryptedSecretStore(
        session, master_key=_TEST_KEY_B, master_key_version=2,
    )
    assert await new_store.get(ref) == "payload"


@pytest.mark.asyncio
async def test_rewrap_rejects_wrong_length_new_key(
    session: AsyncSession,
) -> None:
    store = LocalEncryptedSecretStore(session, master_key=_TEST_KEY)
    ref = await store.put(namespace="ns", value="x")
    with pytest.raises(SecretStoreError, match="new_master_key length"):
        await store.rewrap(ref, new_master_key=b"too-short")
