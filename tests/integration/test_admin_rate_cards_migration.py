"""Migration 0005 grants `admin:rate_cards` to existing `admin:tokens`
holders and is idempotent (Plan 17 Task 5)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, insert, text
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token


def _alembic_cfg(postgres_url: str) -> Config:
    cfg = Config("database/migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


def _token_scopes(session: object) -> dict[bytes, list[str]]:
    rows = session.execute(text("SELECT token_hash, scopes FROM tokens")).all()  # type: ignore[attr-defined]
    return {bytes(token_hash): list(scopes) for token_hash, scopes in rows}


@pytest.fixture
def seeded_tokens(isolated_migration_postgres_url: str) -> Iterator[None]:
    """Seed pre-0005 tokens in this test's owned disposable database."""
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0004")
    engine = create_engine(isolated_migration_postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(b"a").digest(),
                type="admin",
                scopes=["admin:tokens"],
                team_id=None,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(b"b").digest(),
                type="admin",
                scopes=["admin:tokens", "admin:rate_cards"],
                team_id=None,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(b"c").digest(),
                type="team",
                scopes=["read:own"],
                team_id=None,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()
    try:
        yield
    finally:
        engine.dispose()


def test_migration_grants_new_scope(
    isolated_migration_postgres_url: str,
    seeded_tokens: None,
) -> None:
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0005")
    engine = create_engine(isolated_migration_postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        by_hash = _token_scopes(s)
    a = hashlib.sha256(b"a").digest()
    b = hashlib.sha256(b"b").digest()
    c = hashlib.sha256(b"c").digest()
    assert "admin:rate_cards" in by_hash[a]
    # Idempotent — token b already had the scope; not duplicated.
    assert by_hash[b].count("admin:rate_cards") == 1
    # Non-admin tokens untouched.
    assert "admin:rate_cards" not in by_hash[c]
    engine.dispose()


def test_migration_downgrade_removes_scope(
    isolated_migration_postgres_url: str,
    seeded_tokens: None,
) -> None:
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0005")
    command.downgrade(cfg, "0004")
    engine = create_engine(isolated_migration_postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        rows = _token_scopes(s).values()
    for scopes in rows:
        assert "admin:rate_cards" not in scopes
    engine.dispose()
