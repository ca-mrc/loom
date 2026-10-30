"""Migration 0024 revokes legacy database-backed admin tokens."""

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


def _token_revoked_at(session: object) -> dict[bytes, object]:
    rows = session.execute(text("SELECT token_hash, revoked_at FROM tokens")).all()  # type: ignore[attr-defined]
    return {bytes(token_hash): revoked_at for token_hash, revoked_at in rows}


@pytest.fixture
def seeded_admin_tokens(isolated_migration_postgres_url: str) -> Iterator[None]:
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0023")
    engine = create_engine(isolated_migration_postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(b"legacy-admin").digest(),
                type="admin",
                scopes=["admin:tokens", "admin:rate_cards"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
                revoked_at=None,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(b"team-token").digest(),
                type="team",
                scopes=["read:own"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
                revoked_at=None,
            )
        )
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(b"team-admin-scope").digest(),
                type="team",
                scopes=["read:own", "admin:tokens"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
                revoked_at=None,
            )
        )
        s.commit()
    try:
        yield
    finally:
        engine.dispose()


def test_migration_revokes_legacy_db_admin_tokens(
    isolated_migration_postgres_url: str,
    seeded_admin_tokens: None,
) -> None:
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.upgrade(cfg, "0024")

    engine = create_engine(isolated_migration_postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        by_hash = _token_revoked_at(s)

    assert by_hash[hashlib.sha256(b"legacy-admin").digest()] is not None
    assert by_hash[hashlib.sha256(b"team-admin-scope").digest()] is not None
    assert by_hash[hashlib.sha256(b"team-token").digest()] is None
    engine.dispose()
