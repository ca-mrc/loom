"""Migration 0018 — verify the three new tables + constraints + trigger
apply cleanly against a real Postgres.

Spec: docs/architecture/cluster-deploy.md §Schema additions.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import Connection, create_engine, text
from sqlalchemy.exc import IntegrityError
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")
def postgres_url() -> str:
    """Spin up a fresh Postgres + alembic upgrade head. Shares one
    container across all tests in this module to amortize startup."""
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = url
        repo_root = Path(__file__).resolve().parents[2]
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c",
             "database/migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root, check=True,
        )
        yield url


@pytest.fixture()
def team_id(postgres_url: str) -> str:
    """A throwaway team for each test so cross-test rows don't collide
    on the (team_id, display_name) partial unique index."""
    engine = create_engine(postgres_url)
    tid = uuid4()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO teams (id, name) VALUES (:id, :name)",
        ), {"id": tid, "name": f"test-{tid}"})
        conn.execute(text(
            "INSERT INTO team_quotas (team_id) VALUES (:t)",
        ), {"t": tid})
    return str(tid)


def _insert_connection(
    conn: Connection,
    *,
    team_id: str,
    display_name: str,
    ref: str = "loom://test/ref",
    status: str = "pending",
    provider_type: str = "openai-compatible",
) -> UUID:
    """Insert a provider_connection row with explicit UUID — there's no
    server-side default for `id` (the ORM provides default=uuid4 per the
    convention used by other tables). Returns the new row's UUID."""
    conn_id = uuid4()
    conn.execute(text(
        "INSERT INTO provider_connections "
        "(id, team_id, provider_type, display_name, base_url, "
        " upstream_host, encrypted_api_key_ref, status, created_by) "
        "VALUES (:id, :t, :pt, :n, 'https://x', 'x', :ref, :st, 'admin:0')",
    ), {
        "id": conn_id, "t": team_id, "pt": provider_type,
        "n": display_name, "ref": ref, "st": status,
    })
    return conn_id


def test_three_new_tables_exist(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        names = {row[0] for row in conn.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public'",
        ))}
    assert {"secrets", "provider_connections", "provider_models_cache"}.issubset(names)


def test_secrets_table_columns(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        cols = {row[0] for row in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'secrets'",
        ))}
    # The fixture upgrades through head, including retirement metadata in 0156.
    assert cols == {
        "ref", "ciphertext", "nonce", "master_key_version", "created_at",
        "provider_retired_at",
    }


def test_resolved_egress_ips_is_inet_array(postgres_url: str) -> None:
    """resolved_egress_ips uses native Postgres inet[] (not JSONB).
    Catches a regression to the rev-7 design."""
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        # information_schema reports ARRAY for ARRAY types; udt_name
        # gives the element type. _inet is the array-of-inet UDT.
        result = conn.execute(text(
            "SELECT data_type, udt_name "
            "FROM information_schema.columns "
            "WHERE table_name = 'provider_connections' "
            "  AND column_name = 'resolved_egress_ips'",
        )).one()
    assert result[0] == "ARRAY"
    assert result[1] == "_inet"


def test_check_constraint_rejects_invalid_status(
    postgres_url: str, team_id: str,
) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as conn, pytest.raises(IntegrityError):
        _insert_connection(conn, team_id=team_id, display_name="ss",
                           status="bogus-status")


def test_check_constraint_rejects_invalid_provider_type(
    postgres_url: str, team_id: str,
) -> None:
    engine = create_engine(postgres_url)
    with engine.begin() as conn, pytest.raises(IntegrityError):
        _insert_connection(conn, team_id=team_id, display_name="pt",
                           provider_type="azure-flavor")


def test_partial_unique_index_allows_reuse_after_soft_delete(
    postgres_url: str, team_id: str,
) -> None:
    """A team can re-use a display_name after the previous connection
    with that name has been soft-deleted."""
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        _insert_connection(conn, team_id=team_id, display_name="prod",
                           ref="loom://1")

    # Same name, ACTIVE → must fail.
    with engine.begin() as conn, pytest.raises(IntegrityError):
        _insert_connection(conn, team_id=team_id, display_name="prod",
                           ref="loom://2")

    # Soft-delete the first, then insert with the same name → must succeed.
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE provider_connections SET deleted_at = now() "
            "WHERE team_id = :t AND display_name = 'prod'",
        ), {"t": team_id})
        _insert_connection(conn, team_id=team_id, display_name="prod",
                           ref="loom://3")


def test_updated_at_trigger_fires_on_update(
    postgres_url: str, team_id: str,
) -> None:
    """Every UPDATE bumps updated_at, even when no application-visible
    column changed. Load-bearing for the gateway's indexed-updated_at
    cache invalidation pattern (cluster-deploy.md §Cache durability)."""
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        _insert_connection(conn, team_id=team_id, display_name="utest")
        before = conn.execute(text(
            "SELECT updated_at FROM provider_connections "
            "WHERE team_id = :t AND display_name = 'utest'",
        ), {"t": team_id}).scalar_one()

    # An UPDATE that touches NOTHING (sets a column to its current
    # value) still fires the trigger.
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE provider_connections SET status = status "
            "WHERE team_id = :t AND display_name = 'utest'",
        ), {"t": team_id})
        after = conn.execute(text(
            "SELECT updated_at FROM provider_connections "
            "WHERE team_id = :t AND display_name = 'utest'",
        ), {"t": team_id}).scalar_one()

    assert after > before


def test_provider_connection_delete_cascades_to_models_cache(
    postgres_url: str, team_id: str,
) -> None:
    """ON DELETE CASCADE on provider_models_cache.provider_connection_id
    means hard-deleting the parent (e.g., via the future
    `loom admin providers purge` verb) cleans up the cache rows."""
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn_id = _insert_connection(conn, team_id=team_id,
                                     display_name="casc")
        conn.execute(text(
            "INSERT INTO provider_models_cache "
            "(provider_connection_id, model_id) "
            "VALUES (:c, 'gpt-4o')",
        ), {"c": conn_id})

    with engine.begin() as conn:
        conn.execute(text(
            "DELETE FROM provider_connections WHERE id = :c",
        ), {"c": conn_id})
        remaining = conn.execute(text(
            "SELECT count(*) FROM provider_models_cache "
            "WHERE provider_connection_id = :c",
        ), {"c": conn_id}).scalar_one()

    assert remaining == 0


def test_provider_models_cache_has_preflight_status_columns(
    postgres_url: str, team_id: str,
) -> None:
    """Model discovery and model entitlement are different facts.
    The cache needs nullable preflight columns so untested rows stay
    distinguishable from rows that were proved valid or denied."""
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        cols = {row[0] for row in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'provider_models_cache'",
        ))}
    assert {
        "last_preflight_status",
        "last_preflight_at",
        "last_preflight_http_status",
        "last_preflight_error_code",
        "last_preflight_error_message",
    }.issubset(cols)

    with engine.begin() as conn:
        conn_id = _insert_connection(
            conn, team_id=team_id, display_name="preflight-cols",
        )
        conn.execute(text(
            "INSERT INTO provider_models_cache "
            "(provider_connection_id, model_id, last_preflight_status) "
            "VALUES (:c, 'gpt-4o', 'valid')",
        ), {"c": conn_id})

    with engine.begin() as conn, pytest.raises(IntegrityError):
        conn.execute(text(
            "INSERT INTO provider_models_cache "
            "(provider_connection_id, model_id, last_preflight_status) "
            "VALUES (:c, 'bad-status', 'maybe')",
        ), {"c": conn_id})


def test_team_delete_blocked_by_existing_provider_connections(
    postgres_url: str, team_id: str,
) -> None:
    """team_id FK is ON DELETE RESTRICT (NOT CASCADE) — hard-deleting a
    team with active provider_connections must fail, so the operator
    has to soft-delete the connections first (and let the future
    `loom admin teams purge` reclaim them). Without RESTRICT, a team
    delete would silently nuke the connections AND orphan any
    Trial.provider_connection_id FK pointing at them (the Trial FK has
    no cascade per spec). Important to validate at the DB layer because
    the spec originally said CASCADE — this guards against drift."""
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        _insert_connection(conn, team_id=team_id, display_name="blockme")

    with engine.begin() as conn, pytest.raises(IntegrityError):
        conn.execute(text(
            "DELETE FROM teams WHERE id = :t",
        ), {"t": team_id})

    # Soft-delete the connection, then try team delete — should still
    # fail because the row physically exists (soft-delete only sets
    # deleted_at). Operator must hard-delete connections (e.g., via
    # `loom admin providers purge`) before deleting the team.
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE provider_connections SET deleted_at = now() "
            "WHERE team_id = :t",
        ), {"t": team_id})
    with engine.begin() as conn, pytest.raises(IntegrityError):
        conn.execute(text("DELETE FROM teams WHERE id = :t"), {"t": team_id})

    # Hard-delete the connection, then team delete succeeds.
    with engine.begin() as conn:
        conn.execute(text(
            "DELETE FROM provider_connections WHERE team_id = :t",
        ), {"t": team_id})
    with engine.begin() as conn:
        # team_quotas FK is CASCADE; clean it first to mimic real flow.
        conn.execute(text(
            "DELETE FROM team_quotas WHERE team_id = :t",
        ), {"t": team_id})
        conn.execute(text("DELETE FROM teams WHERE id = :t"), {"t": team_id})


def test_models_cache_hidden_reason_check(
    postgres_url: str, team_id: str,
) -> None:
    """hidden_reason can be NULL, 'operator-hidden', or
    'missing-upstream'. Anything else is rejected. Catches a regression
    to the rev-10 design where 'disabled-pricing' was a value (removed)."""
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn_id = _insert_connection(conn, team_id=team_id, display_name="hr")

    with engine.begin() as conn, pytest.raises(IntegrityError):
        conn.execute(text(
            "INSERT INTO provider_models_cache "
            "(provider_connection_id, model_id, hidden_reason) "
            "VALUES (:c, 'gpt-4o', 'disabled-pricing')",
        ), {"c": conn_id})


def test_orm_models_match_migration_schema(postgres_url: str) -> None:
    """Smoke-check the ORM models import cleanly + their tablenames
    match what migration 0018 created. If a column drifts between the
    migration and the model, the next test that selects via ORM will
    fail more loudly than this one, but this catches the simplest
    typos at module-import time."""
    from loom.db.schema import (
        ProviderConnection,
        ProviderModelCache,
        Secret,
    )

    assert Secret.__tablename__ == "secrets"
    assert ProviderConnection.__tablename__ == "provider_connections"
    assert ProviderModelCache.__tablename__ == "provider_models_cache"

    expected_pc_cols = {
        "id", "team_id", "provider_type", "display_name", "base_url",
        "upstream_host", "resolved_egress_ips", "egress_ips_refreshed_at",
        "egress_ips_min_ttl_seconds", "encrypted_api_key_ref",
        "allowed_models", "status", "last_validated_at",
        "last_validation_error", "pricing_source", "pricing_data",
        "rate_card_provider", "created_by", "deleted_at", "created_at",
        "updated_at", "responses_api_supported", "responses_api_probed_at",
        "responses_api_probe_error", "pricing_config", "max_in_flight",
    }
    actual_pc_cols = {c.name for c in ProviderConnection.__table__.columns}
    assert actual_pc_cols == expected_pc_cols, (
        f"ORM-schema drift: missing={expected_pc_cols - actual_pc_cols}, "
        f"extra={actual_pc_cols - expected_pc_cols}"
    )
    # The fixture migrates through head, so compare the real schema as well
    # as the explicit public/internal column contract above.
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        database_cols = {row[0] for row in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'provider_connections'",
        ))}
    engine.dispose()
    assert database_cols == actual_pc_cols
