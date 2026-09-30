from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import create_engine, inspect, text


def _config(url: str) -> AlembicConfig:
    repo_root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(repo_root / "database" / "migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(repo_root / "database" / "migrations"))
    config.set_main_option("sqlalchemy.url", url)
    return config


def test_0123_creates_exact_native_builder_agent_and_grant_schema(
    isolated_migration_postgres_url: str,
) -> None:
    """Dropping a fence, identity, evidence column, or index must break migration parity."""
    engine = create_engine(isolated_migration_postgres_url)
    command.downgrade(_config(isolated_migration_postgres_url), "0166")
    try:
        inspector = inspect(engine)
        tables = set(inspector.get_table_names(schema="public"))
        assert {
            "personal_dev_native_builder_agents",
            "personal_dev_native_build_grants",
        }.issubset(tables)

        agent_columns = {
            column["name"]: column
            for column in inspector.get_columns(
                "personal_dev_native_builder_agents",
                schema="public",
            )
        }
        assert set(agent_columns) == {
            "instance_id",
            "key_id",
            "provider",
            "platform",
            "protocol_version",
            "host_name",
            "host_architecture",
            "host_boot_id",
            "agent_image",
            "builder_image",
            "runtime_profile_sha256",
            "max_concurrency",
            "managed_grant_ids_json",
            "active_grant_ids_json",
            "available",
            "unavailable_reason",
            "readiness_evidence_sha256",
            "status_json",
            "status_sha256",
            "last_poll_requested_at",
            "last_poll_nonce",
            "first_seen_at",
            "last_seen_at",
            "updated_at",
        }
        assert all(not column["nullable"] for column in agent_columns.values() if column["name"] != "unavailable_reason")
        assert agent_columns["unavailable_reason"]["nullable"]

        grant_columns = {
            column["name"]: column
            for column in inspector.get_columns(
                "personal_dev_native_build_grants",
                schema="public",
            )
        }
        assert set(grant_columns) == {
            "id",
            "candidate_id",
            "attempt_id",
            "attempt_lease_epoch",
            "platform",
            "provider",
            "required_agent_instance_id",
            "required_agent_key_id",
            "agent_image",
            "builder_image",
            "runtime_profile_sha256",
            "contract_json",
            "contract_sha256",
            "source_bucket",
            "source_object_key",
            "artifact_bucket",
            "artifact_object_key",
            "artifact_max_bytes",
            "active_deadline_seconds",
            "state",
            "running_agent_instance_id",
            "last_request_at",
            "last_request_nonce",
            "failure_reason",
            "completion_json",
            "completion_sha256",
            "runtime_evidence_json",
            "runtime_evidence_sha256",
            "artifact_head_json",
            "artifact_head_sha256",
            "queued_at",
            "started_at",
            "heartbeat_at",
            "finished_at",
            "updated_at",
        }
        nullable_grant_columns = {
            "running_agent_instance_id",
            "last_request_at",
            "last_request_nonce",
            "failure_reason",
            "completion_json",
            "completion_sha256",
            "runtime_evidence_json",
            "runtime_evidence_sha256",
            "artifact_head_json",
            "artifact_head_sha256",
            "started_at",
            "heartbeat_at",
            "finished_at",
        }
        assert {
            name for name, column in grant_columns.items() if column["nullable"]
        } == nullable_grant_columns

        unique_constraints = {
            constraint["name"]: tuple(constraint["column_names"])
            for constraint in inspector.get_unique_constraints(
                "personal_dev_native_build_grants",
                schema="public",
            )
        }
        assert unique_constraints[
            "personal_dev_native_build_grants_attempt_platform_uidx"
        ] == ("attempt_id", "attempt_lease_epoch", "platform")

        agent_unique = {
            constraint["name"]: tuple(constraint["column_names"])
            for constraint in inspector.get_unique_constraints(
                "personal_dev_native_builder_agents",
                schema="public",
            )
        }
        assert agent_unique["personal_dev_native_builder_agents_key_uidx"] == ("key_id",)

        foreign_keys = {
            constraint["name"]: (
                constraint["referred_table"],
                constraint["options"].get("ondelete"),
            )
            for constraint in inspector.get_foreign_keys(
                "personal_dev_native_build_grants",
                schema="public",
            )
        }
        assert foreign_keys == {
            "personal_dev_native_build_grants_attempt_fkey": (
                "personal_dev_candidate_build_attempts",
                "CASCADE",
            ),
            "personal_dev_native_build_grants_candidate_fkey": (
                "personal_dev_candidates",
                "RESTRICT",
            ),
            "personal_dev_native_build_grants_required_agent_fkey": (
                "personal_dev_native_builder_agents",
                "RESTRICT",
            ),
            "personal_dev_native_build_grants_running_agent_fkey": (
                "personal_dev_native_builder_agents",
                "RESTRICT",
            ),
        }

        check_names = {
            constraint["name"]
            for table in (
                "personal_dev_native_builder_agents",
                "personal_dev_native_build_grants",
            )
            for constraint in inspector.get_check_constraints(table, schema="public")
        }
        assert {
            "personal_dev_native_builder_agents_identity_check",
            "personal_dev_native_builder_agents_inventory_check",
            "personal_dev_native_builder_agents_status_check",
            "personal_dev_native_build_grants_identity_check",
            "personal_dev_native_build_grants_object_binding_check",
            "personal_dev_native_build_grants_state_check",
            "personal_dev_native_build_grants_terminal_check",
        }.issubset(check_names)

        indexes = {
            index["name"]
            for table in (
                "personal_dev_native_builder_agents",
                "personal_dev_native_build_grants",
            )
            for index in inspector.get_indexes(table, schema="public")
        }
        assert {
            "personal_dev_native_builder_agents_freshness_idx",
            "personal_dev_native_build_grants_picker_idx",
            "personal_dev_native_build_grants_agent_state_idx",
            "personal_dev_native_build_grants_attempt_idx",
        }.issubset(indexes)
    finally:
        engine.dispose()


def test_0123_downgrade_removes_only_native_builder_objects(
    isolated_migration_postgres_url: str,
) -> None:
    """Downgrade must not alter pre-0123 personal candidate objects."""
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        # The shared migration fixture starts at the current head.  Pin this
        # historical downgrade check to the revision it owns before asserting
        # the 0123 -> 0122 behavior.
        command.downgrade(config, "0123")

        with engine.connect() as connection:
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "0123"
            candidate_columns_before = {
                column["name"]
                for column in inspect(connection).get_columns("personal_dev_candidates")
            }

        command.downgrade(config, "0122")

        with engine.connect() as connection:
            inspector = inspect(connection)
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "0122"
            assert "personal_dev_native_builder_agents" not in inspector.get_table_names()
            assert "personal_dev_native_build_grants" not in inspector.get_table_names()
            assert {
                column["name"]
                for column in inspector.get_columns("personal_dev_candidates")
            } == candidate_columns_before
    finally:
        command.upgrade(config, "head")
        engine.dispose()
