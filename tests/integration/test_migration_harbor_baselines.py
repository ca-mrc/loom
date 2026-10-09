"""Baseline subject migration preserves existing attribution and run evidence."""

from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError


def test_baseline_migration_roundtrip_and_evidence_guard(isolated_migration_postgres_url):
    config = Config("database/migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", isolated_migration_postgres_url)
    command.downgrade(config, "0175")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        assert "baseline_session_id" not in {
            column["name"] for column in inspect(engine).get_columns("llm_calls")
        }
        command.upgrade(config, "0176")
        assert "baseline_session_id" in {
            column["name"] for column in inspect(engine).get_columns("llm_calls")
        }
        command.downgrade(config, "0175")
        assert "harbor_baseline_sessions" not in inspect(engine).get_table_names()
        command.upgrade(config, "0176")
        team_id, provider_id, grant_id = uuid4(), uuid4(), uuid4()
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": str(team_id)},
            )
            connection.execute(
                text("""
                INSERT INTO provider_connections (
                    id, team_id, provider_type, display_name, base_url,
                    upstream_host, encrypted_api_key_ref, created_by
                ) VALUES (:id, :team, 'openai-compatible', 'baseline-fixture',
                    'https://provider.invalid/v1', 'provider.invalid', 'fixture', 'migration-test')
            """),
                {"id": provider_id, "team": team_id},
            )
            connection.execute(
                text("""
                INSERT INTO harbor_baseline_sessions (
                    id, team_id, provider_connection_id, model, label, token_hash,
                    expires_at, max_calls, max_input_tokens, max_output_tokens, max_total_tokens
                ) VALUES (:id, :team, :provider, 'gpt-4o', 'migration-fixture',
                    decode(repeat('ab', 32), 'hex'), now() + interval '1 minute', 1, 4000, 100, 4100)
            """),
                {"id": grant_id, "team": team_id, "provider": provider_id},
            )
        with pytest.raises(DBAPIError, match="retained Harbor baseline evidence"):
            command.downgrade(config, "0175")
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0176"
            assert connection.scalar(text("SELECT count(*) FROM harbor_baseline_sessions")) == 1
    finally:
        engine.dispose()
