"""Provider Connection rows for integration tests that submit model-backed work.

Hosted model-backed submissions require an explicit, authorized
OpenAI-compatible Provider Connection (#2054); there is no platform-credential
fallback. Tests seed one with the models they submit.
"""

from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID, uuid4

from sqlalchemy import delete, insert
from sqlalchemy.orm import Session

from loom.db.schema import ProviderConnection, ProviderModelCache


def insert_openai_connection(
    session: Session,
    *,
    team_id: UUID,
    model_ids: Iterable[str],
    display_name: str = "Test OpenAI-compatible",
) -> UUID:
    """Insert a valid connection for `team_id` with preflighted models.

    The caller commits."""
    connection_id = uuid4()
    session.execute(
        insert(ProviderConnection).values(
            id=connection_id,
            team_id=team_id,
            provider_type="openai-compatible",
            display_name=display_name,
            base_url="https://api.example.test/v1",
            upstream_host="api.example.test",
            resolved_egress_ips=["203.0.113.10"],
            encrypted_api_key_ref=f"test://{connection_id}",
            status="valid",
            pricing_source="tokens-only",
            created_by="test:fixture",
        )
    )
    for model_id in model_ids:
        session.execute(
            insert(ProviderModelCache).values(
                provider_connection_id=connection_id,
                model_id=model_id,
                last_preflight_status="valid",
            )
        )
    return connection_id


def delete_connection(session: Session, connection_id: UUID) -> None:
    """Remove a connection seeded by `insert_openai_connection`. The caller
    commits, after deleting rows (trials, batches) that reference it."""
    session.execute(
        delete(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == connection_id,
        )
    )
    session.execute(delete(ProviderConnection).where(ProviderConnection.id == connection_id))
