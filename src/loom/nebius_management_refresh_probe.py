"""Read-only runtime compatibility inspection for a drained management refresh.

No registry, worker, row lock or provider call belongs here. Private operation
plans are inspected inside one bounded database snapshot and never reported.
The protected parent must bind the settings and qualify the actual probe Pod.
"""
from __future__ import annotations

import asyncio
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Text, cast, func, select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.nebius_application_contract import (
    ApplicationRegistrationV1,
    ApplicationReleaseV1,
    SharedDevelopmentBindingV1,
)

SCHEMA = "loom.nebius-management-refresh-probe.v1"
_ACTIVE_PHASES = ("pending", "running", "blocked")
_PLAN_KEYS = {"schema_version", "registration", "release", "shared", "files", "platform_envelope"}
_TRANSITION_KEYS = {"source_operation_id", "requires_previous_retirement"}
_IDENTITY_FIELDS = ("data_environment_id", "cluster_id", "platform_namespace")
_MAX_OPERATIONS = 4096
_MAX_PLAN_BYTES = 1024 * 1024
_MAX_TOTAL_BYTES = 32 * 1024 * 1024


class RefreshProbeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    mode: Literal["manager", "shared"]
    namespace: str = Field(pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
    expected_revision: str = Field(pattern=r"^[a-zA-Z0-9_]{1,64}$")
    shared: SharedDevelopmentBindingV1


def _qualify_plan(operation: NebiusApplicationOperation, settings: RefreshProbeSettings) -> None:
    plan = operation.plan_json
    transition = operation.action != "create"
    if (set(plan) != _PLAN_KEYS | (_TRANSITION_KEYS if transition else set())
            or plan["schema_version"] != "loom.nebius-application-plan.v1"):
        raise ValueError
    if transition and (plan["requires_previous_retirement"] is not True
            or not UUID(plan["source_operation_id"]).int
            or UUID(plan["source_operation_id"]) == operation.operation_id):
        raise ValueError
    row = ApplicationRegistrationV1.model_validate(plan["registration"])
    release = ApplicationReleaseV1.model_validate(plan["release"])
    shared = SharedDevelopmentBindingV1.model_validate(plan["shared"])
    active = operation.action in {"create", "update", "resume"}
    desired = "active" if active else "suspended" if operation.action == "suspend" else "destroyed"
    if ((row.application_id, row.owner_user_id, row.deployment_generation, row.access_generation)
            != (operation.application_id, operation.owner_user_id, operation.deployment_generation,
                operation.access_generation)
            or row.desired_state != desired or row.release_id != release.release_id
            or row.data_environment_id != shared.data_environment_id or row.cluster_id != shared.cluster_id
            or release.schema_revision != shared.schema_revision
            or any(getattr(shared, key) != getattr(settings.shared, key) for key in _IDENTITY_FIELDS)
            or (active and shared.schema_revision != settings.shared.schema_revision)):
        raise ValueError
    envelope = plan["platform_envelope"]
    if (not isinstance(envelope, dict)
            or set(envelope) != {"cpu_millis", "memory_mib", "storage_mib", "ephemeral_storage_mib"}
            or any(type(value) is not int or value < 0 for value in envelope.values())
            or envelope["storage_mib"] != 0):
        raise ValueError
    files = plan["files"]
    if not isinstance(files, dict) or not files or any(not isinstance(group, list) for group in files.values()):
        raise ValueError
    documents = [document for group in files.values() for document in group]
    if not documents or any(not isinstance(document, dict) or document.get("kind") not in {
        "Namespace", "ServiceAccount", "RoleBinding", "Deployment", "Service", "NetworkPolicy", "Ingress",
    } or (document["metadata"]["name"] if document["kind"] == "Namespace" else document["metadata"].get("namespace"))
            != row.application_namespace for document in documents):
        raise ValueError
    # Stop plans deliberately retain the old generation's manifests and release.
    # Do not rerender them, require current-schema cleanup, or rewrite their bytes.


async def database_snapshot(url: URL, settings: RefreshProbeSettings) -> dict[str, Any]:
    """Qualify actual schema and supported active plans without altering work."""
    engine = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=10,
        isolation_level="REPEATABLE READ", connect_args={"connect_timeout": 10,
            "options": "-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=5000"})
    try:
        async with asyncio.timeout(150):
            settings = RefreshProbeSettings.model_validate(settings.model_dump())
            factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
            async with factory.begin() as session:
                if await session.scalar(text("SHOW transaction_read_only")) != "on":
                    raise ValueError
                revisions = (await session.scalars(text("SELECT version_num FROM alembic_version LIMIT 2"))).all()
                if revisions != [settings.expected_revision]:
                    raise ValueError
                checked = 0
                if settings.mode == "manager":
                    size = func.octet_length(cast(NebiusApplicationOperation.plan_json, Text))
                    count, total, largest = (await session.execute(select(
                        func.count(), func.coalesce(func.sum(size), 0), func.coalesce(func.max(size), 0),
                    ).where(NebiusApplicationOperation.phase.in_(_ACTIVE_PHASES)))).one()
                    if count > _MAX_OPERATIONS or total > _MAX_TOTAL_BYTES or largest > _MAX_PLAN_BYTES:
                        raise ValueError
                    operations = (await session.scalars(select(NebiusApplicationOperation)
                        .where(NebiusApplicationOperation.phase.in_(_ACTIVE_PHASES)))).all()
                    if len(operations) != count:
                        raise ValueError
                    for operation in operations:
                        _qualify_plan(operation, settings)
                    checked = len(operations)
                return {"schema": SCHEMA, "status": "qualified", "mode": settings.mode,
                    "revision": settings.expected_revision, "operations_checked": checked}
    except Exception:
        raise ValueError("refresh_probe_unqualified") from None
    finally:
        await engine.dispose()
