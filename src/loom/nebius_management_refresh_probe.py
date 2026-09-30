"""Read-only runtime compatibility inspection for a drained management refresh.

No registry, worker, row lock or provider call belongs here. Private operation
plans are inspected inside one bounded database snapshot and never reported.
The protected parent must bind the settings and qualify the actual probe Pod.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import Text, cast, func, select, text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.nebius_application_contract import (
    ApplicationRegistrationV1,
    ApplicationReleaseV1,
    SharedDevelopmentBindingV1,
)

SCHEMA = "loom.nebius-management-refresh-probe.v1"
FAILURE_STAGES = frozenset({"settings", "database_url", "database", "read_only", "schema", "operations"})
FAILURE_ERRORS = frozenset({"ValueError", "ValidationError", "KeyError", "FileNotFoundError",
    "TimeoutError", "OperationalError", "ProgrammingError", "OtherError"})
SETTINGS_PATH = Path("/var/run/loom-management-refresh/probe.json")
_ACTIVE_PHASES = ("pending", "running", "blocked")
_PLAN_KEYS = {"schema_version", "registration", "release", "shared", "files", "platform_envelope"}
_TRANSITION_KEYS = {"source_operation_id", "requires_previous_retirement"}
_IDENTITY_FIELDS = ("data_environment_id", "cluster_id", "platform_namespace")
_MAX_OPERATIONS = 4096
_MAX_PLAN_BYTES = 1024 * 1024
_MAX_TOTAL_BYTES = 32 * 1024 * 1024


class RefreshProbeError(ValueError):
    """Retain only closed diagnostic categories, never private database details."""

    def __init__(self, stage: str, error: Exception):
        super().__init__("refresh_probe_unqualified")
        self.stage = stage if stage in FAILURE_STAGES else "database"
        kind = type(error).__name__
        self.error_type = kind if kind in FAILURE_ERRORS else "OtherError"


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
            or (active and operation.phase in _ACTIVE_PHASES and shared.schema_revision != settings.shared.schema_revision)):
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
    stage = "database"
    try:
        async with asyncio.timeout(150):
            settings = RefreshProbeSettings.model_validate(settings.model_dump())
            if settings.mode == "shared" and settings.expected_revision != settings.shared.schema_revision:
                raise ValueError
            factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
            async with factory.begin() as session:
                # AsyncSession begins lazily: establish TLS/authentication before
                # classifying failures of the read-only enforcement query.
                await session.connection()
                stage = "read_only"
                if await session.scalar(text("SHOW transaction_read_only")) != "on":
                    raise ValueError
                stage = "schema"
                revisions = (await session.scalars(text("SELECT version_num FROM alembic_version LIMIT 2"))).all()
                if revisions != [settings.expected_revision]:
                    raise ValueError
                checked = 0
                if settings.mode == "manager":
                    stage = "operations"
                    # Cloud retirement reads earlier generations' frozen plans.
                    # Qualify their contract too, without requiring old cleanup
                    # versions to match the new shared schema or rerendering them.
                    active_applications = select(NebiusApplicationOperation.application_id).where(
                        NebiusApplicationOperation.phase.in_(_ACTIVE_PHASES))
                    relevant = NebiusApplicationOperation.application_id.in_(active_applications)
                    size = func.octet_length(cast(NebiusApplicationOperation.plan_json, Text))
                    count, total, largest = (await session.execute(select(
                        func.count(), func.coalesce(func.sum(size), 0), func.coalesce(func.max(size), 0),
                    ).where(relevant))).one()
                    if count > _MAX_OPERATIONS or total > _MAX_TOTAL_BYTES or largest > _MAX_PLAN_BYTES:
                        raise ValueError
                    operations = (await session.scalars(select(NebiusApplicationOperation)
                        .where(relevant))).all()
                    if len(operations) != count:
                        raise ValueError
                    indexed = {operation.operation_id: operation for operation in operations}
                    for operation in operations:
                        _qualify_plan(operation, settings)
                        if operation.action == "create":
                            if operation.deployment_generation != 1 or operation.access_generation != 1:
                                raise ValueError
                        else:
                            source = indexed.get(UUID(operation.plan_json["source_operation_id"]))
                            if (source is None or source.application_id != operation.application_id
                                    or source.owner_user_id != operation.owner_user_id
                                    or source.deployment_generation != operation.deployment_generation - 1
                                    or source.access_generation != operation.access_generation - 1):
                                raise ValueError
                    checked = sum(operation.phase in _ACTIVE_PHASES for operation in operations)
                return {"schema": SCHEMA, "status": "qualified", "mode": settings.mode,
                    "revision": settings.expected_revision, "operations_checked": checked}
    except Exception as error:
        raise RefreshProbeError(stage, error) from None
    finally:
        await engine.dispose()


def refresh_database_url(value: str, settings: RefreshProbeSettings) -> URL:
    """Accept only the retained, TLS-verified namespace-local service identity."""
    try:
        namespace = settings.namespace if settings.mode == "manager" else settings.shared.platform_namespace
        url = make_url(value)
        if (url.drivername not in {"postgresql", "postgresql+psycopg"} or url.username != "loom_service" or not url.password
                or url.host != f"loom-postgres.{namespace}.svc" or url.port != 5432 or url.database != "loom"
                or dict(url.query) != {"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"}):
            raise ValueError
        return url.set(drivername="postgresql+psycopg")
    except Exception:
        raise ValueError("refresh_probe_unqualified") from None


def main() -> int:
    stage = "settings"
    try:
        with SETTINGS_PATH.open("rb") as stream:
            raw = stream.read(262145)
        if len(raw) > 262144:
            raise ValueError
        settings = RefreshProbeSettings.model_validate_json(raw)
        stage = "database_url"
        url = refresh_database_url(os.environ["LOOM_REFRESH_DB_URL"], settings)
        stage = "database"
        report = asyncio.run(database_snapshot(url, settings))
    except Exception as error:
        failure = error if isinstance(error, RefreshProbeError) else RefreshProbeError(stage, error)
        print(json.dumps({"schema": SCHEMA, "status": "unqualified", "stage": failure.stage,
            "error_type": failure.error_type}, sort_keys=True))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
