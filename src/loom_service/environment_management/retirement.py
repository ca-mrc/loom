"""Exact-target retirement of superseded, pre-execution personal environments.

This registry cannot run the legacy create queue. The protected installation
supplies immutable identities; ordinary owners still request retained destroy.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Literal, Self
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.nebius_environment_schema import (
    NebiusEnvironment,
    NebiusEnvironmentOperation,
    NebiusEnvironmentResource,
)
from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom_service.environment_management.kubernetes_credentials import (
    ProjectedKubernetesConnection,
    ProjectedKubernetesCredentials,
)
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.registry import (
    EnvironmentRegistry,
    ManagementError,
    registration_view,
)
from loom_service.environment_management.retained_destroy import EnvironmentRetainedDestroy
from loom_service.environment_management.runtime import NebiusManagementAuth
from loom_service.environment_management.worker import EnvironmentWorker


class RetirementTarget(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    operation_id: UUID
    source_operation_id: UUID
    registration: EnvironmentRegistrationV1
    namespace_uids: dict[str, UUID]

    @model_validator(mode="after")
    def _retained_personal(self) -> Self:
        row = self.registration
        if (not self.operation_id.int or not self.source_operation_id.int
                or row.scope != "personal" or row.binding_mode != "generated"
                or row.desired_state != "destroyed" or row.deployment_generation < 2
                or set(self.namespace_uids) != set(row.namespaces)
                or any(not uid.int for uid in self.namespace_uids.values())
                or len(set(self.namespace_uids.values())) != 3):
            raise ValueError("invalid pre-execution retirement target")
        return self


class RetirementRegistry(EnvironmentRegistry):
    """Validate under the existing environment/operation locks, before mutation.

The inherited lease, heartbeat, completion and uncertain-effect handling remain
unchanged. Each also checks the frozen target, not only the first claim.
"""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], target: RetirementTarget):
        super().__init__(session_factory)
        self.target = target

    async def _locked_operation(
        self, session: AsyncSession, operation_id: UUID,
    ) -> tuple[NebiusEnvironmentOperation, NebiusEnvironment, datetime]:
        target = self.target
        if operation_id != target.operation_id:
            raise ManagementError("retirement_target_unqualified")
        operation, environment, now = await super()._locked_operation(session, operation_id)
        expected = target.registration
        source = await session.get(NebiusEnvironmentOperation, target.source_operation_id)
        source_registration = expected.model_dump(mode="json") | {
            "desired_state": "active", "deployment_generation": expected.deployment_generation - 1,
        }
        if (registration_view(environment) != expected or operation.action != "destroy_retained"
                or operation.environment_id != expected.environment_id
                or operation.owner_user_id != expected.owner_user_id
                or operation.deployment_generation != expected.deployment_generation
                or operation.plan_json.get("registration") != expected.model_dump(mode="json")
                or operation.plan_json.get("source_operation_id") != str(target.source_operation_id)
                or source is None or source.action != "create" or source.phase != "blocked"
                or source.error_code != "environment_destroy_requested" or source.lease_token is not None
                or source.environment_id != expected.environment_id or source.owner_user_id != expected.owner_user_id
                or source.deployment_generation != expected.deployment_generation - 1
                or source.plan_json.get("registration") != source_registration):
            raise ManagementError("retirement_target_unqualified")
        rows = (await session.scalars(select(NebiusEnvironmentResource).where(
            NebiusEnvironmentResource.operation_id == target.source_operation_id,
        ))).all()
        namespaces = {row.payload_json["metadata"]["name"]: row.provider_identity for row in rows
            if row.kind == "kubernetes" and row.payload_json.get("kind") == "Namespace"}
        material = next((row for row in rows if row.resource_key == "credentials:material"), None)
        if (namespaces != {name: str(uid) for name, uid in target.namespace_uids.items()}
                or material is None or material.phase != "planned" or material.provider_identity is not None):
            raise ManagementError("retirement_target_unqualified")
        return operation, environment, now


async def reconcile_retirement(
    session_factory: async_sessionmaker[AsyncSession], target: RetirementTarget,
    kubernetes: KubernetesEnvironmentProvider,
) -> str:
    """One exact attempt; report registry state, not the worker's return value."""
    registry = RetirementRegistry(session_factory, target)
    provider = EnvironmentRetainedDestroy(registry, kubernetes, cloud=None, child=None)
    await EnvironmentWorker(registry, provider).reconcile_once(target.operation_id)
    async with session_factory.begin() as session:
        operation, _, _ = await registry._locked_operation(session, target.operation_id)
        return operation.phase


class RetirementSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: Literal["loom.nebius-retirement.v1"]
    namespace: str = Field(pattern=r"^loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?$")
    kubernetes: ProjectedKubernetesConnection
    targets: tuple[RetirementTarget, ...] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def _mounted_identity(self) -> Self:
        names = [name for target in self.targets for name in target.namespace_uids]
        if (len(set(names)) != len(names) or self.namespace in names
                or len({target.registration.cluster_id for target in self.targets}) != 1
                or self.kubernetes.ca_file != Path("/var/run/loom-retirement-kubernetes/ca.crt")
                or self.kubernetes.token_file != Path("/var/run/loom-retirement-kubernetes/token")):
            raise ValueError("retirement_settings_unqualified")
        return self


def retirement_database_url(value: str, namespace: str) -> URL:
    try:
        url = make_url(value)
        if (url.drivername != "postgresql" or url.username != "loom_service" or not url.password
                or url.host != f"loom-postgres.{namespace}.svc" or url.port != 5432 or url.database != "loom"
                or dict(url.query) != {"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"}):
            raise ValueError
        return url.set(drivername="postgresql+psycopg")
    except Exception:
        raise ValueError("retirement_database_unqualified") from None


async def run_retirement(settings: RetirementSettings, database_url: str) -> None:
    url = retirement_database_url(database_url, settings.namespace)
    credentials = ProjectedKubernetesCredentials(settings.kubernetes)
    engine = create_async_engine(url, pool_size=2, max_overflow=0, pool_timeout=10,
        connect_args={"connect_timeout": 10, "options": "-c statement_timeout=10000 -c lock_timeout=5000"})
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with asyncio.timeout(1700), httpx.AsyncClient(
            base_url=settings.kubernetes.endpoint, verify=credentials.ssl_context,
            auth=NebiusManagementAuth(settings.kubernetes, credentials), trust_env=False, follow_redirects=False, timeout=30,
        ) as http:
            kube = KubernetesEnvironmentProvider(http)
            # Qualify every target before any one is claimed. Credentials and
            # origin come only from the fixed projected mount, never kubeconfig.
            for target in settings.targets:
                registry = RetirementRegistry(factory, target)
                async with factory.begin() as session:
                    await registry._locked_operation(session, target.operation_id)
                for name, uid in target.namespace_uids.items():
                    namespace = await kube._request("GET", "/api/v1/namespaces/" + name)
                    if namespace is None:
                        raise ManagementError("retirement_namespace_unqualified")
                    kube._identity(namespace, {"metadata": {"name": name, "labels": {
                        "loom.nebius/environment-id": str(target.registration.environment_id),
                        "loom.nebius/incarnation": str(target.registration.incarnation),
                    }}}, str(uid))
            for target in settings.targets:
                while True:
                    phase = await reconcile_retirement(factory, target, kube)
                    if phase == "completed":
                        break
                    if phase == "blocked":
                        raise ManagementError("retirement_operation_blocked")
                    # Same immutable operation/lease, never the general queue.
                    await asyncio.sleep(2)
    finally:
        await engine.dispose()
        await credentials.close()


def main() -> int:
    try:
        with Path("/var/run/loom-retirement/retirement.json").open("rb") as stream:
            raw = stream.read(262145)
        if len(raw) > 262144:
            raise ValueError
        settings = RetirementSettings.model_validate_json(raw)
        asyncio.run(run_retirement(settings, os.environ["LOOM_RETIREMENT_DB_URL"]))
        print(json.dumps({"status": "retirement_completed"}))
        return 0
    except Exception:
        print(json.dumps({"status": "retirement_blocked"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
