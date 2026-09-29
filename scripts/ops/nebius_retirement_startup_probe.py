"""Fixed read-only startup observation in the original retirement Pod context.

Executed as checked-in source with the original image, not an installed ops
module. Never invoke the worker, retirement reconciler or locking registry API.
An observed result establishes current startup behavior, not cleanup authority.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select, text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.nebius_environment_schema import (
    NebiusEnvironmentOperation,
    NebiusEnvironmentResource,
)
from loom_service.environment_management.kubernetes_credentials import (
    ProjectedKubernetesCredentials,
)
from loom_service.environment_management.kubernetes_provider import KubernetesEnvironmentProvider
from loom_service.environment_management.retirement import (
    RetirementSettings,
    RetirementTarget,
    retirement_database_url,
)
from loom_service.environment_management.runtime import NebiusManagementAuth

SCHEMA = "loom.nebius-retirement-startup-probe.v1"
SETTINGS_PATH = Path("/var/run/loom-retirement/retirement.json")
ERROR_TYPES = frozenset({"ValueError", "KeyError", "ValidationError", "FileNotFoundError", "PermissionError",
    "SSLError", "TimeoutError", "OperationalError", "ProgrammingError", "InternalError", "StatementError",
    "HTTPStatusError", "ConnectError", "ConnectTimeout", "ReadTimeout", "RemoteProtocolError",
    "ProviderBlockedError", "ProviderRetryError"})


async def database_snapshot(url: URL, targets: tuple[RetirementTarget, ...]) -> list[dict[str, Any]]:
    """One server-enforced read-only snapshot; no flushes, claims or row locks."""
    engine = create_async_engine(url, pool_size=1, max_overflow=0, pool_timeout=10,
        isolation_level="REPEATABLE READ", connect_args={"connect_timeout": 10,
            "options": "-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=5000"})
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False, autoflush=False)
        async with factory.begin() as session:
            if await session.scalar(text("SHOW transaction_read_only")) != "on":
                raise ValueError("read_only_required")
            result = []
            for target in targets:
                operation = await session.get(NebiusEnvironmentOperation, target.operation_id)
                source = await session.get(NebiusEnvironmentOperation, target.source_operation_id)
                expected = target.registration
                if (operation is None or source is None or operation.action != "destroy_retained"
                        or operation.environment_id != expected.environment_id
                        or operation.owner_user_id != expected.owner_user_id
                        or operation.deployment_generation != expected.deployment_generation
                        or operation.plan_json.get("registration") != expected.model_dump(mode="json")
                        or operation.plan_json.get("source_operation_id") != str(target.source_operation_id)
                        or source.action != "create" or source.environment_id != expected.environment_id
                        or source.owner_user_id != expected.owner_user_id
                        or source.deployment_generation != expected.deployment_generation - 1):
                    raise ValueError("retirement_operation_binding")
                # Select status/presence only, never resource payloads or raw
                # provider identities, for the externally returned report.
                resources = (await session.execute(select(
                    NebiusEnvironmentResource.phase,
                    NebiusEnvironmentResource.provider_identity.is_not(None),
                ).where(NebiusEnvironmentResource.operation_id == target.operation_id))).all()
                result.append({"operation_id": str(target.operation_id), "phase": operation.phase,
                    "runner_epoch": operation.runner_epoch, "lease_present": operation.lease_token is not None,
                    "error_present": operation.error_code is not None, "resource_count": len(resources),
                    "effects_started": any(phase != "planned" or present for phase, present in resources)})
            return result
    finally:
        await engine.dispose()


def _failure(stage: str, checks: list[str], operations: list[dict[str, Any]], error: Exception) -> dict[str, Any]:
    kind = type(error).__name__
    return {"schema": SCHEMA, "status": "unavailable", "stage": stage, "checks": checks, "operations": operations,
        "error_type": kind if kind in ERROR_TYPES else "OtherError",
        "http_status": error.response.status_code if isinstance(error, httpx.HTTPStatusError) else None}


async def observe_startup(settings: RetirementSettings, database_url: str) -> dict[str, Any]:
    checks: list[str] = []
    operations: list[dict[str, Any]] = []
    credentials: ProjectedKubernetesCredentials | None = None
    stage = "database_binding"
    try:
        async with asyncio.timeout(150):
            url = retirement_database_url(database_url, settings.namespace)
            checks.append(stage)
            stage = "kubernetes_ca"
            credentials = ProjectedKubernetesCredentials(settings.kubernetes)
            checks.append(stage)
            stage = "kubernetes_token"
            await credentials.get_token()
            checks.append(stage)
            stage = "database"
            operations = await database_snapshot(url, settings.targets)
            checks.append(stage)
            stage = "kubernetes_get"
            async with httpx.AsyncClient(base_url=settings.kubernetes.endpoint, verify=credentials.ssl_context,
                    auth=NebiusManagementAuth(settings.kubernetes, credentials), trust_env=False,
                    follow_redirects=False, timeout=15, headers={"Accept-Encoding": "identity"}) as http:
                for target in settings.targets:
                    for name, uid in target.namespace_uids.items():
                        stage = "kubernetes_get"
                        async with http.stream("GET", "/api/v1/namespaces/" + name) as response:
                            response.raise_for_status()
                            if response.status_code != 200 or response.headers.get("content-encoding", "identity") != "identity":
                                raise ValueError("namespace_response")
                            body = bytearray()
                            async for chunk in response.aiter_bytes(chunk_size=8192):
                                if len(body) + len(chunk) > 65536:
                                    raise ValueError("namespace_response")
                                body.extend(chunk)
                        stage = "kubernetes_identity"
                        namespace = json.loads(body)
                        if (not isinstance(namespace, dict) or namespace.get("kind") != "Namespace"
                                or namespace.get("apiVersion") != "v1"
                                or namespace.get("metadata", {}).get("ownerReferences")):
                            raise ValueError("namespace_identity")
                        KubernetesEnvironmentProvider._identity(namespace, {"metadata": {"name": name, "labels": {
                            "loom.nebius/environment-id": str(target.registration.environment_id),
                            "loom.nebius/incarnation": str(target.registration.incarnation),
                        }}}, str(uid))
            checks.append("kubernetes")
        return {"schema": SCHEMA, "status": "observed", "stage": "complete", "checks": checks, "operations": operations}
    except Exception as error:
        return _failure(stage, checks, operations, error)
    finally:
        if credentials is not None:
            await credentials.close()


def main() -> int:
    stage = "settings"
    try:
        with SETTINGS_PATH.open("rb") as stream:
            raw = stream.read(262145)
        if len(raw) > 262144:
            raise ValueError("settings_size")
        settings = RetirementSettings.model_validate_json(raw)
        stage = "database_binding"
        database_url = os.environ["LOOM_RETIREMENT_DB_URL"]
        report = asyncio.run(observe_startup(settings, database_url))
    except Exception as error:
        report = _failure(stage, [], [], error)
    print(json.dumps(report, sort_keys=True))
    # Zero indicates a completed diagnostic protocol, not retirement readiness.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
