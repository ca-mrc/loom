"""Fixed read-only probe executed inside the existing management service.

Keep imports compatible with the installed pre-retirement manager. This file is
sent as protected source, not imported from that older image. No worker runs here.
"""
from __future__ import annotations

import json
import os
import re
import signal
import sys
from typing import Any
from uuid import UUID

from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from loom.db.nebius_environment_schema import (
    NebiusEnvironment,
    NebiusEnvironmentOperation,
    NebiusEnvironmentResource,
)
from loom.nebius_environment_contract import EnvironmentRegistrationV1
from loom_service.environment_management.registry import registration_view

CHECKS = ("registration", "operation_binding", "operation_plan", "source_binding", "source_fenced",
          "source_plan", "namespace_identities", "material_undelivered")
ERRORS = ("ValueError", "KeyError", "ValidationError", "TimeoutError", "OperationalError", "ProgrammingError")


def observe(database_url: str, targets: list[dict[str, Any]]) -> dict[str, Any]:
    """A server-enforced read-only snapshot, never locks, leases or reconciliation."""
    engine = create_engine(database_url, pool_size=1, max_overflow=0, pool_timeout=10,
        isolation_level="REPEATABLE READ", connect_args={"connect_timeout": 10,
            "options": "-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=5000"})
    try:
        with Session(engine, autoflush=False) as session, session.begin():
            if session.scalar(text("SHOW transaction_read_only")) != "on":
                raise ValueError("read_only_required")
            results = []
            for target in targets:
                expected = EnvironmentRegistrationV1.model_validate(target["registration"])
                operation = session.get(NebiusEnvironmentOperation, UUID(target["operation_id"]))
                environment = session.get(NebiusEnvironment, expected.environment_id)
                source = session.get(NebiusEnvironmentOperation, UUID(target["source_operation_id"]))
                registration = expected.model_dump(mode="json")
                source_registration = registration | {"desired_state": "active",
                    "deployment_generation": expected.deployment_generation - 1}
                rows = session.scalars(select(NebiusEnvironmentResource).where(
                    NebiusEnvironmentResource.operation_id == UUID(target["source_operation_id"]))).all()
                namespaces = {row.payload_json["metadata"]["name"]: row.provider_identity for row in rows
                    if row.kind == "kubernetes" and row.payload_json.get("kind") == "Namespace"}
                material = next((row for row in rows if row.resource_key == "credentials:material"), None)
                checks = {
                    "registration": environment is not None and registration_view(environment) == expected,
                    "operation_binding": operation is not None and operation.action == "destroy_retained"
                        and operation.environment_id == expected.environment_id and operation.owner_user_id == expected.owner_user_id
                        and operation.deployment_generation == expected.deployment_generation,
                    "operation_plan": operation is not None and operation.plan_json.get("registration") == registration
                        and operation.plan_json.get("source_operation_id") == target["source_operation_id"],
                    "source_binding": source is not None and source.action == "create"
                        and source.environment_id == expected.environment_id and source.owner_user_id == expected.owner_user_id
                        and source.deployment_generation == expected.deployment_generation - 1,
                    "source_fenced": source is not None and source.phase == "blocked"
                        and source.error_code == "environment_destroy_requested" and source.lease_token is None,
                    "source_plan": source is not None and source.plan_json.get("registration") == source_registration,
                    "namespace_identities": namespaces == target["namespace_uids"],
                    "material_undelivered": material is not None and material.phase == "planned" and material.provider_identity is None,
                }
                results.append({"operation_id": target["operation_id"], "checks": checks})
            return {"status": "observed", "read_only": True, "targets": results}
    finally:
        engine.dispose()


def _deadline(signum: int, frame: Any) -> None:
    raise TimeoutError


def main() -> int:
    stage = "inputs"
    try:
        signal.signal(signal.SIGALRM, _deadline)
        signal.alarm(60)
        if len(sys.argv) != 3 or len(sys.argv[2].encode()) > 65536:
            raise ValueError
        namespace, targets = sys.argv[1], json.loads(sys.argv[2])
        if (re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", namespace) is None
                or not isinstance(targets, list) or not 1 <= len(targets) <= 16):
            raise ValueError
        for target in targets:
            if set(target) != {"operation_id", "source_operation_id", "registration", "namespace_uids"}:
                raise ValueError
            row = EnvironmentRegistrationV1.model_validate(target["registration"])
            for value in (target["operation_id"], target["source_operation_id"], *target["namespace_uids"].values()):
                if str(UUID(value)) != value or not UUID(value).int:
                    raise ValueError
            if (row.scope != "personal" or row.binding_mode != "generated" or row.desired_state != "destroyed"
                    or row.deployment_generation < 2 or set(target["namespace_uids"]) != set(row.namespaces)):
                raise ValueError
        stage = "database"
        url = make_url(os.environ["LOOM_SVC_DB_URL"])
        if (url.drivername != "postgresql" or url.username != "loom_service" or not url.password
                or url.host != f"loom-postgres.{namespace}.svc" or url.port != 5432 or url.database != "loom"
                or dict(url.query) != {"sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt"}):
            raise ValueError
        stage = "registry"
        result = observe(url.set(drivername="postgresql+psycopg").render_as_string(hide_password=False), targets)
    except Exception as error:
        kind = type(error).__name__
        result = {"status": "unavailable", "stage": stage, "error_type": kind if kind in ERRORS else "OtherError"}
    finally:
        signal.alarm(0)
    print(json.dumps(result))
    # A completed diagnostic is not a readiness claim, even when unavailable.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
