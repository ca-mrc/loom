#!/usr/bin/env python3
"""Read-only census of the execution leases that keep rollout non-idle."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.deploy_nebius_platform import (  # noqa: E402
    DeploymentError,
    Kubectl,
    verify_cluster_identity,
)

_SCHEMA = "loom.nebius-execution-lease-census.v1"
_SAFE_CODE = re.compile(r"[a-z][a-z0-9_.-]{0,119}\Z")
_MAX_LEASES = 128
_MAX_COMMANDS = 256

# This program is deliberately self-contained: it runs inside the installed
# control-plane Pod, where its DB URL and CA are already mounted. It performs
# no import of the checked-out models and makes no writes.
_CENSUS_PROGRAM = r'''
import asyncio
import json
import re
from datetime import datetime
from uuid import UUID
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from loom_control_plane.config import ControlPlaneSettings

_LEASE_SQL = text("""
SELECT
  e.id AS lease_id,
  e.request_id,
  e.trial_id,
  e.team_id,
  e.execution_role,
  e.parent_lease_id,
  e.attempt,
  e.verifier_retry,
  e.generation,
  e.resource_generation,
  e.desired_state,
  e.observed_state,
  e.cleanup_state,
  e.cleanup_requested_at,
  e.cleanup_deadline_at,
  e.namespace_name,
  e.job_name,
  e.job_uid,
  e.pod_uid,
  e.kubernetes_resource_version,
  e.node_name,
  e.deadline_at,
  e.pod_scheduled_at,
  e.pod_started_at,
  e.pod_terminated_at,
  e.last_reconciled_at,
  e.last_heartbeat_at,
  e.last_event_ordinal,
  e.revoked_at,
  e.finalized_at,
  e.deleted_at,
  e.output_commit_state,
  e.output_committed_at,
  e.materialization_state,
  e.materialization_attempts,
  e.materialization_next_attempt_at,
  e.materialization_claim_expires_at,
  e.materialization_started_at,
  e.materialization_committed_at,
  e.materialization_error_code,
  e.source_cleanup_state,
  e.source_retain_until,
  e.source_cleanup_attempts,
  e.error_class,
  e.error_code,
  e.created_at,
  e.updated_at,
  t.state AS trial_state,
  t.failure_reason AS trial_failure_reason,
  t.finished_at AS trial_finished_at,
  t.attempt_count AS trial_attempt_count,
  (t.result IS NOT NULL) AS trial_result_present
FROM execution_leases e
JOIN trials t ON t.id = e.trial_id
WHERE e.revoked_at IS NULL
   OR e.cleanup_state <> 'complete'
   OR e.materialization_state IN ('pending','running')
ORDER BY e.created_at ASC, e.id ASC
LIMIT 129
""")
_COMMAND_COUNT_SQL = text("SELECT count(*) FROM execution_commands WHERE lease_id = :lease_id")
_COMMAND_SQL = text("""
SELECT generation, sequence, command_type, state, delivery_count,
       available_at, claim_expires_at, acknowledged_at,
       last_error_code, updated_at
FROM execution_commands
WHERE lease_id = :lease_id
ORDER BY generation ASC, sequence ASC, id ASC
LIMIT 257
""")
_ACTIVITY_SQL = text("""
SELECT count(*)
FROM execution_leases
WHERE revoked_at IS NULL
   OR cleanup_state <> 'complete'
   OR materialization_state IN ('pending','running')
""")


def _value(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (datetime, UUID)):
        return str(value)
    raise ValueError("unsupported value")


def _code(value):
    if value is None:
        return None
    return value if isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_.-]{0,119}", value) else "other"


def _row(row):
    result = {}
    for key, value in row.items():
        if key in {"materialization_error_code", "error_class", "error_code", "trial_failure_reason"}:
            result[key] = _code(value)
        else:
            result[key] = _value(value)
    return result


async def _run():
    settings = ControlPlaneSettings()
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions.begin() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='15s'"))
            schema_head = await session.scalar(text("SELECT version_num FROM alembic_version"))
            active_count = int(await session.scalar(_ACTIVITY_SQL))
            leases = (await session.execute(_LEASE_SQL)).mappings().all()
            if active_count > 128 or len(leases) != active_count:
                raise ValueError("lease census bound changed")
            rows = []
            for lease in leases:
                lease_id = lease["lease_id"]
                command_count = int(await session.scalar(_COMMAND_COUNT_SQL, {"lease_id": lease_id}))
                if command_count > 256:
                    raise ValueError("command census bound changed")
                commands = (await session.execute(_COMMAND_SQL, {"lease_id": lease_id})).mappings().all()
                if len(commands) != command_count:
                    raise ValueError("command census truncated")
                item = _row(lease)
                item["activity_reasons"] = [
                    reason for reason, active in (
                        ("lease_not_revoked", lease["revoked_at"] is None),
                        ("cleanup_incomplete", lease["cleanup_state"] != "complete"),
                        ("materialization_pending", lease["materialization_state"] in {"pending", "running"}),
                    ) if active
                ]
                item["commands"] = [_row(command) for command in commands]
                rows.append(item)
            print(json.dumps({
                "read_only": True,
                "schema_head": _value(schema_head),
                "active_count": active_count,
                "leases": rows,
            }, sort_keys=True))
    finally:
        await engine.dispose()


asyncio.run(_run())
'''


def _validate_remote(value: object) -> dict[str, Any]:
    if not isinstance(value, dict) or value.get("read_only") is not True:
        raise DeploymentError("lease census did not prove read-only execution")
    # 0180 adds preview storage, 0181 extends image preparation identity and
    # 0182 adds llm_calls projections and 0183 adds provider request pools;
    # none changes the fixed
    # lease/command/trial read columns.
    if value.get("schema_head") not in {"0175", "0176", "0177", "0178", "0179", "0180", "0181", "0182", "0183"}:
        raise DeploymentError("unsupported installed schema")
    count = value.get("active_count")
    leases = value.get("leases")
    if (
        type(count) is not int
        or not 0 <= count <= _MAX_LEASES
        or not isinstance(leases, list)
        or len(leases) != count
    ):
        raise DeploymentError("lease census cardinality is unqualified")
    for lease in leases:
        if not isinstance(lease, dict) or not isinstance(lease.get("lease_id"), str):
            raise DeploymentError("lease census identity is unqualified")
        try:
            if not UUID(lease["lease_id"]).int:
                raise ValueError
        except (ValueError, AttributeError):
            raise DeploymentError("lease census identity is unqualified") from None
        for key in ("team_id", "trial_id"):
            binding = lease.get(key)
            if not isinstance(binding, str):
                raise DeploymentError("lease census binding is unqualified")
            try:
                if not UUID(binding).int:
                    raise ValueError
            except (ValueError, AttributeError):
                raise DeploymentError("lease census binding is unqualified") from None
        commands = lease.get("commands")
        if not isinstance(commands, list) or len(commands) > _MAX_COMMANDS:
            raise DeploymentError("lease census commands are unqualified")
    return value


def census(api: Kubectl, *, namespace: str, expected_cluster_id: str) -> dict[str, Any]:
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", namespace):
        raise DeploymentError("invalid namespace")
    data = api.get("configmap", "loom-platform-config", namespace)["data"]
    config = json.loads(data["environment.json"])
    if config.get("namespace") != namespace:
        raise DeploymentError("configured namespace does not match selected target")
    verify_cluster_identity(api, config, expected_cluster_id)
    remote = json.loads(
        api.run(
            "exec",
            "-n",
            namespace,
            "deployment/loom-control-plane",
            "--",
            "python",
            "-c",
            _CENSUS_PROGRAM,
            timeout=90,
        )
    )
    result = _validate_remote(remote)
    return {
        "schema_version": _SCHEMA,
        "status": "observed",
        "observed_at": datetime.now().astimezone().isoformat(),
        "cluster_id": expected_cluster_id,
        "namespace": namespace,
        "execution_namespace": config.get("execution_namespace"),
        **result,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--namespace", default="loom-nebius-platform")
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = census(
            Kubectl(args.kubeconfig),
            namespace=args.namespace,
            expected_cluster_id=args.expected_cluster_id,
        )
        args.evidence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        output = args.evidence_dir / "lease-census.json"
        output.write_text(json.dumps(result, sort_keys=True) + "\n")
        output.chmod(0o600)
        print(json.dumps({"status": result["status"], "active_count": result["active_count"]}))
        return 0
    except Exception as error:
        # Do not export SQL, credentials, raw errors, or command payloads.
        result = {
            "schema_version": _SCHEMA,
            "status": "blocked",
            "error_type": type(error).__name__,
        }
        args.evidence_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        output = args.evidence_dir / "lease-census.json"
        output.write_text(json.dumps(result, sort_keys=True) + "\n")
        output.chmod(0o600)
        print(json.dumps({"status": "blocked"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
