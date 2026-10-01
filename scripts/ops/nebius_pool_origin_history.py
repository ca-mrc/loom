"""Fixed management-history readback for the protected closed pool cutover.

No CLI, probe Job, general SQL, writes, credential delivery or admission opening.
The entry supplies the completed predecessor's distinct management DB binding.
Every pending-origin page is qualified against original registration/source history.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_pool_migration import (
    PoolGuardDatabase,
    PoolGuardTarget,
    PoolMigrationError,
    PoolMigrationRequest,
    migration_contract,
)
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI

from loom.nebius_platform_render import digest
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority
from loom_service.pool_management.origin import (
    PoolApplicationHistoryV1,
    PoolOperationHistoryV1,
    qualify_pool_history,
)


def _origins(origins: tuple[PoolWorkOriginV1, ...]) -> tuple[PoolWorkOriginV1, ...]:
    if not isinstance(origins, tuple) or len(origins) > 128:
        raise ValueError("pool_management_history_page_unqualified")
    return tuple(PoolWorkOriginV1.model_validate(row.model_dump()) for row in origins)


def pool_management_history_sql(origins: tuple[PoolWorkOriginV1, ...]) -> str:
    """Project only relevant history from one real READ ONLY database snapshot."""
    rows = [row.model_dump(mode="json") for row in _origins(origins)]
    encoded = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode().hex()
    return f"""BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='2s'; SET LOCAL search_path=pg_catalog,public,pg_temp;
DO $pool_management_history$
BEGIN
    IF (SELECT version_num FROM public.alembic_version) IS DISTINCT FROM '0172'
    THEN RAISE EXCEPTION 'pool management history schema unqualified'; END IF;
END $pool_management_history$;
WITH origins AS (
    SELECT value AS origin, ordinal FROM jsonb_array_elements(
        convert_from(decode('{encoded}','hex'),'UTF8')::jsonb) WITH ORDINALITY input(value,ordinal)
), history AS (
    SELECT origins.ordinal, origins.origin,
        CASE WHEN a.application_id IS NULL THEN NULL ELSE json_build_object(
            'application_id',a.application_id,'incarnation',a.incarnation,
            'data_environment_id',a.data_environment_id,'cluster_id',a.cluster_id,
            'owner_user_id',a.owner_user_id,'owner_team_id',a.owner_team_id,
            'deployment_generation',a.deployment_generation) END AS application,
        CASE WHEN o.operation_id IS NULL THEN NULL ELSE json_build_object(
            'application_id',o.application_id,'owner_user_id',o.owner_user_id,
            'deployment_generation',o.deployment_generation,'action',o.action,
            'plan_schema',o.plan_json->'schema_version',
            'registration',o.plan_json->'registration','release',o.plan_json->'release') END AS operation
      FROM origins LEFT JOIN public.nebius_applications a
        ON a.application_id=(origin->'application'->>'application_id')::uuid
      LEFT JOIN public.nebius_application_operations o ON o.application_id=a.application_id
        AND o.deployment_generation=(origin->'application'->>'deployment_generation')::bigint
)
SELECT json_build_object('schema','loom.pool-management-history.v1','schema_revision','0172',
    'read_only',current_setting('transaction_read_only')='on',
    'rows',COALESCE(json_agg(history ORDER BY ordinal),'[]'::json)) FROM history;
ROLLBACK;
"""


def qualify_management_history_page(report: Any, *, origins: tuple[PoolWorkOriginV1, ...],
                                    participant: PoolParticipantV1, cluster_id: str) -> tuple[int, ...]:
    """Reject incomplete/reordered/foreign projections; never manufacture origins."""
    origins = _origins(origins)
    if (not isinstance(report, dict) or set(report) != {"schema", "schema_revision", "read_only", "rows"}
            or report["schema"] != "loom.pool-management-history.v1" or report["schema_revision"] != "0172"
            or report["read_only"] is not True or not isinstance(report["rows"], list)
            or len(report["rows"]) != len(origins)):
        raise ValueError("pool_management_history_report_unqualified")
    priorities = []
    for ordinal, (origin, row) in enumerate(zip(origins, report["rows"], strict=True), start=1):
        if (not isinstance(row, dict) or set(row) != {"ordinal", "origin", "application", "operation"}
                or type(row["ordinal"]) is not int or row["ordinal"] != ordinal
                or row["origin"] != origin.model_dump(mode="json")):
            raise ValueError("pool_management_history_origin_unqualified")
        priorities.append(qualify_pool_history(origin, participant=participant, cluster_id=cluster_id, workload_kind="trial",
            application=PoolApplicationHistoryV1.model_validate(row["application"]) if row["application"] is not None else None,
            operation=PoolOperationHistoryV1.model_validate(row["operation"]) if row["operation"] is not None else None))
    return tuple(priorities)


@dataclass(frozen=True, repr=False)
class PoolManagementHistoryTarget:
    namespace: str
    namespace_uid: UUID
    controller: dict[str, Any]
    database: PoolGuardDatabase


def _target_contract(target: PoolManagementHistoryTarget) -> dict[str, Any]:
    return {"namespace": target.namespace, "namespace_uid": str(target.namespace_uid), "controller": target.controller,
        "database": {"statefulset": target.database.statefulset, "service": target.database.service,
            "credential_uid": str(target.database.credential_uid),
            "credential_resource_version": target.database.credential_resource_version}}


class KubectlPoolHistoryAPI(KubectlPoolGuardAPI):
    """Read management history through its retained database, never a participant.

    The connected protected entry derives this target from the completed manager
    predecessor, independently of the participant database roster. The parent
    freezes producers and qualifies their current old-or-journaled-new templates.
    """

    def __init__(self, *, request: PoolMigrationRequest, target: PoolManagementHistoryTarget,
                 kubeconfig: Path, executable: Path):
        try:
            binding = request.registration.binding
            manager = target.controller
            if ((target.namespace, str(target.namespace_uid)) != (binding.namespace, binding.namespace_uid)
                    or manager.get("apiVersion") != "apps/v1" or manager.get("kind") != "Deployment"
                    or manager["metadata"].get("name") != "loom-service"
                    or manager["metadata"].get("namespace") != target.namespace):
                raise ValueError
            _uid(manager)
            _snapshot(manager)
            super().__init__(request=request, kubeconfig=kubeconfig, executable=executable)
            self.target = target
            self.history_sha256 = digest(_target_contract(target))
        except Exception:
            raise PoolMigrationError("management_history_configuration") from None

    def qualify_pending_origins(self, target: PoolGuardTarget, origins: tuple[PoolWorkOriginV1, ...]) -> None:
        try:
            if (target not in self.request.guards or digest(migration_contract(self.request)) != self.contract_sha256
                    or digest(_target_contract(self.target)) != self.history_sha256
                    or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
                raise ValueError
            participant, = (row for row in self.request.registration.spec.participants if row.participant_id == target.participant_id)
            origins = _origins(origins)
            for origin in origins:
                pool_request_priority(participant, origin, workload_kind="trial")
            query = pool_management_history_sql(origins)
            before = self._database(self.target, url_variable="LOOM_SVC_DB_URL")
            report = self._run(["exec", "-n", self.target.namespace, "pod/" + before["metadata"]["name"], "-c", "loom-postgres", "--",
                "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", "-c", query])
            qualify_management_history_page(report, origins=origins, participant=participant, cluster_id=self.request.registration.spec.cluster_id)
            if _uid(self._database(self.target, url_variable="LOOM_SVC_DB_URL")) != _uid(before):
                raise ValueError
        except Exception:
            raise PoolMigrationError("management_origin_history") from None
