"""Fixed idle-guard commands bound to retained controller and database Pods.

Callable only by the protected migration, not an operator CLI. The parent owns
publication/predecessor qualification. This adapter neither releases intake nor
changes controller or Kubernetes authority. Ambiguous exec outcomes only read back.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import ipaddress
import os
import re
import secrets
import subprocess
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlencode
from uuid import UUID

from pydantic import PostgresDsn
from scripts.ops import nebius_certificates as private_state
from scripts.ops.deploy_nebius_platform import rollout_guard_observation_sql
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_evidence import _matches_backup_template
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_pool_migration import (
    PoolGuardDatabase,
    PoolGuardTarget,
    PoolMigrationError,
    PoolMigrationRequest,
    migration_contract,
)
from sqlalchemy.engine import make_url

from loom.nebius_application_database import _MIGRATION_READY
from loom.nebius_platform_render import digest
from loom.nebius_pool_contract import PoolParticipantV1
from loom.nebius_pool_priority import PoolWorkOriginV1, pool_request_priority
from loom.nebius_rollout_guard import ACTIVITY_SQL, LOCK_KEY
from loom_service.environment_management.candidates import _json

# The same settings instance is checked and used to open SQL. No new option or
# probe module is required in retained old images: their existing typed settings
# and idle-guard functions are sufficient. Never include a URL in exec arguments
# or serialize the settings/exception, including on validation or driver failure.
_BOUND_GUARD_COMMAND = """import asyncio, hmac, json, sys
from loom_control_plane.config import ControlPlaneSettings
from loom.nebius_rollout_guard import acquire, observe
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

async def run(settings):
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    try:
        async with AsyncSession(engine) as session, session.begin():
            action = {"acquire": acquire, "observe": observe}[sys.argv[1]]
            return await action(session, owner=sys.argv[2], candidate=sys.argv[3])
    finally:
        await engine.dispose()

try:
    if len(sys.argv) != 6 or sys.argv[1] not in {"acquire", "observe"}:
        raise ValueError()
    settings = ControlPlaneSettings()
    actual = hmac.new(bytes.fromhex(sys.argv[4]), settings.db_engine_url.encode(), "sha256").hexdigest()
    if not hmac.compare_digest(actual, sys.argv[5]):
        raise ValueError()
    print(json.dumps(asyncio.run(run(settings))))
except Exception:
    print("Pool guard database unqualified; preserve recovery evidence", file=sys.stderr)
    raise SystemExit(1)
"""

# Read-only: use each retained image's real settings, including its normal
# environment/.env precedence. No connection, import-selected module, SQL or
# credential appears in the command interface or its diagnostic output.
_BOUND_DATABASE_COMMAND = """import hmac, json, sys
try:
    if len(sys.argv) != 4:
        raise ValueError()
    if sys.argv[1] == "controller":
        from loom_control_plane.config import ControlPlaneSettings
        value = ControlPlaneSettings().db_engine_url
    elif sys.argv[1] == "service":
        from loom_service.config import LoomServiceSettings
        value = LoomServiceSettings().db_engine_url
    elif sys.argv[1] == "actuator":
        from loom_execution_actuator.config import ExecutionActuatorSettings
        value = ExecutionActuatorSettings().db_url
    else:
        raise ValueError()
    actual = hmac.new(bytes.fromhex(sys.argv[2]), value.encode(), "sha256").hexdigest()
    if not hmac.compare_digest(actual, sys.argv[3]):
        raise ValueError()
    print(json.dumps({"status": "qualified"}))
except Exception:
    print("Pool runtime database unqualified", file=sys.stderr)
    raise SystemExit(1)
"""


# The optional UID-aware production reader is an ordinary-rollout prerequisite.
# Old images fail here before downtime; never fall back to nodes/proxy, operator
# credentials or another endpoint to make a retained actuator appear ready.
_BOUND_TELEMETRY_COMMAND = """import asyncio, json, sys

async def run():
    from loom_execution_actuator.config import ExecutionActuatorSettings
    from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
    if len(sys.argv) != 5:
        raise ValueError()
    settings = ExecutionActuatorSettings()
    if (settings.namespace != sys.argv[1] or settings.target_id != sys.argv[2]
            or settings.kubernetes_connection is not None):
        raise ValueError()
    api = InClusterKubernetesJobApi()
    try:
        summary = await api.resource_summary(node_name=sys.argv[3], expected_node_uid=sys.argv[4])
        for field, counter in (("cpu", "usageCoreNanoSeconds"), ("memory", "workingSetBytes"), ("fs", "usedBytes")):
            value = summary["node"][field][counter]
            if type(value) is not int or value < 0:
                raise ValueError()
        return {"status": "qualified", "node_name": sys.argv[3], "node_uid": sys.argv[4]}
    finally:
        await api.close()

try:
    print(json.dumps(asyncio.run(run())))
except Exception:
    print("Pool runtime telemetry unqualified", file=sys.stderr)
    raise SystemExit(1)
"""


class PoolDatabaseReadTarget(Protocol):
    """Retained namespace-local database scope; not authority to acquire a guard."""

    @property
    def namespace(self) -> str: ...
    @property
    def namespace_uid(self) -> UUID: ...
    @property
    def controller(self) -> dict[str, Any]: ...
    @property
    def database(self) -> PoolGuardDatabase | None: ...


def qualify_database_destination(url: str, namespace: str) -> None:
    """Only the directly qualified namespace-local backend, not a proxy alias."""
    destination = make_url(url)
    if (destination.drivername not in {"postgresql", "postgresql+psycopg", "postgresql+asyncpg"}
            or destination.host != f"loom-postgres.{namespace}.svc" or destination.port not in (None, 5432)
            or destination.database != "loom" or not destination.username or not destination.password
            or destination.query.keys() - {"sslmode", "sslrootcert", "sslcert", "sslkey", "connect_timeout", "application_name"}
            or any(not isinstance(value, str) for value in destination.query.values())):
        raise ValueError("pool database destination unqualified")


def _runtime_pod_spec(actual: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    """Normalize only Kubernetes's standard, explicitly requested token mount.

    ServiceAccount admission changes Pods, not Deployment/ReplicaSet templates.
    Never treat an arbitrary projected volume or an extra mount as a default.
    """
    value = copy.deepcopy(actual)
    retained = {row["name"] for row in expected.get("volumes", [])}
    added = [row for row in value.get("volumes", []) if row["name"] not in retained]
    if not added:
        return value
    if expected.get("automountServiceAccountToken") is not True or len(added) != 1:
        raise ValueError
    volume, = added
    name = volume["name"]
    if (re.fullmatch(r"kube-api-access-[a-z0-9]{5}", name) is None
            or volume != {"name": name, "projected": {"defaultMode": 420, "sources": [
                {"serviceAccountToken": {"expirationSeconds": 3607, "path": "token"}},
                {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                {"downwardAPI": {"items": [{"path": "namespace", "fieldRef": {
                    "apiVersion": "v1", "fieldPath": "metadata.namespace"}}]}},
            ]}}):
        raise ValueError
    for field in ("containers", "initContainers"):
        for container, wanted in zip(value.get(field, []), expected.get(field, []), strict=True):
            mount, = (row for row in container.get("volumeMounts", []) if row["name"] == name)
            if (mount != {"name": name, "readOnly": True, "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount"}
                    or mount["readOnly"] is not True):
                raise ValueError
            container["volumeMounts"].remove(mount)
            if not container["volumeMounts"] and "volumeMounts" not in wanted:
                del container["volumeMounts"]
    value["volumes"].remove(volume)
    if not value["volumes"] and "volumes" not in expected:
        del value["volumes"]
    return value


def _backlog_cursor(value: str | None) -> str:
    if value is None:
        return ""
    kind, identity = value.split(":")
    if kind not in {"batch", "trial"} or str(UUID(identity)) != identity or not UUID(identity).int:
        raise ValueError("pool_backlog_cursor_unqualified")
    return value


def pool_cutover_readiness_sql(*, owner: str, candidate: str, logical_pool_id: str, after: str | None) -> str:
    """Read one fixed page; no SQL, credentials or queue mutations from callers.

    Retry/target/quota backoff is deliberately not a filter: retained work may
    become eligible after reopening. Include unfanned native batches as well as
    trial/build consumers. A legacy batch without a frozen pool is ambiguous,
    not proof that it belongs to another physical pool; require its original
    provenance or drain it through the retained path.
    """
    if (str(UUID(owner)) != owner or not UUID(owner).int or re.fullmatch(r"[0-9a-f]{40}", candidate) is None
            or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", logical_pool_id) is None):
        raise ValueError("pool_cutover_database_scope_unqualified")
    cursor = _backlog_cursor(after)
    readiness_sha256 = hashlib.sha256(_MIGRATION_READY.encode()).hexdigest()
    return f"""BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='2s'; SET LOCAL search_path=pg_catalog,public,pg_temp;
DO $pool_cutover_readiness$
DECLARE access_ready BOOLEAN;
BEGIN
    IF (SELECT version_num FROM public.alembic_version) IS DISTINCT FROM '0173'
    THEN RAISE EXCEPTION 'pool cutover schema unqualified'; END IF;
    IF NOT pg_try_advisory_xact_lock({LOCK_KEY}) OR EXISTS (
        SELECT 1 FROM public.nebius_rollout_guard
        WHERE id<>1 OR owner<>'{owner}' OR candidate_sha<>'{candidate}'
    ) OR EXISTS (SELECT 1 FROM ({ACTIVITY_SQL}) activity
        WHERE trials<>0 OR executions<>0 OR builds<>0 OR build_cleanup<>0)
      OR EXISTS (SELECT 1 FROM public.nebius_pool_execution_outbox WHERE phase NOT IN ('cancelled','released'))
      OR EXISTS (SELECT 1 FROM public.nebius_pool_build_outbox WHERE phase NOT IN ('cancelled','released'))
    THEN RAISE EXCEPTION 'pool cutover database not idle'; END IF;
    IF to_regnamespace('loom_application_access') IS NOT NULL THEN
        IF NOT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE p.oid=to_regprocedure('loom_application_access.migration_ready()')
              AND p.proowner=n.nspowner AND p.prolang=(SELECT oid FROM pg_language WHERE lanname='plpgsql')
              AND p.prorettype='boolean'::regtype AND p.prosecdef
              AND p.proconfig=ARRAY['search_path=pg_catalog, pg_temp']::text[]
              AND encode(sha256(convert_to(p.prosrc,'UTF8')),'hex')='{readiness_sha256}')
        THEN RAISE EXCEPTION 'pool cutover access guard unavailable'; END IF;
        EXECUTE 'SELECT loom_application_access.migration_ready()' INTO access_ready;
        IF access_ready IS DISTINCT FROM TRUE
        THEN RAISE EXCEPTION 'pool cutover application access active'; END IF;
    END IF;
END $pool_cutover_readiness$;
WITH pending AS (
    SELECT 'batch:' || b.id::text AS key, b.pool_origin AS origin,
           COALESCE(b.pool_origin->>'submission_id'=b.id::text,FALSE) AS source_matches
      FROM public.batches b
     WHERE b.backend='nebius' AND b.state NOT IN ('finished','cancelled')
       AND (b.service_execution_runtime_profile IS NULL
            OR b.service_execution_runtime_profile->>'logical_pool_id'='{logical_pool_id}')
    UNION ALL
    SELECT 'trial:' || t.id::text AS key, t.pool_origin AS origin,
           CASE WHEN t.batch_id IS NULL THEN TRUE
                ELSE COALESCE(t.team_id=b.team_id AND t.pool_origin=b.pool_origin
                     AND t.pool_origin->>'submission_id'=b.id::text,FALSE) END AS source_matches
      FROM public.trials t LEFT JOIN public.batches b ON b.id=t.batch_id
     WHERE t.state NOT IN ('succeeded','failed','cancelled') AND t.cancellation_requested_at IS NULL
       AND t.family_key IS NULL AND t.requires_caps->>'worker_pool'='{logical_pool_id}'
       AND (t.execution_route_pool_name IS NULL OR t.execution_route_pool_name='{logical_pool_id}')
       AND ((t.batch_id IS NULL AND t.requires_caps->>'backend'='nebius')
            OR (t.batch_id IS NOT NULL AND b.backend='nebius'))
), page AS (
    SELECT key,origin,source_matches FROM pending WHERE key COLLATE "C">'{cursor}' COLLATE "C"
     ORDER BY key COLLATE "C" LIMIT 128
)
SELECT json_build_object('status','observed','schema_revision','0173',
    'rows',COALESCE(json_agg(page ORDER BY key COLLATE "C"),'[]'::json)) FROM page;
ROLLBACK;
"""


def qualify_cutover_readiness_page(report: Any, *, participant: PoolParticipantV1, after: str | None) -> tuple[PoolWorkOriginV1, ...]:
    """Check a bound page, not management's retained application history.

    The connected caller must separately qualify every personal origin against
    the management registry. Parsing or a fixed SQL read is not that authority.
    """
    cursor = _backlog_cursor(after)
    if (not isinstance(report, dict) or set(report) != {"status", "schema_revision", "rows"}
            or report["status"] != "observed" or report["schema_revision"] != "0173"
            or not isinstance(report["rows"], list) or len(report["rows"]) > 128):
        raise ValueError("pool_cutover_database_report_unqualified")
    origins = []
    for row in report["rows"]:
        if (not isinstance(row, dict) or set(row) != {"key", "origin", "source_matches"}
                or row["source_matches"] is not True or not isinstance(row["key"], str)):
            raise ValueError("pool_cutover_pending_source_unqualified")
        key = _backlog_cursor(row["key"])
        if key <= cursor:
            raise ValueError("pool_cutover_pending_page_unqualified")
        cursor = key
        origin = PoolWorkOriginV1.model_validate(row["origin"])
        pool_request_priority(participant, origin, workload_kind="trial")
        if key.startswith("batch:") and str(origin.submission_id) != key.removeprefix("batch:"):
            raise ValueError("pool_cutover_pending_source_unqualified")
        origins.append(origin)
    return tuple(origins)


def pool_runtime_role_sql(*, owner: str, candidate: str, action: str) -> str:
    """Fixed participant ACL transition, never a general SQL or bootstrap flag.

    Both actions require the exact durable idle guard. Staging adds only the
    actuator's local journals and source-lock columns; it cannot open intake or
    grant access to management capacity/credential state. Unknown exec results
    are recovered with observation, not an automatic repeated write.
    """
    if (str(UUID(owner)) != owner or re.fullmatch(r"[0-9a-f]{40}", candidate) is None
            or action not in {"stage", "observe"}):
        raise ValueError("pool_runtime_role_scope_unqualified")
    grants = """
        GRANT SELECT, INSERT, UPDATE ON nebius_pool_execution_outbox, nebius_pool_build_outbox TO loom_actuator;
        GRANT SELECT ON tasks, batches TO loom_actuator;
        GRANT UPDATE (registered_at) ON tasks TO loom_actuator;
        GRANT UPDATE (pool_origin) ON batches TO loom_actuator;
        GRANT SELECT ON task_bundle_sources, task_bundle_source_incarnations, task_bundle_source_references TO loom_actuator;
        GRANT UPDATE (created_at) ON task_bundle_sources TO loom_actuator;
        GRANT INSERT ON task_bundle_source_references TO loom_actuator;
    """ if action == "stage" else ""
    # pool_origin is immutable under the published trigger: the column grant
    # permits source-row locks, not class promotion. Task content is read-only;
    # its registration timestamp is the only non-content lock column. Registered
    # source rows are immutable (including created_at); source refs may be pinned,
    # never published, retired or unpinned by this execution/build login.
    return f"""BEGIN {'READ ONLY' if action == 'observe' else ''};
SET LOCAL statement_timeout='10s'; SET LOCAL lock_timeout='2s'; SET LOCAL search_path=pg_catalog,public,pg_temp;
DO $pool_runtime_role$
DECLARE item RECORD;
BEGIN
    IF NOT pg_try_advisory_xact_lock({LOCK_KEY}) OR NOT EXISTS (
        SELECT 1 FROM nebius_rollout_guard WHERE id=1 AND owner='{owner}' AND candidate_sha='{candidate}'
    ) THEN RAISE EXCEPTION 'pool runtime role guard unqualified'; END IF;
    IF (SELECT version_num FROM alembic_version) IS DISTINCT FROM '0173' OR EXISTS (
        SELECT 1 FROM ({ACTIVITY_SQL}) activity
        WHERE trials<>0 OR executions<>0 OR builds<>0 OR build_cleanup<>0
    ) OR EXISTS (SELECT 1 FROM nebius_pool_execution_outbox WHERE phase NOT IN ('cancelled','released'))
      OR EXISTS (SELECT 1 FROM nebius_pool_build_outbox WHERE phase NOT IN ('cancelled','released'))
    THEN RAISE EXCEPTION 'pool runtime role database is not closed and idle'; END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='loom_actuator' AND rolcanlogin
        AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls))
      OR EXISTS (SELECT 1 FROM pg_auth_members WHERE member='loom_actuator'::regrole)
    THEN RAISE EXCEPTION 'pool runtime role identity unqualified'; END IF;
    {grants}
    FOR item IN SELECT unnest(ARRAY['nebius_pool_execution_outbox','nebius_pool_build_outbox']) AS name LOOP
        IF NOT has_table_privilege('loom_actuator',item.name,'SELECT')
          OR NOT has_table_privilege('loom_actuator',item.name,'INSERT')
          OR NOT has_table_privilege('loom_actuator',item.name,'UPDATE')
          OR has_table_privilege('loom_actuator',item.name,'DELETE,TRUNCATE,REFERENCES,TRIGGER')
        THEN RAISE EXCEPTION 'pool runtime journal authority unqualified'; END IF;
    END LOOP;
    FOR item IN SELECT c.relname,a.attname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_attribute a ON a.attrelid=c.oid WHERE n.nspname='public'
        AND c.relname IN ('tasks','batches','task_bundle_sources')
        AND a.attnum>0 AND NOT a.attisdropped LOOP
        IF has_column_privilege('loom_actuator',format('public.%I',item.relname),item.attname,'UPDATE')
           IS DISTINCT FROM ((item.relname='tasks' AND item.attname='registered_at')
                          OR (item.relname='batches' AND item.attname='pool_origin')
                          OR (item.relname='task_bundle_sources' AND item.attname='created_at'))
        THEN RAISE EXCEPTION 'pool runtime source authority unqualified'; END IF;
    END LOOP;
    IF NOT has_table_privilege('loom_actuator','tasks','SELECT')
      OR NOT has_table_privilege('loom_actuator','batches','SELECT')
      OR has_table_privilege('loom_actuator','tasks','INSERT,DELETE,TRUNCATE,REFERENCES,TRIGGER')
      OR has_table_privilege('loom_actuator','batches','INSERT,DELETE,TRUNCATE,REFERENCES,TRIGGER')
      OR has_any_column_privilege('loom_actuator','tasks','INSERT,REFERENCES')
      OR has_any_column_privilege('loom_actuator','batches','INSERT,REFERENCES')
    THEN RAISE EXCEPTION 'pool runtime source authority unqualified'; END IF;
    FOR item IN SELECT unnest(ARRAY['task_bundle_sources','task_bundle_source_incarnations',
        'task_bundle_source_references']) AS name LOOP
        IF NOT has_table_privilege('loom_actuator',item.name,'SELECT')
          OR has_table_privilege('loom_actuator',item.name,'DELETE,TRUNCATE,REFERENCES,TRIGGER')
          OR has_any_column_privilege('loom_actuator',item.name,'REFERENCES')
          OR has_any_column_privilege('loom_actuator',item.name,'INSERT')
             IS DISTINCT FROM (item.name='task_bundle_source_references')
          OR (item.name='task_bundle_source_references'
              AND NOT has_table_privilege('loom_actuator',item.name,'INSERT'))
          OR (item.name<>'task_bundle_sources' AND has_any_column_privilege('loom_actuator',item.name,'UPDATE'))
        THEN RAISE EXCEPTION 'pool runtime source journal authority unqualified'; END IF;
    END LOOP;
    FOR item IN SELECT unnest(ARRAY['task_bundle_source_writes','task_bundle_source_versions']) AS name LOOP
        IF has_table_privilege('loom_actuator',item.name,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
          OR has_any_column_privilege('loom_actuator',item.name,'SELECT,INSERT,UPDATE,REFERENCES')
        THEN RAISE EXCEPTION 'pool runtime source publication authority unqualified'; END IF;
    END LOOP;
    FOR item IN SELECT unnest(ARRAY['nebius_pool_bindings','nebius_pool_participants','nebius_pool_requests',
        'nebius_pool_machines','nebius_pool_machine_credentials','nebius_pool_captures','nebius_pool_observations',
        'nebius_pool_effects','nebius_pool_cleanup_observations','nebius_pool_cancellations']) AS name LOOP
        IF has_table_privilege('loom_actuator',item.name,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
          OR has_any_column_privilege('loom_actuator',item.name,'SELECT,INSERT,UPDATE,REFERENCES')
        THEN RAISE EXCEPTION 'pool runtime management authority unqualified'; END IF;
    END LOOP;
END $pool_runtime_role$;
COMMIT;
SELECT json_build_object('status','{'staged' if action == 'stage' else 'qualified'}');
"""


class KubectlPoolGuardAPI:
    def __init__(self, *, request: PoolMigrationRequest, kubeconfig: Path, executable: Path):
        try:
            if (not kubeconfig.is_absolute() or kubeconfig != kubeconfig.resolve()
                    or not executable.is_absolute()):
                raise ValueError
            self.request = request
            self.contract_sha256 = digest(migration_contract(request))
            self.kubeconfig = kubeconfig
            self.kubeconfig_sha256 = hashlib.sha256(private_state._private_read(kubeconfig, limit=512 * 1024)).hexdigest()
            cache = kubeconfig.parent / ".loom-pool-kubectl-cache"
            private_state._private_directory(cache)
            self.prefix = [str(executable), "--kubeconfig", str(kubeconfig), "--request-timeout=30s", "--cache-dir", str(cache)]
        except Exception:
            raise PoolMigrationError("guard_configuration") from None

    def _run(self, args: list[str]) -> dict[str, Any]:
        value = subprocess.run([*self.prefix, *args], capture_output=True, timeout=40, check=False,
            env={"PATH": os.defpath, "LANG": "C.UTF-8"})
        if value.returncode or len(value.stdout) > 4 * 1024**2:
            raise ValueError
        result = _json(value.stdout)
        if not isinstance(result, dict):
            raise ValueError
        return result

    def _get(self, kind: str, name: str, namespace: str | None = None) -> dict[str, Any]:
        return self._run(["get", kind, name, *(["-n", namespace] if namespace else []), "-o", "json"])

    def _pods(self, namespace: str, app: str) -> dict[str, Any]:
        # kubectl's generic list printer drops resourceVersion/continuation.
        # Read the fixed API collection so completeness remains verifiable.
        return self._run(["get", "--raw", f"/api/v1/namespaces/{namespace}/pods?labelSelector=app%3D{app}&limit=100"])

    def _namespaces(self, target: PoolDatabaseReadTarget) -> None:
        for name, uid in (("kube-system", self.request.registration.binding.kube_system_uid),
                (target.namespace, str(target.namespace_uid))):
            namespace = self._get("namespace", name)
            if (namespace.get("apiVersion") != "v1" or namespace.get("kind") != "Namespace"
                    or namespace["metadata"].get("name") != name or _uid(namespace) != uid):
                raise ValueError
            _snapshot(namespace)  # Reject deletion or a foreign owner.

    @staticmethod
    def _owner(document: dict[str, Any], *, kind: str, name: str, uid: str) -> None:
        owners = document["metadata"].get("ownerReferences", [])
        if len(owners) != 1:
            raise ValueError
        actual = dict(owners[0])
        actual.pop("blockOwnerDeletion", None)
        if actual != {"apiVersion": "apps/v1", "kind": kind, "name": name, "uid": uid, "controller": True}:
            raise ValueError

    def _runtime(self, target: PoolDatabaseReadTarget, *, original: dict[str, Any] | None = None) -> dict[str, Any]:
        self._namespaces(target)
        original = target.controller if original is None else original
        namespace, name = original["metadata"]["namespace"], original["metadata"]["name"]
        if namespace != target.namespace:
            if not isinstance(target, PoolGuardTarget):
                raise ValueError
            participant, = (row for row in self.request.registration.spec.participants if row.participant_id == target.participant_id)
            if namespace != participant.execution_namespace.name:
                raise ValueError
            current = self._get("namespace", namespace)
            if (current.get("apiVersion") != "v1" or current.get("kind") != "Namespace"
                    or current["metadata"].get("name") != namespace or _uid(current) != str(participant.execution_namespace.uid)):
                raise ValueError
            _snapshot(current)
        selector = original["spec"]["selector"]
        if (original.get("apiVersion") != "apps/v1" or original.get("kind") != "Deployment"
                or set(selector) != {"matchLabels"} or len(selector["matchLabels"]) != 1
                or type(original["spec"].get("replicas")) is not int or original["spec"]["replicas"] != 1):
            raise ValueError
        label, = selector["matchLabels"]
        if label not in {"app", "app.kubernetes.io/name"} or selector["matchLabels"][label] != name:
            raise ValueError
        controller = self._get("deployment", name, namespace)
        if _uid(controller) != _uid(original) or _snapshot(controller) != _snapshot(original):
            raise ValueError
        status = controller.get("status", {})
        if (status.get("observedGeneration", 0) < controller["metadata"].get("generation", 1)
                or any(type(status.get(key)) is not int or status[key] != 1
                    for key in ("replicas", "updatedReplicas", "availableReplicas", "readyReplicas"))):
            raise ValueError
        query = urlencode({"labelSelector": label + "=" + name, "limit": 100})
        listing = self._run(["get", "--raw", f"/api/v1/namespaces/{namespace}/pods?{query}"])
        if (listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                or listing.get("metadata", {}).get("continue") or not listing.get("metadata", {}).get("resourceVersion")
                or len(listing.get("items", [])) != 1):
            raise ValueError
        pod = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
        meta = pod["metadata"]
        _uid(pod)
        if (pod["apiVersion"] != "v1" or pod["kind"] != "Pod" or meta.get("namespace") != namespace
                or meta.get("deletionTimestamp") or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", meta["name"])
                or meta.get("labels", {}).get(label) != name):
            raise ValueError
        owners = meta.get("ownerReferences", [])
        if len(owners) != 1 or not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", owners[0]["name"]):
            raise ValueError
        replica = self._get("replicaset", owners[0]["name"], namespace)
        if (replica.get("apiVersion") != "apps/v1" or replica.get("kind") != "ReplicaSet"
                or replica["metadata"].get("namespace") != namespace
                or replica["metadata"].get("name") != owners[0]["name"] or replica["metadata"].get("deletionTimestamp")):
            raise ValueError
        self._owner(replica, kind="Deployment", name=name, uid=_uid(controller))
        self._owner(pod, kind="ReplicaSet", name=replica["metadata"]["name"], uid=_uid(replica))
        expected = controller["spec"]["template"]["spec"]
        actual = _runtime_pod_spec(pod["spec"], expected)
        if (not _matches_backup_template(replica["spec"]["template"]["spec"], expected)
                or not _matches_backup_template(actual, expected)
                or actual.get("initContainers", []) != expected.get("initContainers", [])
                or actual.get("securityContext", {}) != expected.get("securityContext", {})
                or actual.get("ephemeralContainers", []) != expected.get("ephemeralContainers", [])
                or actual.get("serviceAccountName", "default") != expected.get("serviceAccountName", "default")
                or any(actual.get(field, False) != expected.get(field, False)
                    for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"))
                or len(expected["containers"]) != 1
                or (name in {"loom-control-plane", "loom-service"} and expected["containers"][0]["name"] != name)
                or pod.get("status", {}).get("phase") != "Running"):
            raise ValueError
        for container, wanted in zip(actual["containers"], expected["containers"], strict=True):
            if (container.keys() - wanted.keys() - {"imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy"}
                    or container.get("securityContext", {}) != wanted.get("securityContext", {})):
                raise ValueError
        states = pod["status"].get("containerStatuses", [])
        if len(states) != 1 or states[0].get("name") != expected["containers"][0]["name"] or states[0].get("ready") is not True:
            raise ValueError
        self._namespaces(target)
        return pod

    def _database_url(self, target: PoolDatabaseReadTarget, *,
                      url_variable: Literal["LOOM_CP_DB_URL", "LOOM_SVC_DB_URL"] = "LOOM_CP_DB_URL") -> str:
        """Resolve only the pinned credential; backend qualification is separate."""
        self._namespaces(target)
        binding = target.database
        if binding is None:
            raise ValueError
        return self._workload_database_url(target.controller, url_variable=url_variable,
            credential_uid=binding.credential_uid, credential_resource_version=binding.credential_resource_version)

    def _workload_database_url(self, original: dict[str, Any], *, url_variable: str,
                               credential_uid: UUID, credential_resource_version: str) -> str:
        namespace = original["metadata"]["namespace"]
        containers = original["spec"]["template"]["spec"]["containers"]
        if len(containers) != 1 or containers[0].get("envFrom"):
            raise ValueError
        environment = containers[0]["env"]
        if len({row["name"] for row in environment}) != len(environment):
            raise ValueError
        # The guard command uses db_engine_url: a pooled URL overrides the
        # direct URL. This binding only qualifies the namespace-local direct
        # database; a pool needs separate backend correspondence evidence.
        pool_variable = url_variable + "_POOL"
        if any(row["name"] == pool_variable and row != {"name": pool_variable, "value": ""}
                for row in environment):
            raise ValueError
        entry, = (row for row in environment if row["name"] == url_variable)
        if set(entry) != {"name", "valueFrom"} or set(entry["valueFrom"]) != {"secretKeyRef"}:
            raise ValueError
        reference = entry["valueFrom"]["secretKeyRef"]
        if (reference.keys() - {"name", "key", "optional"} or reference.get("optional", False) is not False
                or any(not isinstance(reference[key], str) or not re.fullmatch(r"[a-zA-Z0-9._-]{1,253}", reference[key])
                    for key in ("name", "key"))):
            raise ValueError
        secret = self._get("secret", reference["name"], namespace)
        if (secret.get("apiVersion") != "v1" or secret.get("kind") != "Secret"
                or secret["metadata"].get("namespace") != namespace or secret["metadata"].get("name") != reference["name"]
                or not credential_uid.int or _uid(secret) != str(credential_uid)
                or not credential_resource_version or secret["metadata"].get("resourceVersion") != credential_resource_version
                or secret["metadata"].get("deletionTimestamp") or secret["metadata"].get("ownerReferences")):
            raise ValueError
        return base64.b64decode(secret["data"][reference["key"]], validate=True).decode()

    def qualify_runtime_database(self, target: PoolGuardTarget, *, original: dict[str, Any],
                                 credential_uid: UUID, credential_resource_version: str) -> None:
        """Prove one retained running consumer uses this participant's backend.

        The protected parent supplies the original workload and pinned credential
        identity. Stopped/journaled replacement workloads are a different phase;
        zero replicas are never accepted as runtime correspondence here.
        """
        try:
            if (target not in self.request.guards or target.database is None
                    or digest(migration_contract(self.request)) != self.contract_sha256
                    or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
                raise ValueError
            participant, = (row for row in self.request.registration.spec.participants if row.participant_id == target.participant_id)
            namespace, name = original["metadata"]["namespace"], original["metadata"]["name"]
            container, = original["spec"]["template"]["spec"]["containers"]
            if namespace == target.namespace and name in {"loom-control-plane", "loom-service"}:
                if (container["name"] != name or (name == "loom-control-plane" and original != target.controller)
                        or credential_uid != target.database.credential_uid
                        or credential_resource_version != target.database.credential_resource_version):
                    raise ValueError
                component = "controller" if name == "loom-control-plane" else "service"
                variable = "LOOM_CP_DB_URL" if component == "controller" else "LOOM_SVC_DB_URL"
            elif (namespace == participant.execution_namespace.name and container["name"] == "actuator"
                    and name in {"loom-execution-actuator", *(row.target_id + "-actuator" for row in participant.targets)}):
                component, variable = "actuator", "LOOM_EXECUTION_ACTUATOR_DB_URL"
            else:
                raise ValueError
            self._qualify_runtime_binding(target, original=original, component=component, url_variable=variable,
                credential_uid=credential_uid, credential_resource_version=credential_resource_version)
        except Exception:
            raise PoolMigrationError("runtime_database") from None

    def _telemetry_nodes(self, host: str) -> dict[str, str]:
        """Complete pool roster plus the actuator host, including scale-zero.

        The host supplies an in-cluster TLS/authority/network check even when no
        execution nodes exist. This is not proof of a future worker's reachability;
        newly created workers still require startup and task acceptance.
        """
        nodes = inventory_resources(lambda method, path: self._run(["get", "--raw", path]), "v1", "nodes", "Node")
        selector = self.request.registration.spec.node_selector
        if (not selector or len({row["metadata"]["name"] for row in nodes}) != len(nodes)
                or len({_uid(row) for row in nodes}) != len(nodes)
                or host not in {row["metadata"]["name"] for row in nodes}):
            raise ValueError
        selected = {}
        for node in nodes:
            meta = node["metadata"]
            name = meta["name"]
            if name != host and any(meta.get("labels", {}).get(key) != value for key, value in selector.items()):
                continue
            if (not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", name)
                    or meta.get("deletionTimestamp") is not None):
                raise ValueError
            selected[name] = _uid(node)
        return dict(sorted(selected.items()))

    def qualify_runtime_telemetry(self, target: PoolGuardTarget, *, original: dict[str, Any]) -> None:
        """Read direct statistics inside the exact retained actuator Pod.

        No token issuance, permission changes, database connection, arbitrary
        command or operator-credential forwarding. Recheck Pod and Node identity
        after all reads; a stale success cannot approve a replacement runtime.
        """
        try:
            if (target not in self.request.guards
                    or digest(migration_contract(self.request)) != self.contract_sha256
                    or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
                raise ValueError
            participant, = (row for row in self.request.registration.spec.participants if row.participant_id == target.participant_id)
            namespace, name = original["metadata"]["namespace"], original["metadata"]["name"]
            container, = original["spec"]["template"]["spec"]["containers"]
            settings = {row["name"]: row for row in container.get("env", [])}
            target_id = settings["LOOM_EXECUTION_ACTUATOR_TARGET_ID"]["value"]
            participant.target(target_id, "trial")
            if (namespace != participant.execution_namespace.name or container["name"] != "actuator"
                    or name not in {"loom-execution-actuator", target_id + "-actuator"}
                    or original["spec"]["template"]["spec"].get("serviceAccountName") != "loom-execution-actuator"
                    or len(settings) != len(container["env"])
                    or settings["LOOM_EXECUTION_ACTUATOR_NAMESPACE"].get("value") != namespace):
                raise ValueError
            before = self._runtime(target, original=original)
            host = before["spec"]["nodeName"]
            nodes = self._telemetry_nodes(host)
            for node_name, uid in nodes.items():
                report = self._run(["exec", "-n", namespace, "pod/" + before["metadata"]["name"], "-c", "actuator", "--",
                    "python", "-c", _BOUND_TELEMETRY_COMMAND, namespace, target_id, node_name, uid])
                if report != {"status": "qualified", "node_name": node_name, "node_uid": uid}:
                    raise ValueError
            after = self._runtime(target, original=original)
            if (_uid(after) != _uid(before) or after["spec"]["nodeName"] != host or self._telemetry_nodes(host) != nodes
                    or digest(migration_contract(self.request)) != self.contract_sha256
                    or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
                raise ValueError
        except Exception:
            raise PoolMigrationError("runtime_telemetry") from None

    def _qualify_runtime_binding(self, target: PoolDatabaseReadTarget, *, original: dict[str, Any],
                                 component: str, url_variable: str, credential_uid: UUID,
                                 credential_resource_version: str,
                                 database_variable: Literal["LOOM_CP_DB_URL", "LOOM_SVC_DB_URL"] = "LOOM_CP_DB_URL") -> None:
        """Shared read-only probe; callers first qualify their fixed target scope."""
        namespace = original["metadata"]["namespace"]
        container, = original["spec"]["template"]["spec"]["containers"]
        before_database = self._database(target, url_variable=database_variable)
        before = self._runtime(target, original=original)
        url = self._workload_database_url(original, url_variable=url_variable,
            credential_uid=credential_uid, credential_resource_version=credential_resource_version)
        qualify_database_destination(url, target.namespace)
        # CP/API use PostgresDsn; the actuator intentionally retains a str.
        expected_url = url if component == "actuator" else str(PostgresDsn(url))
        nonce = secrets.token_hex(32)
        response = hmac.new(bytes.fromhex(nonce), expected_url.encode(), "sha256").hexdigest()
        report = self._run(["exec", "-n", namespace, "pod/" + before["metadata"]["name"], "-c", container["name"], "--",
            "python", "-c", _BOUND_DATABASE_COMMAND, component, nonce, response])
        if (report != {"status": "qualified"}
                or _uid(self._runtime(target, original=original)) != _uid(before)
                or self._workload_database_url(original, url_variable=url_variable,
                    credential_uid=credential_uid, credential_resource_version=credential_resource_version) != url
                or _uid(self._database(target, url_variable=database_variable)) != _uid(before_database)):
            raise ValueError

    def _database(self, target: PoolDatabaseReadTarget, *,
                  url_variable: Literal["LOOM_CP_DB_URL", "LOOM_SVC_DB_URL"] = "LOOM_CP_DB_URL") -> dict[str, Any]:
        """Bind a read to the original controller's namespace-local database."""
        url = self._database_url(target, url_variable=url_variable)
        binding = target.database
        if binding is None:
            raise ValueError
        qualify_database_destination(url, target.namespace)
        database = self._get("statefulset", "loom-postgres", target.namespace)
        service = self._get("service", "loom-postgres", target.namespace)
        for actual, wanted in ((database, binding.statefulset), (service, binding.service)):
            if _uid(actual) != _uid(wanted) or _snapshot(actual) != _snapshot(wanted):
                raise ValueError
        spec, status = database["spec"], database.get("status", {})
        if (type(spec.get("replicas")) is not int or spec["replicas"] != 1
                or spec.get("serviceName") != "loom-postgres" or spec.get("ordinals", {}).get("start", 0) != 0
                or spec["selector"] != {"matchLabels": {"app": "loom-postgres"}}
                or service["spec"]["selector"] != {"app": "loom-postgres"}
                or service["spec"].get("type", "ClusterIP") != "ClusterIP"
                or len(service["spec"]["ports"]) != 1 or service["spec"]["ports"][0]["port"] != 5432
                or service["spec"]["ports"][0]["targetPort"] != 5432
                or status.get("observedGeneration", 0) < database["metadata"].get("generation", 1)
                or any(type(status.get(key)) is not int or status[key] != 1
                    for key in ("replicas", "readyReplicas", "currentReplicas", "updatedReplicas"))
                or not status.get("currentRevision") or status["currentRevision"] != status.get("updateRevision")):
            raise ValueError
        listing = self._pods(target.namespace, "loom-postgres")
        if (listing.get("apiVersion") != "v1" or listing.get("kind") != "PodList"
                or listing.get("metadata", {}).get("continue") or not listing.get("metadata", {}).get("resourceVersion")
                or len(listing.get("items", [])) != 1):
            raise ValueError
        pod = {"apiVersion": "v1", "kind": "Pod", **listing["items"][0]}
        meta = pod["metadata"]
        _uid(pod)
        if (pod["apiVersion"] != "v1" or pod["kind"] != "Pod" or meta.get("namespace") != target.namespace
                or meta.get("name") != "loom-postgres-0" or meta.get("deletionTimestamp")
                or meta.get("labels", {}).get("app") != "loom-postgres"
                or meta["labels"].get("controller-revision-hash") != status["currentRevision"]):
            raise ValueError
        self._owner(pod, kind="StatefulSet", name="loom-postgres", uid=_uid(database))
        expected = copy.deepcopy(spec["template"]["spec"])
        claims = spec.get("volumeClaimTemplates", [])
        if len(claims) != 1 or claims[0]["metadata"].get("name") != "data":
            raise ValueError
        expected.setdefault("volumes", []).append({"name": "data", "persistentVolumeClaim": {"claimName": "data-loom-postgres-0"}})
        actual = pod["spec"]
        # StatefulSet may order its injected PVC volume before other volumes.
        actual = {**actual, "volumes": sorted(actual.get("volumes", []), key=lambda row: row["name"])}
        expected["volumes"].sort(key=lambda row: row["name"])
        if (not _matches_backup_template(actual, expected)
                or actual.get("initContainers", []) != expected.get("initContainers", [])
                or actual.get("securityContext", {}) != expected.get("securityContext", {})
                or actual.get("ephemeralContainers", []) != expected.get("ephemeralContainers", [])
                or actual.get("serviceAccountName", "default") != expected.get("serviceAccountName", "default")
                or any(actual.get(field, False) != expected.get(field, False)
                    for field in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"))
                or len(expected["containers"]) != 1 or expected["containers"][0]["name"] != "loom-postgres"
                or pod.get("status", {}).get("phase") != "Running"):
            raise ValueError
        container, wanted = actual["containers"][0], expected["containers"][0]
        if (container.keys() - wanted.keys() - {"imagePullPolicy", "terminationMessagePath", "terminationMessagePolicy"}
                or container.get("securityContext", {}) != wanted.get("securityContext", {})):
            raise ValueError
        states = pod["status"].get("containerStatuses", [])
        if len(states) != 1 or states[0].get("name") != "loom-postgres" or states[0].get("ready") is not True:
            raise ValueError
        self._database_backend(target, service=service, pod=pod)
        self._namespaces(target)
        return pod

    def _database_backend(self, target: PoolDatabaseReadTarget, *, service: dict[str, Any], pod: dict[str, Any]) -> None:
        """Qualify actual Service routing, not equal URLs or matching selectors."""
        listing = self._run(["get", "--raw", f"/apis/discovery.k8s.io/v1/namespaces/{target.namespace}/endpointslices?labelSelector=kubernetes.io%2Fservice-name%3Dloom-postgres&limit=100"])
        if (listing.get("apiVersion") != "discovery.k8s.io/v1" or listing.get("kind") != "EndpointSliceList"
                or listing.get("metadata", {}).get("continue")
                or not isinstance(listing.get("metadata", {}).get("resourceVersion"), str)
                or not 0 < len(listing["metadata"]["resourceVersion"]) <= 128
                or not isinstance(listing.get("items"), list) or not 0 < len(listing["items"]) <= 2
                or service["spec"].get("publishNotReadyAddresses", False) is not False):
            raise ValueError
        status = pod["status"]
        primary = ipaddress.ip_address(status["podIP"])
        addresses = status["podIPs"]
        if not isinstance(addresses, list) or not 0 < len(addresses) <= 2:
            raise ValueError
        pod_ips = {str(ipaddress.ip_address(row["ip"])) for row in addresses if set(row) == {"ip"}}
        families = service["spec"].get("ipFamilies", ["IPv" + str(primary.version)])
        if (len(pod_ips) != len(addresses) or str(primary) not in pod_ips or not isinstance(families, list)
                or not 0 < len(families) <= 2 or len(set(families)) != len(families)
                or not set(families) <= {"IPv4", "IPv6"}):
            raise ValueError
        expected = {address for address in pod_ips if "IPv" + str(ipaddress.ip_address(address).version) in families}
        if len(expected) != len(families):
            raise ValueError
        seen: set[str] = set()
        slices: set[str] = set()
        service_port, = service["spec"]["ports"]
        for row in listing["items"]:
            metadata = row["metadata"]
            if (row.get("apiVersion", "discovery.k8s.io/v1") != "discovery.k8s.io/v1"
                    or row.get("kind", "EndpointSlice") != "EndpointSlice" or metadata.get("namespace") != target.namespace
                    or metadata.get("deletionTimestamp") or metadata.get("labels", {}).get("kubernetes.io/service-name") != "loom-postgres"
                    or _uid(row) in slices or row["addressType"] not in families):
                raise ValueError
            slices.add(_uid(row))
            owner, = metadata["ownerReferences"]
            owner = dict(owner)
            blocking = owner.pop("blockOwnerDeletion", False)
            if (type(blocking) is not bool or owner != {"apiVersion": "v1", "kind": "Service", "name": "loom-postgres",
                    "uid": _uid(service), "controller": True}):
                raise ValueError
            port, = row["ports"]
            if (port.keys() - {"name", "port", "protocol", "appProtocol"}
                    or type(port.get("port")) is not int or port["port"] != 5432
                    or port.get("protocol", "TCP") != "TCP" or port.get("name") != service_port.get("name")
                    or port.get("appProtocol") != service_port.get("appProtocol")):
                raise ValueError
            endpoint, = row["endpoints"]
            conditions, reference = endpoint["conditions"], endpoint["targetRef"]
            if (conditions.get("ready") is not True or conditions.get("serving", True) is not True
                    or conditions.get("terminating", False) is not False
                    or reference.keys() - {"kind", "namespace", "name", "uid", "apiVersion", "resourceVersion"}
                    or reference.get("apiVersion", "v1") != "v1"
                    or any(reference.get(key) != value for key, value in {"kind": "Pod", "namespace": target.namespace,
                        "name": pod["metadata"]["name"], "uid": _uid(pod)}.items())):
                raise ValueError
            address, = endpoint["addresses"]
            parsed = ipaddress.ip_address(address)
            if str(parsed) != address or row["addressType"] != "IPv" + str(parsed.version) or address not in expected or address in seen:
                raise ValueError
            seen.add(address)
        if seen != expected:
            raise ValueError

    def guard(self, target: PoolGuardTarget, action: str) -> dict[str, Any]:
        try:
            allowed = {"observe": {"open", "held", "skipped_locked"}, "acquire": {"acquired", "skipped_busy", "skipped_locked"}}
            if (action not in allowed or target not in self.request.guards
                    or digest(migration_contract(self.request)) != self.contract_sha256
                    or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
                raise ValueError
            database = self._database(target) if target.database is not None else None
            if action == "observe" and database is not None:
                query = rollout_guard_observation_sql(owner=str(self.request.registration.spec.operation_id),
                    candidate=self.request.registration.candidate["candidate_sha"])
                report = self._run(["exec", "-n", target.namespace, "pod/" + database["metadata"]["name"], "-c", "loom-postgres", "--",
                    "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", "-c", query])
                if set(report) != {"status"} or report["status"] not in allowed[action] or _uid(self._database(target)) != _uid(database):
                    raise ValueError
                return {"status": report["status"]}
            before = self._runtime(target)
            if database is not None:
                nonce = secrets.token_hex(32)
                expected = hmac.new(bytes.fromhex(nonce), str(PostgresDsn(self._database_url(target))).encode(), "sha256").hexdigest()
                command = ["python", "-c", _BOUND_GUARD_COMMAND, action,
                    str(self.request.registration.spec.operation_id), self.request.registration.candidate["candidate_sha"], nonce, expected]
            else:
                # Historical unbound callers retain their old command. The
                # protected cutover input loader requires every database bound.
                command = ["python", "-m", "loom.nebius_rollout_guard", action, "--owner", str(self.request.registration.spec.operation_id),
                    "--candidate", self.request.registration.candidate["candidate_sha"]]
            report = self._run(["exec", "-n", target.namespace, "pod/" + before["metadata"]["name"], "-c", "loom-control-plane", "--",
                *command])
            if report.get("status") not in allowed[action] or _uid(self._runtime(target)) != _uid(before):
                raise ValueError
            if database is not None and _uid(self._database(target)) != _uid(database):
                raise ValueError
            return {"status": report["status"]}
        except Exception:
            raise PoolMigrationError("guard_" + action if action in {"observe", "acquire"} else "guard_scope") from None

    def runtime_role(self, target: PoolGuardTarget, action: str) -> dict[str, Any]:
        """Stage/qualify one exact participant DB; leave its intake closed."""
        try:
            if (action not in {"stage", "observe"} or target not in self.request.guards or target.database is None
                    or digest(migration_contract(self.request)) != self.contract_sha256
                    or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
                raise ValueError
            before = self._database(target)
            query = pool_runtime_role_sql(owner=str(self.request.registration.spec.operation_id),
                candidate=self.request.registration.candidate["candidate_sha"], action=action)
            report = self._run(["exec", "-n", target.namespace, "pod/" + before["metadata"]["name"], "-c", "loom-postgres", "--",
                "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", "-c", query])
            if (report != {"status": "staged" if action == "stage" else "qualified"}
                    or _uid(self._database(target)) != _uid(before)):
                raise ValueError
            return report
        except Exception:
            raise PoolMigrationError("runtime_role_" + action if action in {"stage", "observe"} else "runtime_role_scope") from None

    def cutover_readiness_page(self, target: PoolGuardTarget, *, after: str | None) -> dict[str, Any]:
        """Read frozen schema/access/backlog through the exact retained DB Pod.

        This neither migrates the schema nor revokes application keys. Unqualified
        backlog must retain its original origin or finish under the old path;
        this observer never backfills, adopts, cancels or retries a write.
        """
        try:
            if (target not in self.request.guards or target.database is None
                    or digest(migration_contract(self.request)) != self.contract_sha256
                    or hashlib.sha256(private_state._private_read(self.kubeconfig, limit=512 * 1024)).hexdigest() != self.kubeconfig_sha256):
                raise ValueError
            participant, = (row for row in self.request.registration.spec.participants if row.participant_id == target.participant_id)
            environment = target.controller["spec"]["template"]["spec"]["containers"][0]["env"]
            pool, = (row for row in environment if row["name"] == "LOOM_CP_SERVICE_EXECUTION_SCHEDULER_POOL_ID")
            if set(pool) != {"name", "value"}:
                raise ValueError
            query = pool_cutover_readiness_sql(owner=str(self.request.registration.spec.operation_id),
                candidate=self.request.registration.candidate["candidate_sha"], logical_pool_id=pool["value"], after=after)
            before = self._database(target)
            report = self._run(["exec", "-n", target.namespace, "pod/" + before["metadata"]["name"], "-c", "loom-postgres", "--",
                "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", "-c", query])
            qualify_cutover_readiness_page(report, participant=participant, after=after)
            if _uid(self._database(target)) != _uid(before):
                raise ValueError
            return report
        except Exception:
            raise PoolMigrationError("cutover_readiness") from None
