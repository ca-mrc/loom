"""Fixed HTTPS transport for the connected closed-runtime cutover.

No CLI, activation, deletion, ambient credential discovery or HTTP write retry.
The protected entry supplies independently qualified publication/predecessor/
backend checks and the original database-bound guard adapter.
"""
from __future__ import annotations

import copy
import json
import ssl
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol
from urllib.parse import urlencode
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_stage import (
    _MARKER,
    HTTPSManagementStageAPI,
    _comparison_snapshot,
    _qualified_defaulted,
    _validate_record,
)
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_cutover import (
    PoolCutoverRequest,
    _contract,
    cutover_documents,
    cutover_material_documents,
    qualify_cutover_image_admission,
    retained_cutover_workloads,
)
from scripts.ops.nebius_pool_migration import (
    PoolGuardTarget,
    PoolMigrationAPI,
    PoolMigrationRequest,
    _hash,
)
from scripts.ops.nebius_pool_migration_guard import qualify_cutover_readiness_page
from scripts.ops.nebius_pool_platform_authority import platform_controller_subjects
from scripts.ops.nebius_pool_projection import _snapshot as _input_snapshot
from scripts.ops.nebius_pool_retirement import (
    _closed,
    qualify_closed_workload_drain,
    retirement_documents,
    stopped_document,
)
from scripts.ops.nebius_pool_retirement_live import HTTPSPoolRetirementAPI
from scripts.ops.nebius_pool_role_fencing import (
    POOL_WRITER_WORKLOAD_COLLECTIONS,
    qualify_retained_writer_bindings,
    qualify_retained_writer_workloads,
)
from scripts.ops.nebius_pool_role_fencing_live import HTTPSPoolRoleFenceAPI

from loom.nebius_platform_render import digest
from loom.nebius_pool_priority import PoolWorkOriginV1

if TYPE_CHECKING:
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh


class PoolCutoverChecks(Protocol):
    def preflight(self, request: PoolCutoverRequest) -> None: ...
    def qualify_initial_capacity(self, request: PoolCutoverRequest) -> None: ...
    def qualify_quiescence(self) -> None: ...


class PoolCutoverHistory(Protocol):
    def qualify_binding(self, request: PoolMigrationRequest, manager: dict[str, Any]) -> None: ...
    def qualify_active_pool(self) -> None: ...
    def qualify_manager_database(self, *, expected: dict[str, Any] | None = None) -> None: ...
    def qualify_manager_pool_settings(self, *, expected: dict[str, Any]) -> None: ...
    def qualify_manager_legacy_settings(self, *, expected: dict[str, Any]) -> None: ...
    def qualify_gateway_runtime(self, *, original: dict[str, Any], expected: dict[str, Any]) -> None: ...
    def open_pool(self, *, original: dict[str, Any], expected: dict[str, Any]) -> None: ...
    def activation_pool(self, action: Literal["observe", "fence"]) -> str: ...
    def recovery_pool_drained(self) -> bool: ...
    def machine_retirement(self, action: Literal["observe", "revoke"]) -> Literal["active", "revoked"]: ...
    def qualify_closed_pool(self) -> None:
        """Read current closed registration and dedicated credentials, never replay it."""
        ...
    def qualify_pending_origins(self, target: PoolGuardTarget, origins: tuple[PoolWorkOriginV1, ...]) -> None:
        """Qualify retained management registration/history, not just JSON shape."""
        ...


class PoolCutoverGuards(Protocol):
    request: PoolMigrationRequest
    def guard(self, target: PoolGuardTarget, action: str) -> dict[str, Any]: ...
    def runtime_role(self, target: PoolGuardTarget, action: str) -> dict[str, Any]: ...
    def activation_guard(self, target: PoolGuardTarget, action: Literal["observe", "release", "fence"]) -> str: ...
    def recovery_participant_drained(self, target: PoolGuardTarget) -> bool: ...
    def release_recovery_guard(self, target: PoolGuardTarget) -> str: ...
    def cutover_readiness_page(self, target: PoolGuardTarget, *, after: str | None) -> dict[str, Any]: ...
    def qualify_runtime_database(self, target: PoolGuardTarget, *, original: dict[str, Any],
                                 credential_uid: UUID, credential_resource_version: str,
                                 expected: dict[str, Any] | None = None) -> None: ...
    def qualify_runtime_telemetry(self, target: PoolGuardTarget, *, original: dict[str, Any],
                                  expected: dict[str, Any] | None = None) -> None: ...
    def qualify_runtime_pool_settings(self, target: PoolGuardTarget, *, original: dict[str, Any],
                                      expected: dict[str, Any]) -> None: ...
    def qualify_runtime_legacy_settings(self, target: PoolGuardTarget, *, original: dict[str, Any],
                                        expected: dict[str, Any]) -> None: ...


class HTTPSPoolCutoverAPI(HTTPSManagementStageAPI):
    def __init__(self, *, request: PoolCutoverRequest, tokens: dict[UUID, str], migration: PoolMigrationAPI,
                 guards: PoolCutoverGuards, checks: PoolCutoverChecks, history: PoolCutoverHistory, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None,
                 state_dir: Path | None = None, anchor_dir: Path | None = None,
                 refresh: PoolManagerRefresh | None = None,
                 source_credentials: dict[str, str] | None = None):
        registration = request.fencing.retirement.migration.registration
        if guards.request != request.fencing.retirement.migration:
            raise ValueError("pool cutover guard binding differs")
        history.qualify_binding(guards.request, request.manager)
        if ((state_dir is None) != (anchor_dir is None)
                or (state_dir is not None and anchor_dir is not None and (
                    state_dir != state_dir.resolve() or anchor_dir != anchor_dir.resolve()
                    or state_dir == anchor_dir or state_dir in anchor_dir.parents or anchor_dir in state_dir.parents))):
            raise ValueError("pool cutover journal binding differs")
        self.state_dir, self.anchor_dir = state_dir, anchor_dir
        self.refresh = refresh
        if refresh is not None:
            from scripts.ops.nebius_pool_refresh import PoolManagerRefresh

            if type(refresh) is not PoolManagerRefresh:
                raise ValueError("pool refresh projection type differs")
            context = refresh.qualify().context
            if (context.request != request or state_dir != Path(context.operation['state_dir'])
                    or anchor_dir != Path(context.operation['anchor_dir'])):
                raise ValueError("pool refresh projection scope differs")
        self.request, self.migration, self.guards, self.checks = request, migration, guards, checks
        self.history = history
        self.binding = registration.binding
        try:
            self._input_snapshot: object = _input_snapshot(request)
        except TypeError:
            self._input_snapshot = None
        self.catalog = cutover_documents(request)
        self.contract_sha256 = digest(_contract(request, self.catalog))
        self.originals = {**retirement_documents(request.fencing.retirement), **self.catalog["producers"]}
        self._source_credentials = copy.deepcopy(source_credentials)
        self.documents = {**cutover_material_documents(request, tokens, self._source_credentials),
            **{_key(row): row for phase in ("configuration", "authority", "workload") for row in self.catalog[phase]}}
        self.namespaces = {row.namespace: str(row.namespace_uid) for row in guards.request.guards}
        self.namespaces.update({ns.name: str(ns.uid) for row in registration.spec.participants
            for ns in (row.execution_namespace, row.build_namespace)})
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)
        self.retirement = HTTPSPoolRetirementAPI(request=request.fencing.retirement, guards=guards,
            api_server=api_server, ssl_context=ssl_context, token=token)
        self.fencing = HTTPSPoolRoleFenceAPI(request=request.fencing, retirement=self.retirement,
            api_server=api_server, ssl_context=ssl_context, token=token)
        self.resources = self

    def __exit__(self, *_args: object) -> None:
        self.client.close()
        self.retirement.client.close()
        self.fencing.client.close()

    def verify_identity(self, binding: ManagementBinding) -> None:
        super().verify_identity(binding)
        for name, uid in self.namespaces.items():
            namespace = self._request("GET", "/api/v1/namespaces/" + name)
            if (namespace is None or namespace.get("apiVersion") != "v1" or namespace.get("kind") != "Namespace"
                    or namespace["metadata"].get("name") != name or _uid(namespace) != uid):
                raise ValueError("pool cutover namespace differs")
            _snapshot(namespace)

    def _scope(self) -> None:
        # The contract is pure in these complete typed inputs. Comparing them
        # avoids rendering every workload for each individual live GET. Never
        # reuse the identity observation below or any journal/runtime evidence.
        if self._input_snapshot is None:
            unchanged = self.contract_sha256 == digest(_contract(self.request, cutover_documents(self.request)))
        else:
            try:
                unchanged = self._input_snapshot == _input_snapshot(self.request)
            except TypeError:
                unchanged = False
        if not unchanged:
            raise ValueError("pool cutover inputs changed")
        qualify_cutover_image_admission(self.request)
        self.verify_identity(self.binding)

    def _approved(self, document: dict[str, Any], *, writing: bool = False) -> str:
        try:
            value = copy.deepcopy(document)
            expected = self.documents[_key(value)]
            annotations = value["metadata"].get("annotations", {})
            operation = annotations.pop(_MARKER, None)
            if writing or operation is not None:
                if str(UUID(operation)) != operation or not UUID(operation).int:
                    raise ValueError
            if not annotations and "annotations" not in expected["metadata"]:
                value["metadata"].pop("annotations", None)
            if value != expected:
                raise ValueError
            version, resource, namespaced = {
                "Secret": ("v1", "secrets", True), "ConfigMap": ("v1", "configmaps", True),
                "ServiceAccount": ("v1", "serviceaccounts", True), "Deployment": ("apps/v1", "deployments", True),
                "Role": ("rbac.authorization.k8s.io/v1", "roles", True),
                "RoleBinding": ("rbac.authorization.k8s.io/v1", "rolebindings", True),
                "ClusterRole": ("rbac.authorization.k8s.io/v1", "clusterroles", False),
                "ClusterRoleBinding": ("rbac.authorization.k8s.io/v1", "clusterrolebindings", False),
            }[value["kind"]]
            if value["apiVersion"] != version:
                raise ValueError
            prefix = "/api/v1" if version == "v1" else "/apis/" + version
            return prefix + ("/namespaces/" + value["metadata"]["namespace"] if namespaced else "") + "/" + resource
        except Exception:
            raise ValueError("resource outside fixed pool cutover stage") from None

    def preflight(self, request: PoolCutoverRequest) -> None:
        if request != self.request:
            raise ValueError("pool cutover request differs")
        self._scope()
        self.qualify_writer_bindings()
        self.checks.preflight(request)
        if request.application_delivery is not None:
            self.checks.qualify_initial_capacity(request)
        self._qualify_database_readiness()
        self.qualify_writer_bindings()

    def _recorded_writer_authority(self, inventory: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
        """Observe approved successor grants, including uncertain CREATE intents.

        An operation label or desired renderer alone never approves a writer.
        Parent anchor, retained closed/fenced receipts and authority-stage intent
        must correspond. No journal update, CREATE retry or resource adoption.
        """
        from scripts.ops.nebius_pool_gateway_retirement import gateway_retirement_options

        state, anchor = self.state_dir, self.anchor_dir
        if state is None or anchor is None:
            return {}
        spec = self.guards.request.registration.spec
        marker = anchor / (str(spec.operation_id) + "-cutover.json")
        parent_path, path = state / "cutover.json", state / "authority" / "stage.json"
        if not marker.exists() and not marker.is_symlink():
            if any(item.exists() or item.is_symlink() for item in (parent_path, path)):
                raise ValueError
            return {}
        identity = {"schema": "loom.nebius-pool-cutover.v1", "operation_id": str(spec.operation_id),
            "state_dir": str(state), "contract_sha256": self.contract_sha256}
        record = json.loads(private_state._private_read(parent_path, limit=4 * 1024**2))
        if (json.loads(private_state._private_read(marker)) != identity
                or set(record) != {*identity, "producers", "fenced", "runtime_access", "phases", "runtime"}
                or any(record[key] != value for key, value in identity.items())
                or not isinstance(record["phases"], dict)):
            raise ValueError
        if "authority" not in record["phases"]:
            if path.exists() or path.is_symlink():
                raise ValueError
            return {}
        fenced = record["fenced"]
        if (not isinstance(fenced, dict) or set(fenced) != {"migration.json", "retirement.json", "role-fencing.json"}
                or any(_hash(state / "writers" / name) != checksum for name, checksum in fenced.items())):
            raise ValueError
        _closed(self.guards.request, state / "writers", state / "writer-anchor")
        # An interrupted stage may have recorded its parent intent before its
        # child file exists. No successor is approved until the child intent does.
        checksum = record["phases"]["authority"]
        if not path.exists() and not path.is_symlink():
            if checksum is not None:
                raise ValueError
            return {}
        if checksum is not None and checksum != _hash(path):
            raise ValueError
        documents = {_key(row): row for row in self.catalog["authority"]}
        stage = json.loads(private_state._private_read(path, limit=4 * 1024**2))
        _validate_record(stage, {"schema": "loom.nebius-management-stage.v1", "binding": asdict(self.binding),
            "revision": digest(documents), "phase": "pool-cutover-authority"}, documents)
        options = gateway_retirement_options(self.request, state=state, anchor=anchor)
        live = {_key(row): row for rows in inventory.values() for row in rows}
        approved = {}
        for key, item in stage["resources"].items():
            actual = live.get(key)
            if checksum is not None and item["status"] != "created":
                raise ValueError
            if item["status"] == "prepared":
                if actual is not None:
                    raise ValueError
                continue
            if actual is None:
                if item["status"] == "created":
                    raise ValueError
                continue
            # Aggregation is authority, not an API-server default. A partial
            # stage's contains-oriented dry-run record cannot approve it.
            if "aggregationRule" in actual:
                raise ValueError
            if options is not None:
                if (checksum is None or item['status'] != 'created' or _uid(actual) != item['uid']
                        or not any(_snapshot(actual) == _snapshot(wanted) for wanted in options[key])):
                    raise ValueError
            elif (_comparison_snapshot(actual) != item["expected"]
                    or (item["status"] == "created" and (
                        _uid(actual) != item["uid"] or _snapshot(actual) != item["observed"]))):
                raise ValueError
            approved[key] = actual
        return approved

    def _retained_workload_projection(self, observed: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        if self.refresh is None:
            return (self.originals if self.state_dir is None or self.anchor_dir is None else
                retained_cutover_workloads(self.request, state_dir=self.state_dir, anchor_dir=self.anchor_dir, observed=observed))
        context = self.refresh.qualify().context
        if (context.request != self.request or self.state_dir != Path(context.operation['state_dir'])
                or self.anchor_dir != Path(context.operation['anchor_dir'])):
            raise ValueError("pool refresh projection scope changed")
        options = self.refresh.workload_options()
        expected = {}
        for key, original in self.originals.items():
            desired, = (row for row in options[key] if _matches(observed[key], row, _uid(original)))
            expected[key] = desired
        return expected

    def qualify_writer_bindings(self) -> None:
        """Discover retained grants and consumers at one API-server revision."""
        try:
            revision: str | None = None

            def read(method: str, path: str) -> dict[str, Any] | None:
                nonlocal revision
                # One API-server snapshot for RBAC and workload collections. A
                # continuation already pins its initial revision and cannot be
                # combined with an explicit resourceVersion query.
                if revision is not None and "continue=" not in path:
                    path += "&" + urlencode({"resourceVersion": revision, "resourceVersionMatch": "Exact"})
                page = self._request(method, path)
                current = None if page is None else page.get("metadata", {}).get("resourceVersion")
                if not isinstance(current, str) or not 0 < len(current) <= 1024 or revision not in (None, current):
                    raise ValueError
                revision = current
                return page

            inventory = {resource: inventory_resources(read, "rbac.authorization.k8s.io/v1", resource, kind)
                for resource, kind in (("roles", "Role"), ("clusterroles", "ClusterRole"),
                    ("rolebindings", "RoleBinding"), ("clusterrolebindings", "ClusterRoleBinding"))}
            qualify_retained_writer_bindings(self.request.fencing, inventory,
                staged_authority=self._recorded_writer_authority(inventory), platform_authority=self.request.platform_authority)
        except Exception:
            raise ValueError("pool_retained_writer_binding_inventory_unqualified") from None
        try:
            workloads = {resource: inventory_resources(read, api, resource, kind, include_terminal_pods=True)
                for api, resource, kind in POOL_WRITER_WORKLOAD_COLLECTIONS}
            observed = {_key(row): row for resource in ("deployments", "cronjobs") for row in workloads[resource]}
            expected = self._retained_workload_projection(observed)
            qualify_retained_writer_workloads(self.request.fencing, workloads, originals=self.originals, expected=expected,
                platform_subjects=platform_controller_subjects(self.request.platform_authority))
            if self._retained_workload_projection(observed) != expected:
                raise ValueError
        except Exception:
            raise ValueError("pool_retained_writer_workload_inventory_unqualified") from None

    def _qualify_database_readiness(self) -> None:
        """Readiness precedes downtime and is rechecked after producers drain.

        Reuse the fixed schema/access/backlog queries; this path neither applies
        DDL nor retires application credentials. Even an empty participant queue
        still qualifies the distinct management database and its schema.
        """
        for target in self.guards.request.guards:
            participant, = (row for row in self.guards.request.registration.spec.participants
                if row.participant_id == target.participant_id)
            after = None
            while True:
                report = self.guards.cutover_readiness_page(target, after=after)
                origins = qualify_cutover_readiness_page(report, participant=participant, after=after)
                self.history.qualify_pending_origins(target, origins)
                if len(report["rows"]) < 128:
                    break
                after = report["rows"][-1]["key"]

    def qualify_quiescence(self) -> None:
        self._scope()
        self._qualify_database_readiness()
        self.checks.qualify_quiescence()

    def qualify_runtime_access(self, participant_id: UUID, action: str) -> None:
        self._scope()
        guard, = (row for row in self.guards.request.guards if row.participant_id == participant_id)
        if action not in {"stage", "observe"} or self.guards.runtime_role(guard, action) != {"status": "staged" if action == "stage" else "qualified"}:
            raise ValueError("pool cutover runtime role unqualified")

    def _workload_path(self, key: str) -> str:
        original = self.originals[key]
        resource = "cronjobs" if original["kind"] == "CronJob" else "deployments"
        return "/apis/" + str(original["apiVersion"]) + "/namespaces/" + str(original["metadata"]["namespace"]) + "/" + resource + "/" + str(original["metadata"]["name"])

    def read_workload(self, key: str) -> dict[str, Any]:
        self._scope()
        original = self.originals[key]
        actual = self._request("GET", self._workload_path(key))
        if (actual is None or _uid(actual) != _uid(original)
                or any(actual.get(field) != original[field] for field in ("apiVersion", "kind"))
                or any(actual["metadata"].get(field) != original["metadata"][field] for field in ("namespace", "name"))):
            raise ValueError("pool cutover workload identity differs")
        _snapshot(actual)
        return actual

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        try:
            original = self.originals[key]
            freeze = key in self.catalog["stopped"] and desired == self.catalog["stopped"][key]
            expected = (original if freeze else self.catalog["stopped"][key] if key in self.catalog["stopped"]
                else stopped_document(self.request.fencing.retirement, key))
            if ((not freeze and desired != self.catalog["runtime"].get(key))
                    or not _matches(before, expected, _uid(original))):
                raise ValueError
            version = before["metadata"]["resourceVersion"]
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            self._scope()
            if not freeze:
                if any(self.guards.guard(target, "observe").get("status") != "held" for target in self.guards.request.guards):
                    raise ValueError
                self.fencing.verify_readonly()
            patches = [{"op": "test", "path": "/metadata/uid", "value": _uid(original)},
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "test", "path": "/spec", "value": before["spec"]},
                {"op": "add", "path": "/metadata/annotations", "value": desired["metadata"]["annotations"]},
                {"op": "replace", "path": "/spec", "value": desired["spec"]}]
            with self.client.stream("PATCH", self._workload_path(key) + ("?dryRun=All" if preview else ""),
                    json=patches, headers={"Content-Type": "application/json-patch+json"}) as response:
                if response.status_code not in {200, 409, 422} or response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise ValueError
                content = bytearray()
                for chunk in response.iter_bytes(chunk_size=16384):
                    if len(content) + len(chunk) > 4 * 1024**2:
                        raise ValueError
                    content.extend(chunk)
                value = json.loads(content)
                if not isinstance(value, dict):
                    raise ValueError
                if response.status_code in {409, 422}:
                    if (value.get("apiVersion") != "v1" or value.get("kind") != "Status" or value.get("status") != "Failure"
                            or value.get("code") != response.status_code or value.get("reason") != {409: "Conflict", 422: "Invalid"}[response.status_code]):
                        raise ValueError
                    return None
                if _uid(value) != _uid(original):
                    raise ValueError
                _qualified_defaulted(desired, value)
                return value
        except Exception:
            raise ValueError("pool cutover workload update unconfirmed") from None

    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return self._patch(key, before, desired, preview=True)

    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._patch(key, before, desired, preview=False) is not None

    def drained_workload(self, key: str, desired: dict[str, Any]) -> bool:
        current = self.read_workload(key)
        namespace = current["metadata"]["namespace"]
        prefix = ("/apis/apps/v1/namespaces/" + namespace + "/replicasets" if current["kind"] == "Deployment"
            else "/apis/batch/v1/namespaces/" + namespace + "/jobs")
        children = self._request("GET", prefix + "?limit=1000")
        pods = self._request("GET", "/api/v1/namespaces/" + namespace + "/pods?limit=1000")
        if children is None or pods is None or _stable(self.read_workload(key)) != _stable(current):
            raise ValueError("pool cutover drain changed during observation")
        return qualify_closed_workload_drain(original=self.originals[key], desired=desired,
            current=current, children=children, pods=pods)
