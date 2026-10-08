"""Connected protected cutover through closed runtime staging.

The entry must qualify publication, predecessor, backend and complete writer
inventory. Producer drain is not database-access retirement: quiescence also
qualifies application access, schema readiness and trusted queued origins. This
parent composes the real closure/fencing/material stages and preserves their
receipts across runtime replacement. It never opens admission or starts a Pod.
"""
from __future__ import annotations

import base64
import copy
import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_stage import (
    ManagementStageAPI,
    _qualified_defaulted,
    _stage_fixed_documents,
)
from scripts.ops.nebius_management_supplied import _defaulted as material_defaulted
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_application_delivery import (
    ApplicationBuildDeliveryRequest,
    render_application_build_delivery,
    source_material_json,
)
from scripts.ops.nebius_pool_material import machine_documents
from scripts.ops.nebius_pool_migration import (
    PoolMigrationAPI,
    _hash,
    close_and_register_pool,
    migration_contract,
)
from scripts.ops.nebius_pool_platform_authority import PoolPlatformAuthority
from scripts.ops.nebius_pool_projection import pure_projection
from scripts.ops.nebius_pool_retirement import MARKER as RETIREMENT_MARKER
from scripts.ops.nebius_pool_retirement import (
    _closed,
    _read_closed_migration,
    retirement_documents,
    stopped_documents,
)
from scripts.ops.nebius_pool_role_fencing import (
    PoolRoleFenceAPI,
    PoolRoleFenceRequest,
    fence_pool_roles,
    role_fence_documents,
)
from scripts.ops.nebius_pool_runtime import (
    PoolCollectorCredential,
    participant_readonly_roles,
    wire_collector,
    wire_manager,
    wire_participant,
)

from loom.execution_image_admission import ImageAdmissionKeyring, verify_execution_image_admission
from loom.nebius_platform_render import digest
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.pool_management.installation_render import render_gateway

MARKER = "loom.nebius/pool-cutover-operation"


@dataclass(frozen=True, repr=False)
class PoolCutoverRequest:
    fencing: PoolRoleFenceRequest
    manager: dict[str, Any]
    services: tuple[dict[str, Any], ...]
    collector_config: dict[str, Any]
    profiles: dict[UUID, ServiceExecutionRuntimeProfileV1]
    management_origin: str
    kubernetes_endpoint: str
    collector_credential: PoolCollectorCredential
    platform_authority: PoolPlatformAuthority | None = None
    application_delivery: ApplicationBuildDeliveryRequest | None = None
    platform_consumers: tuple[dict[str, Any], ...] = ()


class PoolCutoverAPI(Protocol):
    @property
    def migration(self) -> PoolMigrationAPI: ...
    @property
    def fencing(self) -> PoolRoleFenceAPI: ...
    @property
    def resources(self) -> ManagementStageAPI: ...

    def preflight(self, request: PoolCutoverRequest) -> None:
        """Qualify immutable publication/predecessor/backend and writer inventory."""
        ...

    def qualify_quiescence(self) -> None:
        """No active application access; ready schema and original backlog provenance."""
        ...

    def qualify_runtime_access(self, participant_id: UUID, action: str) -> None:
        """Fixed stage/observe of every retained participant runtime DB role."""
        ...

    def read_workload(self, key: str) -> dict[str, Any]: ...
    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        """None only for a definite dry-run rejection; no persistent write."""
        ...
    def patch_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        """False only for definite rejection; unknown outcomes are never retried."""
        ...

    def drained_workload(self, key: str, desired: dict[str, Any]) -> bool: ...


def cutover_documents(request: PoolCutoverRequest) -> dict[str, Any]:
    """Generate targets from retained originals, never accept arbitrary manifests."""
    qualify_cutover_image_admission(request)
    return _cutover_documents(request)


def qualify_cutover_image_admission(request: PoolCutoverRequest) -> None:
    """Keep current-clock admission checks outside input-only manifest reuse."""
    try:
        keyring = ImageAdmissionKeyring.from_json(json.dumps(
            request.fencing.retirement.migration.registration.spec.profiles.image_admission_keyring,
            sort_keys=True, separators=(",", ":")))
        for profile in request.profiles.values():
            verify_execution_image_admission(profile.image_admission, keyring=keyring,
                required_image_refs=profile.published_image_refs())
    except Exception:
        raise ValueError("pool_participant_runtime_unqualified") from None


@pure_projection
def _cutover_documents(request: PoolCutoverRequest) -> dict[str, Any]:
    """Private input-derived projection; the public entry rechecks admission."""
    migration = request.fencing.retirement.migration
    PoolCollectorCredential.model_validate(request.collector_credential.model_dump())
    spec, binding = migration.registration.spec, migration.registration.binding
    originals = retirement_documents(request.fencing.retirement)
    platform_consumer_documents(request)
    role_fence_documents(request.fencing)
    participants = {row.participant_id: row for row in spec.participants}
    if (set(request.profiles) != set(participants) or len(request.services) != len(participants)
            or {row["metadata"]["namespace"] for row in request.services} != {row.namespace for row in migration.guards}):
        raise ValueError("pool cutover service roster differs")
    producers = {_key(row): row for row in (request.manager, *request.services)}
    if len(producers) != 1 + len(participants) or len({_uid(row) for row in producers.values()} | {_uid(row) for row in originals.values()}) != len(producers) + len(originals):
        raise ValueError("pool cutover workload identity differs")
    application = request.application_delivery
    if bool(spec.profiles.application_images) != (application is not None):
        raise ValueError('pool application delivery scope differs')
    application_configuration: tuple[dict[str, Any], ...] = ()
    if application is None:
        manager = wire_manager(request=migration, original=request.manager)
    else:
        delivered = render_application_build_delivery(before=application.before, pool=spec,
            active=request.manager, candidate=migration.registration.candidate,
            profile=application.profile, repo_root=application.repo_root,
            source_delivery_version=application.source_delivery_version)
        manager = delivered.deployment
        application_configuration = delivered.configuration
    runtime = {_key(request.manager): manager}
    for guard in migration.guards:
        participant = participants[guard.participant_id]
        service, = (row for row in request.services if row["metadata"]["namespace"] == guard.namespace)
        actuator, = (row for row in request.fencing.retirement.actuators
            if row["metadata"]["namespace"] == participant.execution_namespace.name and row["metadata"]["name"] == "loom-execution-actuator")
        guests = tuple(row for row in request.fencing.retirement.actuators
            if row["metadata"]["namespace"] == participant.execution_namespace.name and row is not actuator)
        wired = wire_participant(request=migration, participant_id=guard.participant_id,
            management_origin=request.management_origin, actuator=actuator, service=service,
            runtime_profile=request.profiles[guard.participant_id], guest_actuators=guests)
        runtime.update({_key(row): row for row in wired.values()})
    development, = (row for row in spec.participants if row.environment_class == "development")
    collector, = (row for row in request.fencing.retirement.collectors if row["metadata"]["namespace"] == development.execution_namespace.name)
    observer = wire_collector(request=migration, original=collector, config_map=request.collector_config,
        management_origin=request.management_origin)
    runtime.update({_key(row): row for row in observer["workload"]})
    gateway = render_gateway(spec, namespace=binding.namespace,
        service_image=migration.registration.candidate["images"]["service"]["image_ref"], kubernetes_endpoint=request.kubernetes_endpoint)
    # Keep retirement markers: normal standalone rollouts must remain fenced.
    for key, row in runtime.items():
        row = _snapshot(row)
        row["metadata"].setdefault("annotations", {})[MARKER] = str(spec.operation_id)
        if key in originals:
            row["metadata"]["annotations"][RETIREMENT_MARKER] = str(spec.operation_id)
        runtime[key] = row
    stopped = {}
    for key, row in producers.items():
        value = _snapshot(row)
        value["spec"]["replicas"] = 0
        value["metadata"].setdefault("annotations", {})[MARKER] = str(spec.operation_id)
        stopped[key] = value
    reader_identity = tuple(row for row in participant_readonly_roles(request=migration)
        if row["kind"] in {"ClusterRole", "ClusterRoleBinding"})
    return {"producers": producers, "stopped": stopped, "runtime": runtime,
        "configuration": (*gateway["configuration"], *observer["configuration"], *reader_identity, *application_configuration),
        "authority": gateway["authority"], "workload": gateway["workload"]}


def platform_consumer_documents(request: PoolCutoverRequest) -> dict[str, dict[str, Any]]:
    """Read-only roots qualified by the entry's retained foundation renderer.

    They never become retirement, drain, runtime or resource mutation targets.
    No execution/build ServiceAccount may gain a consumer exemption.
    """
    try:
        migration = request.fencing.retirement.migration
        namespaces = {row.namespace for row in migration.guards}
        writer_namespaces = {ns.name for row in migration.registration.spec.participants
            for ns in (row.execution_namespace, row.build_namespace)}
        originals = {**retirement_documents(request.fencing.retirement),
            **{_key(row): row for row in (request.manager, *request.services)}}
        identities = {_uid(row) for row in originals.values()}
        result: dict[str, dict[str, Any]] = {}
        for row in request.platform_consumers:
            key, uid = _key(row), _uid(row)
            namespace = row["metadata"]["namespace"]
            if ((row["apiVersion"], row["kind"], row["metadata"]["name"]) not in {
                    ("apps/v1", "Deployment", "loom-web"), ("apps/v1", "Deployment", "loom-llm-gateway"),
                    ("batch/v1", "CronJob", "loom-platform-backup")}
                    or namespace not in namespaces or namespace in writer_namespaces
                    or key in originals or key in result or uid in identities):
                raise ValueError
            pod = (row["spec"]["jobTemplate"]["spec"]["template"]["spec"] if row["kind"] == "CronJob"
                else row["spec"]["template"]["spec"])
            if pod.get("serviceAccountName") != "loom-platform" or pod.get("automountServiceAccountToken") is not False:
                raise ValueError
            _snapshot(row)
            identities.add(uid)
            result[key] = copy.deepcopy(row)
        return result
    except Exception:
        raise ValueError("pool platform consumers unqualified") from None


def _contract(request: PoolCutoverRequest, documents: dict[str, Any]) -> dict[str, Any]:
    return {"migration": migration_contract(request.fencing.retirement.migration),
        **({"platform_consumers": {key: {"uid": _uid(row), "document": _stable(row)}
            for key, row in platform_consumer_documents(request).items()}} if request.platform_consumers else {}),
        **({'application_source': request.application_delivery.source_credential.model_dump(mode='json')}
            if request.application_delivery is not None else {}),
        **({"platform_authority": request.platform_authority.model_dump(mode="json")} if request.platform_authority is not None else {}),
        "roles": {"originals": [_stable(row) for row in request.fencing.originals], "targets": role_fence_documents(request.fencing)},
        "writers": {key: {"uid": _uid(row), "document": _stable(row)} for key, row in retirement_documents(request.fencing.retirement).items()},
        "producers": {key: {"uid": _uid(row), "document": _stable(row)} for key, row in documents["producers"].items()},
        "collector_config": {"uid": _uid(request.collector_config), "document": _stable(request.collector_config)},
        "collector_credential": request.collector_credential.model_dump(mode="json"),
        "documents": {key: value for key, value in documents.items() if key != "producers"}}


def cutover_material_documents(request: PoolCutoverRequest, tokens: dict[UUID, str],
        source_credentials: dict[str, str] | None = None) -> dict[str, dict[str, Any]]:
    """Fixed machine tokens and, only for builder cutover, pinned source material."""
    migration = request.fencing.retirement.migration
    documents = machine_documents(migration, tokens)
    application = request.application_delivery
    if application is None:
        if source_credentials is not None:
            raise ValueError('pool application source outside scope')
        return documents
    if source_credentials is None:
        raise ValueError('pool application source unavailable')
    payload = source_material_json(source_credentials, application.source_credential)
    manager = cutover_documents(request)['runtime'][_key(request.manager)]
    source, = (row for row in manager['spec']['template']['spec']['volumes']
        if row['name'] == 'application-source-credentials')
    document = {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque', 'immutable': True,
        'metadata': {'name': source['secret']['secretName'], 'namespace': migration.registration.binding.namespace,
            'labels': {'loom.nebius/management-installation': migration.registration.binding.installation_id,
                'loom.nebius/pool-operation': str(migration.registration.spec.operation_id)}},
        'data': {'credentials.json': base64.b64encode(payload.encode()).decode()}}
    if _key(document) in documents:
        raise ValueError('pool application source identity collision')
    documents[_key(document)] = document
    return documents


def _read_cutover_record(request: PoolCutoverRequest, documents: dict[str, Any],
                         state: Path, anchor: Path) -> dict[str, Any] | None:
    """The same anchored recovery validation serves preflight and mutation."""
    return _read_cutover_evidence(request, documents, state, anchor,
        contract_sha256=digest(_contract(request, documents)))


def _read_cutover_evidence(request: PoolCutoverRequest, documents: dict[str, Any], state: Path, anchor: Path, *,
                           contract_sha256: str, migration_contract_sha256: str | None = None) -> dict[str, Any] | None:
    """Fresh private evidence; supplied expectations must come from pure rendering.

    This is the shared reader, not a cache or a caller-selected authority. The
    ordinary entry derives expectations above; startup derives the same values
    with its other immutable targets in one complete typed-input projection.
    """
    if (state != state.resolve() or anchor != anchor.resolve() or state == anchor
            or state in anchor.parents or anchor in state.parents):
        raise ValueError
    migration = request.fencing.retirement.migration
    operation = str(migration.registration.spec.operation_id)
    identity = {"schema": "loom.nebius-pool-cutover.v1", "operation_id": operation,
        "state_dir": str(state), "contract_sha256": contract_sha256}
    marker, path = anchor / (operation + "-cutover.json"), state / "cutover.json"
    if not marker.exists() and not marker.is_symlink():
        if state.exists() or state.is_symlink():
            raise ValueError
        return None
    if json.loads(private_state._private_read(marker)) != identity:
        raise ValueError
    record = json.loads(private_state._private_read(path, limit=4 * 1024**2))
    if (not isinstance(record, dict) or set(record) != {*identity, "producers", "fenced", "runtime_access", "phases", "runtime"}
            or any(record[key] != value for key, value in identity.items())
            or set(record["producers"]) != set(documents["producers"])
            or set(record["runtime"]) != set(documents["runtime"])
            or not isinstance(record["runtime_access"], dict)
            or set(record["runtime_access"]) != {str(row.participant_id) for row in migration.guards}
            or any(value not in {"prepared", "intent", "staged"} for value in record["runtime_access"].values())
            or not set(record["phases"]) <= {"material", "configuration", "authority", "workload"}):
        raise ValueError
    for group, targets in (("producers", documents["stopped"]), ("runtime", documents["runtime"])):
        for key, item in record[group].items():
            if (set(item) != {"phase", "expected"} or item["phase"] not in {"prepared", "intent", "stopped"}
                    or (item["phase"] == "prepared") != (item["expected"] is None)):
                raise ValueError
            if item["expected"] is not None and _qualified_defaulted(targets[key], item["expected"]) != item["expected"]:
                raise ValueError
    writer_state, writer_anchor = state / "writers", state / "writer-anchor"
    if record["fenced"] is not None:
        if (not isinstance(record["fenced"], dict)
                or set(record["fenced"]) != {"migration.json", "retirement.json", "role-fencing.json"}
                or any(_hash(writer_state / name) != checksum for name, checksum in record["fenced"].items())):
            raise ValueError
        if migration_contract_sha256 is None:
            # Ordinary callers retain the original post-read requalification.
            # Startup alone supplies its bundle and rechecks it before return.
            _closed(migration, writer_state, writer_anchor)
        else:
            _read_closed_migration(migration, writer_state, writer_anchor, contract_sha256=migration_contract_sha256)
    elif (any(value != "prepared" for value in record["runtime_access"].values()) or record["phases"]
            or any(item["phase"] != "prepared" for item in record["runtime"].values())):
        raise ValueError
    for phase, checksum in record["phases"].items():
        if checksum is not None and _hash(state / phase / "stage.json") != checksum:
            raise ValueError
    return record


def retained_cutover_workloads(request: PoolCutoverRequest, *, state_dir: Path, anchor_dir: Path,
                               observed: dict[str, dict[str, Any]] | None = None) -> dict[str, dict[str, Any]]:
    """Read only: derive each original or exactly journal-qualified successor.

    This is not drain, runtime-health or database evidence. It selects the right
    workload for those checks without requiring a retired Pod to run again.
    An unanchored state file or a zero replica count is never migration evidence.
    """
    try:
        documents = cutover_documents(request)
        originals = retirement_documents(request.fencing.retirement)
        expected = {**originals, **documents["producers"]}
        record = _read_cutover_record(request, documents, state_dir, anchor_dir)
        if record is None:
            return copy.deepcopy(expected)
        for key, item in record["producers"].items():
            if item["phase"] != "prepared":
                expected[key] = item["expected"]
        state, anchor = state_dir / "writers", state_dir / "writer-anchor"
        operation = str(request.fencing.retirement.migration.registration.spec.operation_id)
        marker, path = anchor / (operation + "-retirement.json"), state / "retirement.json"
        if marker.exists() or marker.is_symlink():
            identity = {"schema": "loom.nebius-pool-retirement.v1", "operation_id": operation, "state_dir": str(state),
                "closure_sha256": _closed(request.fencing.retirement.migration, state, anchor),
                "originals_sha256": digest({key: {"uid": _uid(row), "document": _stable(row)} for key, row in originals.items()})}
            child = json.loads(private_state._private_read(path, limit=4 * 1024**2))
            if (json.loads(private_state._private_read(marker)) != identity
                    or set(child) != {*identity, "workloads"}
                    or any(child[key] != value for key, value in identity.items())
                    or set(child["workloads"]) != set(originals)
                    or any(value not in {"prepared", "intent", "stopped"} for value in child["workloads"].values())):
                raise ValueError
            stopped = stopped_documents(request.fencing.retirement)
            for key, phase in child["workloads"].items():
                if phase != "prepared":
                    expected[key] = stopped[key]
        elif path.exists() or path.is_symlink() or record["fenced"] is not None:
            raise ValueError
        for key, item in record["runtime"].items():
            if item["phase"] != "prepared":
                expected[key] = item["expected"]
        # Startup keeps the closed parent immutable. An uncertain write may be
        # either exact template, so callers must supply observation, not guess.
        from scripts.ops.nebius_pool_startup import startup_workload_options

        choices = startup_workload_options(request, state_dir=state_dir, anchor_dir=anchor_dir)
        if choices is not None:
            for key in expected:
                options = choices[key]
                if observed is not None:
                    original = originals[key] if key in originals else documents["producers"][key]
                    options = tuple(row for row in options if _matches(observed[key], row, _uid(original)))
                if len(options) != 1:
                    raise ValueError
                expected[key] = options[0]
        return copy.deepcopy(expected)
    except Exception:
        raise ValueError("pool_workload_recovery_unqualified") from None


def _updates(*, api: PoolCutoverAPI, originals: dict[str, Any], targets: dict[str, Any],
             items: dict[str, Any], save: Callable[[], None], pending: str) -> str | None:
    for key, original in originals.items():
        item = items[key]
        actual = api.read_workload(key)
        if item["phase"] == "prepared":
            if not _matches(actual, original, _uid(original)):
                raise ValueError
            preview = api.preview_workload(key, actual, targets[key])
            if preview is None:
                return pending  # No mutation intent or retry of a rejected dry run.
            item["expected"] = _qualified_defaulted(targets[key], preview)
            item["phase"] = "intent"
            save()
            try:
                accepted = api.patch_workload(key, actual, targets[key])
            except Exception:
                accepted = True
            if accepted is False:
                item.update(phase="prepared", expected=None)
                save()
                return pending
            if accepted is not True:
                raise ValueError
            actual = api.read_workload(key)
        if not _matches(actual, item["expected"], _uid(original)):
            raise ValueError
        if item["phase"] != "stopped":
            item["phase"] = "stopped"
            save()
        if api.drained_workload(key, item["expected"]) is not True:
            return "pending_producer_drain" if pending == "pending_producer_update" else "pending_runtime_drain"
    for key, original in originals.items():
        if not _matches(api.read_workload(key), items[key]["expected"], _uid(original)):
            raise ValueError
        if api.drained_workload(key, items[key]["expected"]) is not True:
            return "pending_producer_drain" if pending == "pending_producer_update" else "pending_runtime_drain"
    return None


def stage_pool_cutover(*, request: PoolCutoverRequest, tokens: dict[UUID, str], api: PoolCutoverAPI,
                       state_dir: Path, anchor_dir: Path,
                       source_credentials: dict[str, str] | None = None) -> dict[str, Any]:
    """Freeze → close → retire/fence → material/ACLs → disabled runtime.

    Runtime replacement changes the original templates. Recovery after that
    boundary qualifies retained child hashes, roles and current target templates;
    it must NOT replay retirement/fencing against the original stopped templates.
    Activation, rollback and durable refresh completion remain subsequent gates.
    """
    try:
        state, anchor = state_dir.absolute(), anchor_dir.absolute()
        if (state != state.resolve() or anchor != anchor.resolve() or state == anchor
                or state in anchor.parents or anchor in state.parents):
            raise ValueError
        documents = cutover_documents(request)
        migration = request.fencing.retirement.migration
        material = cutover_material_documents(request, tokens, source_credentials)  # Before downtime.
        operation = str(migration.registration.spec.operation_id)
        identity = {"schema": "loom.nebius-pool-cutover.v1", "operation_id": operation,
            "state_dir": str(state), "contract_sha256": digest(_contract(request, documents))}
        writer_state, writer_anchor = state / "writers", state / "writer-anchor"
        with private_state._locked_state(anchor):
            path, marker = state / "cutover.json", anchor / (operation + "-cutover.json")
            if any(item.exists() or item.is_symlink() for item in (
                    state / "startup.json", anchor / (operation + "-startup.json"))):
                raise ValueError  # Recovery now belongs to startup, never replay closure.
            record = _read_cutover_record(request, documents, state, anchor)
            if record is None:
                api.preflight(request)
                private_state._atomic_json(marker, identity)
                private_state._private_directory(state)
                record = {**identity, "producers": {key: {"phase": "prepared", "expected": None} for key in documents["producers"]},
                    "fenced": None, "runtime_access": dict.fromkeys((str(row.participant_id) for row in migration.guards), "prepared"), "phases": {},
                    "runtime": {key: {"phase": "prepared", "expected": None} for key in documents["runtime"]}}

            def save() -> None:
                private_state._atomic_json(path, record)

            def result(status: str) -> dict[str, Any]:
                return {"status": status, "operation_id": operation, "writer_migration_complete": False}

            save()
            api.preflight(request)
            runtime_started = any(item["phase"] != "prepared" for item in record["runtime"].values())
            if not runtime_started:
                pending = _updates(api=api, originals=documents["producers"], targets=documents["stopped"],
                    items=record["producers"], save=save, pending="pending_producer_update")
                if pending:
                    return result(pending)
            api.qualify_quiescence()
            if record["fenced"] is None:
                closed = close_and_register_pool(request=migration, api=api.migration,
                    state_dir=writer_state, anchor_dir=writer_anchor)
                if closed["status"] != "pool_registered_closed":
                    return closed
                fenced = fence_pool_roles(request=request.fencing, api=api.fencing,
                    state_dir=writer_state, anchor_dir=writer_anchor)
                if fenced["status"] != "participant_roles_restricted":
                    return fenced
                record["fenced"] = {name: _hash(writer_state / name) for name in ("migration.json", "retirement.json", "role-fencing.json")}
                save()

            def verify_fenced() -> None:
                if any(api.migration.guard(target, "observe").get("status") != "held" for target in migration.guards):
                    raise ValueError
                targets = role_fence_documents(request.fencing)
                if any(not _matches(api.fencing.read_role(_key(row)), targets[_key(row)], _uid(row)) for row in request.fencing.originals):
                    raise ValueError
                api.fencing.verify_readonly()
                for key, original in documents["producers"].items():
                    item = record["runtime"][key]
                    desired = item["expected"] if item["phase"] != "prepared" else record["producers"][key]["expected"]
                    if not _matches(api.read_workload(key), desired, _uid(original)) or api.drained_workload(key, desired) is not True:
                        raise ValueError
                # Before each stage, all retained writers must be stopped in
                # precisely the old or journaled new template, never a third one.
                stopped = stopped_documents(request.fencing.retirement)
                for key, original in retirement_documents(request.fencing.retirement).items():
                    item = record["runtime"].get(key)
                    desired = (item["expected"] if item is not None and item["phase"] != "prepared"
                        else stopped[key])
                    if not _matches(api.read_workload(key), desired, _uid(original)) or api.drained_workload(key, desired) is not True:
                        raise ValueError

            verify_fenced()
            for guard in migration.guards:
                key = str(guard.participant_id)
                if record["runtime_access"][key] == "prepared":
                    record["runtime_access"][key] = "intent"
                    save()
                    try:
                        api.qualify_runtime_access(guard.participant_id, "stage")
                    except Exception:
                        pass  # Unknown SQL outcome: only exact qualification may recover.
                api.qualify_runtime_access(guard.participant_id, "observe")
                record["runtime_access"][key] = "staged"
                save()
            for phase in ("material", "configuration", "authority", "workload"):
                verify_fenced()
                if phase not in record["phases"]:
                    record["phases"][phase] = None
                    save()
                if phase == "material":
                    revision = digest({'contract': migration_contract(migration), 'documents': material})
                    _stage_fixed_documents(documents=material, revision=revision, phase='pool-machine-material',
                        binding=migration.registration.binding, api=api.resources, state_dir=state / phase,
                        default_document=material_defaulted)
                else:
                    stage_documents = {_key(row): row for row in documents[phase]}
                    _stage_fixed_documents(documents=stage_documents, revision=digest(stage_documents), phase="pool-cutover-" + phase,
                        binding=migration.registration.binding, api=api.resources, state_dir=state / phase)
                checksum = _hash(state / phase / "stage.json")
                if record["phases"][phase] not in {None, checksum}:
                    raise ValueError
                record["phases"][phase] = checksum
                save()
            stopped = stopped_documents(request.fencing.retirement)
            previous = {key: (documents["stopped"][key] if key in documents["stopped"]
                else stopped[key]) for key in documents["runtime"]}
            retained = {**retirement_documents(request.fencing.retirement), **documents["producers"]}
            previous = copy.deepcopy(previous)
            for key, document in previous.items():
                document["metadata"]["uid"] = _uid(retained[key])
            pending = _updates(api=api, originals=previous, targets=documents["runtime"],
                items=record["runtime"], save=save, pending="pending_runtime_update")
            if pending:
                return result(pending)
            verify_fenced()
            api.qualify_quiescence()
            for guard in migration.guards:
                api.qualify_runtime_access(guard.participant_id, "observe")
            # Replacement can take long enough for a previously qualified
            # gateway/catalog/credential to drift. Recheck every retained stage
            # with GETs only; completion must not silently repair or recreate it.
            for phase, checksum in record["phases"].items():
                child = state / phase / "stage.json"
                if checksum is None or _hash(child) != checksum:
                    raise ValueError
                staged = json.loads(private_state._private_read(child, limit=4 * 1024**2))
                for item in staged["resources"].values():
                    if item["status"] != "created":
                        raise ValueError
                    api.resources.verify_identity(migration.registration.binding)
                    actual = api.resources.get_resource(item["desired"])
                    if actual is None or _uid(actual) != item["uid"] or _snapshot(actual) != item["observed"]:
                        raise ValueError
            api.resources.verify_identity(migration.registration.binding)
            return result("pool_runtime_staged_closed")
    except Exception:
        raise ValueError("pool_cutover_unconfirmed_preserve_evidence") from None
