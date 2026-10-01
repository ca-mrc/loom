"""Private cutover publication and bound readers; no command or activation.

The protected installer must still supply complete installed writer/schema
qualification, activation, rollback and successor-refresh completion. This module
resolves protected publication, binds completed history and qualifies retained
runtime databases and the physical provider before yielding the existing readers.
It imports no ambient kubeconfig and replays no old installation. No live writes
occur here.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Annotated, Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _uid
from scripts.ops.nebius_management_entry import EntryError, _private, connected_checks
from scripts.ops.nebius_management_prerequisites import HTTPSManagementPrerequisites
from scripts.ops.nebius_management_refresh_predecessor import (
    CompletedRefresh,
    CompletedUpgrade,
    RefreshPredecessorV1,
    UpgradePredecessorV1,
    load_completed_refresh,
    load_completed_upgrade,
)
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_cutover import (
    PoolCutoverRequest,
    cutover_documents,
    retained_cutover_workloads,
)
from scripts.ops.nebius_pool_material import machine_documents
from scripts.ops.nebius_pool_migration import PoolGuardTarget, PoolMigrationRequest
from scripts.ops.nebius_pool_migration_guard import (
    KubectlPoolGuardAPI,
    qualify_database_destination,
)
from scripts.ops.nebius_pool_origin_history import (
    KubectlPoolHistoryAPI,
    derive_management_history_target,
)
from scripts.ops.nebius_pool_registration import PoolRegistrationRequest
from scripts.ops.nebius_pool_retirement import PoolRetirementRequest
from scripts.ops.nebius_pool_role_fencing import PoolRoleFenceRequest

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_control_plane.execution_placement import quota_identity
from loom_execution_capacity_collector.config import NebiusCapacitySourceSettings
from loom_execution_capacity_collector.nebius import NebiusCapacityReader
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
    _json,
)
from loom_service.pool_management.installation import PoolInstallation


class PoolCutoverPrivateInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    schema_version: Literal["loom.nebius-pool-cutover-private-inputs.v1"]
    original_upgrade: UpgradePredecessorV1
    predecessor: Annotated[UpgradePredecessorV1 | RefreshPredecessorV1, Field(discriminator="kind")]
    installation: PoolInstallation
    publication: ProtectedPublication
    candidate: dict[str, Any]
    profile: dict[str, Any]
    guards: tuple[PoolGuardTarget, ...]
    actuators: tuple[dict[str, Any], ...]
    collectors: tuple[dict[str, Any], ...]
    roles: tuple[dict[str, Any], ...]
    services: tuple[dict[str, Any], ...]
    collector_config: dict[str, Any]
    profiles: dict[UUID, ServiceExecutionRuntimeProfileV1]
    machine_token_files: dict[UUID, Path]
    foundation_candidate: str = Field(pattern=r"^[0-9a-f]{40}$")


@dataclass(frozen=True, repr=False)
class PoolCutoverContext:
    operation: dict[str, Any]
    inputs: PoolCutoverPrivateInputs
    original: CompletedUpgrade
    predecessor: CompletedUpgrade | CompletedRefresh
    request: PoolCutoverRequest
    tokens: dict[UUID, str]


def _operation(value: dict[str, Any]) -> tuple[UUID, Path]:
    fields = {"schema", "operation_id", "source_sha", "candidate", "installation_id", "namespace",
        "state_dir", "anchor_dir", "inputs_path", "inputs_sha256"}
    if (set(value) != fields or value["schema"] != "loom.nebius-pool-cutover-operation.v1"
            or any(not isinstance(row, str) or not 0 < len(row) <= 1024 for row in value.values())
            or value["source_sha"] != value["candidate"]
            or re.fullmatch(r"[0-9a-f]{40}", value["candidate"]) is None
            or re.fullmatch(r"[0-9a-f]{64}", value["inputs_sha256"]) is None):
        raise ValueError
    for key in ("operation_id", "installation_id"):
        if not UUID(value[key]).int or str(UUID(value[key])) != value[key]:
            raise ValueError
    for key in ("inputs_path", "state_dir", "anchor_dir"):
        path = Path(value[key])
        if not path.is_absolute() or path != path.resolve():
            raise ValueError
    operation = UUID(value["operation_id"])
    directory = Path(value["inputs_path"]).parent
    if (directory.name != str(operation) or directory.parent.name != "pool-cutover"
            or directory.parent.parent.name != "nebius-management"
            or Path(value["inputs_path"]) != directory / "inputs.json"
            or Path(value["state_dir"]) != directory / "state"
            or Path(value["anchor_dir"]) != directory / "anchor"):
        raise ValueError
    return operation, directory.parent.parent


def load_pool_cutover_inputs(operation: dict[str, Any]) -> PoolCutoverContext:
    """Qualify exact private data before any credential exchange or downtime.

    Completed predecessor receipts derive the manager; it is not a supplied
    manifest. Participant/backend and publication values remain claims requiring
    live qualification by the connected protected parent, not approval by parsing.
    """
    try:
        operation_id, root = _operation(operation)
        raw = _private(Path(operation["inputs_path"]), 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation["inputs_sha256"]:
            raise ValueError
        inputs = PoolCutoverPrivateInputs.model_validate(_json(raw))
        original = load_completed_upgrade(inputs.original_upgrade)
        if Path(original.selector.operation["inputs_path"]).parent.parent != root:
            raise ValueError
        predecessor: CompletedUpgrade | CompletedRefresh
        if isinstance(inputs.predecessor, UpgradePredecessorV1):
            if inputs.predecessor != inputs.original_upgrade:
                raise ValueError
            predecessor = original
        else:
            predecessor = load_completed_refresh(inputs.predecessor, original=original)
        binding = original.upgrade.setup.binding
        config = original.deployment.installation.foundation.platform_config
        spec = inputs.installation
        if ((str(spec.operation_id), str(spec.installation_id), binding.namespace) !=
                (str(operation_id), operation["installation_id"], operation["namespace"])
                or str(spec.installation_id) != binding.installation_id
                or (spec.cluster_id, spec.node_group_id) != (config["cluster_id"], config["execution_node_group_id"])
                or inputs.candidate.get("candidate_sha") != operation["candidate"]
                or inputs.profile.get("candidate_sha") != operation["candidate"]
                or inputs.publication.source_sha != operation["candidate"]
                or any(target.database is None or target.database.actuator_credential_uid is None
                    or target.database.actuator_credential_resource_version is None for target in inputs.guards)):
            raise ValueError
        migration = PoolMigrationRequest(PoolRegistrationRequest(spec, binding, inputs.candidate), inputs.guards)
        request = PoolCutoverRequest(PoolRoleFenceRequest(PoolRetirementRequest(migration,
            inputs.actuators, inputs.collectors), inputs.roles), predecessor.active, inputs.services,
            inputs.collector_config, inputs.profiles, "https://" + predecessor.deployment.public_host,
            "https://kubernetes.default.svc")
        cutover_documents(request)
        names = {row.machine_id for row in spec.machines}
        paths = set(inputs.machine_token_files.values())
        connection = original.original_inputs.operator_connection
        protected = {*original.history, *predecessor.history, connection.ca_file, connection.credentials_file,
            original.original_inputs.operator_cloud_credentials, original.original_inputs.ingress_config,
            Path(operation["inputs_path"])}
        if set(inputs.machine_token_files) != names or len(paths) != len(names) or paths & protected:
            raise ValueError
        tokens = {identity: _private(path, 65536).decode() for identity, path in inputs.machine_token_files.items()}
        machine_documents(migration, tokens)
        return PoolCutoverContext(dict(operation), inputs, original, predecessor, request, tokens)
    except Exception:
        raise EntryError("pool cutover private inputs unqualified") from None


@dataclass(frozen=True, repr=False)
class ConnectedPoolReaders:
    base: HTTPSManagementPrerequisites
    ssl_context: ssl.SSLContext
    token: str
    guards: KubectlPoolGuardAPI
    history: KubectlPoolHistoryAPI


def qualify_pool_runtime_databases(context: PoolCutoverContext, guards: KubectlPoolGuardAPI) -> None:
    """Match every retained participant consumer to its actual database.

    Fresh operations require original running Pods. Recovery instead selects
    exact journaled stopped/rewired templates and checks their retained database
    references; the cutover parent still owns drain and later startup acceptance.
    No state file is created and no stopped workload is restarted by this read.
    """
    try:
        migration = context.request.fencing.retirement.migration
        if guards.request != migration or load_pool_cutover_inputs(context.operation) != context:
            raise ValueError
        state, anchor = Path(context.operation["state_dir"]), Path(context.operation["anchor_dir"])
        expected = retained_cutover_workloads(context.request, state_dir=state, anchor_dir=anchor)
        for target in migration.guards:
            database = target.database
            if database is None or database.actuator_credential_uid is None or database.actuator_credential_resource_version is None:
                raise ValueError
            participant, = (row for row in migration.registration.spec.participants if row.participant_id == target.participant_id)
            service, = (row for row in context.request.services if row["metadata"]["namespace"] == target.namespace)
            actuators = tuple(row for row in context.request.fencing.retirement.actuators
                if row["metadata"]["namespace"] == participant.execution_namespace.name)
            for original in (target.controller, service, *actuators):
                namespace, name = original["metadata"]["namespace"], original["metadata"]["name"]
                desired = expected[_key(original)]
                current = guards._get("deployment", name, namespace)
                if not _matches(current, desired, _uid(original)):
                    raise ValueError
                actuator = namespace != target.namespace
                uid = database.actuator_credential_uid if actuator else database.credential_uid
                version = database.actuator_credential_resource_version if actuator else database.credential_resource_version
                if desired["spec"]["replicas"] == 1:
                    guards.qualify_runtime_database(target, original=original,
                        credential_uid=uid, credential_resource_version=version)
                else:
                    if type(desired["spec"]["replicas"]) is not int or desired["spec"]["replicas"] != 0:
                        raise ValueError
                    backend = guards._database(target)
                    variable = "LOOM_EXECUTION_ACTUATOR_DB_URL" if actuator else "LOOM_CP_DB_URL" if name == "loom-control-plane" else "LOOM_SVC_DB_URL"
                    url = guards._workload_database_url(original, url_variable=variable,
                        credential_uid=uid, credential_resource_version=version)
                    qualify_database_destination(url, target.namespace)
                    if (_uid(guards._database(target)) != _uid(backend)
                            or guards._workload_database_url(original, url_variable=variable,
                                credential_uid=uid, credential_resource_version=version) != url
                            or not _matches(guards._get("deployment", name, namespace), desired, _uid(original))):
                        raise ValueError
        if (retained_cutover_workloads(context.request, state_dir=state, anchor_dir=anchor) != expected
                or load_pool_cutover_inputs(context.operation) != context):
            raise ValueError
    except Exception:
        raise EntryError("pool cutover runtime databases unqualified") from None


def qualify_pool_manager_database(context: PoolCutoverContext, history: KubectlPoolHistoryAPI) -> None:
    """Use the predecessor's management binding, never a participant credential."""
    try:
        history.qualify_binding(context.request.fencing.retirement.migration, context.request.manager)
        if load_pool_cutover_inputs(context.operation) != context:
            raise ValueError
        state, anchor = Path(context.operation["state_dir"]), Path(context.operation["anchor_dir"])
        expected = retained_cutover_workloads(context.request, state_dir=state, anchor_dir=anchor)
        original, target = context.request.manager, history.target
        desired = expected[_key(original)]
        current = history._get("deployment", "loom-service", target.namespace)
        if not _matches(current, desired, _uid(original)):
            raise ValueError
        if desired["spec"]["replicas"] == 1:
            history.qualify_manager_database()
        else:
            if type(desired["spec"]["replicas"]) is not int or desired["spec"]["replicas"] != 0:
                raise ValueError
            backend = history._database(target, url_variable="LOOM_SVC_DB_URL")
            binding = target.database
            url = history._workload_database_url(original, url_variable="LOOM_SVC_DB_URL",
                credential_uid=binding.credential_uid, credential_resource_version=binding.credential_resource_version)
            qualify_database_destination(url, target.namespace)
            if (_uid(history._database(target, url_variable="LOOM_SVC_DB_URL")) != _uid(backend)
                    or history._workload_database_url(original, url_variable="LOOM_SVC_DB_URL",
                        credential_uid=binding.credential_uid, credential_resource_version=binding.credential_resource_version) != url
                    or not _matches(history._get("deployment", "loom-service", target.namespace), desired, _uid(original))):
                raise ValueError
        if (retained_cutover_workloads(context.request, state_dir=state, anchor_dir=anchor) != expected
                or load_pool_cutover_inputs(context.operation) != context):
            raise ValueError
    except Exception:
        raise EntryError("pool cutover management database unqualified") from None


def qualify_pool_provider(context: PoolCutoverContext, base: HTTPSManagementPrerequisites) -> None:
    """Resolve the retained collector's physical group and native quota scope.

    This read neither requests nodes nor reserves a share of provider headroom.
    A scale-zero group is valid; actual workload fit/telemetry belongs to startup.
    The operator credential stays local and is never copied into the collector.
    """
    try:
        original = context.request.collector_config
        namespace, name = original["metadata"]["namespace"], original["metadata"]["name"]
        path = "/api/v1/namespaces/" + namespace + "/configmaps/" + name
        current = base._request("GET", path)
        if current is None or not _matches(current, original, _uid(original)):
            raise ValueError
        raw, spec = original["data"], context.inputs.installation
        foundation = context.original.deployment.installation.foundation.platform_config
        credential = context.original.original_inputs.operator_cloud_credentials
        before = _private(credential, 1024**2)
        values: dict[str, Any] = {}
        for field, definition in NebiusCapacitySourceSettings.model_fields.items():
            key = "LOOM_EXECUTION_CAPACITY_COLLECTOR_" + field.upper()
            if field == "nebius_credentials_file":
                values[field] = credential
            elif field.startswith("kubernetes_"):
                if raw.get(key):
                    raise ValueError
                values[field] = None
            elif key in raw:
                values[field] = raw[key]
            elif not definition.is_required():
                values[field] = definition.default
            else:
                raise ValueError
        # Every field is explicit: ambient collector variables cannot override
        # this protected read or supply a missing ConfigMap setting.
        settings = NebiusCapacitySourceSettings(_env_file=None, **values)
        if ((settings.nebius_project_id, settings.nebius_quota_parent_id, settings.nebius_region,
                settings.nebius_node_group_id, spec.cluster_id) != (
                    foundation["project_id"], foundation["quota_parent_id"], foundation["region"],
                    foundation["execution_node_group_id"], foundation["cluster_id"])
                or spec.node_group_id != settings.nebius_node_group_id):
            raise ValueError

        async def observe() -> None:
            async with asyncio.timeout(120):
                reader = NebiusCapacityReader(settings)
                try:
                    snapshot = await reader.capture_pool(expected_cluster_id=spec.cluster_id)
                    if (snapshot.node_group is None or snapshot.node_group.id != spec.node_group_id
                            or {key: quota_identity(value) for key, value in snapshot.quota_resources.items()} != spec.quota_identities
                            or snapshot.provider_capacity_state != "available"
                            or snapshot.autoscaler_state not in {"ready", "scaling"}):
                        raise ValueError
                finally:
                    await reader.close()

        asyncio.run(observe())
        current = base._request("GET", path)
        if (_private(credential, 1024**2) != before
                or current is None or not _matches(current, original, _uid(original))):
            raise ValueError
    except Exception:
        raise EntryError("pool cutover provider unqualified") from None


async def qualify_pool_publication(context: PoolCutoverContext, http: httpx.AsyncClient) -> None:
    """Resolve protected bytes using predecessor trust, never the supplied catalog.

    Publication can replace signed code/image fields but not each participant's
    retained environment policy. The existing runtime renderer checks the latter;
    all replaced fields must now agree with the same authenticated publication.
    """
    try:
        inputs = context.inputs
        installation = context.predecessor.deployment.installation
        if inputs.installation.profiles.image_admission_keyring != installation.keyring:
            raise ValueError
        catalog = GitHubCandidateCatalog(http,
            token=context.original.upgrade.original.material["loom-management-publications"]["token"],
            publications=[inputs.publication], registry_prefix=installation.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
        selected = await catalog.resolve(inputs.publication.candidate_id)
        if (selected.candidate != inputs.candidate or selected.profile != inputs.profile
                or context.request.fencing.retirement.migration.registration.candidate != selected.candidate):
            raise ValueError
        runtime = ServiceExecutionRuntimeProfileV1.model_validate(selected.profile)
        fields = ("candidate_sha", "task_image_ref", "runtime_image_ref", "agent_image_ref",
            "runtime_binary_sha256", "image_admission")
        for profile in context.request.profiles.values():
            if any(getattr(profile, field) != getattr(runtime, field) for field in fields):
                raise ValueError
        if (any((row.candidate_sha, row.runtime_image_ref, row.runtime_binary_sha256) != (
                    runtime.candidate_sha, runtime.runtime_image_ref, runtime.runtime_binary_sha256)
                for row in inputs.installation.profiles.execution)
                or any(row.settings.service_image != runtime.task_image_ref
                    for row in inputs.installation.profiles.task_images)):
            raise ValueError
    except Exception:
        raise EntryError("pool cutover publication unqualified") from None


async def _connected_publication(context: PoolCutoverContext) -> None:
    async with asyncio.timeout(180):
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_pool_publication(context, http)


@contextmanager
def connected_pool_readers(context: PoolCutoverContext) -> Iterator[ConnectedPoolReaders]:
    """One qualified native authority for HTTPS and fixed SQL transports.

    Copy only its current bounded bearer and pinned CA into a fresh private
    kubeconfig. No exec plugin, client certificate, ambient context, proxy, CA
    fallback or credential from the ingress kubeconfig is inherited. The existing
    readers hash that exact file before operations. It is erased on exit, including
    failures; recovery obtains a fresh token instead of journaling a credential.
    """
    if load_pool_cutover_inputs(context.operation) != context:
        raise EntryError("pool cutover context changed before connection")
    try:
        asyncio.run(_connected_publication(context))
    except Exception:
        raise EntryError("pool cutover publication unqualified") from None
    # Remote qualification is not permission to use drifted private inputs.
    if load_pool_cutover_inputs(context.operation) != context:
        raise EntryError("pool cutover context changed during publication")
    original = context.original
    connection = original.original_inputs.operator_connection
    authority = _private(connection.ca_file, 1024**2)
    with connected_checks(original.original_inputs, original.ingress,
            foundation_candidate=context.inputs.foundation_candidate) as (base, trust, token):
        directory = Path(context.operation["inputs_path"]).parent
        private_state._private_directory(directory)
        with TemporaryDirectory(prefix=".pool-transport-", dir=directory) as temporary:
            try:
                if (not isinstance(token, str) or not 0 < len(token) <= 65536
                        or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in token)
                        or _private(connection.ca_file, 1024**2) != authority):
                    raise ValueError
                credential = base._request("GET", "/api/v1/namespaces/" + original.upgrade.setup.binding.namespace + "/secrets/loom-platform-db")
                if credential is None:
                    raise ValueError
                target = derive_management_history_target(original=original, predecessor=context.predecessor, credential=credential)
                kubeconfig = Path(temporary) / "kubeconfig.json"
                private_state._atomic_json(kubeconfig, {"apiVersion": "v1", "kind": "Config",
                    "clusters": [{"name": "loom-pool", "cluster": {"server": connection.endpoint,
                        "certificate-authority-data": base64.b64encode(authority).decode()}}],
                    "users": [{"name": "loom-pool-operator", "user": {"token": token}}],
                    "contexts": [{"name": "loom-pool", "context": {"cluster": "loom-pool", "user": "loom-pool-operator"}}],
                    "current-context": "loom-pool"})
                migration = context.request.fencing.retirement.migration
                executable = Path(original.ingress["kubectl"])
                guards = KubectlPoolGuardAPI(request=migration, kubeconfig=kubeconfig, executable=executable)
                history = KubectlPoolHistoryAPI(request=migration, target=target, kubeconfig=kubeconfig, executable=executable)
                history.qualify_binding(migration, context.request.manager)
                qualify_pool_runtime_databases(context, guards)
                qualify_pool_manager_database(context, history)
                qualify_pool_provider(context, base)
                if load_pool_cutover_inputs(context.operation) != context:
                    raise ValueError
            except EntryError:
                raise
            except Exception:
                raise EntryError("pool cutover operator readers unqualified") from None
            # Do not replace the parent's stage-qualified recovery error with a
            # connection diagnostic. The temporary authority is erased either way.
            yield ConnectedPoolReaders(base, trust, token, guards, history)
