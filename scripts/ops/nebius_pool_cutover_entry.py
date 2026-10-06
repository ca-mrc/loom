"""Private cutover qualification and complete protected operation composition.

This module resolves protected publication, binds completed history, qualifies
retained runtime databases and the physical provider, and composes the journaled
parent. Its fixed complete operation owns startup, opening and explicit recovery;
individual stages are not public commands. No ambient kubeconfig or old installer
replay is used. Registration stages its Job only under the held local guards.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import re
import ssl
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Annotated, Any, Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_upgrade_prerequisites import ApplicationUpgradePrerequisites
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
from scripts.ops.nebius_pool_activation_live import HTTPSPoolActivationAPI
from scripts.ops.nebius_pool_application_delivery import (
    ApplicationBuildDeliveryRequest,
    ApplicationSourceCredentialPin,
    derive_application_build_deployment,
    qualify_application_source_material,
)
from scripts.ops.nebius_pool_cutover import (
    PoolCutoverRequest,
    cutover_documents,
    retained_cutover_workloads,
)
from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_dormant import DormantPoolConsumer
from scripts.ops.nebius_pool_material import machine_documents
from scripts.ops.nebius_pool_migration import (
    PoolGuardTarget,
    PoolMigrationError,
    PoolMigrationRequest,
)
from scripts.ops.nebius_pool_migration_guard import (
    TELEMETRY_FAILURE_STAGES,
    KubectlPoolGuardAPI,
    qualify_database_destination,
)
from scripts.ops.nebius_pool_operation import PoolOperationError, run_pool_operation
from scripts.ops.nebius_pool_origin_history import (
    KubectlPoolHistoryAPI,
    derive_management_history_target,
)
from scripts.ops.nebius_pool_platform_authority import PoolPlatformAuthority
from scripts.ops.nebius_pool_registration import (
    HTTPSPoolRegistrationAPI,
    PoolRegistrationRequest,
    stage_pool_registration,
)
from scripts.ops.nebius_pool_retirement import PoolRetirementRequest
from scripts.ops.nebius_pool_role_fencing import PoolRoleFenceRequest
from scripts.ops.nebius_pool_runtime import PoolCollectorCredential
from scripts.ops.nebius_pool_startup import startup_workload_options
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI

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

_CONNECTION_ERRORS = {
    'pool cutover publication unqualified': 'publication',
    'pool cutover operator readers unqualified': 'operator_readers',
    'pool cutover runtime databases unqualified': 'runtime_databases',
    'pool cutover runtime telemetry unqualified': 'runtime_telemetry',
    'pool cutover management database unqualified': 'management_database',
    'pool cutover provider unqualified': 'provider',
    'pool cutover connected scope unqualified': 'connected_scope',
    'pool cutover context changed before connection': 'private_inputs',
    'pool cutover context changed during publication': 'private_inputs',
    **{'pool cutover runtime telemetry ' + stage + ' unqualified': 'runtime_telemetry_' + stage
        for stage in TELEMETRY_FAILURE_STAGES},
}

if TYPE_CHECKING:
    from scripts.ops.nebius_pool_refresh import PoolManagerRefresh


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
    dormant_consumers: tuple[DormantPoolConsumer, ...] = ()
    roles: tuple[dict[str, Any], ...]
    services: tuple[dict[str, Any], ...]
    platform_consumers: tuple[dict[str, Any], ...] = ()
    collector_config: dict[str, Any]
    collector_credential: PoolCollectorCredential
    application_source_credential: ApplicationSourceCredentialPin | None = None
    source_delivery_version: Literal['v1', 'v2'] = Field(default='v1', exclude_if=lambda value: value == 'v1')
    platform_authority: PoolPlatformAuthority
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
                or str(inputs.platform_authority.kube_system_uid) != binding.kube_system_uid
                or (spec.cluster_id, spec.node_group_id) != (config["cluster_id"], config["execution_node_group_id"])
                or inputs.candidate.get("candidate_sha") != operation["candidate"]
                or inputs.profile.get("candidate_sha") != operation["candidate"]
                or inputs.publication.source_sha != operation["candidate"]
                or any(target.database is None or target.database.actuator_credential_uid is None
                    or target.database.actuator_credential_resource_version is None for target in inputs.guards)):
            raise ValueError
        migration = PoolMigrationRequest(PoolRegistrationRequest(spec, binding, inputs.candidate), inputs.guards)
        application_delivery = None
        if bool(spec.profiles.application_images) != (inputs.application_source_credential is not None):
            raise ValueError
        if inputs.application_source_credential is not None:
            application_delivery = ApplicationBuildDeliveryRequest(predecessor.deployment, inputs.profile,
                original.upgrade.setup.repo_root, inputs.application_source_credential, inputs.source_delivery_version)
        if inputs.platform_consumers:
            from scripts.ops.nebius_management_stage import _qualified_defaulted

            from loom.nebius_platform_render import build_platform

            foundation = predecessor.deployment.installation
            retained_config = foundation.foundation.platform_config
            if retained_config["namespace"] not in {row.namespace for row in inputs.guards}:
                raise ValueError
            rendered = build_platform(retained_config, inputs.candidate, inputs.profile, foundation.keyring,
                repo_root=original.upgrade.setup.repo_root)
            expected = {_key(row): row for rows in rendered.values() for row in rows
                if (row["kind"], row["metadata"]["name"]) in {("Deployment", "loom-web"),
                    ("Deployment", "loom-llm-gateway"), ("CronJob", "loom-platform-backup")}}
            for row in inputs.platform_consumers:
                _qualified_defaulted(expected[_key(row)], row)
        request = PoolCutoverRequest(PoolRoleFenceRequest(PoolRetirementRequest(migration,
            inputs.actuators, inputs.collectors, inputs.dormant_consumers), inputs.roles), predecessor.active, inputs.services,
            inputs.collector_config, inputs.profiles, "https://" + predecessor.deployment.public_host,
            "https://kubernetes.default.svc", inputs.collector_credential, inputs.platform_authority, application_delivery,
            inputs.platform_consumers)
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


@dataclass(frozen=True, repr=False)
class PoolCutoverEntryChecks:
    """Requalify the same protected scope at each parent's preflight barrier."""

    context: PoolCutoverContext
    readers: ConnectedPoolReaders
    refresh: PoolManagerRefresh | None = None

    def current(self) -> dict[str, str] | None:
        try:
            context, readers = self.context, self.readers
            migration = context.request.fencing.retirement.migration
            if (load_pool_cutover_inputs(context.operation) != context
                    or readers.guards.request != migration
                    or readers.base.api_server.rstrip("/") != context.original.original_inputs.operator_connection.endpoint.rstrip("/")):
                raise ValueError
            if self.refresh is not None and self.refresh.qualify().context != context:
                raise ValueError
            readers.history.qualify_binding(migration, context.request.manager)
            application = context.request.application_delivery
            if application is None:
                return None
            shared = application.before.installation.applications
            if shared is None:
                raise ValueError
            namespace = shared.shared.platform_namespace
            guard, = (row for row in migration.guards if row.namespace == namespace)
            source = readers.base._request('GET', '/api/v1/namespaces/' + namespace + '/secrets/loom-platform-storage')
            if source is None:
                raise ValueError
            return qualify_application_source_material(before=application.before, controller=guard.controller,
                secret=source, pin=application.source_credential)
        except Exception:
            raise EntryError("pool cutover connected scope unqualified") from None

    def preflight(self, request: PoolCutoverRequest) -> None:
        try:
            if request != self.context.request:
                raise ValueError
            self.current()
            qualify_pool_runtime_databases(self.context, self.readers.guards)
            if self.refresh is None:
                qualify_pool_manager_database(self.context, self.readers.history)
            else:
                qualify_pool_manager_database(self.context, self.readers.history, refresh=self.refresh)
            qualify_pool_provider(self.context, self.readers.base)
            self.current()
        except Exception:
            raise EntryError("pool cutover connected prerequisites unqualified") from None

    def qualify_initial_capacity(self, request: PoolCutoverRequest) -> None:
        """New delivery must fit; retained recovery must not require spare space."""
        try:
            if request != self.context.request:
                raise ValueError
            self.current()
            application = request.application_delivery
            if application is not None and self.refresh is None:
                deployment = derive_application_build_deployment(application.before,
                    request.fencing.retirement.migration.registration.spec)
                upgrade = self.context.original.upgrade
                fit = replace(upgrade, setup=replace(upgrade.setup, deployment=deployment,
                    candidate=self.context.inputs.candidate, profile=self.context.inputs.profile))
                ApplicationUpgradePrerequisites(base=self.readers.base,
                    settings=self.context.original.inputs.prerequisites).platform_capacity(fit)
            self.current()
        except Exception:
            raise EntryError("pool cutover initial capacity unqualified") from None

    def qualify_quiescence(self) -> None:
        # The HTTPS parent independently checks schema, retired application
        # access and origin history through the fixed READ ONLY database pages.
        # Keep the physical/runtime/private bindings current at that same barrier.
        self.preflight(self.context.request)


@dataclass(frozen=True, repr=False)
class _ConnectedPoolMigration:
    checks: PoolCutoverEntryChecks
    registration: HTTPSPoolRegistrationAPI
    qualify: Callable[[], None]

    def preflight(self, request: PoolMigrationRequest) -> None:
        if request != self.checks.context.request.fencing.retirement.migration:
            raise EntryError("pool cutover migration scope differs")
        self.qualify()

    def guard(self, target: PoolGuardTarget, action: str) -> dict[str, Any]:
        self.checks.current()
        migration = self.checks.context.request.fencing.retirement.migration
        if target not in migration.guards or action not in {"observe", "acquire"}:
            raise EntryError("pool cutover guard scope differs")
        return self.checks.readers.guards.guard(target, action)

    def register(self, state_dir: Path) -> dict[str, Any] | None:
        self.checks.current()
        context = self.checks.context
        migration = context.request.fencing.retirement.migration
        if (state_dir != Path(context.operation["state_dir"]) / "writers" / "registration"
                or self.registration.request != migration.registration
                or any(self.guard(target, "observe").get("status") != "held" for target in migration.guards)):
            raise EntryError("pool cutover registration scope unqualified")
        stage_pool_registration(request=migration.registration, api=self.registration, state_dir=state_dir)
        # A created Job is not a committed registration receipt. The existing
        # proof reader checks its actual Pod, successful exit and bounded log.
        return self.registration.registration_report(state_dir)


def _runtime_workload_options(context: PoolCutoverContext) -> tuple[dict[str, tuple[dict[str, Any], ...]], bool]:
    state, anchor = Path(context.operation["state_dir"]), Path(context.operation["anchor_dir"])
    successors = startup_workload_options(context.request, state_dir=state, anchor_dir=anchor)
    if successors is not None:
        return successors, True
    return {key: (row,) for key, row in retained_cutover_workloads(context.request,
        state_dir=state, anchor_dir=anchor).items()}, False


def qualify_pool_runtime_databases(context: PoolCutoverContext, guards: KubectlPoolGuardAPI) -> None:
    """Bind retained consumers to their database and running actuator telemetry.

    Fresh operations require original running Pods. Recovery instead selects
    exact journaled templates and checks their retained database references.
    Anchored startup recovery accepts either side of an uncertain CAS without
    requiring successor readiness; runtime acceptance is a separate later gate.
    """
    try:
        migration = context.request.fencing.retirement.migration
        if guards.request != migration or load_pool_cutover_inputs(context.operation) != context:
            raise ValueError
        expected, startup = _runtime_workload_options(context)
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
                current = guards._get("deployment", name, namespace)
                desired, = (row for row in expected[_key(original)] if _matches(current, row, _uid(original)))
                actuator = namespace != target.namespace
                uid = database.actuator_credential_uid if actuator else database.credential_uid
                version = database.actuator_credential_resource_version if actuator else database.credential_resource_version
                if desired["spec"]["replicas"] == 1 and not startup:
                    guards.qualify_runtime_database(target, original=original,
                        credential_uid=uid, credential_resource_version=version)
                    if actuator:
                        guards.qualify_runtime_telemetry(target, original=original)
                else:
                    if type(desired["spec"]["replicas"]) is not int or desired["spec"]["replicas"] not in ({0, 1} if startup else {0}):
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
        if (_runtime_workload_options(context) != (expected, startup)
                or load_pool_cutover_inputs(context.operation) != context):
            raise ValueError
    except PoolMigrationError as error:
        if error.stage in {'runtime_telemetry_' + stage for stage in TELEMETRY_FAILURE_STAGES}:
            detail = error.stage.removeprefix('runtime_telemetry_')
            raise EntryError('pool cutover runtime telemetry ' + detail + ' unqualified') from None
        surface = "telemetry" if error.stage == "runtime_telemetry" else "databases"
        raise EntryError("pool cutover runtime " + surface + " unqualified") from None
    except Exception:
        raise EntryError("pool cutover runtime databases unqualified") from None


def qualify_pool_manager_database(context: PoolCutoverContext, history: KubectlPoolHistoryAPI, *,
                                  refresh: PoolManagerRefresh | None = None) -> None:
    """Use the predecessor's management binding, never a participant credential."""
    try:
        history.qualify_binding(context.request.fencing.retirement.migration, context.request.manager)
        if load_pool_cutover_inputs(context.operation) != context:
            raise ValueError
        if refresh is not None and refresh.qualify().context != context:
            raise ValueError
        expected, startup = (_runtime_workload_options(context) if refresh is None else (refresh.workload_options(), True))
        original, target = context.request.manager, history.target
        current = history._get("deployment", "loom-service", target.namespace)
        desired, = (row for row in expected[_key(original)] if _matches(current, row, _uid(original)))
        if desired["spec"]["replicas"] == 1 and not startup:
            history.qualify_manager_database()
        else:
            if type(desired["spec"]["replicas"]) is not int or desired["spec"]["replicas"] not in ({0, 1} if startup else {0}):
                raise ValueError
            backend = history._database(target, url_variable="LOOM_SVC_DB_URL")
            binding = target.database
            reference = original if refresh is None else desired
            url = history._workload_database_url(reference, url_variable="LOOM_SVC_DB_URL",
                credential_uid=binding.credential_uid, credential_resource_version=binding.credential_resource_version)
            qualify_database_destination(url, target.namespace)
            if (_uid(history._database(target, url_variable="LOOM_SVC_DB_URL")) != _uid(backend)
                    or history._workload_database_url(reference, url_variable="LOOM_SVC_DB_URL",
                        credential_uid=binding.credential_uid, credential_resource_version=binding.credential_resource_version) != url
                    or not _matches(history._get("deployment", "loom-service", target.namespace), desired, _uid(original))):
                raise ValueError
        current_options = _runtime_workload_options(context) if refresh is None else (refresh.workload_options(), True)
        if (current_options != (expected, startup)
                or load_pool_cutover_inputs(context.operation) != context):
            raise ValueError
    except Exception:
        raise EntryError("pool cutover management database unqualified") from None


def _collector_cloud_credential(context: PoolCutoverContext, base: HTTPSManagementPrerequisites) -> bytes:
    """Read only the existing, hash-bound collector Secret; never an operator key."""
    namespace = context.request.collector_config["metadata"]["namespace"]
    name = "loom-execution-capacity-collector-nebius"
    secret = base._request("GET", "/api/v1/namespaces/" + namespace + "/secrets/" + name)
    pin = context.request.collector_credential
    if (secret is None or secret.get("apiVersion") != "v1" or secret.get("kind") != "Secret"
            or secret.get("type") != "Opaque" or secret.get("stringData")
            or secret["metadata"].get("deletionTimestamp") is not None
            or (secret["metadata"].get("namespace"), secret["metadata"].get("name"),
                secret["metadata"].get("uid"), secret["metadata"].get("resourceVersion")) !=
                (namespace, name, str(pin.uid), pin.resource_version)
            or set(secret["data"]) != {"credentials.json"}):
        raise ValueError
    encoded = secret["data"]["credentials.json"]
    if not isinstance(encoded, str) or not 0 < len(encoded) <= 4 * ((1024**2 + 2) // 3):
        raise ValueError
    value = base64.b64decode(encoded, validate=True)
    if not 0 < len(value) <= 1024**2 or hashlib.sha256(value).hexdigest() != pin.sha256:
        raise ValueError
    return value


def qualify_pool_provider(context: PoolCutoverContext, base: HTTPSManagementPrerequisites) -> None:
    """Qualify the actual collector principal, physical group and quota scope.

    This read neither requests nodes nor reserves a share of provider headroom.
    A scale-zero group is valid; actual workload fit/telemetry belongs to startup.
    A private temporary file carries only the existing pinned collector key; it
    is erased on success or failure. Operator credentials cannot prove this gate.
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
        values: dict[str, Any] = {}
        for field, definition in NebiusCapacitySourceSettings.model_fields.items():
            key = "LOOM_EXECUTION_CAPACITY_COLLECTOR_" + field.upper()
            if field == "nebius_credentials_file":
                continue  # Supplied explicitly from the pinned Secret below.
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
        # Validate configuration before any credential is read or exchanged.
        settings = NebiusCapacitySourceSettings(_env_file=None,
            nebius_credentials_file=Path("/var/run/loom-owned/credentials/nebius-credentials.json"), **values)
        if ((settings.nebius_project_id, settings.nebius_quota_parent_id, settings.nebius_region,
                settings.nebius_node_group_id, spec.cluster_id) != (
                    foundation["project_id"], foundation["quota_parent_id"], foundation["region"],
                    foundation["execution_node_group_id"], foundation["cluster_id"])
                or spec.node_group_id != settings.nebius_node_group_id):
            raise ValueError

        async def observe(settings: NebiusCapacitySourceSettings) -> None:
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

        before = _collector_cloud_credential(context, base)
        directory = Path(context.operation["inputs_path"]).parent
        private_state._private_directory(directory)
        with TemporaryDirectory(prefix=".pool-collector-", dir=directory) as temporary:
            credential = Path(temporary) / "nebius-credentials.json"
            private_state._write_private(credential, before)
            settings = settings.model_copy(update={"nebius_credentials_file": credential})
            asyncio.run(observe(settings))
            if _private(credential, 1024**2) != before:
                raise ValueError
        current = base._request("GET", path)
        if (_collector_cloud_credential(context, base) != before
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
                    for row in inputs.installation.profiles.task_images)
                or any(row.settings.service_image != runtime.task_image_ref
                    for row in inputs.installation.profiles.application_images)):
            raise ValueError
    except Exception:
        raise EntryError("pool cutover publication unqualified") from None


async def _connected_publication(context: PoolCutoverContext) -> None:
    async with asyncio.timeout(180):
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_pool_publication(context, http)


@contextmanager
def connected_pool_readers(context: PoolCutoverContext, *, refresh: PoolManagerRefresh | None = None) -> Iterator[ConnectedPoolReaders]:
    """One qualified native authority for HTTPS and fixed SQL transports.

    Copy only its current bounded bearer and pinned CA into a fresh private
    kubeconfig. No exec plugin, client certificate, ambient context, proxy, CA
    fallback or credential from the ingress kubeconfig is inherited. The existing
    readers hash that exact file before operations. It is erased on exit, including
    failures; recovery obtains a fresh token instead of journaling a credential.
    """
    if load_pool_cutover_inputs(context.operation) != context:
        raise EntryError("pool cutover context changed before connection")
    if refresh is not None and refresh.qualify().context != context:
        raise EntryError("pool refresh context differs before connection")
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
                if refresh is None:
                    qualify_pool_manager_database(context, history)
                else:
                    qualify_pool_manager_database(context, history, refresh=refresh)
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


@contextmanager
def connected_pool_api(context: PoolCutoverContext, *, refresh: PoolManagerRefresh | None = None) -> Iterator[HTTPSPoolCutoverAPI]:
    """Compose the fixed parent and children with one qualified operator scope.

    The protected handler must call the journaled parent; this factory creates
    no cluster resource and opens no intake. No injectable operator commands,
    checks, registration targets or alternative state directories are accepted.
    Credentials and all child HTTP clients expire with this context.
    """
    connection = connected_pool_readers(context) if refresh is None else connected_pool_readers(context, refresh=refresh)
    with connection as readers:
        checks = PoolCutoverEntryChecks(context, readers, refresh)
        source_credentials = checks.current()
        with HTTPSPoolRegistrationAPI(request=context.request.fencing.retirement.migration.registration,
                api_server=readers.base.api_server, ssl_context=readers.ssl_context, token=readers.token) as registration:
            migration = _ConnectedPoolMigration(checks, registration, lambda: api.preflight(context.request))
            with HTTPSPoolCutoverAPI(request=context.request, tokens=context.tokens,
                    migration=migration, guards=readers.guards, checks=checks, history=readers.history,
                    api_server=readers.base.api_server, ssl_context=readers.ssl_context, token=readers.token,
                    state_dir=Path(context.operation["state_dir"]), anchor_dir=Path(context.operation["anchor_dir"]),
                    refresh=refresh, source_credentials=source_credentials) as api:
                yield api


@contextmanager
def connected_pool_startup_api(context: PoolCutoverContext) -> Iterator[HTTPSPoolStartupAPI]:
    """Continue the exact closed parent, with the same private authority lifetime.

    Construction performs no mutation or admission opening. The journaled
    startup stage owns ordering; this is not an independently exposed command.
    """
    with connected_pool_api(context) as parent:
        yield HTTPSPoolStartupAPI(parent=parent)


@contextmanager
def connected_pool_activation_api(context: PoolCutoverContext) -> Iterator[HTTPSPoolActivationAPI]:
    """Use the exact parent's credential lifetime for journaled activation/recovery.

    Construction performs no mutation or runtime-health check. Only the anchored
    stage may dispatch activation; no standalone operational command is exposed.
    """
    with connected_pool_api(context) as parent:
        yield HTTPSPoolActivationAPI(parent=parent)


def execute_pool_cutover(context: PoolCutoverContext, action: str) -> dict[str, Any]:
    """Bind the complete fixed direction to freshly qualified private inputs."""
    if action not in {'preflight', 'install', 'rollback'} or load_pool_cutover_inputs(context.operation) != context:
        raise EntryError('pool operation private binding differs')
    try:
        with connected_pool_api(context) as parent:
            result = run_pool_operation(parent=parent, tokens=context.tokens, action=action)
            return {**result, 'telemetry': parent.guards.telemetry_report()}
    except PoolOperationError:
        raise
    except EntryError as error:
        # Only exact, locally authored prerequisite codes cross the gateway.
        # Unknown errors remain coarse; no exception text or provider payload
        # becomes an operational diagnostic or permission to retry a write.
        raise PoolOperationError(_CONNECTION_ERRORS.get(str(error), 'connection')) from None
    except Exception:
        raise PoolOperationError('connection') from None
