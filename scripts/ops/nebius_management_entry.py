"""Private fixed management entrypoint; no credentials or manifests from Actions."""
from __future__ import annotations

import asyncio
import hashlib
import json
import ssl
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import fields
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

import loom_bundle_checksum  # noqa: F401 -- qualify the installed first-party wheel
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_application_setup import (
    ApplicationSetupMaterial,
    ApplicationSetupRequest,
    _documents,
)
from scripts.ops.nebius_application_upgrade_prerequisites import (
    ApplicationUpgradePrerequisites,
    UpgradePrerequisiteSettings,
)
from scripts.ops.nebius_ingress_bootstrap import validate_config
from scripts.ops.nebius_ingress_gateway import TLSBinding
from scripts.ops.nebius_ingress_operation import LiveIngressAPI
from scripts.ops.nebius_management_bootstrap import BootstrapBinding
from scripts.ops.nebius_management_gateway import (
    DIAGNOSTIC_STAGES,
    safe_report,
    validate_action,
    validate_operation,
)
from scripts.ops.nebius_management_install import (
    ManagementInstallRequest,
    install_management,
    render_installation,
)
from scripts.ops.nebius_management_live import HTTPSManagementInstallationAPI
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_prerequisites import (
    HTTPSManagementPrerequisites,
    ManagementPrerequisiteSettings,
)
from scripts.ops.nebius_management_supplied import _KEYS
from scripts.ops.nebius_management_upgrade import (
    ManagementUpgradeRequest,
    _original,
    upgrade_management,
)
from scripts.ops.nebius_management_upgrade_live import HTTPSManagementUpgradeAPI

from loom.nebius_kubernetes import NebiusKubernetesConnection, NebiusKubernetesCredentials
from loom_service.environment_management.candidates import _json
from loom_service.environment_management.deployment import ManagementDeployment


class EntryError(RuntimeError):
    """Private input values and paths are never diagnostics."""


class PrivateInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)
    schema_version: Literal["loom.nebius-management-private-inputs.v1"]
    binding: BootstrapBinding
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    prerequisites: ManagementPrerequisiteSettings
    operator_connection: NebiusKubernetesConnection
    operator_cloud_credentials: Path
    ingress_config: Path
    foundation_candidate: str = Field(pattern=r"^[0-9a-f]{40}$")
    material_files: dict[str, dict[str, Path]]


class UpgradePrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.nebius-management-upgrade-private-inputs.v1']
    original_operation: dict[str, Any]
    binding: ManagementBinding
    shared_namespace_uid: UUID
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    prerequisites: UpgradePrerequisiteSettings
    foundation_candidate: str = Field(pattern=r'^[0-9a-f]{40}$')
    material_files: dict[str, Path]


def _private(path: Path, limit: int) -> bytes:
    if not path.is_absolute() or path != path.resolve():
        raise EntryError("private management input unavailable")
    return private_state._private_read(path, limit=limit)


def load_inputs(operation: dict[str, Any]) -> tuple[PrivateInputs, ManagementInstallRequest, dict[str, Any]]:
    try:
        validate_operation(operation)
        raw = _private(Path(operation["inputs_path"]), 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation["inputs_sha256"]:
            raise ValueError()
        inputs = PrivateInputs.model_validate(_json(raw))
        if ((inputs.binding.installation_id, inputs.binding.namespace) != (operation["installation_id"], operation["namespace"])
                or inputs.candidate.get("candidate_sha") != operation["candidate"]
                or inputs.profile.get("candidate_sha") != operation["candidate"]
                or inputs.material_files.keys() != _KEYS.keys()):
            raise ValueError()
        connection = inputs.operator_connection
        operator_files = {connection.ca_file, connection.credentials_file, inputs.operator_cloud_credentials,
                          inputs.ingress_config, Path(operation["inputs_path"])}
        material: dict[str, dict[str, str]] = {}
        seen: set[Path] = set()
        for name, keys in _KEYS.items():
            selected = inputs.material_files[name]
            if selected.keys() != keys:
                raise ValueError()
            material[name] = {}
            for key, path in selected.items():
                if path in operator_files or path in seen:
                    raise ValueError()
                seen.add(path)
                value = _private(path, 65536).decode()
                if not value:
                    raise ValueError()
                material[name][key] = value
        request = ManagementInstallRequest(binding=inputs.binding, deployment=inputs.deployment,
                                           candidate=inputs.candidate, profile=inputs.profile, material=material)
        render_installation(request)
        config = inputs.deployment.installation.foundation.platform_config
        ingress = _json(_private(inputs.ingress_config, 16384))
        validate_config(ingress)
        if (connection.endpoint != config["kubernetes_api_server"].rstrip("/")
                or ingress["api_server"].rstrip("/") != connection.endpoint
                or ingress["cluster_id"] != config["cluster_id"]):
            raise ValueError()
        for path in (connection.ca_file, connection.credentials_file, inputs.operator_cloud_credentials):
            _private(path, 1024**2)
        return inputs, request, ingress
    except Exception:
        raise EntryError("management private installation inputs unqualified") from None


def load_upgrade_inputs(operation: dict[str, Any]) -> tuple[UpgradePrivateInputs, ManagementUpgradeRequest, PrivateInputs, dict[str, Any]]:
    try:
        validate_operation(operation)
        if operation['schema'] != 'loom.nebius-management-upgrade-operation.v1':
            raise ValueError
        raw = _private(Path(operation['inputs_path']), 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
            raise ValueError
        inputs = UpgradePrivateInputs.model_validate(_json(raw))
        old = inputs.original_operation
        validate_operation(old)
        root = Path(operation['inputs_path']).parent.parent
        if (old['schema'] != 'loom.nebius-management-operation.v1' or Path(old['inputs_path']) != root / 'inputs.json'
                or any(old[key] != operation[key] for key in ('installation_id', 'namespace'))
                or (inputs.binding.installation_id, inputs.binding.namespace) != (operation['installation_id'], operation['namespace'])
                or inputs.candidate.get('candidate_sha') != operation['candidate']
                or inputs.profile.get('candidate_sha') != operation['candidate']):
            raise ValueError
        original_inputs, original, ingress = load_inputs(old)
        operator_files = {original_inputs.operator_connection.ca_file, original_inputs.operator_connection.credentials_file,
            original_inputs.operator_cloud_credentials, original_inputs.ingress_config, Path(old['inputs_path']),
            Path(operation['inputs_path'])}
        names = {field.name for field in fields(ApplicationSetupMaterial)}
        if (set(inputs.material_files) != names or len(set(inputs.material_files.values())) != len(names)
                or operator_files & set(inputs.material_files.values())):
            raise ValueError
        material = ApplicationSetupMaterial(**{name: _private(path, 65536).decode()
            for name, path in inputs.material_files.items()})
        setup = ApplicationSetupRequest(deployment=inputs.deployment, candidate=inputs.candidate, profile=inputs.profile,
            binding=inputs.binding, shared_namespace_uid=str(inputs.shared_namespace_uid),
            repo_root=Path(__file__).resolve().parents[2], material=material)
        _documents(setup, 'material')  # Validate the exact fixed consumer material without writes.
        request = ManagementUpgradeRequest(original=original, setup=setup,
            original_state=Path(old['state_dir']), original_anchor=Path(old['anchor_dir']))
        _original(request)  # An incomplete/lost bootstrap is never an upgrade.
        return inputs, request, original_inputs, ingress
    except Exception:
        raise EntryError('management private upgrade inputs unqualified') from None


async def _operator_transport(connection: NebiusKubernetesConnection) -> tuple[ssl.SSLContext, str]:
    credentials = NebiusKubernetesCredentials(connection)
    try:
        return credentials.ssl_context, await credentials.get_token()
    finally:
        await credentials.close()


@contextmanager
def connected_checks(inputs: PrivateInputs, ingress: dict[str, Any], *,
        foundation_candidate: str | None = None) -> Iterator[tuple[HTTPSManagementPrerequisites, ssl.SSLContext, str]]:
    # Obtain a bounded operator bearer token through its explicit SDK; never use
    # it in runtime subject checks or copy the credential into any workload.
    connection = inputs.operator_connection
    context, token = asyncio.run(_operator_transport(connection))
    installed_ingress = LiveIngressAPI(Path(ingress["kubeconfig"]), binding=TLSBinding(**ingress["binding"]),
        executable=Path(ingress["kubectl"]),
        candidate=inputs.foundation_candidate if foundation_candidate is None else foundation_candidate,
        cluster_id=ingress["cluster_id"], api_server=ingress["api_server"],
        ingress_class=ingress["ingress_class"], image=ingress["image"])
    certificate = private_state.load_installation(Path(ingress["certificate_config"]))
    with HTTPSManagementPrerequisites(settings=inputs.prerequisites, ingress=installed_ingress,
        certificate_config=certificate, ingress_state=Path(ingress["state_dir"]),
        operator_cloud_credentials=inputs.operator_cloud_credentials, api_server=connection.endpoint,
        ssl_context=context, token=token) as checks:
        yield checks, context, token


@contextmanager
def connected_api(inputs: PrivateInputs, request: ManagementInstallRequest,
                  ingress: dict[str, Any]) -> Iterator[HTTPSManagementInstallationAPI]:
    with connected_checks(inputs, ingress) as (checks, context, token):
        connection = inputs.operator_connection
        yield HTTPSManagementInstallationAPI(request=request, api_server=connection.endpoint,
            ssl_context=context, token=token, runtime_ca_pem=_private(connection.ca_file, 1024**2).decode(), checks=checks)


@contextmanager
def connected_upgrade_api(inputs: UpgradePrivateInputs, request: ManagementUpgradeRequest,
        original_inputs: PrivateInputs, ingress: dict[str, Any]) -> Iterator[HTTPSManagementUpgradeAPI]:
    with connected_checks(original_inputs, ingress, foundation_candidate=inputs.foundation_candidate) as (base, context, token):
        connection = original_inputs.operator_connection
        checks = ApplicationUpgradePrerequisites(base=base, settings=inputs.prerequisites)
        with HTTPSManagementUpgradeAPI(request=request, api_server=connection.endpoint, ssl_context=context,
            token=token, runtime_ca_pem=_private(connection.ca_file, 1024**2).decode(), checks=checks) as api:
            yield api


def main(operation_path: str, action: str) -> int:
    qualified: dict[str, Any] | None = None
    api: HTTPSManagementInstallationAPI | HTTPSManagementUpgradeAPI | None = None
    try:
        if action not in {"qualify", "preflight", "install", "rollback"}:
            raise ValueError()
        operation = _json(_private(Path(operation_path), 16384))
        validate_action(action, operation)
        if action == "qualify":
            if operation['schema'] == 'loom.nebius-management-refresh-operation.v1':
                # Refresh has deferred pool/rollback readers. Qualify the real
                # dependency closure before marking this tooling installation
                # complete, without opening private inputs or any transport.
                from scripts.ops.nebius_management_refresh_entry import load_refresh_inputs

            elif operation['schema'] == 'loom.nebius-pool-cutover-operation.v1':
                from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs

            elif operation['schema'] == 'loom.nebius-pool-startup-repair-operation.v1':
                from scripts.ops.nebius_pool_repair_entry import load_pool_repair_inputs

            print(json.dumps({"status": "tooling_qualified"}))
            return 0
        result: dict[str, Any]
        if operation['schema'] == 'loom.nebius-pool-startup-repair-operation.v1':
            from scripts.ops.nebius_pool_repair_entry import (
                execute_pool_repair,
                load_pool_repair_inputs,
            )

            repair = load_pool_repair_inputs(operation)
            qualified = operation
            result = execute_pool_repair(repair, action)
        elif operation['schema'] == 'loom.nebius-pool-cutover-operation.v1':
            from scripts.ops.nebius_pool_cutover_entry import (
                execute_pool_cutover,
                load_pool_cutover_inputs,
            )

            pool = load_pool_cutover_inputs(operation)
            qualified = operation
            result = execute_pool_cutover(pool, action)
        elif operation['schema'] == 'loom.nebius-management-refresh-operation.v1':
            from scripts.ops.nebius_management_refresh_entry import (
                execute_refresh,
                load_refresh_inputs,
            )

            refresh = load_refresh_inputs(operation)
            qualified = operation
            result = execute_refresh(refresh, operation, action)
        elif operation['schema'] == 'loom.nebius-management-retirement-recovery-operation.v1':
            from scripts.ops.nebius_management_retirement_recovery_entry import (
                execute_recovery,
                load_recovery_inputs,
            )

            recovery = load_recovery_inputs(operation)
            qualified = operation
            result = execute_recovery(recovery, operation, action)
        elif operation['schema'] == 'loom.nebius-management-retirement-diagnostic-operation.v1':
            from scripts.ops.nebius_management_retirement_diagnostic_entry import (
                execute_diagnostic,
                load_diagnostic_inputs,
            )

            diagnostic = load_diagnostic_inputs(operation)
            qualified = operation
            result = execute_diagnostic(diagnostic, operation, action)
        elif operation['schema'] == 'loom.nebius-management-retirement-operation.v1':
            from scripts.ops.nebius_management_retirement_entry import (
                execute_retirement,
                load_retirement_inputs,
            )

            retirement = load_retirement_inputs(operation)
            qualified = operation
            result = execute_retirement(retirement, operation, action)
        elif operation['schema'] == 'loom.nebius-management-upgrade-operation.v1':
            upgrade_inputs, upgrade_request, original_inputs, ingress = load_upgrade_inputs(operation)
            qualified = operation
            with connected_upgrade_api(upgrade_inputs, upgrade_request, original_inputs, ingress) as upgrade_api:
                api = upgrade_api
                if action == 'preflight':
                    upgrade_api.preflight(upgrade_request)
                    result = {'status': 'preflight_qualified'}
                else:
                    result = upgrade_management(request=upgrade_request, api=upgrade_api,
                        state_dir=Path(operation['state_dir']), anchor_dir=Path(operation['anchor_dir']))
        else:
            inputs, request, ingress = load_inputs(operation)
            qualified = operation
            with connected_api(inputs, request, ingress) as installation_api:
                api = installation_api
                if action == "preflight":
                    installation_api.preflight(request, render_installation(request))
                    result = {"status": "preflight_qualified"}
                else:
                    result = install_management(request=request, api=installation_api, state_dir=Path(operation["state_dir"]),
                                                anchor_dir=Path(operation["anchor_dir"]))
        report = {**result, **{key: operation[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}}
        print(json.dumps(safe_report(json.dumps(report).encode(), operation), sort_keys=True))
        return 0
    except Exception as error:
        if qualified is not None:
            stage = getattr(error, "stage", None)
            if stage is None:
                stage = getattr(api, "diagnostic_stage", None) if api is not None else "connection"
            if stage == "prerequisites" and api is not None:
                stage = getattr(api.checks, "diagnostic_stage", None)
            if not isinstance(stage, str) or stage not in DIAGNOSTIC_STAGES:
                stage = "operation"
            failure = {"status": "blocked", "stage": stage,
                       **{key: qualified[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}}
            if qualified['schema'] in {'loom.nebius-management-refresh-operation.v1', 'loom.nebius-pool-cutover-operation.v1',
                    'loom.nebius-pool-startup-repair-operation.v1'}:
                failure['operation_id'] = qualified['operation_id']
            if qualified['schema'] == 'loom.nebius-pool-startup-repair-operation.v1':
                failure['original_operation_id'] = qualified['original_operation_id']
            if qualified['schema'] == 'loom.nebius-management-refresh-operation.v1':
                capacity = getattr(error, 'capacity', None)
                if stage == 'refresh_platform_capacity' and isinstance(capacity, dict):
                    failure['capacity'] = capacity
            print(json.dumps(safe_report(json.dumps(failure).encode(), qualified), sort_keys=True))
            # Zero here means a bound protocol response was delivered. Only the
            # outer rollout CLI decides success, and blocked always exits one.
            return 0
        print(json.dumps({"status": "blocked", "reason": "management operation incomplete; retain private recovery state"}))
        return 1
