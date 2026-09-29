"""Exact private inputs for a repeatable, protected retained-manager refresh."""
from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from scripts.ops.nebius_application_upgrade_prerequisites import (
    ApplicationUpgradePrerequisites,
    UpgradePrerequisiteSettings,
)
from scripts.ops.nebius_management_entry import EntryError, _private, connected_checks
from scripts.ops.nebius_management_gateway import validate_operation
from scripts.ops.nebius_management_refresh import ManagementRefreshRenderRequest, render_refresh
from scripts.ops.nebius_management_refresh_connected import HTTPSManagementRefreshInstaller
from scripts.ops.nebius_management_refresh_install import (
    ManagementRefreshInstallError,
    ManagementRefreshInstallRequest,
    refresh_management,
)
from scripts.ops.nebius_management_refresh_predecessor import (
    CompletedRefresh,
    CompletedUpgrade,
    RefreshPredecessorV1,
    UpgradePredecessorV1,
    load_completed_refresh,
    load_completed_upgrade,
)
from scripts.ops.nebius_management_refresh_resources import ManagementRefreshResourcesRequest
from scripts.ops.nebius_management_refresh_switch import ManagementRefreshSwitchRequest

from loom_service.environment_management.deployment import ManagementDeployment


class RefreshPrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    schema_version: Literal['loom.nebius-management-refresh-private-inputs.v1']
    original_upgrade: UpgradePredecessorV1
    predecessor: Annotated[UpgradePredecessorV1 | RefreshPredecessorV1, Field(discriminator='kind')]
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    manager_revision: str = Field(pattern=r'^[a-zA-Z0-9_]{1,64}$')
    target_manager_revision: str = Field(pattern=r'^[a-zA-Z0-9_]{1,64}$')
    prerequisites: UpgradePrerequisiteSettings
    foundation_candidate: str = Field(pattern=r'^[0-9a-f]{40}$')


@dataclass(frozen=True, repr=False)
class RefreshContext:
    inputs: RefreshPrivateInputs
    original: CompletedUpgrade
    predecessor: CompletedUpgrade | CompletedRefresh
    request: ManagementRefreshInstallRequest


def load_refresh_inputs(operation: dict[str, Any]) -> RefreshContext:
    try:
        validate_operation(operation)
        if operation['schema'] != 'loom.nebius-management-refresh-operation.v1':
            raise ValueError
        path = Path(operation['inputs_path'])
        raw = _private(path, 4 * 1024**2)
        if hashlib.sha256(raw).hexdigest() != operation['inputs_sha256']:
            raise ValueError
        inputs = RefreshPrivateInputs.model_validate_json(raw)
        original = load_completed_upgrade(inputs.original_upgrade)
        setup = original.upgrade.setup
        root = Path(inputs.original_upgrade.operation['inputs_path']).parent.parent
        operation_id = UUID(operation['operation_id'])
        if (path != root / 'refresh' / str(operation_id) / 'inputs.json'
                or (setup.binding.installation_id, setup.binding.namespace) != (operation['installation_id'], operation['namespace'])
                or inputs.candidate.get('candidate_sha') != operation['candidate']
                or inputs.profile.get('candidate_sha') != operation['candidate']):
            raise ValueError
        predecessor: CompletedUpgrade | CompletedRefresh
        if isinstance(inputs.predecessor, UpgradePredecessorV1):
            if inputs.predecessor != inputs.original_upgrade:
                raise ValueError
            predecessor = original
        else:
            if inputs.predecessor.operation_id == operation_id:
                raise ValueError
            predecessor = load_completed_refresh(inputs.predecessor, original=original)
        render = ManagementRefreshRenderRequest(predecessor.deployment, inputs.deployment, predecessor.active,
            inputs.candidate, inputs.profile, Path(__file__).resolve().parents[2])
        render_refresh(render)
        resources = ManagementRefreshResourcesRequest(ManagementRefreshSwitchRequest(render, operation_id),
            setup.binding, setup.shared_namespace_uid, inputs.manager_revision, inputs.target_manager_revision)
        request = ManagementRefreshInstallRequest(resources, {**predecessor.history, path: operation['inputs_sha256']},
            original.upgrade.original_anchor)
        return RefreshContext(inputs, original, predecessor, request)
    except Exception:
        raise EntryError('management private refresh inputs unqualified') from None


@contextmanager
def connected_refresh_api(context: RefreshContext, operation: dict[str, Any]) -> Iterator[HTTPSManagementRefreshInstaller]:
    original = context.original
    with connected_checks(original.original_inputs, original.ingress,
            foundation_candidate=context.inputs.foundation_candidate) as (base, trust, token):
        connection = original.original_inputs.operator_connection
        checks = ApplicationUpgradePrerequisites(base=base, settings=context.inputs.prerequisites)
        with HTTPSManagementRefreshInstaller(request=context.request, original=original, predecessor=context.predecessor,
            state_dir=Path(operation['state_dir']), api_server=connection.endpoint, ssl_context=trust, token=token,
            runtime_ca_pem=_private(connection.ca_file, 1024**2).decode(), checks=checks) as api:
            yield api


def execute_refresh(context: RefreshContext, operation: dict[str, Any], action: str) -> dict[str, Any]:
    if action not in {'preflight', 'install'}:
        raise EntryError('refresh action outside fixed authority')
    api: HTTPSManagementRefreshInstaller | None = None
    try:
        with connected_refresh_api(context, operation) as connected:
            api = connected
            if action == 'preflight':
                api.preflight(context.request)
                return {'status': 'preflight_qualified', 'operation_id': operation['operation_id']}
            return refresh_management(request=context.request, api=api,
                state_dir=Path(operation['state_dir']), anchor_dir=Path(operation['anchor_dir']))
    except Exception as error:
        stage = getattr(error, 'stage', 'connection')
        if stage == 'prerequisites' and api is not None:
            stage = api.diagnostic_stage or stage
        if not isinstance(stage, str):
            stage = 'connection'
        raise ManagementRefreshInstallError('refresh_' + stage.replace('-', '_')) from None
