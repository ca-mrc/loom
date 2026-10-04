"""Exact private inputs for a repeatable, protected retained-manager refresh."""
from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
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
from scripts.ops.nebius_management_gateway import (
    GatewayError,
    validate_capacity_report,
    validate_operation,
)
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
from scripts.ops.nebius_management_refresh_supersession import (
    FailedRefreshProof,
    SupersededRefreshV1,
    load_failed_refresh,
)
from scripts.ops.nebius_management_refresh_switch import ManagementRefreshSwitchRequest
from scripts.ops.nebius_pool_predecessor import (
    CompletedPoolCutover,
    PoolPredecessorV1,
    load_completed_pool,
)

from loom_service.environment_management.deployment import ManagementDeployment


class RefreshPrivateInputs(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)

    schema_version: Literal['loom.nebius-management-refresh-private-inputs.v1']
    original_upgrade: UpgradePredecessorV1
    predecessor: Annotated[UpgradePredecessorV1 | RefreshPredecessorV1 | PoolPredecessorV1, Field(discriminator='kind')]
    deployment: ManagementDeployment
    candidate: dict[str, Any]
    profile: dict[str, Any]
    manager_revision: str = Field(pattern=r'^[a-zA-Z0-9_]{1,64}$')
    target_manager_revision: str = Field(pattern=r'^[a-zA-Z0-9_]{1,64}$')
    prerequisites: UpgradePrerequisiteSettings
    foundation_candidate: str = Field(pattern=r'^[0-9a-f]{40}$')
    supersedes: SupersededRefreshV1 | None = None


@dataclass(frozen=True, repr=False)
class RefreshContext:
    inputs: RefreshPrivateInputs
    original: CompletedUpgrade
    predecessor: CompletedUpgrade | CompletedRefresh | CompletedPoolCutover
    request: ManagementRefreshInstallRequest
    superseded: FailedRefreshProof | None = None


def load_refresh_inputs(operation: dict[str, Any]) -> RefreshContext:
    return _load_refresh_inputs(operation, (), ())


def _load_refresh_inputs(operation: dict[str, Any], ancestors: tuple[str, ...], configurations: tuple[str, ...]) -> RefreshContext:
    try:
        validate_operation(operation)
        if (operation['schema'] != 'loom.nebius-management-refresh-operation.v1'
                or operation['operation_id'] in ancestors or len(ancestors) > 8):
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
        predecessor: CompletedUpgrade | CompletedRefresh | CompletedPoolCutover
        if isinstance(inputs.predecessor, UpgradePredecessorV1):
            if inputs.predecessor != inputs.original_upgrade:
                raise ValueError
            predecessor = original
        elif isinstance(inputs.predecessor, RefreshPredecessorV1):
            if inputs.predecessor.operation_id == operation_id:
                raise ValueError
            predecessor = load_completed_refresh(inputs.predecessor, original=original)
        else:
            if inputs.predecessor.operation['operation_id'] == str(operation_id):
                raise ValueError
            predecessor = load_completed_pool(inputs.predecessor, original=original)
        render = ManagementRefreshRenderRequest(predecessor.deployment, inputs.deployment, predecessor.active,
            inputs.candidate, inputs.profile, Path(__file__).resolve().parents[2])
        config_name = render_refresh(render).config['metadata']['name']
        # Refresh resources are create-only. Never admit a successor that would
        # require adopting or replacing an ancestor's immutable configuration.
        if config_name in configurations:
            raise ValueError
        superseded = None
        history = {**predecessor.history, path: operation['inputs_sha256']}
        if inputs.supersedes is not None:
            previous = _load_refresh_inputs(inputs.supersedes.operation, (*ancestors, str(operation_id)),
                (*configurations, config_name))
            if (previous.inputs.original_upgrade != inputs.original_upgrade
                    or previous.inputs.predecessor != inputs.predecessor
                    or previous.inputs.manager_revision != inputs.manager_revision
                    or previous.request.installation_anchor != original.upgrade.original_anchor):
                raise ValueError
            superseded = load_failed_refresh(previous.request, inputs.supersedes)
            if any(path in history and history[path] != checksum for path, checksum in superseded.history.items()):
                raise ValueError
            history.update(superseded.history)
        if len(history) > 128:
            raise ValueError
        resources = ManagementRefreshResourcesRequest(ManagementRefreshSwitchRequest(render, operation_id,
            superseded.stopped if superseded is not None else None),
            setup.binding, setup.shared_namespace_uid, inputs.manager_revision, inputs.target_manager_revision)
        pool_baseline = (predecessor if isinstance(predecessor, CompletedPoolCutover) else
            predecessor.pool_baseline if isinstance(predecessor, CompletedRefresh) else None)
        request = ManagementRefreshInstallRequest(resources, history, original.upgrade.original_anchor,
            None if pool_baseline is None else pool_baseline.selector.model_dump(mode='json'))
        return RefreshContext(inputs, original, predecessor, request, superseded)
    except Exception:
        raise EntryError('management private refresh inputs unqualified') from None


@contextmanager
def connected_refresh_api(context: RefreshContext, operation: dict[str, Any]) -> Iterator[HTTPSManagementRefreshInstaller]:
    original = context.original
    with ExitStack() as stack:
        base, trust, token = stack.enter_context(connected_checks(original.original_inputs, original.ingress,
            foundation_candidate=context.inputs.foundation_candidate))
        connection = original.original_inputs.operator_connection
        checks = ApplicationUpgradePrerequisites(base=base, settings=context.inputs.prerequisites)
        pool = None
        if (context.request.pool_baseline is not None or isinstance(context.predecessor, CompletedPoolCutover)
                or (isinstance(context.predecessor, CompletedRefresh) and context.predecessor.pool_baseline is not None)):
            from scripts.ops.nebius_pool_cutover_entry import connected_pool_api
            from scripts.ops.nebius_pool_refresh import PoolManagerRefresh
            from scripts.ops.nebius_pool_refresh_live import HTTPSPoolRefreshAPI

            if not isinstance(context.predecessor, (CompletedPoolCutover, CompletedRefresh)):
                raise EntryError('pool refresh predecessor differs')
            projection = PoolManagerRefresh(original, context.predecessor, context.request,
                Path(operation['state_dir']), context.superseded)
            # Pool authority keeps its own fixed reader/credential lifetime; do
            # not widen the retained manager reader into execution namespaces.
            parent = stack.enter_context(connected_pool_api(projection.qualify().context, refresh=projection))
            pool = HTTPSPoolRefreshAPI(parent=parent)
        api = stack.enter_context(HTTPSManagementRefreshInstaller(request=context.request, original=original,
            predecessor=context.predecessor, superseded=context.superseded, pool=pool,
            state_dir=Path(operation['state_dir']), api_server=connection.endpoint, ssl_context=trust, token=token,
            runtime_ca_pem=_private(connection.ca_file, 1024**2).decode(), checks=checks))
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
        capacity = None
        if stage == 'platform_capacity' and api is not None:
            detail = getattr(api.checks, 'capacity_diagnostic', None)
            if isinstance(detail, dict):
                try:
                    capacity = validate_capacity_report(detail)
                except GatewayError:
                    pass  # Invalid details stay coarse; never expose a provider payload.
        raise ManagementRefreshInstallError('refresh_' + stage.replace('-', '_'), capacity=capacity) from None
