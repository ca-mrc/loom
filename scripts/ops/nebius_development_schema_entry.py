"""Installed development-only schema dispatch using the existing private target."""
from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from scripts.ops.nebius_development_schema_upgrade import (
    DevelopmentSchemaAPI,
    DevelopmentSchemaReference,
    RetainedDevelopmentSchema,
    load_development_schema_upgrade,
    run_development_schema_upgrade,
)
from scripts.ops.nebius_environment_lifecycle_entry import (
    LifecycleCommandV2,
    LifecycleConfigurationV2,
    LifecycleOperationV2,
)


@contextmanager
def connected_api(retained: RetainedDevelopmentSchema, *,
        unchanged: Callable[[], Any]) -> Iterator[DevelopmentSchemaAPI]:
    from scripts.ops.nebius_development_schema_live import connected_schema_api

    with connected_schema_api(retained, unchanged=unchanged) as api:
        yield api


def execute_development_schema_lifecycle(*, configuration: LifecycleConfigurationV2,
        request: LifecycleCommandV2, operation: LifecycleOperationV2,
        unchanged: Callable[[], Any]) -> dict[str, Any]:
    """Run in a worker thread; the retained parent owns the shared handoff lock."""
    unchanged()
    if (request.scope != configuration.scope or request.scope.authority != 'development'
            or operation.kind != 'schema-upgrade' or configuration.operations.get(request.operation_id) != operation):
        raise ValueError('development_schema_lifecycle_scope_changed')
    reference = DevelopmentSchemaReference(operation_id=request.operation_id,
        inputs_path=operation.inputs_path, inputs_sha256=operation.inputs_sha256)
    retained = load_development_schema_upgrade(reference, operator=True)
    participant = retained.projection.participant
    expected = {name: getattr(participant, name) for name in (
        'installation_id', 'pool_id', 'participant_id', 'environment_id', 'incarnation')}
    if request.scope.model_dump() != dict(expected, authority=participant.environment_class):
        raise ValueError('development_schema_lifecycle_scope_changed')
    unchanged()
    with connected_api(retained, unchanged=unchanged) as api:
        outcome = run_development_schema_upgrade(retained=retained, api=api, action=request.action)
    unchanged()
    if (outcome.get('operation_id') != str(request.operation_id) or outcome.get('admission_open') is not False
            or outcome.get('status') not in {'prepared', 'pending', 'complete'}):
        raise ValueError('development_schema_lifecycle_result_unqualified')
    status = 'observed' if request.action == 'status' and outcome['status'] == 'complete' else outcome['status']
    return dict(schema_version='loom.environment-lifecycle-result.v2', action=request.action,
        operation_id=str(request.operation_id), scope=request.scope.model_dump(mode='json'),
        phase='schema-upgrade', status=status, admission_open=False, execution_verified=False)
