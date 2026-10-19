"""Selected worker/build admission boundaries, never platform ownership changes.

The protected activation parent supplies exact UID-bound originals from completed
startup/foundation history. Intent projection does not authorize a write: the
parent must prove closed participant admission, current platform/runtime readiness,
empty task inventory and functioning native-build policy before applying changes.
"""
from __future__ import annotations

from typing import Any

from scripts.ops.nebius_environment_boundary_live import (
    EnvironmentBoundaryWorkloads as EnvironmentBoundaryWorkloads,
)
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid

from loom.nebius_pool_contract import PoolParticipantV1


def boundary_targets(*, participant: PoolParticipantV1, reader_count: int,
        originals: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Remove closed-intake quotas without introducing per-environment budgets.

    Capacity remains the shared allocator's responsibility. Only the selected
    build namespace receives the existing narrow native-builder PSA exception;
    its already-installed admission policy must be observed before/after that CAS.
    """
    try:
        participant = PoolParticipantV1.model_validate(participant.model_dump())
        execution, build = participant.execution_namespace, participant.build_namespace
        namespace_key = 'Namespace:-:' + build.name
        quotas = {'ResourceQuota:' + execution.name + ':loom-execution-disabled': {
            'hard': {'pods': str(reader_count), 'count/jobs.batch': '0'}},
            'ResourceQuota:' + build.name + ':loom-execution-disabled': {'hard': {'pods': '0'}}}
        if (type(reader_count) is not int or reader_count < 1
                or set(originals) != {namespace_key, *quotas}):
            raise ValueError
        for key, document in originals.items():
            _uid(document)
            if key != _key(document) or document['apiVersion'] != 'v1' or document['metadata'].get('deletionTimestamp'):
                raise ValueError
            if key in quotas and document['spec'] != quotas[key]:
                raise ValueError
        namespace = originals[namespace_key]
        if (_uid(namespace) != str(build.uid) or namespace['metadata'].get('namespace') is not None
                or namespace['metadata']['labels']['pod-security.kubernetes.io/enforce'] != 'restricted'):
            raise ValueError
        targets = {key: _snapshot(doc) for key, doc in originals.items()}
        targets[namespace_key]['metadata']['labels']['pod-security.kubernetes.io/enforce'] = 'privileged'
        for key in quotas:
            targets[key]['spec'] = {}
        return targets
    except Exception:
        raise ValueError('environment_boundary_intent_unqualified') from None
