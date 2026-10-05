"""Strict recovery reports cannot turn malformed/foreign counts into drain."""
from __future__ import annotations

from uuid import uuid4

import pytest
from tests.integration.test_nebius_pool_installation import installation

from loom_service.pool_management.capacity import digest
from loom_service.pool_management.installation import PoolInstallation


@pytest.mark.parametrize('scope', ['pool', 'participant'])
@pytest.mark.parametrize('damage', ['boolean', 'negative', 'string', 'missing', 'extra', 'scope', 'writable'])
def test_recovery_report_rejects_unqualified_counts(scope, damage):
    from scripts.ops.nebius_pool_recovery_database import (
        qualify_participant_recovery_drain,
        qualify_pool_recovery_drain,
    )

    spec = PoolInstallation.model_validate(installation()[0])
    operation, participant, candidate = spec.operation_id, spec.participants[0].participant_id, 'a' * 40
    counts = ({'unstarted_requests': 0, 'active_requests': 0, 'unconfirmed_creates': 0, 'unqualified_releases': 0}
        if scope == 'pool' else {'trials': 0, 'executions': 0, 'builds': 0, 'build_cleanup': 0,
            'execution_outboxes': 0, 'build_outboxes': 0})
    report = {'schema': 'loom.pool-recovery-drain.v1' if scope == 'pool' else 'loom.pool-participant-drain.v1',
        'operation_id': str(operation), 'read_only': True, 'counts': counts}
    if scope == 'pool':
        report['installation_sha256'] = digest(spec.model_dump(mode='json'))
    else:
        report.update(participant_id=str(participant), candidate_sha=candidate)
    def qualify(value):
        return (qualify_pool_recovery_drain(spec, value) if scope == 'pool'
            else qualify_participant_recovery_drain(operation, participant, candidate, value))
    assert qualify(report) is True
    key = next(iter(counts))
    counts[key] = 1
    assert qualify(report) is False
    counts[key] = 0
    if damage in {'boolean', 'negative', 'string'}:
        counts[key] = {'boolean': False, 'negative': -1, 'string': '0'}[damage]
    elif damage == 'missing':
        del counts[key]
    elif damage == 'extra':
        report['private'] = 'unrequested-data'
    elif damage == 'scope':
        report['operation_id'] = str(uuid4())
    else:
        report['read_only'] = 1
    with pytest.raises(ValueError):
        qualify(report)
