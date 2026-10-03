"""Recovery reopening uses one exact retained database without retries."""
from __future__ import annotations

from uuid import uuid4

import pytest
from tests.ops.test_nebius_pool_database_guard import database_guard as database_guard
from tests.ops.test_nebius_pool_database_guard import management_inputs as management_inputs
from tests.ops.test_nebius_pool_database_guard import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_database_guard import runtime_inputs as runtime_inputs


@pytest.mark.parametrize('damage', [None, 'backend', 'credential', 'authority', 'late_authority',
    'schema', 'operation', 'participant', 'candidate', 'extra', 'wrong_effect', 'lost_reply'])
def test_recovery_release_is_fixed_identity_bound_and_observation_only_on_unknown(database_guard, damage):
    from scripts.ops.nebius_pool_migration import PoolMigrationError

    api, state = database_guard
    operation, candidate = api.request.registration.spec.operation_id, api.request.registration.candidate['candidate_sha']
    report = {'schema': 'loom.pool-recovery-release.v1', 'operation_id': str(operation),
        'participant_id': str(state.target.participant_id), 'candidate_sha': candidate, 'status': 'open'}
    if damage == 'backend':
        state.after_drift = True
    elif damage == 'credential':
        state.secret['metadata']['resourceVersion'] = 'changed'
    elif damage == 'authority':
        api.kubeconfig.write_bytes(b'private-changed-authority')
    elif damage == 'schema':
        report['schema'] = 'foreign'
    elif damage in {'operation', 'participant'}:
        report[damage + '_id'] = str(uuid4())
    elif damage == 'candidate':
        report['candidate_sha'] = 'b' * 40
    elif damage == 'extra':
        report['extra'] = 'private-marker'
    elif damage == 'wrong_effect':
        report['status'] = 'fenced'

    def execute(query):
        from scripts.ops.nebius_pool_recovery_release import pool_guard_recovery_release_sql

        assert query == pool_guard_recovery_release_sql(operation, state.target.participant_id, candidate)
        if damage == 'late_authority':
            api.kubeconfig.write_bytes(b'private-changed-authority')
        if damage == 'lost_reply':
            raise OSError('private-marker')
        return report

    state.exec_hook = execute
    if damage is None:
        assert api.release_recovery_guard(state.target) == 'open'
    else:
        with pytest.raises(PoolMigrationError) as error:
            api.release_recovery_guard(state.target)
        assert 'private-' not in str(error.value)
    writes = [call for call in state.calls if call[0] == 'exec']
    assert len(writes) == (0 if damage in {'authority', 'credential'} else 1)
    if writes:
        assert writes[0][:7] == ['exec', '-n', state.target.namespace, 'pod/loom-postgres-0', '-c', 'loom-postgres', '--']
