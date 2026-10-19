"""A closed physical-pool correction cannot change workload or machine authority."""
from __future__ import annotations

import copy
import importlib
from uuid import uuid4

import pytest
from tests.integration.test_nebius_pool_installation import installation

from loom_service.pool_management.installation import PoolInstallation
from loom_service.pool_management.participant_refresh import PoolParticipantRefresh


def selection():
    before, _ = installation(('staging', 'development'))
    before['quota_identities']['storage'][3] = 'compute.disk.size.network-ssd'
    after = copy.deepcopy(before)
    after['policy_revision'] = before['policy_revision'] + 1
    after['quota_identities']['storage'][3] = 'compute.disk.size.network-ssd-non-replicated'
    return {'schema_version': 'loom.closed-pool-storage-refresh.v1',
        'operation_id': str(uuid4()), 'target_disk_type': 'NETWORK_SSD_NON_REPLICATED',
        'before': before, 'after': after}


def module():
    name = 'scripts.ops.nebius_pool_storage_binding'
    assert importlib.util.find_spec(name) is not None, 'closed physical storage successor is missing'
    return importlib.import_module(name)


def test_storage_successor_preserves_pool_workloads_credentials_and_original_input():
    payload = selection()
    frozen = copy.deepcopy(payload)
    selected = module().ClosedPoolStorageRefresh.model_validate(payload)
    assert payload == frozen
    assert selected.after.quota_identities['storage'][3] == 'compute.disk.size.network-ssd-non-replicated'
    assert selected.after.policy_revision == selected.before.policy_revision + 1
    assert selected.operation_id != selected.before.operation_id
    assert selected.before.model_dump(exclude={'policy_revision', 'quota_identities'}) == (
        selected.after.model_dump(exclude={'policy_revision', 'quota_identities'}))
    # No active participant-release route may deserialize this closed-only scope.
    with pytest.raises(ValueError):
        PoolParticipantRefresh.model_validate(selected.model_dump(mode='json'))


@pytest.mark.parametrize('damage', ['quota-parent', 'quota-region', 'quota-unit', 'other-quota',
    'node-group', 'selector', 'admission', 'machine', 'participant', 'profile',
    'wrong-quota', 'unknown-disk', 'unchanged-quota', 'operation', 'installation-operation', 'revision'])
def test_storage_successor_rejects_every_unrelated_or_ambiguous_authority_change(damage):
    payload = selection()
    before, after = payload['before'], payload['after']
    if damage in {'quota-parent', 'quota-region', 'quota-unit'}:
        index = {'quota-parent': 0, 'quota-region': 1, 'quota-unit': 4}[damage]
        after['quota_identities']['storage'][index] += '-changed'
    elif damage == 'other-quota':
        after['quota_identities']['vcpu'][3] += '-changed'
    elif damage == 'node-group':
        after['node_group_id'] += '-changed'
        after['node_selector']['nebius.com/node-group-id'] = after['node_group_id']
    elif damage == 'selector':
        after['node_selector']['foreign'] = 'true'
    elif damage == 'admission':
        after['admission']['build_concurrency_limit'] += 1
    elif damage == 'machine':
        after['machines'][0]['credential_epoch'] += 1
    elif damage == 'participant':
        after['participants'][0]['binding_revision'] += 1
    elif damage == 'profile':
        after['profiles']['execution'][0]['runtime_binary_sha256'] = 'sha256:' + 'f' * 64
    elif damage == 'wrong-quota':
        after['quota_identities']['storage'][3] = 'compute.disk.size.unrelated'
    elif damage == 'unknown-disk':
        payload['target_disk_type'] = 'UNQUALIFIED_DISK'
    elif damage == 'unchanged-quota':
        payload['target_disk_type'] = 'NETWORK_SSD'
        after['quota_identities']['storage'] = copy.deepcopy(before['quota_identities']['storage'])
    elif damage == 'operation':
        payload['operation_id'] = before['operation_id']
    elif damage == 'installation-operation':
        after['operation_id'] = str(uuid4())
    else:
        after['policy_revision'] += 1
    if damage in {'node-group', 'selector'}:
        for kind, field in (('execution', 'runtime'), ('task_images', 'target')):
            for profile in after['profiles'][kind]:
                profile[field]['node_selector'].update(after['node_selector'])
    # Individually valid installations do not authorize the delta between them.
    PoolInstallation.model_validate(before)
    PoolInstallation.model_validate(after)
    with pytest.raises(ValueError):
        module().ClosedPoolStorageRefresh.model_validate(payload)
