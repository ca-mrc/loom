"""Bound aggregate verification work independently of byte size or object count."""
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom_control_plane.object_version_recovery import RecoveryRequest


def payload(counts):
    return {
        'operation_id': str(UUID(int=1)), 'trial_id': str(UUID(int=2)),
        'artifact_id': str(UUID(int=3)),
        'expected_storage_sha256': 'sha256:' + 'a' * 64,
        'expected_index_sha256': 'sha256:' + 'b' * 64,
        'objects': [
            {'registry_id': str(UUID(int=100 + i)), 'version_id': 'v0',
             **({'equivalent_version_ids': [f'v{v}' for v in range(count)]} if count > 1 else {})}
            for i, count in enumerate(counts)
        ],
    }


@pytest.mark.parametrize('counts', [[8] * 32, [32] * 8, [32] * 7 + [31, 1], [18] * 4])
def test_request_accepts_complete_inventories_within_256_copy_budget(counts):
    data = payload(counts)
    request = RecoveryRequest.model_validate(data)
    assert [len(item.equivalent_version_ids or [item.version_id]) for item in request.objects] == counts


@pytest.mark.parametrize('counts', [[32] * 8 + [1], [32] * 7 + [31, 2], [18] * 15])
def test_request_rejects_aggregate_copy_overflow_even_with_fewer_than_32_objects(counts):
    with pytest.raises(ValidationError, match='version copy limit'):
        RecoveryRequest.model_validate(payload(counts))


def test_single_object_inventory_still_has_a_bound():
    with pytest.raises(ValidationError):
        RecoveryRequest.model_validate(payload([33]))


def test_legacy_request_identity_is_unchanged():
    data = payload([1, 8])
    assert RecoveryRequest.model_validate(data).identity() == data
