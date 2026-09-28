"""Owner requests cannot select deployment authority or forge ready evidence."""
from uuid import UUID

import pytest
from pydantic import ValidationError

RELEASE = '20000000-0000-4000-8000-000000000001'


@pytest.mark.parametrize('extra', [{'owner_user_id': RELEASE}, {'namespace': 'loom-prod'},
    {'service_image_ref': 'unreviewed:latest'}, {'shared': {}}, {'completion_json': {}},
    {'slug': 'shared'}, {'release_id': str(UUID(int=0))}])
def test_create_request_rejects_owner_supplied_authority(extra):
    from loom.nebius_application_contract import ApplicationCreateRequestV1

    with pytest.raises(ValidationError):
        ApplicationCreateRequestV1.model_validate({'slug': 'alice', 'release_id': RELEASE} | extra)


@pytest.mark.parametrize('payload', [
    {'action': 'update', 'expected_generation': 1},
    {'action': 'suspend', 'expected_generation': 1, 'release_id': RELEASE},
    {'action': 'resume', 'expected_generation': True},
    {'action': 'resume', 'expected_generation': '1'},
    {'action': 'destroy_retained', 'expected_generation': 0},
    {'action': 'purge', 'expected_generation': 1},
    {'action': 'update', 'expected_generation': 1, 'release_id': str(UUID(int=0))},
])
def test_transition_request_rejects_ambiguous_or_unsafe_actions(payload):
    from loom.nebius_application_contract import ApplicationOperationRequestV1

    with pytest.raises(ValidationError):
        ApplicationOperationRequestV1.model_validate(payload)
