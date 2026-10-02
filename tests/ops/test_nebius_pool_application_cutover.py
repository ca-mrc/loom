"""Connect catalog-derived personal builds to the real protected cutover journal."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest
from tests.integration.test_nebius_pool_installation import add_application_builder
from tests.ops.test_nebius_pool_cutover import CutoverAPI
from tests.ops.test_nebius_pool_cutover_entry import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    application_material as application_material,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    checks as checks,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    cloud as cloud,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    collector_inputs as collector_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    completed_upgrade as completed_upgrade,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    cutover_inputs as cutover_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    database_guard as database_guard,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    entry_inputs as entry_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    fencing_inputs as fencing_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    installation as installation,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    management_inputs as management_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    material as material,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    private_cutover as private_cutover,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    private_upgrade as private_upgrade,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    retirement_inputs as retirement_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    runtime_inputs as runtime_inputs,
)
from tests.ops.test_nebius_pool_cutover_entry import (
    save_private,
)
from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as original_platform_inputs


@pytest.fixture
def platform_inputs():
    values = copy.deepcopy(original_platform_inputs.__wrapped__())
    # The migration fixture orders production, staging, then shared development.
    # Give the completed manager that actual retained dev foundation from birth.
    values[0].update(namespace='loom-nebius-platform-2', execution_namespace='loom-nebius-exec-2')
    return values


@pytest.fixture
def builder_cutover_inputs(private_cutover, build_inputs):
    operation, payload, root = private_cutover
    before = root.deployment
    shared = before.installation.applications.shared
    spec = payload['installation']
    development, = (row for row in spec['participants'] if row['environment_class'] == 'development')
    development['environment_id'] = str(shared.data_environment_id)
    recipe = build_inputs[0].recipe.model_copy(update={'schema_revision': shared.schema_revision})
    spec, machine, token = add_application_builder(spec, recipe)
    # Legacy task fixtures use artifacts; personal source uses the retained
    # shared source bucket, as the actual installation requires.
    spec['profiles']['application_images'][0]['settings']['source_bucket'] = (
        before.installation.foundation.platform_config['buckets']['source'])
    payload['installation'] = spec
    path = Path(operation['inputs_path']).parent / 'application-builder-token'
    path.write_text(token)
    path.chmod(0o600)
    payload['machine_token_files'][str(machine)] = str(path)
    credentials = {'access-key': 'cutover-source-access', 'secret-key': 'cutover-source-secret'}
    payload['application_source_credential'] = {'uid': str(uuid4()), 'resource_version': '31',
        'sha256': hashlib.sha256(json.dumps(credentials, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}
    save_private(operation, payload)
    return operation, payload, root, credentials


def test_private_builder_cutover_stages_material_and_readers_before_closed_manager(builder_cutover_inputs):
    from scripts.ops.nebius_ingress_stage import _key
    from scripts.ops.nebius_pool_cutover import cutover_documents, stage_pool_cutover
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs

    operation, _, _, credentials = builder_cutover_inputs
    context = load_pool_cutover_inputs(operation)
    request = context.request
    documents = cutover_documents(request)
    manager = _key(request.manager)
    pod = documents['runtime'][manager]['spec']['template']['spec']
    volumes = {row['name']: row for row in pod['volumes']}
    source = 'Secret:' + request.manager['metadata']['namespace'] + ':' + volumes['application-source-credentials']['secret']['secretName']
    configuration = {key for key in (_key(row) for row in documents['configuration'])
        if key.startswith(('ConfigMap:', 'Role:', 'RoleBinding:'))}
    assert any(key.startswith('Role:loom-nebius-exec-2-build:') for key in configuration)
    api = CutoverAPI(request)
    for document in api.documents.values():
        document['metadata'].setdefault('resourceVersion', '1')
    patch = api.patch_workload

    def qualified_patch(key, before, desired):
        if key == manager and any(row['name'] == 'application-source-credentials'
                for row in desired['spec']['template']['spec']['volumes']):
            assert source in api.resources.resources
            assert configuration <= set(api.resources.resources)
            assert desired['spec']['replicas'] == 0
        return patch(key, before, desired)

    api.patch_workload = qualified_patch
    arguments = dict(request=request, tokens=context.tokens, api=api, source_credentials=credentials,
        state_dir=Path(operation['state_dir']), anchor_dir=Path(operation['anchor_dir']))
    result = stage_pool_cutover(**arguments)
    assert result['status'] == 'pool_runtime_staged_closed'
    delivered = api.resources.resources[source]
    assert delivered['immutable'] is True
    assert set(delivered['data']) == {'credentials.json'}
    assert json.loads(base64.b64decode(delivered['data']['credentials.json'])) == credentials
    assert api.documents[manager]['spec']['replicas'] == 0
    writes = len(api.patches)
    assert stage_pool_cutover(**arguments) == result
    assert len(api.patches) == writes
    public_record = (Path(operation['state_dir']) / 'cutover.json').read_text()
    assert all(value not in public_record for value in credentials.values())


def test_builder_cutover_rejects_wrong_source_before_producer_shutdown(builder_cutover_inputs):
    from scripts.ops.nebius_pool_cutover import stage_pool_cutover
    from scripts.ops.nebius_pool_cutover_entry import load_pool_cutover_inputs

    operation, _, _, credentials = builder_cutover_inputs
    context = load_pool_cutover_inputs(operation)
    api = CutoverAPI(context.request)
    with pytest.raises(ValueError):
        stage_pool_cutover(request=context.request, tokens=context.tokens, api=api,
            source_credentials={**credentials, 'secret-key': 'wrong-source'},
            state_dir=Path(operation['state_dir']), anchor_dir=Path(operation['anchor_dir']))
    assert api.patches == [] and api.resources.resources == {}
