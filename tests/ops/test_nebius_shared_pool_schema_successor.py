"""The completed split-schema development upgrade roots later shared handoff."""
from __future__ import annotations

from uuid import uuid4

import pytest
from tests.ops.test_nebius_development_pool_retained import (
    RuntimeParentAPI,
    foundation_publication,
    runtime_install_request,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    ROOT,
    PreactivationAPI,
    _advanced_staging_source,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    application_management_inputs as application_management_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import application_material as application_material
from tests.ops.test_nebius_shared_pool_handoff import build_inputs as build_inputs
from tests.ops.test_nebius_shared_pool_handoff import capacity_checks as capacity_checks
from tests.ops.test_nebius_shared_pool_handoff import cloud as cloud
from tests.ops.test_nebius_shared_pool_handoff import collector_cloud as collector_cloud
from tests.ops.test_nebius_shared_pool_handoff import completed_pool as completed_pool
from tests.ops.test_nebius_shared_pool_handoff import connected as connected
from tests.ops.test_nebius_shared_pool_handoff import development_inputs as development_inputs
from tests.ops.test_nebius_shared_pool_handoff import entry as entry
from tests.ops.test_nebius_shared_pool_handoff import handoff as handoff
from tests.ops.test_nebius_shared_pool_handoff import installation as installation
from tests.ops.test_nebius_shared_pool_handoff import inventory as inventory
from tests.ops.test_nebius_shared_pool_handoff import live as live
from tests.ops.test_nebius_shared_pool_handoff import management_inputs as management_inputs
from tests.ops.test_nebius_shared_pool_handoff import manager_entry as manager_entry
from tests.ops.test_nebius_shared_pool_handoff import material as material
from tests.ops.test_nebius_shared_pool_handoff import original_cloud as original_cloud
from tests.ops.test_nebius_shared_pool_handoff import (
    original_development_inputs as original_development_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_manager_entry as original_manager_entry,
)
from tests.ops.test_nebius_shared_pool_handoff import (
    original_platform_inputs as original_platform_inputs,
)
from tests.ops.test_nebius_shared_pool_handoff import original_pool_inputs as original_pool_inputs
from tests.ops.test_nebius_shared_pool_handoff import platform_inputs as platform_inputs
from tests.ops.test_nebius_shared_pool_handoff import pool_entry as pool_entry
from tests.ops.test_nebius_shared_pool_handoff import pool_inputs as pool_inputs
from tests.ops.test_nebius_shared_pool_handoff import preflight as preflight
from tests.ops.test_nebius_shared_pool_handoff import provider_checks as provider_checks
from tests.ops.test_nebius_shared_pool_handoff import publication as publication
from tests.ops.test_nebius_shared_pool_handoff import published_source as published_source
from tests.ops.test_nebius_shared_pool_handoff import publisher_cloud as publisher_cloud
from tests.ops.test_nebius_shared_pool_handoff import retained as retained
from tests.ops.test_nebius_shared_pool_handoff import route as route
from tests.ops.test_nebius_shared_pool_handoff import source_checkout as source_checkout
from tests.ops.test_nebius_shared_pool_handoff import staging_history as staging_history
from tests.ops.test_nebius_shared_pool_handoff import staging_observation as staging_observation
from tests.ops.test_nebius_shared_pool_handoff import tls_material as tls_material

pytestmark = [
    pytest.mark.parametrize('manager_entry', ['foundation-runtime-build-material-split-schema'], indirect=True),
    pytest.mark.parametrize('published_source', ['0175'], indirect=True),
    pytest.mark.parametrize('retained', [False], indirect=True),
]


@pytest.fixture
def shared_runtime(completed_pool, publisher_cloud, published_source):
    runtime, request = runtime_install_request(completed_pool, publisher_cloud,
        data_publication=foundation_publication(completed_pool))
    plan = runtime.prepare_runtime_install(request)
    api = RuntimeParentAPI(request, plan)
    api.database_complete = api.catalog_complete = True
    assert runtime.install_development_runtime(request=request, api=api, execute=True)['status'] == 'development_runtime_installed_closed'
    return request, api


@pytest.mark.parametrize('staging_observation', ['independent'], indirect=True)
@pytest.mark.parametrize('storage_first', [False, True])
@pytest.mark.timeout(300)
def test_schema_successor_roots_staging_refresh_and_later_handoff(
        shared_runtime, staging_history, staging_observation, completed_pool, handoff, tmp_path, storage_first):
    import copy
    import hashlib
    import json
    import ssl
    from dataclasses import asdict
    from pathlib import Path

    import httpx
    from scripts.ops import nebius_certificates as state
    from scripts.ops import nebius_pool_preactivation_operation as parent
    from scripts.ops import nebius_pool_storage_operation as storage
    from scripts.ops import nebius_shared_pool_entry as entry
    from scripts.ops.nebius_development_runtime_observation import (
        _READ_PATHS,
        HTTPSDevelopmentRuntimeObserver,
    )
    from scripts.ops.nebius_development_runtime_retained import load_completed_runtime
    from scripts.ops.nebius_shared_pool_handoff import prepare_shared_pool_handoff
    from scripts.ops.nebius_shared_pool_handoff_live import observe_shared_pool_guard_bindings
    from tests.ops.test_nebius_development_pool_retained import runtime_http_inventory
    from tests.ops.test_nebius_development_schema_consumers import completed_schema_successor
    from tests.ops.test_nebius_development_schema_projection import next_publication
    from tests.ops.test_nebius_development_schema_upgrade import retain_runtime_inputs
    from tests.ops.test_nebius_shared_pool_handoff import ClosedStorageAPI

    request, runtime_api = shared_runtime
    args = dict(runtime=request, observation=staging_observation, legacy=staging_history, repo_root=ROOT)
    original = load_completed_runtime(args['runtime'])
    connection = completed_pool[1]['operator_connection']
    runtime = retain_runtime_inputs(original, connection)
    physical = None
    if storage_first:
        correction = storage.retain_closed_storage(runtime=runtime, operation_id=uuid4(),
            target_disk_type='NETWORK_SSD_NON_REPLICATED')
        correction_api = ClosedStorageAPI(correction)
        correction_api.collector.ready = correction_api.publication_ready = True
        assert storage.run_closed_storage(retained=correction, api=correction_api)
        physical = storage.load_completed_storage(runtime=runtime, reference=correction.reference)
    successor = completed_schema_successor(dict(runtime=original, operation_id=uuid4(),
        data_publication=next_publication(original.request.database.data_runtime),
        manager_publication=next_publication(original.request.database.manager.publication, seed='c'),
        **({} if physical is None else {'storage': physical})),
        connection, tmp_path, runtime_reference=runtime)
    if physical is not None:
        assert successor.storage == physical
        assert successor.projection.before_pool == physical.retained.registration.spec
        assert successor.projection.after_pool.quota_identities['storage'][3] == 'compute.disk.size.network-ssd-non-replicated'
    source = _advanced_staging_source(args, tmp_path)
    retained = parent.retain_preactivation_catalog(runtime=runtime, source=source,
        operation_id=uuid4(), repo_root=ROOT, schema_successor=successor.reference)
    assert retained.inputs.schema_version == 'loom.pool-preactivation-inputs.v2'
    assert retained.inputs.refresh.before == successor.projection.after_pool
    assert retained.inputs.manager == successor.resources['Deployment:loom-nebius-management-dev:loom-service']
    assert successor.files.items() <= retained.files.items()
    api = PreactivationAPI(retained)
    api.manager.ready = True
    assert parent.run_preactivation_catalog(retained=retained, api=api, repo_root=ROOT)
    completed = parent.load_completed_preactivation(runtime=runtime, reference=retained.reference, repo_root=ROOT)
    selected_args = {**args, 'observation': source, 'schema_successor': successor, 'preactivation': completed}
    plan = prepare_shared_pool_handoff(**selected_args)
    assert plan.development.runtime == original
    assert plan.development.selection.manager_revision == '0178'
    assert plan.migration.registration.spec == retained.inputs.refresh.after
    assert plan.migration.registration.candidate == successor.projection.manager_publication.bundle.candidate
    assert plan.storage == physical
    if physical is not None:
        assert plan.migration.registration.spec.quota_identities['storage'][3] == 'compute.disk.size.network-ssd-non-replicated'
        assert physical.files.keys() <= plan.history.keys()
        assert retained.inputs.storage == physical.retained.reference
        foreign = copy.deepcopy(physical)
        object.__setattr__(foreign.retained, 'reference', foreign.retained.reference.model_copy(update={'operation_id': uuid4()}))
        with pytest.raises(ValueError):
            prepare_shared_pool_handoff(**selected_args, storage=foreign)
    assert all(path.read_bytes() == raw for path, raw in original.files.items())
    # Exercise the actual GET observer and private input loader against the
    # upgraded resources, while the original completed installation is intact.
    _, documents, handler = runtime_http_inventory(request, runtime_api, completed_pool[3].server, handoff[2])
    for row in plan.effective_runtime_resources.values():
        version, plural, cluster = _READ_PATHS[row['kind']]
        prefix = '/api/v1' if version == 'v1' else '/apis/' + version
        path = prefix + ('' if cluster else '/namespaces/' + row['metadata']['namespace']) + '/' + plural + '/' + row['metadata']['name']
        documents[path] = copy.deepcopy(row)
    for row in documents.values():
        if row['kind'] == 'Secret':
            row['metadata'].setdefault('resourceVersion', '501')
    with HTTPSDevelopmentRuntimeObserver(request=request, api_server=connection['endpoint'],
            ssl_context=ssl.create_default_context(), preactivation=completed, schema_successor=successor) as observer:
        assert observer.storage == physical
        for transport in (observer, observer.manager, observer.foundation):
            client_type = type(transport.client)
            transport.client.close()
            transport.client = client_type(base_url=transport.api_server, transport=httpx.MockTransport(handler))
        bindings = observe_shared_pool_guard_bindings(**selected_args, development=observer)
    manager = request.database.manager.retained.request.retained
    registration = request.database.manager.retained.request.registration
    owner = Path(manager.operation['inputs_path']).parents[3]
    root = owner / '.loom/nebius-shared-pool' / manager.binding.installation_id / str(registration.spec.operation_id)
    root.mkdir(parents=True, mode=0o700)
    state._atomic_json(root / 'staging.json', source)
    state._atomic_json(root / 'source.json', successor.projection.manager_publication.source.model_dump(mode='json'))
    guard, = (row for row in bindings.migration.guards if row.namespace == 'loom-dev')
    inputs = dict(schema_version='loom.nebius-shared-pool-inputs.v3', runtime=runtime.model_dump(mode='json'),
        schema_successor=successor.reference.model_dump(mode='json'), preactivation=retained.reference.model_dump(mode='json'),
        legacy=staging_history.selector.model_dump(mode='json'), staging_observation_file=str(root / 'staging.json'),
        staging_observation_sha256=hashlib.sha256((root / 'staging.json').read_bytes()).hexdigest(),
        development_guard=json.loads(json.dumps(asdict(guard), default=str)), operator_connection=connection)
    if physical is not None:
        inputs.update(schema_version='loom.nebius-shared-pool-inputs.v4',
            storage=physical.retained.reference.model_dump(mode='json'))
    state._atomic_json(root / 'inputs.json', inputs)
    operation = dict(schema='loom.nebius-shared-pool-operation.v1',
        source_sha=successor.projection.manager_publication.source.source_sha,
        installation_id=manager.binding.installation_id, namespace=manager.binding.namespace,
        operation_id=str(registration.spec.operation_id), inputs_path=str(root / 'inputs.json'),
        inputs_sha256=hashlib.sha256((root / 'inputs.json').read_bytes()).hexdigest())
    loaded = entry._load_inputs(operation, source_record=root / 'source.json', include_operator=True)
    assert loaded.bindings == bindings
    assert loaded.runtime == request
    assert successor.files.items() <= loaded.files.items()
    with pytest.raises(ValueError):
        entry.SharedPoolPrivateInputs.model_validate({**inputs, 'schema_version': 'loom.nebius-shared-pool-inputs.v2'})
    with pytest.raises(ValueError):
        prepare_shared_pool_handoff(**{**selected_args, 'schema_successor': None})
    with pytest.raises(ValueError):
        parent.retain_preactivation_catalog(runtime=runtime, source=source, operation_id=uuid4(),
            predecessor=retained.reference, repo_root=ROOT)
