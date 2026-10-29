"""Normal idle rollout consumes the retained shared operation, including recovery."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from scripts.ops import nebius_idle_rollout as idle
from scripts.ops import nebius_pool_participant_operation as operation
from tests.ops.test_nebius_pool_participant_adapter import connection as connection
from tests.ops.test_nebius_pool_participant_delivery import (
    test_retained_shared_release_uses_actual_idle_and_deploy_lifecycle as _exercise_release,
)
from tests.ops.test_nebius_pool_participant_operation import catalog_inputs as catalog_inputs
from tests.ops.test_nebius_pool_participant_operation import management_inputs as management_inputs
from tests.ops.test_nebius_pool_participant_operation import operation_inputs as operation_inputs
from tests.ops.test_nebius_pool_participant_operation import platform_inputs as platform_inputs
from tests.ops.test_nebius_pool_participant_operation import release_inputs as release_inputs
from tests.ops.test_nebius_pool_participant_operation import (
    staging_attachment_inputs as staging_attachment_inputs,
)


@pytest.mark.timeout(120)
def test_completed_release_is_the_next_receipt_selected_predecessor(
        operation_inputs, connection, tmp_path, monkeypatch):
    import ssl

    import httpx
    from scripts.ops import nebius_pool_participant_completion as completion
    from scripts.ops import nebius_shared_pool_forward_census as census
    from scripts.ops.nebius_ingress_stage import _key, _uid
    from scripts.ops.nebius_management_switch import _stable
    from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
    from scripts.ops.nebius_pool_participant_prepare import capture_participant_release
    from scripts.ops.nebius_pool_participant_resource import _resource_path

    _exercise_release(
        operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)
    loader = getattr(completion, 'load_completed_participant_rollout', None)
    assert callable(loader), 'a released operation must have a semantic predecessor loader'
    retained = connection[0].retained
    original_loader = getattr(completion, 'load_participant_predecessor', None)
    assert callable(original_loader), 'recovery cannot derive its original semantic predecessor'
    assert original_loader(initial=operation_inputs['predecessor'], retained=retained) == operation_inputs['predecessor']
    before = {path: path.read_bytes() for root in (retained.state_dir, retained.anchor_dir)
        for path in root.rglob('*') if path.is_file()}
    successor = loader(reference=retained.reference, state_root=retained.state_dir.parent,
        anchor_dir=retained.anchor_dir)
    assert {path: path.read_bytes() for root in (retained.state_dir, retained.anchor_dir)
        for path in root.rglob('*') if path.is_file()} == before
    selected = retained.release.refresh.participant_id
    guard, = (row for row in successor.migration.guards if row.participant_id == selected)
    old_guard, = (row for row in retained.inputs.previous.guards if row.participant_id == selected)
    kube = connection[0].kube
    assert _stable(guard.controller) == _stable(kube.objects['deployment', 'loom-control-plane'])
    assert _uid(guard.controller) == _uid(old_guard.controller)
    assert guard.controller['metadata']['annotations']['loom.nebius/participant-candidate-operation'] == str(retained.reference.operation_id)
    assert _stable(guard.database.statefulset) == _stable(kube.objects['statefulset', 'loom-postgres'])
    assert _stable(guard.database.service) == _stable(kube.objects['service', 'loom-postgres'])
    assert guard.database.credential_uid == old_guard.database.credential_uid
    assert guard.database.credential_resource_version == old_guard.database.credential_resource_version
    assert tuple(row for row in successor.migration.guards if row.participant_id != selected) == tuple(
        row for row in retained.inputs.previous.guards if row.participant_id != selected)
    assert successor.migration.registration.spec == retained.release.refresh.after
    assert successor.manager['spec']['template']['spec']['volumes'] != retained.inputs.manager['spec']['template']['spec']['volumes']
    assert successor.catalog['metadata']['name'] == 'loom-pool-profiles-' + retained.reference.operation_id.hex
    assert retained.state_dir / 'completion/release.json' in successor.history
    assert retained.state_dir / 'pool-refresh/dispatch.json' in successor.history
    assert retained.state_dir.parent / 'latest.json' not in successor.history

    # Prepare another real release using the receipt-derived starting point,
    # including the updated manager/catalog and the unchanged peer.
    resources = {_resource_path(row): row for row in successor.resources.values()}
    cluster = successor.migration.registration.binding.kube_system_uid

    def read(request):
        assert request.method == 'GET'
        if request.url.path == '/api/v1/namespaces/kube-system':
            return httpx.Response(200, json={'apiVersion': 'v1', 'kind': 'Namespace',
                'metadata': {'name': 'kube-system', 'uid': cluster}})
        row = resources.get(request.url.path)
        return httpx.Response(404) if row is None else httpx.Response(200, json=row)

    bindings = SimpleNamespace(plan=SimpleNamespace(development=SimpleNamespace(runtime=SimpleNamespace(
        request=SimpleNamespace(database=SimpleNamespace(manager=SimpleNamespace(deployment=SimpleNamespace(
            public_host='manage.example.test'))))))))
    # Once a selected release has completed, its retained render defines the
    # baseline even if the installed tooling no longer contains those templates.
    newer_tooling = tmp_path / 'newer-tooling-without-original-templates'
    newer_tooling.mkdir()
    with monkeypatch.context() as patch, ManagementKubernetesTransport(
            api_server=retained.inputs.config['kubernetes_api_server'], ssl_context=ssl.create_default_context()) as transport:
        patch.setattr(census, '_forward_options', lambda *_: pytest.fail('next release cannot replay the initial baseline'))
        transport.client.close()
        transport.client = httpx.Client(base_url=transport.api_server, transport=httpx.MockTransport(read))
        captured = capture_participant_release(predecessor=successor, bindings=bindings,
            shared_state=operation_inputs['predecessor'].state_dir, repo_root=newer_tooling, transport=transport)
    assert captured.manager == successor.manager and captured.catalog == successor.catalog
    assert {_key(row): row for row in captured.current} == {
        _key(row): successor.resources[_key(row)] for row in retained.inputs.current if row['kind'] != 'Job'}
    inputs = copy.deepcopy(operation_inputs)
    inputs.update(predecessor=successor, operation_id=uuid4(), manager=captured.manager,
        catalog=captured.catalog, current=captured.current)
    from tests.support.execution_image_admission import signed_image_admission_bundle

    from loom.nebius_platform_render import build_platform
    from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

    inputs['candidate'].update(candidate_sha='e' * 40, run_id=813)
    for image in inputs['candidate']['images'].values():
        image['image_ref'] = image['image_ref'].split('@')[0] + '@sha256:' + 'f' * 64
    refs = tuple(inputs['candidate']['images'][name]['image_ref'] for name in ('service', 'execution_runtime'))
    profile = inputs['runtime_profile'].model_dump(mode='json')
    profile.update(candidate_sha='e' * 40, task_image_ref=refs[0], runtime_image_ref=refs[1],
        image_admission=signed_image_admission_bundle(refs).model_dump(mode='json'))
    inputs['runtime_profile'] = ServiceExecutionRuntimeProfileV1.model_validate(profile)
    inputs['source'] = inputs['source'].model_copy(update={'source_sha': 'e' * 40})
    inputs['publication'] = inputs['publication'].model_copy(update={'source_sha': 'e' * 40, 'run_id': 813})
    inputs['rendered_files'] = build_platform(inputs['config'], inputs['candidate'], profile,
        successor.migration.registration.spec.profiles.image_admission_keyring, repo_root=inputs['repo_root'])
    following = operation.retain_participant_rollout(**inputs)
    assert original_loader(initial=operation_inputs['predecessor'], retained=following) == successor
    assert following.inputs.previous == successor.migration
    assert following.release.refresh.before == retained.release.refresh.after
    assert following.release.refresh.after.policy_revision == retained.release.refresh.after.policy_revision + 1
    assert loader(reference=retained.reference, state_root=retained.state_dir.parent,
        anchor_dir=retained.anchor_dir) == successor
    # Loading recovery's predecessor is independent of the mutable current head.
    head_options = dict(initial=operation_inputs['predecessor'], state_root=retained.state_dir.parent,
        anchor_dir=retained.anchor_dir)
    completion.initialize_participant_head(**head_options)
    completion.promote_participant_head(**head_options, expected=operation_inputs['predecessor'], reference=retained.reference)
    assert original_loader(initial=operation_inputs['predecessor'], retained=retained) == operation_inputs['predecessor']
    assert original_loader(initial=operation_inputs['predecessor'], retained=following) == successor
    before = {path: path.read_bytes() for root in (retained.state_dir.parent, retained.anchor_dir)
        for path in root.rglob('*') if path.is_file()}
    original_loader(initial=operation_inputs['predecessor'], retained=following)
    assert {path: path.read_bytes() for root in (retained.state_dir.parent, retained.anchor_dir)
        for path in root.rglob('*') if path.is_file()} == before
    marker = retained.anchor_dir / (str(retained.reference.operation_id) + '-participant-rollout.json')
    marker.unlink()
    with pytest.raises(ValueError, match='predecessor'):
        original_loader(initial=operation_inputs['predecessor'], retained=following)
    assert not marker.exists(), 'recovery loading must not reconstruct a lost predecessor anchor'


@pytest.mark.timeout(120)
def test_completed_predecessor_rejects_pending_or_changed_history(
        operation_inputs, connection, tmp_path, monkeypatch):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_pool_participant_completion as completion

    _exercise_release(
        operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)
    loader = getattr(completion, 'load_completed_participant_rollout', None)
    assert callable(loader), 'a released flag alone is not completed predecessor evidence'
    retained = connection[0].retained

    def load():
        return loader(reference=retained.reference, state_root=retained.state_dir.parent,
            anchor_dir=retained.anchor_dir)

    baseline = load()
    record_path = retained.state_dir / 'completion/release.json'
    source = record_path.read_bytes()
    for status in ('prepared', 'release_intent'):
        record = json.loads(source)
        record['status'] = status
        private_state._atomic_json(record_path, record)
        with pytest.raises(ValueError, match='completed participant'):
            load()
    private_state._atomic_json(record_path, json.loads(source))
    for path in (record_path, retained.anchor_dir / (str(retained.reference.operation_id) + '-completion.json'),
            retained.state_dir / 'pool-refresh/dispatch.json', retained.state_dir / 'inputs.json',
            next(iter(retained.inputs.history))):
        original = path.read_bytes()
        path.unlink()
        with pytest.raises(ValueError, match='completed participant'):
            load()
        assert not path.exists(), 'history loading must not recreate missing evidence'
        private_state._write_private(path, original)
        private_state._atomic_json(path, {'altered': True})
        with pytest.raises(ValueError, match='completed participant'):
            load()
        private_state._atomic_json(path, json.loads(original))
    assert load() == baseline


@pytest.mark.timeout(180)
def test_latest_participant_head_advances_only_from_exact_completed_predecessor(
        operation_inputs, connection, tmp_path, monkeypatch):
    from scripts.ops import nebius_certificates as private_state
    from scripts.ops import nebius_pool_participant_completion as completion

    initialize = getattr(completion, 'initialize_participant_head', None)
    assert callable(initialize), 'normal releases need a retained compare-and-swap head'
    initial = operation_inputs['predecessor']
    retained = connection[0].retained
    options = dict(initial=initial, state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir)
    assert initialize(**options) == initial
    assert initialize(**options) == initial
    pointer = retained.state_dir.parent / 'latest.json'
    initial_bytes = pointer.read_bytes()
    with pytest.raises(ValueError, match='participant head'):
        completion.promote_participant_head(**options, expected=initial, reference=retained.reference)
    assert pointer.read_bytes() == initial_bytes
    _exercise_release(
        operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)

    wrong = replace(initial, state_dir=tmp_path)
    with pytest.raises(ValueError, match='participant head'):
        completion.promote_participant_head(**options, expected=wrong, reference=retained.reference)
    assert pointer.read_bytes() == initial_bytes
    # A committed pointer update whose acknowledgement is lost must be safe to
    # retry; the completed release's frozen history must not contain that pointer.
    atomic = private_state._atomic_json

    def lose_ack(path, payload):
        atomic(path, payload)
        if path == pointer:
            raise OSError('simulated head-promotion acknowledgement loss')

    with monkeypatch.context() as patch:
        patch.setattr(private_state, '_atomic_json', lose_ack)
        with pytest.raises(ValueError, match='participant head'):
            completion.promote_participant_head(**options, expected=initial, reference=retained.reference)
    successor = completion.promote_participant_head(**options, expected=initial, reference=retained.reference)
    promoted_bytes = pointer.read_bytes()
    assert promoted_bytes != initial_bytes
    assert completion.load_participant_head(**options) == successor
    assert pointer not in successor.history
    assert completion.promote_participant_head(**options, expected=initial, reference=retained.reference) == successor
    assert pointer.read_bytes() == promoted_bytes
    assert initialize(**options) == successor, 'initialization must never rewind a completed head'
    with pytest.raises(ValueError, match='participant head'):
        completion.promote_participant_head(**options, expected=successor, reference=retained.reference)
    assert pointer.read_bytes() == promoted_bytes
    # Even a fully completed competing release from the same old predecessor
    # cannot overwrite the already advanced head. Exercise the real receipt
    # loader for both releases, not a mocked completion object.
    from tests.ops import test_nebius_pool_participant_adapter as transport

    competing_inputs = copy.deepcopy(operation_inputs)
    competing_inputs['operation_id'] = uuid4()
    competing_path = tmp_path / 'competing'
    competing_path.mkdir()
    with monkeypatch.context() as patch:
        competing = transport.connection.__wrapped__(competing_inputs, patch)
        _exercise_release(
            competing_inputs, competing, competing_path, patch, recover=False, file_writes=True)
    with pytest.raises(ValueError, match='participant head'):
        completion.promote_participant_head(**options, expected=initial, reference=competing[0].retained.reference)
    assert pointer.read_bytes() == promoted_bytes
    pointer.unlink()
    with pytest.raises(ValueError, match='participant head'):
        initialize(**options)
    assert not pointer.exists(), 'lost latest pointer must not reset to the initial handoff'
    private_state._write_private(pointer, promoted_bytes)
    value = json.loads(promoted_bytes)
    value['selected']['operation_id'] = str(uuid4())
    private_state._atomic_json(pointer, value)
    with pytest.raises(ValueError, match='participant head'):
        completion.load_participant_head(**options)
    private_state._atomic_json(pointer, json.loads(promoted_bytes))
    # Re-reading old immutable history remains possible after pointer movement.
    assert completion.load_completed_participant_rollout(reference=retained.reference,
        state_root=retained.state_dir.parent, anchor_dir=retained.anchor_dir) == successor


@pytest.mark.timeout(180)
@pytest.mark.parametrize('failure', ['before-promotion', 'after-promotion'])
def test_deployment_reports_success_only_after_history_promotion(
        operation_inputs, connection, tmp_path, monkeypatch, failure):
    from scripts.ops import nebius_pool_participant_entry as entry
    from scripts.ops.nebius_pool_participant_completion import (
        initialize_participant_head,
        load_participant_head,
        promote_participant_head,
    )

    assert hasattr(entry, 'run_participant_release'), 'protected delivery does not finalize actual rollout history'
    retained = connection[0].retained
    options = dict(initial=operation_inputs['predecessor'], state_root=retained.state_dir.parent,
        anchor_dir=retained.anchor_dir)
    initialize_participant_head(**options)
    finalized, statuses, arguments = [], [], []

    def promote(**kwargs):
        finalized.append(True)
        if failure == 'before-promotion' and len(finalized) == 1:
            raise ValueError('simulated promotion failure')
        result = promote_participant_head(**kwargs)
        if failure == 'after-promotion' and len(finalized) == 1:
            raise ValueError('simulated promotion acknowledgement loss')
        return result

    monkeypatch.setattr(entry, 'promote_participant_head', promote)
    original_rollout = idle.rollout

    def connected(args, **kwargs):
        arguments.append((args, kwargs))
        github = idle.github

        def report(path, payload):
            if path.endswith('/statuses'):
                statuses.append(payload['state'])
                if payload['state'] == 'success':
                    assert finalized
                    assert load_participant_head(**options).retained.reference == retained.reference
            return github(path, payload)

        with monkeypatch.context() as patch:
            patch.setattr(idle, 'github', report)
            patch.setattr(idle, 'rollout', original_rollout)
            kube = connection[0].kube
            kube.kubeconfig = args.kubeconfig
            return entry.run_participant_release(args, retained=retained,
                initial=options['initial'], expected=options['initial'],
                pool=kwargs['pool'], writer=kwargs['candidate_writer'], kube=kube,
                candidate_ancestry=idle.candidate_follows)

    monkeypatch.setattr(idle, 'rollout', connected)
    if failure:
        with pytest.raises(ValueError, match='simulated promotion'):
            _exercise_release(
                operation_inputs, connection, tmp_path, monkeypatch, recover=False, file_writes=True)
        assert statuses == ['in_progress', 'failure']
        args, kwargs = arguments[-1]
        failed, = args.evidence_dir.glob('deployment-*.json')
        evidence = json.loads(failed.read_bytes())
        assert evidence['status'] == 'failed' and evidence['dispatch_paused'] is False
        assert idle.recovery_record(evidence)['shared_pool_release'] == retained.reference.model_dump(mode='json')
        args.recovery_evidence = failed
        effects = list(connection[0].effects)
        assert connected(args, **kwargs)['status'] == 'complete'
        assert connection[0].effects == effects, 'promotion recovery must not replay pool effects'
        assert len(finalized) == 2
    assert statuses[-2:] == ['in_progress', 'success']
