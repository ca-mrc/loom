from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tests.cluster import test_nebius_management_refresh as native_refresh
from tests.cluster.test_nebius_management_refresh import _add_refresh_diagnostics

NAMESPACE = 'owned-management'
PRIVATE = 'https://private.invalid/ token=do-not-record-secret'


def metadata(name='loom-service'):
    return SimpleNamespace(name=name, uid='native-uid', generation=3, annotations={'private': PRIVATE})


def condition():
    return SimpleNamespace(type='ReplicaFailure', status='True', reason='FailedCreate', message=PRIVATE)


def workload():
    return SimpleNamespace(metadata=metadata(), spec=SimpleNamespace(replicas=0, template={'env': PRIVATE}),
        status=SimpleNamespace(observed_generation=2, replicas=0, updated_replicas=0,
            ready_replicas=0, available_replicas=0, conditions=[condition()]))


def pod():
    waiting = SimpleNamespace(reason='CreateContainerConfigError', message=PRIVATE)
    container = SimpleNamespace(name='service', ready=False, state=SimpleNamespace(waiting=waiting))
    return SimpleNamespace(metadata=metadata('loom-service-rs-pod'), spec={'env': PRIVATE},
        status=SimpleNamespace(phase='Pending', conditions=[condition()],
            init_container_statuses=[], container_statuses=[container]))


def event(name='loom-service-rs', *, namespace=NAMESPACE, event_type='Warning'):
    return SimpleNamespace(metadata=metadata('loom-service-rs.event'), type=event_type,
        involved_object=SimpleNamespace(kind='ReplicaSet', name=name, uid='rs-uid', namespace=namespace),
        reason='FailedCreate', count=2, message=PRIVATE)


def clients():
    core, apps = Mock(), Mock()
    apps.read_namespaced_deployment.return_value = workload()
    apps.list_namespaced_replica_set.return_value = SimpleNamespace(items=[workload()])
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[pod()])
    core.list_namespaced_event.return_value = SimpleNamespace(items=[event()])
    return core, apps


def note(error, label):
    prefix = f'native refresh {label}: '
    return json.loads(next(value.removeprefix(prefix) for value in error.__notes__ if value.startswith(prefix)))


def test_refresh_diagnostics_retain_controller_identity_and_failure_without_specs():
    core, apps = clients()
    core.list_namespaced_event.return_value.items.extend([
        event('unrelated-controller'), event(namespace='foreign-management'), event(event_type='Normal')])
    error = AssertionError('disposable Deployment did not create a Pod')

    _add_refresh_diagnostics(core, apps, NAMESPACE, 0, error)

    assert error.__notes__[0] == 'native refresh iteration: 0'
    deployment = note(error, 'Deployment')
    assert deployment['metadata'] == {'name': 'loom-service', 'uid': 'native-uid', 'generation': 3}
    assert deployment['desired_replicas'] == 0
    assert deployment['status']['observed_generation'] == 2
    assert deployment['status']['replicas'] == 0
    assert deployment['conditions'] == [{'type': 'ReplicaFailure', 'status': 'True', 'reason': 'FailedCreate'}]
    assert note(error, 'ReplicaSets')[0]['conditions'] == deployment['conditions']
    observed_pod = note(error, 'Pods')[0]
    assert observed_pod['phase'] == 'Pending'
    assert observed_pod['containers'] == [{'name': 'service', 'ready': False,
        'waiting_reason': 'CreateContainerConfigError'}]
    warnings = note(error, 'Warning events')
    assert len(warnings) == 1
    assert warnings[0]['reason'] == 'FailedCreate'
    assert warnings[0]['object'] == {'kind': 'ReplicaSet', 'name': 'loom-service-rs', 'uid': 'rs-uid'}
    text = '\n'.join(error.__notes__)
    assert PRIVATE not in text
    assert all(value not in text for value in ('private.invalid', 'do-not-record-secret', 'template', 'env', 'annotations', 'message'))
    apps.read_namespaced_deployment.assert_called_once_with('loom-service', NAMESPACE, _request_timeout=(2, 3))
    apps.list_namespaced_replica_set.assert_called_once_with(NAMESPACE,
        label_selector='app=loom-service', limit=64, _request_timeout=(2, 3))
    core.list_namespaced_pod.assert_called_once_with(NAMESPACE,
        label_selector='app=loom-service', limit=64, _request_timeout=(2, 3))
    core.list_namespaced_event.assert_called_once_with(NAMESPACE,
        field_selector='type=Warning', limit=64, _request_timeout=(2, 3))


def test_refresh_diagnostics_all_failed_reads_preserve_original_and_unknown_iteration():
    core, apps = clients()
    reads = [apps.read_namespaced_deployment, apps.list_namespaced_replica_set,
        core.list_namespaced_pod, core.list_namespaced_event]
    for read in reads:
        read.side_effect = TimeoutError(PRIVATE)
    original = AssertionError('original Pod creation assertion')

    with pytest.raises(AssertionError) as caught:
        try:
            raise original
        except AssertionError as error:
            _add_refresh_diagnostics(core, apps, NAMESPACE, None, error)
            raise

    assert caught.value is original
    assert str(caught.value) == 'original Pod creation assertion'
    assert original.__notes__[0] == 'native refresh iteration: null'
    assert all(value.endswith('TimeoutError') for value in original.__notes__[1:])
    assert len(original.__notes__) == 5
    assert PRIVATE not in '\n'.join(original.__notes__)
    for read in reads:
        assert read.call_count == 1
        assert read.call_args.kwargs['_request_timeout'] == (2, 3)


def test_refresh_diagnostics_malformed_snapshot_does_not_block_other_reads():
    core, apps = clients()
    apps.read_namespaced_deployment.return_value = SimpleNamespace(metadata=None)
    core.list_namespaced_pod.return_value = SimpleNamespace(items=[SimpleNamespace(status=None)])
    error = AssertionError('original failure')

    _add_refresh_diagnostics(core, apps, NAMESPACE, 1, error)

    assert 'native refresh Deployment: AttributeError' in error.__notes__
    assert 'native refresh Pods: AttributeError' in error.__notes__
    assert note(error, 'ReplicaSets')[0]['metadata']['uid'] == 'native-uid'
    assert note(error, 'Warning events')[0]['reason'] == 'FailedCreate'
    early = AssertionError('client initialization failed')
    _add_refresh_diagnostics(None, None, NAMESPACE, None, early)
    assert early.__notes__[0] == 'native refresh iteration: null'
    assert all(value.endswith('AttributeError') for value in early.__notes__[1:])


def test_refresh_diagnostics_bound_records_conditions_and_container_statuses():
    core, apps = clients()
    native = workload()
    native.status.conditions *= 40
    apps.read_namespaced_deployment.return_value = native
    pending = pod()
    pending.status.container_statuses *= 40
    core.list_namespaced_pod.return_value.items = [pending]
    core.list_namespaced_event.return_value.items *= 100
    error = AssertionError('original failure')

    _add_refresh_diagnostics(core, apps, NAMESPACE, 0, error)

    assert len(note(error, 'Deployment')['conditions']) == 16
    assert len(note(error, 'Pods')[0]['containers']) == 32
    assert len(note(error, 'Warning events')) == 64
    assert all(len(value) < 17000 for value in error.__notes__)


def test_refresh_interrupt_skips_diagnostics_and_still_stops_owned_cluster(monkeypatch, tmp_path):
    cluster = Mock()
    interrupt = KeyboardInterrupt('cancel native test')
    start = Mock(return_value=cluster)
    load = Mock(side_effect=interrupt)
    diagnostics = Mock()
    monkeypatch.setattr(native_refresh, '_start_k3s', start)
    monkeypatch.setattr(native_refresh, '_load_client', load)
    monkeypatch.setattr(native_refresh, '_add_refresh_diagnostics', diagnostics)
    request = SimpleNamespace(after=SimpleNamespace(namespace=NAMESPACE))

    with pytest.raises(KeyboardInterrupt) as caught:
        native_refresh.test_repeat_refresh_preserves_retained_identity_and_observes_native_drain(request, tmp_path)

    assert caught.value is interrupt
    diagnostics.assert_not_called()
    start.assert_called_once_with(ephemeral_storage_floor='2Gi')
    load.assert_called_once_with(cluster, required_addons=('dns',))
    cluster.stop.assert_called_once_with()
