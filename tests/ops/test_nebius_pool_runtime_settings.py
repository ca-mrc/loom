"""Actual settings/file readers; no database, cloud or Kubernetes writes."""
from __future__ import annotations

import copy
import hashlib
import hmac
import json
import os
import subprocess
import sys

import pytest
from tests.ops.test_nebius_pool_runtime import desired_profile, env
from tests.ops.test_nebius_pool_runtime import guest_runtime_inputs as guest_runtime_inputs
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture(params=['controller', 'actuator', 'guest', 'service', 'manager', 'gateway'])
def settings_case(request, guest_runtime_inputs, tmp_path):
    from scripts.ops.nebius_pool_runtime import wire_manager, wire_participant

    from loom_service.pool_management.installation_render import render_gateway

    migration, actuators, services, manager, guest = guest_runtime_inputs
    participant = migration.registration.spec.participants[0]
    component = 'actuator' if request.param == 'guest' else request.param
    targets = wire_participant(request=migration, participant_id=participant.participant_id,
        management_origin='https://manage.example.com', actuator=actuators[participant.participant_id],
        service=services[participant.participant_id], guest_actuators=(guest,),
        runtime_profile=desired_profile(migration, services[participant.participant_id]))
    if component == 'gateway':
        target, = render_gateway(migration.registration.spec, namespace=migration.registration.binding.namespace,
            service_image=migration.registration.candidate['images']['service']['image_ref'],
            kubernetes_endpoint='https://kubernetes.default.svc')['workload']
    else:
        target = (wire_manager(request=migration, original=manager) if component == 'manager'
            else targets[{'controller': 'control_plane', 'guest': 'guest_actuator'}.get(request.param, request.param)])
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    environment.update(LOOM_CP_DB_URL='postgresql+psycopg://fixture:private-db-marker@localhost/loom',
        LOOM_CP_MINIO_ACCESS_KEY='fixture', LOOM_CP_MINIO_SECRET_KEY='fixture', LOOM_CP_STEP_JWT_SIGNING_KEY='private-test-key',
        LOOM_SVC_DB_URL='postgresql+psycopg://fixture:private-db-marker@localhost/loom',
        LOOM_SVC_MINIO_ACCESS_KEY='fixture', LOOM_SVC_MINIO_SECRET_KEY='fixture',
        LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN='private-github-marker',
        LOOM_EXECUTION_ACTUATOR_DB_URL='postgresql+psycopg://fixture:private-db-marker@localhost/loom',
        LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID='fixture-actuator',
        LOOM_POOL_GATEWAY_DB_URL='postgresql+psycopg://fixture:private-db-marker@localhost/loom')
    token = tmp_path / 'token'
    token.write_text('private-machine-marker')
    token.chmod(0o600)
    catalog = tmp_path / 'profiles.json'
    catalog.write_text(migration.registration.spec.profiles.model_dump_json())
    rows = env(target)
    options = {}
    expected = {'component': component}
    if component in {'controller', 'actuator'}:
        variable = 'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON' if component == 'controller' else 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL'
        pool = json.loads(rows[variable]['value'])
        pool['bearer_token_file'] = str(token)
        rows[variable]['value'] = json.dumps(pool)
        options['token_sha256'] = hashlib.sha256(b'private-machine-marker').hexdigest()
        expected.update(global_pool=pool, token_sha256=options['token_sha256'],
            image_admission_keyring=migration.registration.spec.profiles.image_admission_keyring)
        if component == 'controller':
            expected.update(scheduler_enabled=True, materializer_enabled=True)
        else:
            builder = rows.get('LOOM_EXECUTION_ACTUATOR_TASK_IMAGE_BUILDER')
            expected.update(namespace=participant.execution_namespace.name,
                target_id=rows['LOOM_EXECUTION_ACTUATOR_TARGET_ID']['value'],
                task_image_builder=None if builder is None else json.loads(builder['value']), kubernetes_connection=None)
    elif component == 'service':
        expected.update(mode='application', submission_source={'schema_version': 'loom.pool-submission-source.v1', 'kind': 'environment',
            'data_environment_id': str(participant.environment_id), 'application': None},
            runtime_profile=json.loads(rows['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON']['value']))
    elif component == 'manager':
        rows['LOOM_SVC_POOL_PROFILES_FILE']['value'] = str(catalog)
        options['catalog_sha256'] = hashlib.sha256(catalog.read_bytes()).hexdigest()
        expected.update(mode='management', catalog_path=str(catalog), catalog_sha256=options['catalog_sha256'])
    else:
        rows['LOOM_POOL_GATEWAY_BEARER_TOKEN_FILE']['value'] = str(token)
        options['token_sha256'] = hashlib.sha256(b'private-machine-marker').hexdigest()
        machine, = (row for row in migration.registration.spec.machines if row.role == 'gateway')
        expected.update(pool_id=str(migration.registration.spec.pool_id), installation_id=str(migration.registration.spec.installation_id),
            machine_id=str(machine.machine_id), admission_epoch=migration.registration.spec.admission_epoch,
            bearer_token_file=str(token), token_sha256=options['token_sha256'],
            kubernetes=json.loads(rows['LOOM_POOL_GATEWAY_KUBERNETES']['value']))
    environment.update({name: row['value'] for name, row in rows.items() if 'value' in row})
    return component, target, environment, options, expected, token, catalog


def run_probe(settings_case, tmp_path, *, expected=None):
    from scripts.ops.nebius_pool_runtime_settings import BOUND_POOL_SETTINGS_COMMAND

    component, _, environment, _, wanted, _, _ = settings_case
    nonce = 'ab' * 32
    payload = json.dumps(wanted if expected is None else expected, sort_keys=True, separators=(',', ':')).encode()
    signature = hmac.new(bytes.fromhex(nonce), payload, 'sha256').hexdigest()
    return subprocess.run([sys.executable, '-c', BOUND_POOL_SETTINGS_COMMAND, component, nonce, signature],
        env=environment, cwd=tmp_path, capture_output=True, timeout=30, check=False)


def test_fixed_settings_probe_uses_actual_loaded_configuration_and_private_material(settings_case, tmp_path):
    from scripts.ops.nebius_pool_runtime_settings import expected_pool_runtime_settings

    component, target, _, options, expected, _, _ = settings_case
    # Independently derived from the real renderer, not the projection under test.
    assert expected_pool_runtime_settings(component, target, **options) == expected
    result = run_probe(settings_case, tmp_path)
    assert (result.returncode, result.stdout, result.stderr) == (0, b'{"status": "qualified"}\n', b'')


@pytest.mark.parametrize('damage', ['settings', 'missing', 'challenge'])
def test_fixed_settings_probe_rejects_disabled_or_different_runtime_without_exposing_values(settings_case, tmp_path, damage):
    component, _, environment, _, expected, _, _ = settings_case
    variable = {'controller': 'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', 'actuator': 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL',
        'service': 'LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON', 'manager': 'LOOM_SVC_POOL_PROFILES_FILE',
        'gateway': 'LOOM_POOL_GATEWAY_KUBERNETES'}[component]
    if damage == 'missing':
        environment.pop(variable)
    elif damage == 'settings':
        if component == 'manager':
            environment['LOOM_SVC_SERVICE_MODE'] = 'application'
        elif component == 'gateway':
            environment['LOOM_POOL_GATEWAY_MACHINE_ID'] = 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa'
        else:
            value = json.loads(environment[variable])
            value['management_origin' if component != 'service' else 'data_environment_id'] = (
                'https://foreign.example.com' if component != 'service' else 'aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa')
            environment[variable] = json.dumps(value)
    else:
        expected = {**expected, 'foreign': 'private-challenge-marker'}
    result = run_probe(settings_case, tmp_path, expected=expected)
    assert result.returncode == 1 and result.stdout == b''
    assert result.stderr == b'Pool runtime settings unqualified\n'


@pytest.mark.parametrize('settings_case', ['controller', 'actuator', 'guest', 'gateway'], indirect=True)
@pytest.mark.parametrize('damage', ['different', 'missing', 'symlink', 'permissions', 'empty', 'oversized'])
def test_fixed_probe_requires_the_same_owner_only_machine_token(settings_case, tmp_path, damage):
    _, _, _, _, _, token, _ = settings_case
    if damage == 'different':
        token.write_text('private-other-machine-marker')
    elif damage == 'missing':
        token.unlink()
    elif damage == 'symlink':
        source = token.with_suffix('.saved')
        token.rename(source)
        token.symlink_to(source)
    elif damage == 'permissions':
        token.chmod(0o644)
    elif damage == 'empty':
        token.write_bytes(b'')
    else:
        token.write_bytes(b'x' * (16 * 1024 + 1))
    result = run_probe(settings_case, tmp_path)
    assert result.returncode == 1 and result.stdout == b'' and b'private-' not in result.stderr


@pytest.mark.parametrize('settings_case', ['manager'], indirect=True)
@pytest.mark.parametrize('damage', ['changed', 'invalid', 'missing', 'fifo'])
def test_manager_probe_qualifies_the_actual_bounded_profile_catalog(settings_case, tmp_path, damage):
    _, _, _, _, _, _, catalog = settings_case
    if damage == 'changed':
        catalog.write_text(catalog.read_text() + '\n')
    elif damage == 'invalid':
        catalog.write_text('{"private-marker":true}')
    elif damage == 'missing':
        catalog.unlink()
    else:
        catalog.unlink()
        os.mkfifo(catalog)
    result = run_probe(settings_case, tmp_path)
    assert result.returncode == 1 and result.stdout == b'' and b'private-' not in result.stderr


@pytest.mark.parametrize('damage', ['duplicate', 'env_from', 'secret_setting', 'token_scope'])
def test_expected_settings_cannot_use_ambiguous_or_indirect_configuration(settings_case, damage):
    from scripts.ops.nebius_pool_runtime_settings import expected_pool_runtime_settings

    component, target, _, options, _, _, _ = settings_case
    container = target['spec']['template']['spec']['containers'][0]
    if damage == 'duplicate':
        container['env'].append(copy.deepcopy(container['env'][0]))
    elif damage == 'env_from':
        container['envFrom'] = [{'secretRef': {'name': 'foreign'}}]
    elif damage == 'secret_setting':
        variable = {'controller': 'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', 'actuator': 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL',
            'service': 'LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON', 'manager': 'LOOM_SVC_POOL_PROFILES_FILE',
            'gateway': 'LOOM_POOL_GATEWAY_KUBERNETES'}[component]
        row = env(target)[variable]
        row.pop('value')
        row['valueFrom'] = {'secretKeyRef': {'name': 'private-marker', 'key': 'settings'}}
    else:
        options = {**options, 'token_sha256': 'private-invalid-marker'}
    with pytest.raises(ValueError) as error:
        expected_pool_runtime_settings(component, target, **options)
    assert 'private-' not in str(error.value)


def test_fixed_probe_sanitizes_an_unavailable_runtime_module(monkeypatch, capsys):
    import builtins

    from scripts.ops.nebius_pool_runtime_settings import BOUND_POOL_SETTINGS_COMMAND

    original = builtins.__import__
    def unavailable(name, *args, **kwargs):
        if name == 'loom.execution_image_admission':
            raise ValueError('private-image-import-marker')
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', unavailable)
    monkeypatch.setattr(sys, 'argv', ['-c', 'controller', 'ab' * 32, 'cd' * 32])
    with pytest.raises(SystemExit) as error:
        exec(BOUND_POOL_SETTINGS_COMMAND, {})
    captured = capsys.readouterr()
    assert error.value.code == 1 and captured.out == '' and captured.err == 'Pool runtime settings unqualified\n'
