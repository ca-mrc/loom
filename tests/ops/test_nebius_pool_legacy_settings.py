"""Restored templates must correspond to actual non-global process settings."""
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


@pytest.fixture(params=['controller', 'actuator', 'guest', 'service', 'manager'])
def legacy_case(request, guest_runtime_inputs):
    from scripts.ops.nebius_pool_runtime import wire_participant

    migration, actuators, services, manager, guest = guest_runtime_inputs
    target = migration.guards[0]
    component = 'actuator' if request.param == 'guest' else request.param
    original = copy.deepcopy({'controller': target.controller, 'actuator': actuators[target.participant_id],
        'guest': guest, 'service': services[target.participant_id], 'manager': manager}[request.param])
    environment = {key: value for key, value in os.environ.items() if not key.startswith(('LOOM_', 'DATABASE_'))}
    environment.update(LOOM_CP_DB_URL='postgresql+psycopg://fixture:private-db-marker@localhost/loom',
        LOOM_CP_MINIO_ACCESS_KEY='fixture', LOOM_CP_MINIO_SECRET_KEY='fixture', LOOM_CP_STEP_JWT_SIGNING_KEY='private-test-key',
        LOOM_SVC_DB_URL='postgresql+psycopg://fixture:private-db-marker@localhost/loom',
        LOOM_SVC_MINIO_ACCESS_KEY='fixture', LOOM_SVC_MINIO_SECRET_KEY='fixture',
        LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN='private-github-marker',
        LOOM_EXECUTION_ACTUATOR_DB_URL='postgresql+psycopg://fixture:private-db-marker@localhost/loom',
        LOOM_EXECUTION_ACTUATOR_CONTROLLER_ID='fixture-actuator')
    rows = env(original)
    if component == 'controller':
        # The renderer fixture deliberately passes {} for trust; that shape is
        # not a valid installed keyring. Supply a valid retained configuration.
        rows['LOOM_CP_EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON']['value'] = '{"schema_version":1,"keys":[]}'
    environment.update({name: row['value'] for name, row in rows.items() if 'value' in row})
    expected = {'component': component}
    if component in {'controller', 'actuator'}:
        prefix = 'LOOM_CP_' if component == 'controller' else 'LOOM_EXECUTION_ACTUATOR_'
        expected.update(global_pool=None, image_admission_keyring=json.loads(rows.get(
            prefix + 'EXECUTION_IMAGE_ADMISSION_PUBLIC_KEYS_JSON', {'value': '{"schema_version":1,"keys":[]}'})['value']))
        if component == 'controller':
            expected.update(scheduler_enabled=True, materializer_enabled=True)
        else:
            builder = rows.get(prefix + 'TASK_IMAGE_BUILDER')
            expected.update(namespace=migration.registration.spec.participants[0].execution_namespace.name,
                target_id='nebius-guest-fixture' if request.param == 'guest' else 'pool-target-0',
                task_image_builder=None if builder is None else json.loads(builder['value']), kubernetes_connection=None)
    else:
        expected.update(mode='management' if component == 'manager' else 'application',
            submission_source=None, catalog_path=None)
        if component == 'service':
            expected['runtime_profile'] = json.loads(rows['LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON']['value'])
    successors = wire_participant(request=migration, participant_id=target.participant_id,
        management_origin='https://manage.example.com', actuator=actuators[target.participant_id],
        service=services[target.participant_id], guest_actuators=(guest,),
        runtime_profile=desired_profile(migration, services[target.participant_id]))
    variable = {'controller': 'LOOM_CP_SERVICE_EXECUTION_GLOBAL_POOL_JSON', 'actuator': 'LOOM_EXECUTION_ACTUATOR_GLOBAL_POOL',
        'service': 'LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON', 'manager': 'LOOM_SVC_POOL_PROFILES_FILE'}[component]
    successor_value = ('/private-profiles.json' if component == 'manager' else
        env(successors[{'controller': 'control_plane', 'guest': 'guest_actuator'}.get(request.param, request.param)])[variable]['value'])
    return component, original, environment, expected, variable, successor_value


def probe(legacy_case, tmp_path, *, expected=None):
    from scripts.ops.nebius_pool_legacy_settings import BOUND_LEGACY_SETTINGS_COMMAND

    component, _, environment, wanted, _, _ = legacy_case
    nonce = 'cd' * 32
    payload = json.dumps(wanted if expected is None else expected, sort_keys=True, separators=(',', ':')).encode()
    signature = hmac.new(bytes.fromhex(nonce), payload, hashlib.sha256).hexdigest()
    return subprocess.run([sys.executable, '-c', BOUND_LEGACY_SETTINGS_COMMAND, component, nonce, signature],
        env=environment, cwd=tmp_path, capture_output=True, timeout=30, check=False)


def test_legacy_settings_challenge_uses_real_loaders_without_machine_material(legacy_case, tmp_path):
    from scripts.ops.nebius_pool_legacy_settings import expected_legacy_runtime_settings

    component, original, _, expected, _, _ = legacy_case
    assert expected_legacy_runtime_settings(component, original) == expected
    result = probe(legacy_case, tmp_path)
    assert (result.returncode, result.stdout, result.stderr) == (0, b'{"status": "qualified"}\n', b'')


@pytest.mark.parametrize('damage', ['successor', 'effective_settings', 'challenge'])
def test_legacy_probe_refuses_successor_or_changed_runtime_without_printing_settings(legacy_case, tmp_path, damage):
    component, _, environment, expected, variable, successor_value = legacy_case
    if damage == 'successor':
        environment[variable] = successor_value
    elif damage == 'effective_settings':
        name, value = {'controller': ('LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED', 'false'),
            'actuator': ('LOOM_EXECUTION_ACTUATOR_TARGET_ID', 'foreign-target'),
            'service': ('LOOM_SVC_SERVICE_MODE', 'api_only'),
            'manager': ('LOOM_SVC_SERVICE_MODE', 'application')}[component]
        environment[name] = value
    else:
        expected = {**expected, 'unexpected': 'private-challenge-marker'}
    result = probe(legacy_case, tmp_path, expected=expected)
    assert (result.returncode, result.stdout, result.stderr) == (1, b'', b'Legacy runtime settings unqualified\n')


@pytest.mark.parametrize('damage', ['successor', 'duplicate', 'env_from', 'indirect'])
def test_legacy_projection_refuses_global_or_ambiguous_templates(legacy_case, damage):
    from scripts.ops.nebius_pool_legacy_settings import expected_legacy_runtime_settings

    component, original, _, _, variable, successor_value = legacy_case
    container, = original['spec']['template']['spec']['containers']
    if damage == 'successor':
        container['env'].append({'name': variable, 'value': successor_value})
    elif damage == 'duplicate':
        container['env'].append(copy.deepcopy(container['env'][0]))
    elif damage == 'env_from':
        container['envFrom'] = [{'secretRef': {'name': 'private-marker'}}]
    else:
        container['env'].append({'name': variable, 'valueFrom': {'secretKeyRef': {'name': 'private-marker', 'key': 'setting'}}})
    with pytest.raises(ValueError) as error:
        expected_legacy_runtime_settings(component, original)
    assert 'private-' not in str(error.value)
