"""Public lifecycle delivery preserves exact authority and uncertain intent."""
from __future__ import annotations

import base64
import copy
import hashlib
import importlib
import io
import json
import os
import shutil
import struct
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from tests.ops.test_nebius_development_management_gateway import archive
from tests.ops.test_nebius_environment_runtime import runtime_request as runtime_request
from tests.ops.test_nebius_pool_runtime import runtime_inputs as runtime_inputs
from tests.support.nebius_platform import platform_inputs as platform_inputs
from tests.unit.test_nebius_management_render import management_inputs as management_inputs


def gateway():
    return importlib.import_module('scripts.ops.nebius_environment_lifecycle_gateway')


def runner():
    return importlib.import_module('scripts.ops.nebius_environment_lifecycle_rollout')


def scope():
    return dict(installation_id=str(uuid4()), pool_id=str(uuid4()), participant_id=str(uuid4()),
        environment_id=str(uuid4()), incarnation=str(uuid4()), authority='development')


def command():
    return dict(schema_version='loom.environment-lifecycle-command.v2', action='apply', scope=scope(), operation_id=str(uuid4()))


def members():
    root = Path(__file__).resolve().parents[2]
    return {**{name: (root / name).read_bytes() for name in (*gateway().CORE_SOURCES, *gateway().RENDER_SOURCES)},
        'uv': b'test-uv', 'requirements.txt': b'example==1 --hash=sha256:' + b'a' * 64,
        'wheels/loom-0.1.0-py3-none-any.whl': b'wheel', 'wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl': b'wheel',
        'lifecycle-tooling.json': json.dumps(dict(schema_version='loom.environment-lifecycle-tooling.v2',
            source_sha='a' * 40, source_archive_sha256='sha256:' + 'b' * 64)).encode()}


@pytest.mark.parametrize('damage', [None, 'private', 'path', 'missing', 'extra-wheel'])
def test_tooling_archive_bounds_code_members_and_never_includes_private_configuration(damage):
    files = members()
    if damage == 'private':
        files['configuration.json'] = b'private'
    elif damage == 'path':
        files['scripts/ops/../../foreign.py'] = b'foreign'
    elif damage == 'missing':
        del files['scripts/ops/nebius_environment_lifecycle_entry.py']
    elif damage == 'extra-wheel':
        files['wheels/foreign-1-py3-none-any.whl'] = b'foreign'
    if damage:
        with pytest.raises(ValueError):
            gateway().unpack_bundle(archive(files))
    else:
        loaded, _ = gateway().unpack_bundle(archive(files))
        assert set(loaded) == set(files) | {'manifest.json'}


def test_installer_preserves_other_authority_and_rejects_key_scope_changes(tmp_path, monkeypatch):
    module = importlib.import_module('scripts.ops.install_nebius_environment_lifecycle_entrypoint')
    owner, ssh = tmp_path / '.loom', tmp_path / '.ssh'
    owner.mkdir(mode=0o700)
    ssh.mkdir(mode=0o700)
    keys = ssh / 'authorized_keys'
    original = b'# retained foreign authority\n'
    keys.write_bytes(original)
    keys.chmod(0o600)
    inputs = owner / 'inputs.json'
    inputs.write_bytes(b'{}')
    inputs.chmod(0o600)
    selected = scope()
    configuration = owner / 'configuration.json'
    config = dict(schema_version='loom.environment-lifecycle-configuration.v2', scope=selected,
        operations={str(uuid4()): dict(kind='enroll', inputs_path=str(inputs), inputs_sha256=hashlib.sha256(b'{}').hexdigest())})
    configuration.write_text(json.dumps(config))
    configuration.chmod(0o600)
    public_key = 'ssh-ed25519 ' + base64.b64encode(struct.pack('>I', 11) + b'ssh-ed25519' + struct.pack('>I', 32) + b'g' * 32).decode()
    files = members()
    files['scripts/ops/nebius_environment_lifecycle_gateway.py'] = b'def authorized_main(*args): return 37\n'
    content = archive(files)
    options = dict(expected_sha256=hashlib.sha256(content).hexdigest(), public_key=public_key,
        configuration=configuration, configuration_sha256=hashlib.sha256(configuration.read_bytes()).hexdigest(), home=tmp_path)
    calls = []

    def run(argv, *, timeout):
        calls.append(argv)
        return b'{"status":"tooling_qualified"}' if argv[1:3] == ['-I', '-c'] else b''

    monkeypatch.setattr(gateway(), 'run_private', run)
    assert module.install(content, **options)['status'] == 'prepared'
    assert keys.read_bytes() == original and not calls
    installed = module.install(content, **options, apply=True)
    assert keys.read_bytes().startswith(original)
    current, before = keys.read_bytes(), list(calls)
    assert module.install(content, **options, apply=True) == installed
    assert keys.read_bytes() == current and calls == before
    entry, = (owner / 'environment-lifecycle-delivery').glob('*/authority/*/entrypoint.py')
    for requested, code in [('loom-environment-lifecycle-apply-v2', 37), ('bash', 126), ('loom-pool-administration-apply-v2', 126)]:
        result = subprocess.run([sys.executable, '-I', '-S', str(entry)],
            env={'PATH': os.defpath, 'SSH_ORIGINAL_COMMAND': requested}, capture_output=True, timeout=10)
        assert result.returncode == code
    changed = dict(config, scope=dict(selected, participant_id=str(uuid4())))
    configuration.write_text(json.dumps(changed))
    options['configuration_sha256'] = hashlib.sha256(configuration.read_bytes()).hexdigest()
    with pytest.raises(ValueError):
        module.install(content, **options, apply=True)
    assert keys.read_bytes() == current


def test_real_lifecycle_entry_loads_from_only_approved_scripts_and_published_wheels(tmp_path, runtime_request):
    from scripts.ops.nebius_ingress_rollout import build_wheels

    executable = shutil.which('uv')
    assert executable is not None
    build = tmp_path / 'build'
    build.mkdir()
    wheels = build_wheels(build, uv=Path(executable))
    requirements = tmp_path / 'requirements.txt'
    requirements.write_text('')
    content = runner().build_bundle(source=dict(source_sha='a' * 40, source_archive_sha256='sha256:' + 'b' * 64),
        uv=Path(executable), requirements=requirements, wheels=wheels)
    files, _ = gateway().unpack_bundle(content)
    root, site = tmp_path / 'source', tmp_path / 'site'
    render_input = tmp_path / 'render.json'
    render_input.write_text(json.dumps({name: value.model_dump(mode='json') if hasattr(value, 'model_dump') else value
        for name, value in runtime_request.items() if name != 'repo_root'}))
    for name, raw in files.items():
        if name.startswith(('scripts/', 'deploy/')):
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        elif name.startswith('wheels/'):
            with zipfile.ZipFile(io.BytesIO(raw)) as wheel:
                wheel.extractall(site)
    result = subprocess.run([sys.executable, '-I', '-c',
        'import sys,pathlib,json;root=pathlib.Path(sys.argv[1]);site=pathlib.Path(sys.argv[2]);'
        'sys.path[:0]=[str(root),str(site)];'
        'from scripts.ops.nebius_environment_lifecycle_entry import main;'
        'from scripts.ops.nebius_environment_lifecycle import execute_lifecycle;'
        'assert callable(main) and callable(execute_lifecycle);'
        'from scripts.ops.nebius_environment_runtime import render_environment_runtime;'
        'from loom.nebius_environment_foundation import EnvironmentFoundationDeclarationV2;'
        'from loom.nebius_pool_contract import PoolParticipantV1;'
        'from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1;'
        'from loom_service.pool_management.installation import PoolMachineInstallation;'
        'from loom_service.pool_management.profiles import PoolProfileCatalog;'
        'request=json.loads(pathlib.Path(sys.argv[3]).read_text());'
        'models={"declaration":EnvironmentFoundationDeclarationV2,"participant":PoolParticipantV1,'
        '"profiles":PoolProfileCatalog,"machine":PoolMachineInstallation,"runtime_profile":ServiceExecutionRuntimeProfileV1};'
        'request.update({key:model.model_validate(request[key]) for key,model in models.items()});'
        'rendered=render_environment_runtime(**request,repo_root=root);'
        'assert set(rendered.workloads)=={"service","control_plane","actuator"};'
        'assert all(row["spec"]["replicas"]==0 for row in rendered.workloads.values());'
        'assert all(pathlib.Path(m.__file__).is_relative_to(root) for n,m in sys.modules.items() '
        'if n.startswith("scripts.ops.") and getattr(m,"__file__",None));'
        'assert all(pathlib.Path(m.__file__).is_relative_to(site) for n,m in sys.modules.items() '
        'if n.split(".")[0] in ("loom","loom_service") and getattr(m,"__file__",None));'
        'raise SystemExit(main("/missing","a"*64,"apply","{}"))', str(root), str(site), str(render_input)],
        cwd=tmp_path, capture_output=True, timeout=60)
    assert result.returncode == 1 and json.loads(result.stdout)['status'] == 'blocked', result.stderr.decode()


@pytest.mark.parametrize('damage', [None, 'path', 'platform', 'nil', 'wrong-action'])
def test_command_never_selects_paths_code_or_foreign_authority(damage):
    value = command()
    if damage == 'path':
        value['configuration'] = '/private/foreign'
    elif damage == 'platform':
        value['scope']['authority'] = 'platform'
    elif damage == 'nil':
        value['scope']['participant_id'] = '00000000-0000-0000-0000-000000000000'
    elif damage == 'wrong-action':
        value['action'] = 'recover'
    if damage:
        with pytest.raises(ValueError):
            gateway().parse_command(json.dumps(value).encode(), 'apply')
    else:
        assert gateway().parse_command(json.dumps(value).encode(), 'apply') == value


@pytest.mark.parametrize('damage', [None, 'scope', 'secret', 'execution-claim', 'phase'])
def test_report_cannot_export_proofs_or_claim_executed_acceptance(damage):
    incoming = command()
    report = dict(incoming, schema_version='loom.environment-lifecycle-result.v2', status='complete',
        phase='activate', admission_open=True, execution_verified=False)
    report = copy.deepcopy(report)
    if damage == 'scope':
        report['scope']['participant_id'] = str(uuid4())
    elif damage == 'secret':
        report['proof'] = {'private': 'marker'}
    elif damage == 'execution-claim':
        report['execution_verified'] = True
    elif damage == 'phase':
        report['phase'] = 'arbitrary private exception'
    if damage:
        with pytest.raises(ValueError):
            gateway().safe_report(json.dumps(report).encode(), json.dumps(incoming).encode(), 'apply')
    else:
        assert gateway().safe_report(json.dumps(report).encode(), json.dumps(incoming).encode(), 'apply') == report


def test_allowlist_selects_exact_target_and_disallows_alias_concurrency_escape():
    selected = dict(target_id='dev-one', scope=scope(), operations=[str(uuid4())], tooling_source_sha='a' * 40,
        ssh_key_secret='NEBIUS_ENVIRONMENT_LIFECYCLE_DEV_ONE_SSH_KEY')
    raw = json.dumps(dict(schema_version='loom.environment-lifecycle-targets.v2', targets=[selected]))
    assert runner().select_target(raw, target_id='dev-one', authority='development', operation_id=selected['operations'][0]) == selected
    with pytest.raises(ValueError):
        runner().select_target(raw, target_id='dev-one', authority='production', operation_id=selected['operations'][0])
    with pytest.raises(ValueError):
        runner().select_target(raw, target_id='missing', authority='development', operation_id=selected['operations'][0])
    duplicate = dict(selected, target_id='alias')
    with pytest.raises(ValueError):
        runner().select_target(json.dumps(dict(schema_version='loom.environment-lifecycle-targets.v2', targets=[selected, duplicate])),
            target_id='dev-one', authority='development', operation_id=selected['operations'][0])


@pytest.mark.parametrize(('authority', 'ref', 'accepted'), [
    ('development', 'refs/heads/dev', True), ('staging', 'refs/heads/dev', True),
    ('development', 'refs/heads/main', False), ('production', 'refs/heads/dev', False),
    ('production', 'refs/heads/main', True), ('production', 'refs/tags/v1.2.3', True),
    ('production', 'refs/tags/vmalformed', False),
])
def test_workflow_selection_preserves_branch_and_protected_environment_boundary(monkeypatch, authority, ref, accepted):
    selected = dict(target_id='one', scope=dict(scope(), authority=authority), operations=[str(uuid4())],
        tooling_source_sha='a' * 40, ssh_key_secret='NEBIUS_ENVIRONMENT_LIFECYCLE_ONE_SSH_KEY')
    monkeypatch.setenv('GITHUB_REPOSITORY_ID', '1281629473')
    monkeypatch.setenv('GITHUB_REF', ref)
    monkeypatch.setenv('NEBIUS_ENVIRONMENT_LIFECYCLE_TARGETS_JSON',
        json.dumps(dict(schema_version='loom.environment-lifecycle-targets.v2', targets=[selected])))
    args = SimpleNamespace(command='select', target_id='one', authority=authority, operation_id=selected['operations'][0])
    if accepted:
        assert runner()._selected(args) == selected
    else:
        with pytest.raises(ValueError):
            runner()._selected(args)


@pytest.mark.parametrize('damage', [None, 'participant', 'source', 'key'])
def test_allowlist_cannot_change_target_after_the_concurrency_lock_was_selected(monkeypatch, damage):
    selected = dict(target_id='one', scope=scope(), operations=[str(uuid4())],
        tooling_source_sha='a' * 40, ssh_key_secret='NEBIUS_ENVIRONMENT_LIFECYCLE_ONE_SSH_KEY')
    monkeypatch.setenv('GITHUB_REPOSITORY_ID', '1281629473')
    monkeypatch.setenv('GITHUB_REF', 'refs/heads/dev')
    monkeypatch.setenv('NEBIUS_LIFECYCLE_SCOPE_KEY', runner().scope_key(selected['scope']))
    monkeypatch.setenv('NEBIUS_LIFECYCLE_SOURCE_SHA', selected['tooling_source_sha'])
    monkeypatch.setenv('NEBIUS_LIFECYCLE_SSH_KEY_SECRET', selected['ssh_key_secret'])
    if damage == 'participant':
        selected['scope']['participant_id'] = str(uuid4())
    elif damage == 'source':
        selected['tooling_source_sha'] = 'b' * 40
    elif damage == 'key':
        selected['ssh_key_secret'] = 'NEBIUS_ENVIRONMENT_LIFECYCLE_OTHER_SSH_KEY'
    monkeypatch.setenv('NEBIUS_ENVIRONMENT_LIFECYCLE_TARGETS_JSON',
        json.dumps(dict(schema_version='loom.environment-lifecycle-targets.v2', targets=[selected])))
    monkeypatch.setattr(subprocess, 'run', lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=b'a' * 40))
    args = SimpleNamespace(command='run', target_id='one', authority='development', operation_id=selected['operations'][0])
    if damage:
        with pytest.raises(ValueError):
            runner()._selected(args)
    else:
        assert runner()._selected(args) == selected


def test_uncertain_ssh_preserves_original_intent_and_never_retries(tmp_path):
    incoming = command()
    calls = []

    def lost(value, *, action):
        calls.append(value)
        raise subprocess.TimeoutExpired('ssh', 1)

    with pytest.raises(ValueError):
        runner().run_operation(incoming, evidence_dir=tmp_path, transport=lost)
    assert calls == [incoming]
    record = json.loads((tmp_path / 'lifecycle-intent.json').read_text())
    assert record == incoming
    assert not (tmp_path / 'lifecycle-result.json').exists()


def test_fixed_transport_never_uploads_tooling_or_retries(monkeypatch, tmp_path):
    incoming = command()
    seen = []

    def execute(argv, **kwargs):
        seen.append((argv, kwargs))
        raise subprocess.TimeoutExpired(argv, 1)

    monkeypatch.setattr(subprocess, 'run', execute)
    with pytest.raises(ValueError):
        runner().transfer(incoming, action='apply', target='operator@host.example', key=tmp_path / 'key', known_hosts=tmp_path / 'hosts')
    assert len(seen) == 1
    argv, options = seen[0]
    assert argv[-1] == 'loom-environment-lifecycle-apply-v2'
    assert 'StrictHostKeyChecking=yes' in argv and 'IdentitiesOnly=yes' in argv
    assert json.loads(options['input']) == incoming


def test_workflow_uses_validated_participant_lock_and_separate_production_environment():
    import yaml

    root = Path(__file__).resolve().parents[2]
    document = yaml.load((root / '.github/workflows/nebius-environment-lifecycle.yml').read_text(), Loader=yaml.BaseLoader)
    assert set(document['on']) == {'workflow_dispatch'}
    assert 'concurrency' not in document
    run = document['jobs']['run']
    assert run['concurrency']['group'] == 'nebius-environment-${{ needs.select.outputs.scope_key }}'
    assert run['concurrency']['cancel-in-progress'] == 'false'
    assert run['environment']['name'] == '${{ needs.select.outputs.environment }}'
    assert document['jobs']['select']['environment']['name'] == "${{ inputs.authority == 'production' && 'production' || inputs.authority == 'staging' && 'staging' || 'nebius-integration' }}"
    assert 'secrets.' not in json.dumps(document['jobs']['select'])
