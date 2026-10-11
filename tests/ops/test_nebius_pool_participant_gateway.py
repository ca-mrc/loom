"""Participant tooling is pinned separately from candidate data and handoff keys."""
from __future__ import annotations

import copy
import importlib
import io
import json
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from tests.ops.test_nebius_development_management_gateway import archive
from tests.ops.test_nebius_pool_participant_delivery import reference

ROOT = Path(__file__).resolve().parents[2]


def module():
    name = 'scripts.ops.nebius_pool_participant_gateway'
    assert importlib.util.find_spec(name) is not None, 'separately pinned participant tooling is missing'
    return importlib.import_module(name)


def members():
    return {**{name: (ROOT / name).read_bytes() for name in module().SOURCES}, 'uv': b'test-uv',
        'requirements.txt': b'test==1 --hash=sha256:' + b'a' * 64,
        'wheels/loom-0.1.0-py3-none-any.whl': b'test-wheel',
        'wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl': b'test-wheel',
        'participant-tooling.json': json.dumps({'schema_version': 'loom.nebius-participant-tooling.v1',
            'source_sha': 'a' * 40, 'source_archive_sha256': 'sha256:' + 'b' * 64}).encode()}


def command(action='execute'):
    result = {'schema_version': 'loom.nebius-participant-command.v1', 'action': action}
    if action == 'prepare':
        result.update(operation_id=str(reference().operation_id), publication=reference().publication.model_dump(mode='json'))
    else:
        result.update(reference=reference().model_dump(mode='json'), automatic=False)
    return json.dumps(result).encode()


def report(status='complete'):
    return {'schema_version': 'loom.nebius-participant-report.v1', 'status': status, 'release': {
        'reference': reference().model_dump(mode='json'), 'candidate_sha': 'a' * 40,
        'previous_candidate_sha': 'b' * 40, 'guard_owner': 'rollout-' + reference().operation_id.hex,
        'cluster_id': 'test-cluster', 'namespace': 'loom-nebius-platform',
        'execution_namespace': 'loom-nebius-platform-execution', 'environment_url': 'https://platform.example.test'}}


def test_participant_bundle_imports_real_dispatch_without_workspace_modules(tmp_path):
    from scripts.ops.nebius_ingress_rollout import build_wheels
    from scripts.ops.nebius_shared_pool_delivery_gateway import SOURCES as old_sources

    gateway = module()
    assert not any('nebius_pool_participant' in path for path in old_sources), 'do not widen the handoff grant'
    build = tmp_path / 'wheel-build'
    build.mkdir()
    executable = shutil.which('uv')
    assert executable is not None
    wheels = build_wheels(build, uv=Path(executable))
    content = {name: raw for name, raw in members().items() if not name.startswith('wheels/')}
    content.update({'wheels/' + path.name: path.read_bytes() for path in wheels.iterdir()})
    bundle = archive(content)
    files, source = gateway.unpack_bundle(bundle)
    assert source['source_sha'] == 'a' * 40
    release, site = tmp_path / 'isolated', tmp_path / 'wheel-site'
    for name, value in files.items():
        if name.startswith(('scripts/', 'deploy/')):
            path = release / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(value)
        elif name.startswith('wheels/'):
            with zipfile.ZipFile(io.BytesIO(value)) as wheel:
                wheel.extractall(site)
    bootstrap = subprocess.run([sys.executable, '-I', '-S', '-c',
        "import sys;sys.path.insert(0,sys.argv[1]); "
        "from scripts.ops.nebius_pool_participant_gateway import unpack_bundle; "
        "assert unpack_bundle(sys.stdin.buffer.read())[1]['source_sha']=='a'*40", str(release)],
        input=bundle, capture_output=True, cwd=tmp_path, timeout=20)
    assert bootstrap.returncode == 0, bootstrap.stderr.decode()
    loaded = subprocess.run([sys.executable, '-I', '-c',
        "import sys,pathlib;root=pathlib.Path(sys.argv[1]);site=pathlib.Path(sys.argv[2]); "
        "sys.path[:0]=[str(root),str(site)]; "
        "from scripts.ops.nebius_pool_participant_dispatch import main;assert callable(main); "
        "assert all(pathlib.Path(m.__file__).is_relative_to(root) for n,m in sys.modules.items() "
        "if n.startswith('scripts.ops.') and getattr(m,'__file__',None)); "
        "assert all(pathlib.Path(m.__file__).is_relative_to(site) for n,m in sys.modules.items() "
        "if n.split('.')[0] in ('loom','loom_service') and getattr(m,'__file__',None)); "
        "raise SystemExit(main('/missing-private-config','c'*64,'execute','{}'))", str(release), str(site)],
        capture_output=True, cwd=tmp_path, timeout=30)
    assert loaded.returncode == 1, loaded.stderr.decode()
    assert json.loads(loaded.stdout) == {'schema_version': 'loom.nebius-participant-report.v1',
        'status': 'blocked', 'stage': 'request'}


@pytest.mark.parametrize('damage', ['source', 'unknown', 'missing', 'private-config', 'unlisted-wheel'])
def test_tooling_bundle_rejects_unsupported_members_and_private_authority(damage):
    files = members()
    if damage == 'source':
        files['participant-tooling.json'] = b'{"schema_version":"unreviewed"}'
    elif damage == 'unknown':
        files['scripts/ops/runner_code.py'] = b'print(1)'
    elif damage == 'missing':
        del files['scripts/ops/nebius_pool_participant_prepare.py']
    elif damage == 'private-config':
        files['host.json'] = b'private operator paths'
    else:
        files['wheels/foreign-0.1.0-py3-none-any.whl'] = b'foreign'
    with pytest.raises(module().GatewayError):
        module().unpack_bundle(archive(files))


@pytest.mark.parametrize('damage', [None, 'secret', 'private-path', 'wrong-reference', 'wrong-candidate',
    'wrong-guard', 'wrong-action', 'wrong-url', 'unknown-status', 'unknown-stage', 'oversize'])
def test_public_report_is_closed_and_bound_to_the_original_command(damage):
    gateway = module()
    value, incoming = report(), command()
    if damage == 'secret':
        value['diagnostic'] = 'private-secret'
    elif damage == 'private-path':
        value['release']['state_dir'] = '/private/history'
    elif damage == 'wrong-reference':
        value['release']['reference']['inputs_sha256'] = 'f' * 64
    elif damage == 'wrong-candidate':
        value['release']['candidate_sha'] = 'b' * 40
    elif damage == 'wrong-guard':
        value['release']['guard_owner'] = 'rollout-' + 'c' * 32
    elif damage == 'wrong-action':
        value['status'] = 'prepared'
    elif damage == 'wrong-url':
        value['release']['environment_url'] = 'https://private:secret@platform.example.test'
    elif damage == 'unknown-status':
        value['status'] = 'some-new-success'
    elif damage == 'unknown-stage':
        value.update(status='blocked', stage='/private/source/exception')
    raw = json.dumps(value).encode() + (b' ' * 32768 if damage == 'oversize' else b'')
    if damage:
        with pytest.raises(gateway.GatewayError):
            gateway.safe_report(raw, incoming, 'execute')
    else:
        assert gateway.safe_report(raw, incoming, 'execute') == value
        prepared = copy.deepcopy(value)
        prepared['status'] = 'prepared'
        assert gateway.safe_report(json.dumps(prepared).encode(), command('prepare'), 'prepare') == prepared
        blocked = {'schema_version': 'loom.nebius-participant-report.v1', 'status': 'blocked', 'stage': 'installation'}
        assert gateway.safe_report(json.dumps(blocked).encode(), incoming, 'execute') == blocked


def test_fixed_process_command_cannot_select_runner_source_or_expand_action(tmp_path):
    gateway = module()
    release, config = tmp_path / 'installed-release', tmp_path / 'private-host.json'
    argv = gateway.command(release, config, 'c' * 64, 'execute', command())
    assert argv[0] == str(release / 'venv/bin/python') and argv[1:3] == ['-I', '-c']
    assert argv[4:] == [str(release), str(config), 'c' * 64, 'execute', command().decode()]
    for action in ('install', 'shell', 'execute --source /runner/tooling'):
        with pytest.raises(gateway.GatewayError):
            gateway.command(release, config, 'c' * 64, action, command())
