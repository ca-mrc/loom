"""Protected target selection and data-only delivery of installed lifecycle plans."""
from __future__ import annotations

import argparse
import ast
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops import nebius_environment_lifecycle_gateway as gateway

ROOT = Path(__file__).resolve().parents[2]


def select_target(raw: str, *, target_id: str, authority: str, operation_id: str) -> dict[str, Any]:
    """Select from protected configuration; aliases cannot create another lock."""
    if not 0 < len(raw.encode()) <= 65536:
        raise ValueError('environment_lifecycle_targets_unqualified')
    value = gateway._json(raw.encode())
    if (set(value) != {'schema_version', 'targets'} or value['schema_version'] != 'loom.environment-lifecycle-targets.v2'
            or not isinstance(value['targets'], list) or not 0 < len(value['targets']) <= 128):
        raise ValueError('environment_lifecycle_targets_unqualified')
    aliases, identities = set(), set()
    for row in value['targets']:
        if (not isinstance(row, dict) or set(row) != {'target_id', 'scope', 'operations', 'tooling_source_sha', 'ssh_key_secret'}
                or not isinstance(row['target_id'], str) or re.fullmatch(r'[a-z][a-z0-9-]{0,62}', row['target_id']) is None
                or not isinstance(row['tooling_source_sha'], str) or re.fullmatch(r'[0-9a-f]{40}', row['tooling_source_sha']) is None
                or not isinstance(row['ssh_key_secret'], str)
                or re.fullmatch(r'NEBIUS_ENVIRONMENT_LIFECYCLE_[A-Z0-9_]{1,64}_SSH_KEY', row['ssh_key_secret']) is None
                or not isinstance(row['operations'], list) or not 0 < len(row['operations']) <= 128):
            raise ValueError('environment_lifecycle_targets_unqualified')
        gateway.validate_scope(row['scope'])
        for identity in row['operations']:
            gateway._uuid(identity)
        lock = tuple(row['scope'][key] for key in ('pool_id', 'participant_id', 'incarnation'))
        if row['target_id'] in aliases or lock in identities or len(set(row['operations'])) != len(row['operations']):
            raise ValueError('environment_lifecycle_target_ambiguous')
        aliases.add(row['target_id'])
        identities.add(lock)
    selected, = (row for row in value['targets'] if row['target_id'] == target_id)
    if selected['scope']['authority'] != authority or operation_id not in selected['operations']:
        raise ValueError('environment_lifecycle_target_not_authorized')
    return selected


def scope_key(scope: dict[str, Any]) -> str:
    gateway.validate_scope(scope)
    return '-'.join(scope[key] for key in ('pool_id', 'participant_id', 'incarnation'))


def transfer(request: dict[str, Any], *, action: str, target: str, key: Path, known_hosts: Path) -> dict[str, Any]:
    try:
        content = json.dumps(request, sort_keys=True, allow_nan=False).encode()
        gateway.parse_command(content, action)
        if (re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]*@[A-Za-z0-9][A-Za-z0-9_.-]*', target) is None
                or not key.is_absolute() or not known_hosts.is_absolute()):
            raise ValueError
        commands = {value: name for name, value in gateway.COMMANDS.items()}
        argv = ['ssh', '-F', '/dev/null', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'IdentitiesOnly=yes', '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15',
            '-o', 'ServerAliveCountMax=3', '-o', 'UserKnownHostsFile=' + str(known_hosts),
            '-i', str(key), target, commands[action]]
        result = subprocess.run(argv, input=content, capture_output=True, timeout=2500, check=False)
        if result.returncode:
            raise ValueError
        return gateway.safe_report(result.stdout, content, action)
    except Exception:
        raise ValueError('environment_lifecycle_outcome_unknown_preserve_original_intent') from None


def _save(path: Path, value: dict[str, Any]) -> None:
    gateway._write(path, (json.dumps(value, sort_keys=True, allow_nan=False) + '\n').encode())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def run_operation(request: dict[str, Any], *, evidence_dir: Path,
        transport: Callable[..., dict[str, Any]]) -> dict[str, Any]:
    content = json.dumps(request, sort_keys=True, allow_nan=False).encode()
    gateway.parse_command(content, request['action'])
    evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    _save(evidence_dir / 'lifecycle-intent.json', request)
    try:
        result = transport(request, action=request['action'])
        result = gateway.safe_report(json.dumps(result).encode(), content, request['action'])
        _save(evidence_dir / 'lifecycle-result.json', result)
        return result
    except Exception:
        raise ValueError('environment_lifecycle_outcome_unknown_preserve_original_intent') from None


def source_closure() -> tuple[str, ...]:
    """Freeze actual script imports, including lazy imports, into approved bytes."""
    pending, selected = set(gateway.CORE_SOURCES), set()
    while pending:
        name = pending.pop()
        if name in selected:
            continue
        source = ROOT / name
        if not source.is_file() or source != source.resolve():
            raise ValueError('environment_lifecycle_source_missing')
        selected.add(name)
        for node in ast.walk(ast.parse(source.read_text())):
            modules = []
            if isinstance(node, ast.ImportFrom):
                if node.module == 'scripts.ops':
                    modules = ['scripts.ops.' + item.name for item in node.names]
                elif node.module and node.module.startswith('scripts.ops.'):
                    modules = [node.module]
            elif isinstance(node, ast.Import):
                modules = [item.name for item in node.names if item.name.startswith('scripts.ops.')]
            pending.update(module.replace('.', '/') + '.py' for module in modules)
    return tuple(sorted(selected))


def build_bundle(*, source: dict[str, Any], uv: Path, requirements: Path, wheels: Path) -> bytes:
    if set(source) != {'source_sha', 'source_archive_sha256'}:
        raise ValueError('environment_lifecycle_source_unqualified')
    paths = {'uv': uv, 'requirements.txt': requirements,
        **{name: ROOT / name for name in (*source_closure(), *gateway.RENDER_SOURCES)},
        **{'wheels/' + path.name: path for path in wheels.iterdir()}}
    files = {}
    for name, path in paths.items():
        with path.open('rb') as stream:
            files[name] = stream.read(gateway.member_limit(name) + 1)
        if len(files[name]) > gateway.member_limit(name):
            raise ValueError('environment_lifecycle_source_oversized')
    files['lifecycle-tooling.json'] = json.dumps(dict(source,
        schema_version='loom.environment-lifecycle-tooling.v2'), sort_keys=True).encode()
    files['manifest.json'] = json.dumps({name: hashlib.sha256(raw).hexdigest()
        for name, raw in files.items()}, sort_keys=True).encode()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
        for name, raw in sorted(files.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.create_system, info.compress_type, info.external_attr = 3, zipfile.ZIP_DEFLATED, 0o100600 << 16
            archive.writestr(info, raw)
    content = buffer.getvalue()
    gateway.unpack_bundle(content)
    return content


def _selected(args: argparse.Namespace) -> dict[str, Any]:
    if os.environ.get('GITHUB_REPOSITORY_ID') != '1281629473':
        raise ValueError('environment_lifecycle_repository_unqualified')
    ref = os.environ.get('GITHUB_REF', '')
    if ((args.authority != 'production' and ref != 'refs/heads/dev')
            or (args.authority == 'production' and ref != 'refs/heads/main'
                and re.fullmatch(r'refs/tags/v[0-9]+\.[0-9]+\.[0-9]+', ref) is None)):
        raise ValueError('environment_lifecycle_branch_unqualified')
    selected = select_target(os.environ['NEBIUS_ENVIRONMENT_LIFECYCLE_TARGETS_JSON'], target_id=args.target_id,
        authority=args.authority, operation_id=args.operation_id)
    if args.command == 'run':
        if (scope_key(selected['scope']) != os.environ['NEBIUS_LIFECYCLE_SCOPE_KEY']
                or selected['tooling_source_sha'] != os.environ['NEBIUS_LIFECYCLE_SOURCE_SHA']
                or selected['ssh_key_secret'] != os.environ['NEBIUS_LIFECYCLE_SSH_KEY_SECRET']):
            raise ValueError('environment_lifecycle_selection_changed')
        head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, timeout=30, check=False)
        if head.returncode or head.stdout.strip().decode() != selected['tooling_source_sha']:
            raise ValueError('environment_lifecycle_tooling_source_changed')
    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('select', 'run'):
        command = commands.add_parser(name)
        command.add_argument('--target-id', required=True)
        command.add_argument('--authority', choices=('development', 'staging', 'production'), required=True)
        command.add_argument('--operation-id', required=True)
        if name == 'run':
            command.add_argument('--action', choices=tuple(gateway.COMMANDS.values()), required=True)
            command.add_argument('--evidence-dir', type=Path, required=True)
    bundle = commands.add_parser('bundle')
    bundle.add_argument('--source-sha', required=True)
    bundle.add_argument('--requirements', type=Path, required=True)
    bundle.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'bundle':
            from scripts.ops.nebius_development_preflight import prepare_development_source
            from scripts.ops.nebius_ingress_rollout import build_wheels
            from scripts.ops.nebius_pool_participant_rollout import verify_source

            verify_source(args.source_sha)
            source = prepare_development_source(args.source_sha).model_dump(mode='json')
            executable = shutil.which('uv')
            if executable is None:
                raise ValueError
            version = subprocess.run([executable, '--version'], capture_output=True, timeout=15, check=False)
            if version.returncode or version.stdout.strip() != b'uv 0.11.26 (x86_64-unknown-linux-gnu)':
                raise ValueError
            with tempfile.TemporaryDirectory(prefix='loom-environment-tooling-') as temporary:
                wheels = build_wheels(Path(temporary), uv=Path(executable))
                content = build_bundle(source=source, uv=Path(executable), requirements=args.requirements, wheels=wheels)
                verify_source(args.source_sha)
                gateway._write(args.output, content)
            print(json.dumps(dict(status='prepared', source_sha=args.source_sha, bundle_sha256=hashlib.sha256(content).hexdigest())))
            return 0
        selected = _selected(args)
        if args.command == 'select':
            with open(os.environ['GITHUB_OUTPUT'], 'a') as stream:
                outputs = dict(scope_key=scope_key(selected['scope']), source_sha=selected['tooling_source_sha'],
                    ssh_key_secret=selected['ssh_key_secret'],
                    environment={'development': 'nebius-integration', 'staging': 'staging', 'production': 'production'}[args.authority])
                for key, value in outputs.items():
                    stream.write(key + '=' + value + '\n')
            return 0
        request = dict(schema_version='loom.environment-lifecycle-command.v2', action=args.action,
            scope=selected['scope'], operation_id=args.operation_id)
        def transport(value: dict[str, Any], *, action: str) -> dict[str, Any]:
            return transfer(value, action=action, target=os.environ['LOOM_DEPLOY_SSH_TARGET'],
                key=Path(os.environ['LOOM_DEPLOY_SSH_KEY_FILE']), known_hosts=Path(os.environ['LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE']))
        result = run_operation(request, evidence_dir=args.evidence_dir, transport=transport)
        print(json.dumps(result, sort_keys=True))
        return 1 if result['status'] == 'blocked' else 0
    except Exception:
        print(json.dumps(dict(status='blocked', reason='environment_lifecycle_unavailable_preserve_original_intent')))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
