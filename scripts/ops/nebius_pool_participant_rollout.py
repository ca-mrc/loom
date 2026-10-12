"""Runner-owned publication, intent and reporting for fixed shared-pool releases.

Private histories, configuration and Kubernetes authority stay on the installed
operator host. A failed/uncertain dispatch is never retried by this runner.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from scripts.ops import nebius_idle_rollout as idle
from scripts.ops import nebius_pool_participant_gateway as gateway
from scripts.ops.nebius_rollout_reporting import emit_result

from loom.repository_identity import REPOSITORY_ID, is_loom_repository
from loom_service.environment_management.candidates import ProtectedPublication

_SCHEMA = 'loom.nebius-participant-command.v1'
_BINDING = ('candidate_sha', 'previous_candidate_sha', 'guard_owner', 'cluster_id', 'namespace', 'execution_namespace')
ROOT = Path(__file__).resolve().parents[2]
Transport = Callable[..., dict[str, Any]]


class RolloutError(RuntimeError):
    """Fixed public failure; original recovery evidence must be preserved."""


def _identity(kind: str, *parts: object) -> str:
    return str(uuid5(NAMESPACE_URL, f'https://api.github.com/repositories/{REPOSITORY_ID}/loom/{kind}/'
        + '/'.join(str(part) for part in parts)))


def select_publication(*, run_id: int, candidate_sha: str, artifact_name: str) -> ProtectedPublication:
    """Pin the checked publication; the host independently verifies all CI authority."""
    try:
        if type(run_id) is not int or run_id <= 0 or re.fullmatch(r'[0-9a-f]{40}', candidate_sha) is None:
            raise ValueError
        run = idle.github(f'actions/runs/{run_id}')
        if (not isinstance(run, dict) or run['id'] != run_id or run['head_sha'] != candidate_sha
                or type(run['run_attempt']) is not int or run['run_attempt'] <= 0
                or run['head_branch'] != 'dev' or not is_loom_repository(run['repository'])
                or not is_loom_repository(run['head_repository']) or run['status'] != 'completed'
                or run['conclusion'] != 'success' or run['path'] != '.github/workflows/nebius-candidate.yml'
                or run['event'] not in {'push', 'workflow_dispatch'}
                or artifact_name != f"nebius-candidate-{candidate_sha}-{run_id}-{run['run_attempt']}"):
            raise ValueError
        artifacts: list[dict[str, Any]] = []
        page = 1
        while True:
            inventory = idle.github(f'actions/runs/{run_id}/artifacts?per_page=100&page={page}')
            if not isinstance(inventory, dict) or not isinstance(inventory.get('artifacts'), list):
                raise ValueError
            rows = inventory['artifacts']
            artifacts.extend(row for row in rows if row['name'] == artifact_name)
            if len(rows) < 100:
                break
            page += 1
        artifact, = artifacts
        origin = artifact['workflow_run']
        if (artifact['expired'] is not False or origin['id'] != run_id or origin['head_sha'] != candidate_sha
                or origin['head_branch'] != 'dev' or origin['repository_id'] != REPOSITORY_ID
                or origin['head_repository_id'] != REPOSITORY_ID):
            raise ValueError
        matches, page = [], 1
        while True:
            prs = idle.github(f'commits/{candidate_sha}/pulls?per_page=100&page={page}')
            if not isinstance(prs, list):
                raise ValueError
            for selected in prs:
                number = selected['number']
                if type(number) is not int or number <= 0:
                    raise ValueError
                pr = idle.github(f'pulls/{number}')
                if (isinstance(pr, dict) and pr['number'] == number and pr['merged'] is True
                        and pr['state'] == 'closed' and pr['merge_commit_sha'] == candidate_sha
                        and pr['base']['ref'] == 'dev' and is_loom_repository(pr['base']['repo'])):
                    matches.append(number)
            if len(prs) < 100:
                break
            page += 1
        pull_request, = matches
        return ProtectedPublication(candidate_id=_identity('candidate', run_id, run['run_attempt'],
            artifact['id'], artifact['digest']), source_sha=candidate_sha, run_id=run_id,
            run_attempt=run['run_attempt'], artifact_id=artifact['id'],
            artifact_sha256=artifact['digest'], pull_request=pull_request)
    except Exception:
        raise RolloutError('exact merged-dev publication unavailable') from None


def transfer(request: dict[str, Any], *, action: str, target: str, key: Path, known_hosts: Path) -> dict[str, Any]:
    """One fixed SSH command, command data only, strict host authentication."""
    try:
        commands = {value: name for name, value in gateway.COMMANDS.items()}
        content = json.dumps(request, sort_keys=True).encode()
        if (action not in commands or request.get('action') != action or request.get('schema_version') != _SCHEMA
                or not 0 < len(content) <= gateway.MAX_COMMAND
                or re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.-]*@[A-Za-z0-9][A-Za-z0-9_.-]*', target) is None
                or not key.is_absolute() or not known_hosts.is_absolute()):
            raise ValueError
        argv = ['ssh', '-F', '/dev/null', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes',
            '-o', 'IdentitiesOnly=yes', '-o', 'ConnectTimeout=15', '-o', 'ServerAliveInterval=15',
            '-o', 'ServerAliveCountMax=3', '-o', 'UserKnownHostsFile=' + str(known_hosts),
            '-i', str(key), target, commands[action]]
        result = subprocess.run(argv, input=content, capture_output=True, timeout=2400, check=False)
        if result.returncode:
            raise ValueError
        return gateway.safe_report(result.stdout, content, action)
    except Exception:
        raise RolloutError('participant outcome unavailable; preserve original recovery reference') from None


def _save(path: Path, record: dict[str, Any], *, initial: bool = False) -> None:
    raw = (json.dumps(record, sort_keys=True) + '\n').encode()
    if initial:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    else:
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.participant-report-', delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def run_release(*, publication: ProtectedPublication | None, run_id: int, run_attempt: int,
        evidence_dir: Path, transport: Transport, automatic: bool = False,
        recovery: dict[str, Any] | None = None) -> dict[str, Any]:
    """Persist original intent before effects; recovery never prepares new state."""
    record: dict[str, Any] | None = None
    deployment_id = None
    binding = None
    saved = False
    path = evidence_dir / f'deployment-{run_id}-{run_attempt}.json'

    def dispatch(request: dict[str, Any]) -> dict[str, Any]:
        action = request['action']
        result = transport(request, action=action)
        return gateway.safe_report(json.dumps(result).encode(), json.dumps(request).encode(), action)

    def status(state: str, description: str) -> None:
        if deployment_id is not None:
            idle.github(f'deployments/{deployment_id}/statuses', {
                'state': state, 'description': description, 'auto_inactive': state == 'success',
                'log_url': f'https://github.com/{idle.repository_name()}/actions/runs/{run_id}',
                **({'environment_url': binding['environment_url']} if binding else {})})

    try:
        if (any(type(value) is not int or value <= 0 for value in (run_id, run_attempt))
                or type(automatic) is not bool or (publication is None) == (recovery is None)
                or (recovery is not None and automatic) or path.exists() or path.is_symlink()):
            raise ValueError
        evidence_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        if recovery is None:
            assert publication is not None
            prepared = dispatch({'schema_version': _SCHEMA, 'action': 'prepare',
                'operation_id': _identity('rollout', run_id, run_attempt),
                'publication': publication.model_dump(mode='json')})
            if prepared['status'] != 'prepared':
                raise ValueError
            binding = prepared['release']
            decision = idle.deployment_decision(binding, binding['previous_candidate_sha'], binding['candidate_sha'])
            if decision['status'] != 'ready':
                return {**decision, 'candidate_sha': binding['candidate_sha']}
            record = {key: binding[key] for key in _BINDING}
            record['shared_pool_release'] = binding['reference']
            request = {'schema_version': _SCHEMA, 'action': 'execute',
                'reference': binding['reference'], 'automatic': automatic}
        else:
            original = idle.recovery_record(recovery)
            if 'shared_pool_release' not in original or not all(key in original for key in _BINDING):
                raise ValueError
            request = {'schema_version': _SCHEMA, 'action': 'recover',
                'reference': original['shared_pool_release'], 'recovery': original}
            record = dict(original)
        record.update(schema_version='loom.nebius-deployment.v1', mode='apply', status='running',
            run_id=run_id, run_attempt=run_attempt)
        _save(path, record, initial=True)
        saved = True
        deployment = idle.github('deployments', {'ref': record['candidate_sha'], 'environment': 'nebius-integration',
            'auto_merge': False, 'required_contexts': [], 'transient_environment': False, 'production_environment': False,
            'description': 'Protected shared-pool release; original recovery reference retained', 'payload': dict(record)})
        if not isinstance(deployment, dict) or type(deployment.get('id')) is not int or deployment['id'] <= 0:
            raise ValueError
        deployment_id = deployment['id']
        status('in_progress', 'Original release retained; invoking protected shared-pool rollout')
        result = dispatch(request)
        if result['status'] == 'blocked':
            raise ValueError
        observed = result['release']
        if (any(observed[key] != record[key] for key in _BINDING)
                or observed['reference'] != record['shared_pool_release'] or (binding is not None and observed != binding)):
            raise ValueError
        binding = observed
        status('success' if result['status'] == 'complete' else 'inactive',
            ('Candidate deployed; dispatch resumed' if result['status'] == 'complete' else
                'No deployment applied: ' + result['status']))
        record['status'] = result['status']
        _save(path, record)
        return {'status': result['status'], 'candidate_sha': record['candidate_sha']}
    except Exception:
        if saved and record is not None:
            record['status'] = 'failed'
            try:
                _save(path, record)
            except Exception:
                pass  # Original intent remains recoverable locally and on GitHub.
            try:
                status('failure', 'Outcome requires reconciliation with the original release reference')
            except Exception:
                pass  # Keep the original local record; do not retry a GitHub write.
        raise RolloutError('participant rollout incomplete; preserve original recovery evidence') from None


def verify_source(source_sha: str) -> None:
    """Tooling installation requires exact clean integrated source, not owner source."""
    try:
        def git(*args: str) -> bytes:
            result = subprocess.run(['git', *args], cwd=ROOT, capture_output=True, timeout=30, check=False)
            if result.returncode:
                raise ValueError
            return result.stdout.strip()

        if (re.fullmatch(r'[0-9a-f]{40}', source_sha) is None or git('rev-parse', 'HEAD').decode() != source_sha
                or git('status', '--porcelain', '--untracked-files=normal')):
            raise ValueError
        git('merge-base', '--is-ancestor', source_sha, 'refs/remotes/origin/dev')
    except Exception:
        raise RolloutError('participant tooling requires exact clean integrated source') from None


def build_bundle(*, source: dict[str, Any], uv: Path, requirements: Path, wheels: Path) -> bytes:
    """Exact deterministic installer archive; no operation/configuration/history."""
    try:
        if set(source) != {'source_sha', 'source_archive_sha256'}:
            raise ValueError
        paths = {'uv': uv, 'requirements.txt': requirements, **{name: ROOT / name for name in gateway.SOURCES},
            **{'wheels/' + path.name: path for path in wheels.iterdir()}}
        files = {}
        for name, path in paths.items():
            limit = gateway.LIMITS.get(name, gateway.MAX_WHEEL)
            with path.open('rb') as stream:
                files[name] = stream.read(limit + 1)
            if len(files[name]) > limit:
                raise ValueError
        files['participant-tooling.json'] = json.dumps({**source,
            'schema_version': 'loom.nebius-participant-tooling.v1'}, sort_keys=True).encode()
        files['manifest.json'] = json.dumps({name: hashlib.sha256(value).hexdigest()
            for name, value in files.items()}, sort_keys=True).encode()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            for name, value in sorted(files.items()):
                entry = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                entry.create_system = 3
                entry.compress_type = zipfile.ZIP_DEFLATED
                entry.external_attr = 0o100600 << 16
                archive.writestr(entry, value)
        content = buffer.getvalue()
        gateway.unpack_bundle(content)
        return content
    except Exception:
        raise RolloutError('participant tooling bundle unqualified') from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser('run')
    run.add_argument('--publication-run-id', type=int, required=True)
    run.add_argument('--candidate', required=True)
    run.add_argument('--artifact', required=True)
    run.add_argument('--evidence-dir', type=Path, required=True)
    run.add_argument('--automatic', action='store_true')
    run.add_argument('--recovery-evidence', type=Path)
    bundle = commands.add_parser('bundle')
    bundle.add_argument('--source-sha', required=True)
    bundle.add_argument('--requirements', type=Path, required=True)
    bundle.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == 'bundle':
            from scripts.ops.nebius_certificate_gateway import _write
            from scripts.ops.nebius_development_preflight import prepare_development_source
            from scripts.ops.nebius_ingress_rollout import build_wheels

            verify_source(args.source_sha)
            source = prepare_development_source(args.source_sha).model_dump(mode='json')
            executable = shutil.which('uv')
            if executable is None:
                raise ValueError
            version = subprocess.run([executable, '--version'], capture_output=True, timeout=15, check=False)
            if version.returncode or version.stdout.strip() != b'uv 0.11.26 (x86_64-unknown-linux-gnu)':
                raise ValueError
            with tempfile.TemporaryDirectory(prefix='loom-participant-tooling-') as temporary:
                wheels = build_wheels(Path(temporary), uv=Path(executable))
                content = build_bundle(source=source, uv=Path(executable), requirements=args.requirements, wheels=wheels)
                verify_source(args.source_sha)
                _write(args.output, content)
            print(json.dumps({'status': 'prepared', 'source_sha': args.source_sha,
                'bundle_sha256': hashlib.sha256(content).hexdigest()}, sort_keys=True))
            return 0
        if (os.environ.get('GITHUB_REPOSITORY_ID') != str(REPOSITORY_ID)
                or os.environ.get('GITHUB_REF') != 'refs/heads/dev'):
            raise ValueError
        recovery, publication = None, None
        if args.recovery_evidence:
            with args.recovery_evidence.open('rb') as stream:
                content = stream.read(gateway.MAX_COMMAND + 1)
            if len(content) > gateway.MAX_COMMAND:
                raise ValueError
            recovery = idle.recovery_record(gateway._json(content))
            pinned = ProtectedPublication.model_validate(recovery['shared_pool_release']['publication'])
            if (pinned.source_sha != args.candidate or pinned.run_id != args.publication_run_id
                    or args.artifact != f'nebius-candidate-{pinned.source_sha}-{pinned.run_id}-{pinned.run_attempt}'):
                raise ValueError
        else:
            publication = select_publication(run_id=args.publication_run_id, candidate_sha=args.candidate,
                artifact_name=args.artifact)
        target = os.environ['LOOM_DEPLOY_SSH_TARGET']
        key = Path(os.environ['LOOM_DEPLOY_SSH_KEY_FILE'])
        known_hosts = Path(os.environ['LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE'])

        def transport(request: dict[str, Any], *, action: str) -> dict[str, Any]:
            return transfer(request, action=action, target=target, key=key, known_hosts=known_hosts)

        result = run_release(publication=publication, run_id=int(os.environ['GITHUB_RUN_ID']),
            run_attempt=int(os.environ['GITHUB_RUN_ATTEMPT']), evidence_dir=args.evidence_dir,
            transport=transport, automatic=args.automatic, recovery=recovery)
        emit_result(result)
        return 0
    except Exception:
        emit_result({'status': 'failed', 'candidate_sha': getattr(args, 'candidate', '')})
        print('Protected participant operation unavailable; preserve original evidence', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
