"""Fixed gateway-local shared-input observation; no Kubernetes or cloud writes.

This stdlib-only script is delivered by protected inspection over verified SSH.
Secret values stay in private gateway files, never its response or Actions logs.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit
from uuid import UUID, uuid4


class SnapshotError(RuntimeError):
    """Raw API responses, credentials and exception details are not diagnostics."""


def _private_directory(path: Path) -> None:
    if (path != path.resolve() or not path.is_dir() or path.stat().st_uid != os.geteuid()
            or path.stat().st_mode & 0o077):
        raise SnapshotError('private observation directory required')


def capture_shared_inputs(*, read: Callable[[str, str, str | None], dict[str, Any]], root: Path,
        cluster_id: str, namespace: str, namespace_uid: str, kube_system_uid: str) -> dict[str, str]:
    try:
        def get(kind: str, name: str, *, namespaced: bool = True) -> dict[str, Any]:
            row = read(kind.lower(), name, namespace if namespaced else None)
            meta = row['metadata']
            if (row['kind'] != kind or meta['name'] != name or not UUID(meta['uid']).int
                    or not meta.get('resourceVersion') or meta.get('deletionTimestamp')
                    or (namespaced and meta.get('namespace') != namespace)):
                raise ValueError
            return row
        for name, expected in ((namespace, namespace_uid), ('kube-system', kube_system_uid)):
            if get('Namespace', name, namespaced=False)['metadata']['uid'] != expected:
                raise ValueError
        cm = get('ConfigMap', 'loom-platform-config')
        config = json.loads(cm['data']['environment.json'])
        profile = json.loads(cm['data']['profile.json'])
        keyring = json.loads(cm['data']['keyring.json'])
        if (config['cluster_id'] != cluster_id or config['namespace'] != namespace
                or config['environment'] != 'development' or not isinstance(keyring, dict)
                or not re.fullmatch(r'[0-9a-f]{40}', profile['candidate_sha'])):
            raise ValueError
        service = get('Deployment', 'loom-service')
        database, auth = get('Secret', 'loom-platform-db'), get('Secret', 'loom-platform-auth')
        def decode(value: str) -> str:
            if len(value) > 131072:
                raise ValueError
            result = base64.b64decode(value, validate=True).decode('utf-8')
            if not result or len(result.encode()) > 65536:
                raise ValueError
            return result
        url = urlsplit(decode(database['data']['admin-url']))
        if (url.scheme not in {'postgresql', 'postgresql+psycopg'} or url.username != 'postgres'
                or url.hostname != 'loom-postgres.' + namespace + '.svc' or url.port != 5432
                or not re.fullmatch(r'/[A-Za-z0-9_]+', url.path)):
            raise ValueError
        binding = {'cluster_id': cluster_id, 'namespace': namespace, 'namespace_uid': namespace_uid,
            'kube_system_uid': kube_system_uid, 'candidate_sha': profile['candidate_sha'],
            **{key: row['metadata']['uid'] for key, row in (('shared_config_uid', cm),
                ('shared_service_uid', service), ('shared_database_uid', database), ('shared_auth_uid', auth))}}
        files = {'binding.json': json.dumps(binding, sort_keys=True),
            'environment.json': json.dumps(config, sort_keys=True),
            'runtime-profile.json': json.dumps(profile, sort_keys=True), 'keyring.json': json.dumps(keyring, sort_keys=True),
            'database-name': unquote(url.path[1:]), 'ca.crt': decode(database['data']['ca.crt']),
            'secret-store-master-keys': decode(auth['data']['secret-store-master-key'])}
        # Validate everything before writing; each capture is new, never an
        # overwrite of accepted installation input or an earlier observation.
        root.mkdir(mode=0o700, exist_ok=True)
        _private_directory(root)
        identity = str(uuid4())
        output = root / identity
        output.mkdir(mode=0o700)
        for name, content in files.items():
            fd = os.open(output / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, 'w') as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
        return {'status': 'shared_inputs_observed', 'observation_id': identity, 'candidate_sha': profile['candidate_sha']}
    except Exception:
        raise SnapshotError('shared input observation unavailable') from None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('kubeconfig', 'cluster-id', 'namespace', 'namespace-uid', 'kube-system-uid'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    try:
        home = Path.home() / '.loom' / 'nebius-management'
        _private_directory(home)
        inputs = home / 'inputs.json'
        if (inputs != inputs.resolve() or not inputs.is_file() or inputs.stat().st_mode & 0o077
                or inputs.stat().st_uid != os.geteuid() or inputs.stat().st_size > 4 * 1024**2):
            raise ValueError
        original = json.loads(inputs.read_bytes())
        foundation = json.loads(original['deployment']['installation']['foundation']['platform_config_json'])
        if (foundation['namespace'] != args.namespace or foundation['cluster_id'] != args.cluster_id
                or original['binding']['kube_system_uid'] != args.kube_system_uid):
            raise ValueError
        kubeconfig = Path(args.kubeconfig)
        if not kubeconfig.is_absolute() or kubeconfig != kubeconfig.resolve():
            raise ValueError
        def read(kind: str, name: str, namespace: str | None) -> dict[str, Any]:
            command = ['kubectl', '--kubeconfig', str(kubeconfig), '--request-timeout=30s', 'get', kind, name,
                *(['-n', namespace] if namespace is not None else []), '-o', 'json']
            result = subprocess.run(command, capture_output=True, timeout=40, check=False)
            if result.returncode or len(result.stdout) > 4 * 1024**2:
                raise ValueError
            value: dict[str, Any] = json.loads(result.stdout)
            return value
        report = capture_shared_inputs(read=read, root=home / 'shared-input-observations', cluster_id=args.cluster_id,
            namespace=args.namespace, namespace_uid=args.namespace_uid, kube_system_uid=args.kube_system_uid)
        print(json.dumps(report, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({'status': 'blocked'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
