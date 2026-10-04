"""Fixed gateway-local shared-input observation; no Kubernetes or cloud writes.

This stdlib-only script is delivered by protected inspection over verified SSH.
Secret values stay in private gateway files, never its response or Actions logs.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
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


_POOL_COLLECTIONS = {
    'Deployment': ('apps/v1', 'deployments'), 'StatefulSet': ('apps/v1', 'statefulsets'),
    'CronJob': ('batch/v1', 'cronjobs'), 'Service': ('v1', 'services'), 'ConfigMap': ('v1', 'configmaps'),
    'Role': ('rbac.authorization.k8s.io/v1', 'roles'),
    'RoleBinding': ('rbac.authorization.k8s.io/v1', 'rolebindings'),
    'ClusterRole': ('rbac.authorization.k8s.io/v1', 'clusterroles'),
    'ClusterRoleBinding': ('rbac.authorization.k8s.io/v1', 'clusterrolebindings'),
}


def _pool_resources(*, read: Callable[[str, str, str | None], dict[str, Any]],
        listing: Callable[[str, str | None], dict[str, Any]], config_map: dict[str, Any],
        database: dict[str, Any], cluster_id: str, candidate: str, namespace_uid: str) -> str:
    """Retain bounded preparation evidence, never grant authority from a snapshot."""
    config = json.loads(config_map['data']['environment.json'])
    namespace, execution = config['namespace'], config['execution_namespace']
    namespaces = (namespace, execution, execution + '-build')
    if len(set(namespaces)) != 3 or any(not re.fullmatch(r'loom-nebius-[a-z0-9](?:[a-z0-9-]{0,49}[a-z0-9])?', ns)
            for ns in namespaces):
        raise ValueError

    def identity(row: dict[str, Any], kind: str, version: str, ns: str | None) -> None:
        meta = row['metadata']
        if (row.get('kind') != kind or row.get('apiVersion') != version
                or meta.get('namespace') != ns or not UUID(meta['uid']).int
                or not isinstance(meta.get('resourceVersion'), str) or not meta['resourceVersion']
                or not isinstance(meta.get('name'), str)
                or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9:._-]{0,252}', meta['name'])
                or meta.get('deletionTimestamp')):
            raise ValueError

    roots = {ns: read('namespace', ns, None) for ns in namespaces}
    for ns, row in roots.items():
        identity(row, 'Namespace', 'v1', None)
        if row['metadata']['name'] != ns:
            raise ValueError
    if roots[namespace]['metadata']['uid'] != namespace_uid:
        raise ValueError
    resources, collections = list(roots.values()), []
    keys, uids = set(), set()
    total = 0
    for kind, (version, _) in _POOL_COLLECTIONS.items():
        for ns in ((None,) if kind.startswith('Cluster') else namespaces):
            collection = listing(kind, ns)
            metadata = collection.get('metadata', {})
            items = collection.get('items')
            size = len(json.dumps(collection).encode())
            total += size
            if (collection.get('apiVersion') != version or collection.get('kind') != kind + 'List'
                    or not isinstance(metadata.get('resourceVersion'), str) or not metadata['resourceVersion']
                    or metadata.get('continue') or metadata.get('remainingItemCount') not in (None, 0)
                    or not isinstance(items, list) or len(items) > 1024
                    or size > 8 * 1024**2 or total > 32 * 1024**2):
                raise ValueError
            for item in items:
                # Kubernetes typed lists omit item TypeMeta. Explicitly
                # conflicting types still fail; generic List is never accepted.
                item = {'apiVersion': version, 'kind': kind, **item}
                identity(item, kind, version, ns)
                key, uid = (kind, ns, item['metadata']['name']), item['metadata']['uid']
                if key in keys or uid in uids:
                    raise ValueError
                keys.add(key)
                uids.add(uid)
                resources.append(item)
            collections.append({'kind': kind, 'namespace': ns, 'resource_version': metadata['resourceVersion'],
                'count': len(items)})

    def secret(name: str, ns: str = execution) -> dict[str, Any]:
        row = read('secret', name, ns)
        identity(row, 'Secret', 'v1', ns)
        if row['metadata']['name'] != name:
            raise ValueError
        return row

    actuator = secret('loom-execution-actuator-db')
    collector = secret('loom-execution-capacity-collector-nebius')
    if collector.get('type') != 'Opaque' or collector.get('stringData') or set(collector['data']) != {'credentials.json'}:
        raise ValueError
    encoded = collector['data']['credentials.json']
    if not isinstance(encoded, str) or not 0 < len(encoded) <= 4 * ((1024**2 + 2) // 3):
        raise ValueError
    credential = base64.b64decode(encoded, validate=True)
    if not 0 < len(credential) <= 1024**2:
        raise ValueError
    source = secret('loom-platform-storage', namespace)
    if source.get('type') != 'Opaque' or source.get('stringData'):
        raise ValueError
    source_material = {}
    for source_key in ('access-key', 'secret-key'):
        encoded = source['data']['source-' + source_key]
        if not isinstance(encoded, str) or not 0 < len(encoded) <= 4 * ((4096 + 2) // 3):
            raise ValueError
        value = base64.b64decode(encoded, validate=True).decode('ascii')
        if not 0 < len(value) <= 4096 or any(ord(char) < 33 or ord(char) == 127 for char in value):
            raise ValueError
        source_material[source_key] = value
    source_sha256 = hashlib.sha256(json.dumps(source_material, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

    def pin(row: dict[str, Any]) -> dict[str, str]:
        return {'uid': row['metadata']['uid'], 'resource_version': row['metadata']['resourceVersion']}

    # Root configuration and credential generations must survive the capture.
    # Other objects are non-atomic observations, requalified by protected cutover.
    if (read('configmap', 'loom-platform-config', namespace) != config_map
            or read('secret', 'loom-platform-db', namespace) != database
            or secret('loom-execution-actuator-db') != actuator
            or secret('loom-execution-capacity-collector-nebius') != collector
            or secret('loom-platform-storage', namespace) != source
            or any(read('namespace', ns, None)['metadata']['uid'] != row['metadata']['uid']
                for ns, row in roots.items())):
        raise ValueError
    return json.dumps({'schema_version': 'loom.nebius-pool-resource-observation.v1',
        'cluster_id': cluster_id, 'namespace': namespace, 'candidate_sha': candidate,
        'collections': collections, 'resources': resources, 'database_credential': pin(database),
        'actuator_credential': pin(actuator), 'application_source_credential': {**pin(source), 'sha256': source_sha256},
        'collector_credential': {
            **pin(collector), 'sha256': hashlib.sha256(credential).hexdigest()}}, sort_keys=True)


def _private_directory(path: Path) -> None:
    if (path != path.resolve() or not path.is_dir() or path.stat().st_uid != os.geteuid()
            or path.stat().st_mode & 0o077):
        raise SnapshotError('private observation directory required')


def capture_shared_inputs(*, read: Callable[[str, str, str | None], dict[str, Any]], root: Path,
        cluster_id: str, namespace: str, namespace_uid: str, kube_system_uid: str,
        read_collection: Callable[[str, str | None], dict[str, Any]] | None = None) -> dict[str, str]:
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
        if read_collection is not None:
            files['pool-resources.json'] = _pool_resources(read=read, listing=read_collection,
                config_map=cm, database=database, cluster_id=cluster_id, candidate=profile['candidate_sha'],
                namespace_uid=namespace_uid)
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
        def listing(kind: str, namespace: str | None) -> dict[str, Any]:
            version, resource = _POOL_COLLECTIONS[kind]
            prefix = '/api/v1' if version == 'v1' else '/apis/' + version
            path = prefix + ('' if namespace is None else '/namespaces/' + namespace) + '/' + resource + '?limit=1025'
            result = subprocess.run(['kubectl', '--kubeconfig', str(kubeconfig), '--request-timeout=30s',
                'get', '--raw', path], capture_output=True, timeout=40, check=False)
            if result.returncode or len(result.stdout) > 8 * 1024**2:
                raise ValueError
            value: dict[str, Any] = json.loads(result.stdout)
            return value
        report = capture_shared_inputs(read=read, root=home / 'shared-input-observations', cluster_id=args.cluster_id,
            namespace=args.namespace, namespace_uid=args.namespace_uid, kube_system_uid=args.kube_system_uid,
            read_collection=listing)
        print(json.dumps(report, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({'status': 'blocked'}))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
