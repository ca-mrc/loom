"""Selected boundary CAS and native admission proof; no migration-history reads."""
from __future__ import annotations

import json
from typing import Any

from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_development_runtime_retained import qualify_build_policy_spec
from scripts.ops.nebius_environment_runtime_start import _Workloads
from scripts.ops.nebius_ingress_stage import _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_shared_pool_build_activation import build_policy_rejection_probe

from loom_service.pool_management.profiles import _ApplicationImage, _TaskImage


class EnvironmentBoundaryWorkloads(_Workloads):
    """The parent supplies current readiness and only its journaled boundary targets."""
    def __init__(self, api: Any, originals: dict[str, Any], targets: dict[str, Any], region: str,
            readers: dict[str, dict[str, Any]], *, build_policy: dict[str, dict[str, Any]],
            build_profile: _TaskImage | _ApplicationImage | None):
        super().__init__(api, originals, targets, region, readers)
        self.build_policy, self.build_profile = build_policy, build_profile

    def _reader_boundary(self, namespace: str) -> None:
        """Complete inventory: exact running readers, no dormant task producers."""
        owners = {_uid(doc): doc for doc in self.readers.values() if doc['metadata']['namespace'] == namespace}
        collections: dict[str, dict[str, Any]] = {}
        for version, kind, resource in (('v1', 'Pod', 'pods'), ('batch/v1', 'Job', 'jobs'),
                ('batch/v1', 'CronJob', 'cronjobs'), ('apps/v1', 'Deployment', 'deployments'),
                ('apps/v1', 'ReplicaSet', 'replicasets'), ('apps/v1', 'StatefulSet', 'statefulsets'),
                ('apps/v1', 'DaemonSet', 'daemonsets')):
            self.qualify()
            prefix = '/api/v1' if version == 'v1' else '/apis/' + version
            result = self.api._request('GET', prefix + '/namespaces/' + namespace + '/' + resource + '?limit=1000')
            if (result is None or result.get('apiVersion') != version or result.get('kind') != kind + 'List'
                    or not isinstance(result.get('metadata', {}).get('resourceVersion'), str)
                    or not 0 < len(result['metadata']['resourceVersion']) <= 128 or result['metadata'].get('continue')
                    or not isinstance(result.get('items'), list) or len(result['items']) > 1000):
                raise ValueError('environment_boundary_inventory_unavailable')
            rows = [{'apiVersion': version, 'kind': kind, **row} for row in result['items']]
            if (any(row['apiVersion'] != version or row['kind'] != kind or row['metadata']['namespace'] != namespace for row in rows)
                    or len({_uid(row) for row in rows}) != len(rows)
                    or (kind not in {'Deployment', 'ReplicaSet', 'Pod'} and rows)):
                raise ValueError('environment_boundary_foreign_workload')
            collections[kind] = {**result, 'items': rows}
        deployments = {_uid(row): row for row in collections['Deployment']['items']}
        if deployments.keys() != owners.keys() or any(not _matches(deployments[uid], doc, uid) for uid, doc in owners.items()):
            raise ValueError('environment_boundary_readers_changed')
        replicas = {_uid(row): row for row in collections['ReplicaSet']['items']}
        for kind, parents in (('ReplicaSet', owners), ('Pod', replicas)):
            for row in collections[kind]['items']:
                refs = row['metadata'].get('ownerReferences', [])
                parent = parents.get(refs[0].get('uid')) if len(refs) == 1 else None
                if (parent is None or refs[0].get('controller') is not True or refs[0].get('kind') != parent['kind']
                        or refs[0].get('apiVersion') != parent['apiVersion'] or refs[0].get('name') != parent['metadata']['name']):
                    raise ValueError('environment_boundary_foreign_workload')
        for doc in deployments.values():
            if not qualify_started_deployment(current=doc, children=collections['ReplicaSet'], pods=collections['Pod'], region=self.region):
                raise ValueError('environment_boundary_reader_not_ready')
        self.qualify()

    def check_inventory(self) -> None:
        if self.build_profile is None:
            raise ValueError('environment_boundary_build_profile_missing')
        namespaces = {self.build_profile.target.namespace, *(doc['metadata']['namespace'] for doc in self.readers.values())}
        for namespace in sorted(namespaces):
            self._reader_boundary(namespace)

    def policy_ready(self) -> bool:
        self.qualify()
        profile = self.build_profile
        if profile is None or len(self.build_policy) != 2:
            raise ValueError('environment_boundary_build_policy_missing')
        def policies() -> dict[str, dict[str, Any]]:
            result = {}
            for key, expected in self.build_policy.items():
                kind = expected['kind']
                if kind not in {'ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'}:
                    raise ValueError('environment_boundary_build_policy_unqualified')
                plural = 'validatingadmissionpolicies' if kind == 'ValidatingAdmissionPolicy' else 'validatingadmissionpolicybindings'
                current = self.api._request('GET', '/apis/admissionregistration.k8s.io/v1/' + plural + '/' + expected['metadata']['name'])
                if current is None or not _matches(current, expected, _uid(expected)):
                    raise ValueError('environment_boundary_build_policy_changed')
                qualify_build_policy_spec(expected, current)
                result[key] = current
            return result
        before = policies()
        policy, = (row for row in before.values() if row['kind'] == 'ValidatingAdmissionPolicy')
        status, generation = policy.get('status', {}), policy['metadata'].get('generation')
        checking = status.get('typeChecking')
        if isinstance(checking, dict) and checking.get('expressionWarnings'):
            raise ValueError('environment_boundary_build_policy_invalid')
        if (not isinstance(checking, dict) or type(generation) is not int or generation < 1
                or type(status.get('observedGeneration')) is not int or status['observedGeneration'] < generation):
            return False
        probe = build_policy_rejection_probe(namespace=profile.target.namespace,
            service_account=profile.target.service_account_name, config=profile.settings.job_config())
        with self.api.client.stream('POST', '/api/v1/namespaces/' + profile.target.namespace + '/pods?dryRun=All', json=probe) as response:
            if (response.status_code not in {200, 201, 403}
                    or response.headers.get('content-encoding', 'identity').lower() != 'identity'):
                raise ValueError('environment_boundary_build_probe_unqualified')
            raw = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                raw.extend(chunk)
                if len(raw) > 65536:
                    raise ValueError('environment_boundary_build_probe_unqualified')
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise ValueError('environment_boundary_build_probe_unqualified')
            denied = response.status_code == 403
            if denied and (result.get('apiVersion') != 'v1' or result.get('kind') != 'Status'
                    or result.get('status') != 'Failure' or result.get('reason') != 'Forbidden' or result.get('code') != 403
                    or policy['metadata']['name'] not in result.get('message', '')
                    or 'Only sequential prepare, build and publish phases are allowed.' not in result.get('message', '')):
                raise ValueError('environment_boundary_build_probe_unqualified')
        if policies() != before:
            raise ValueError('environment_boundary_build_policy_changed')
        self.qualify()
        return bool(denied)

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> dict[str, Any] | None:
        if desired['kind'] == 'Namespace' and not self.policy_ready():
            return None
        return super()._patch(key, before, desired, preview=preview)

    def workload_ready(self, key: str, expected: dict[str, Any]) -> bool:
        actual = self.read_workload(key)
        if not _matches(actual, expected, _uid(self.originals[key])):
            raise ValueError('environment_boundary_readiness_changed')
        if actual['kind'] == 'Namespace':
            return self.policy_ready()
        if actual['kind'] != 'ResourceQuota' or actual['spec'] != {}:
            raise ValueError('environment_boundary_readiness_unqualified')
        self._reader_boundary(actual['metadata']['namespace'])
        # The quota controller must also observe removal before admission opens.
        return bool(actual.get('status', {}).get('hard', {}) == {})
