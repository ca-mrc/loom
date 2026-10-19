"""Fixed read-only process qualification, with no legacy environment ancestry.

The protected parent supplies UID-bound completed runtime successors and an
independently observed database. This module cannot select or mutate workloads,
issue admission grants, or treat desired manifests as deployment authority.
"""
from __future__ import annotations

import hmac
import json
import secrets
from collections.abc import Callable
from typing import Any, Literal

from pydantic import PostgresDsn
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_pool_migration_guard import (
    _BOUND_DATABASE_COMMAND,
    qualify_database_destination,
)
from scripts.ops.nebius_pool_runtime_settings import (
    BOUND_POOL_SETTINGS_COMMAND,
    expected_pool_runtime_settings,
)

from loom_service.pool_management.capacity import digest

EnvironmentComponent = Literal['service', 'controller', 'actuator']
Read = Callable[[str], dict[str, Any]]
Execute = Callable[[dict[str, Any], str, list[str]], dict[str, Any]]


class EnvironmentRuntimeProbes:
    def __init__(self, *, read: Read, execute: Execute, qualify: Callable[[], None], region: str):
        self.read, self.execute, self.qualify, self.region = read, execute, qualify, region

    def _running(self, wanted: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        self.qualify()
        namespace, name = wanted['metadata']['namespace'], wanted['metadata']['name']
        path = '/apis/apps/v1/namespaces/' + namespace + '/deployments/' + name
        current = self.read(path)
        if not _matches(current, wanted, _uid(wanted)):
            raise ValueError
        children = self.read('/apis/apps/v1/namespaces/' + namespace + '/replicasets?limit=1000')
        pods = self.read('/api/v1/namespaces/' + namespace + '/pods?limit=1000')
        final = self.read(path)
        if (not _matches(final, current, _uid(current))
                or final['metadata']['generation'] != current['metadata']['generation']
                or not qualify_started_deployment(current=final, children=children, pods=pods, region=self.region)):
            raise ValueError
        labels = current['spec']['selector']['matchLabels']
        pod, = (row for row in pods['items'] if all(row['metadata'].get('labels', {}).get(key) == value for key, value in labels.items()))
        self.qualify()
        return final, pod

    @staticmethod
    def _process(pod: dict[str, Any]) -> dict[str, Any]:
        return {'uid': _uid(pod), 'containers': pod['status']['containerStatuses'],
            'init_containers': pod['status'].get('initContainerStatuses', [])}

    def recheck_process(self, *, workload: dict[str, Any], proof: dict[str, Any]) -> None:
        """Close the observation window after all components and data are read."""
        try:
            final, pod = self._running(workload)
            if (set(proof) != {'component', 'deployment_uid', 'pod_uid', 'process_sha256'}
                    or proof['component'] not in {'service', 'controller', 'actuator'}
                    or proof['deployment_uid'] != _uid(final) or proof['pod_uid'] != _uid(pod)
                    or proof['process_sha256'] != digest(self._process(pod))):
                raise ValueError
        except Exception:
            raise ValueError('environment_runtime_process_changed') from None

    def qualify_process(self, *, workload: dict[str, Any], component: EnvironmentComponent,
            database_namespace: str, database_url: str, token_sha256: str | None = None) -> dict[str, Any]:
        """Challenge the selected image's real typed settings and DB URL readers.

        Read the exact Deployment/ReplicaSet/Pod ancestry on both sides. A Pod
        restart, replacement or template change invalidates the whole observation.
        Secret values never enter command arguments, reports or failure messages.
        """
        try:
            if component not in {'service', 'controller', 'actuator'}:
                raise ValueError
            qualify_database_destination(database_url, database_namespace)
            current, before = self._running(workload)
            expected = expected_pool_runtime_settings(component, current, token_sha256=token_sha256)
            url = database_url if component == 'actuator' else str(PostgresDsn(database_url))
            container, = current['spec']['template']['spec']['containers']
            for command, value in ((BOUND_POOL_SETTINGS_COMMAND, json.dumps(expected, sort_keys=True, separators=(',', ':'))),
                    (_BOUND_DATABASE_COMMAND, url)):
                nonce = secrets.token_hex(32)
                response = hmac.new(bytes.fromhex(nonce), value.encode(), 'sha256').hexdigest()
                self.qualify()
                report = self.execute(before, container['name'], ['python', '-c', command, component, nonce, response])
                self.qualify()
                if report != {'status': 'qualified'}:
                    raise ValueError
            final, after = self._running(workload)
            if (_snapshot(final) != _snapshot(current) or final['metadata']['generation'] != current['metadata']['generation']
                    or self._process(before) != self._process(after)):
                raise ValueError
            return {'component': component, 'deployment_uid': _uid(final), 'pod_uid': _uid(after),
                'process_sha256': digest(self._process(after))}
        except Exception:
            raise ValueError('environment_runtime_process_unqualified') from None
