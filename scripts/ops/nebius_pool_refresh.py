"""Manager-only refresh projection rooted in completed pool migration evidence.

This is a read-only identity projection, not a live verifier or an installation
entrypoint. The connected pool reader must still qualify actual workloads,
backends, credentials, permissions and physical provider scope before writes.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_refresh import platform_references_changed, render_refresh
from scripts.ops.nebius_management_refresh_install import (
    ManagementRefreshInstallRequest,
    refresh_manager_options,
    refresh_manager_revision_options,
)
from scripts.ops.nebius_management_refresh_predecessor import (
    CompletedRefresh,
    CompletedUpgrade,
    load_completed_refresh,
    load_completed_upgrade,
)
from scripts.ops.nebius_management_refresh_supersession import (
    FailedRefreshProof,
    load_failed_refresh,
)
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_platform_rollout_successor import PlatformRolloutSuccessor
from scripts.ops.nebius_pool_predecessor import CompletedPoolCutover, load_completed_pool


@dataclass(frozen=True, repr=False)
class PoolManagerRefresh:
    original: CompletedUpgrade
    predecessor: CompletedPoolCutover | CompletedRefresh
    request: ManagementRefreshInstallRequest
    state_dir: Path
    superseded: FailedRefreshProof | None = None
    successor: PlatformRolloutSuccessor | None = None

    def qualify(self) -> CompletedPoolCutover:
        """Re-read root, immediate predecessor and exact inherited pool baseline."""
        try:
            if load_completed_upgrade(self.original.selector) != self.original:
                raise ValueError
            predecessor = self.predecessor
            if isinstance(predecessor, CompletedPoolCutover):
                if load_completed_pool(predecessor.selector, original=self.original) != predecessor:
                    raise ValueError
                pool = predecessor
            elif isinstance(predecessor, CompletedRefresh):
                if (load_completed_refresh(predecessor.selector, original=self.original) != predecessor
                        or predecessor.pool_baseline is None):
                    raise ValueError
                pool = predecessor.pool_baseline
            else:
                raise ValueError
            request, setup = self.request, self.original.upgrade.setup
            render = request.resources.switch.render
            root = Path(self.original.selector.operation['inputs_path']).parent.parent
            expected_state = root / 'refresh' / str(request.resources.switch.operation_id) / 'state'
            if (request.pool_baseline != pool.selector.model_dump(mode='json')
                    or request.installation_anchor != self.original.upgrade.original_anchor
                    or request.resources.binding != setup.binding
                    or request.resources.shared_namespace_uid != setup.shared_namespace_uid
                    or render.before != predecessor.deployment or render.active != predecessor.active
                    or self.state_dir != expected_state or self.state_dir != self.state_dir.resolve()
                    or any(request.history.get(path) != checksum for path, checksum in predecessor.history.items())
                    or _uid(render.active) != _uid(pool.active)):
                raise ValueError
            render_refresh(render)
            references_changed = platform_references_changed(render.before, render.after)
            if request.platform_rollout is None:
                if self.successor is not None or references_changed:
                    raise ValueError
            elif (self.successor is None or self.successor.reference != request.platform_rollout
                    or pool.completion.outcome != 'legacy'
                    or self.successor.config != render.after.installation.foundation.platform_config):
                raise ValueError
            failed = self.superseded
            if failed is None:
                if request.resources.switch.initial_stopped is not None:
                    raise ValueError
            elif (load_failed_refresh(failed.request, failed.selector) != failed
                    or request.resources.switch.initial_stopped != failed.stopped
                    or failed.request.resources.switch.operation_id == request.resources.switch.operation_id
                    or failed.request.installation_anchor != request.installation_anchor
                    or failed.request.resources.binding != request.resources.binding
                    or failed.request.resources.shared_namespace_uid != request.resources.shared_namespace_uid
                    or failed.request.resources.manager_revision != request.resources.manager_revision
                    or failed.request.resources.switch.render.active != render.active
                    or failed.request.resources.switch.render.before != render.before
                    or failed.request.pool_baseline != request.pool_baseline
                    or any(request.history.get(path) != checksum for path, checksum in failed.history.items())):
                raise ValueError
            return pool
        except Exception:
            raise ValueError('pool_refresh_projection_unqualified') from None

    def provider_options(self) -> tuple[dict[str, Any], dict[str, tuple[str, str, str, str, str]]]:
        """Project source-selected primary storage and build-limit references.

        Historical registration and the stopped pool observer remain untouched.
        Live collector identity, credentials and native disk still need the
        existing provider qualifier before and after its bounded observation.
        """
        from loom.nebius_platform_render import EXECUTION_DISK_STORAGE_QUOTAS

        pool = self.qualify()
        context, successor = pool.context, self.successor
        original = context.request.collector_config
        quotas = copy.deepcopy(context.inputs.installation.quota_identities)
        desired = copy.deepcopy(original)
        if successor is None:
            return desired, quotas
        key = 'LOOM_EXECUTION_CAPACITY_COLLECTOR_QUOTA_STORAGE_NAME'
        selected = successor.resources[_key(original)]
        if (selected.get('apiVersion') != 'v1' or selected.get('kind') != 'ConfigMap'
                or _key(selected) != _key(original)
                or original['metadata']['namespace'] != successor.config['execution_namespace']):
            raise ValueError('pool_refresh_provider_projection_unqualified')
        name = EXECUTION_DISK_STORAGE_QUOTAS[successor.config.get('execution_disk_type', 'NETWORK_SSD')]
        desired['data'][key] = name
        if isinstance(successor.config.get('task_image_builder'), dict):
            limit_key = 'LOOM_EXECUTION_CAPACITY_COLLECTOR_BUILD_CONCURRENCY_LIMIT'
            limit = successor.config['task_image_builder'].get('max_concurrent')
            if limit is None:
                desired['data'].pop(limit_key, None)
            else:
                desired['data'][limit_key] = str(limit)
        if selected['data'] != desired['data'] or original['data'][key] != quotas['storage'][3]:
            raise ValueError('pool_refresh_provider_projection_unqualified')
        storage = quotas['storage']
        quotas['storage'] = (*storage[:3], name, storage[4])
        return desired, quotas

    def matches_provider(self, actual: dict[str, Any], expected: dict[str, Any], original: dict[str, Any]) -> bool:
        """Ignore only ordinary kubectl bookkeeping on the retained collector."""
        successor = self.successor
        if successor is None:
            return _matches(actual, expected, _uid(original))
        selected = successor.resources.get(_key(original))
        if (original.get('kind') != 'ConfigMap' or selected is None
                or selected.get('kind') != 'ConfigMap' or _key(selected) != _key(original)
                or original['metadata']['namespace'] != successor.config['execution_namespace']):
            return False
        observed, desired = copy.deepcopy(actual), copy.deepcopy(expected)
        for document in (observed, desired):
            annotations = document['metadata'].get('annotations', {})
            annotations.pop('kubectl.kubernetes.io/last-applied-configuration', None)
            if not annotations:
                document['metadata'].pop('annotations', None)
        return _matches(observed, desired, _uid(original))

    def manager_revision_options(self) -> tuple[str, ...]:
        """Read this phase within the live reader's existing scope qualification."""
        return refresh_manager_revision_options(self.request, state=self.state_dir,
            anchor=self.state_dir.parent / 'anchor')

    def database_documents(self, *, original: dict[str, Any], current: dict[str, Any],
            service: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Compare only a proven primary DB's ordinary rollout markers differently."""
        successor = self.successor
        if (successor is None or original.get('kind') != 'StatefulSet'
                or original['metadata']['name'] != 'loom-postgres'
                or original['metadata']['namespace'] != successor.config['namespace']):
            return current, original
        # The connected caller has already qualified this refresh's pool context.
        # Keep the database rooted in that same immutable participant binding.
        pool = (self.predecessor if isinstance(self.predecessor, CompletedPoolCutover)
            else self.predecessor.pool_baseline)
        if pool is None:
            raise ValueError('pool_refresh_database_projection_unqualified')
        target, = (row for row in pool.context.request.fencing.retirement.migration.guards
            if row.namespace == successor.config['namespace'])
        if (target.database is None or target.database.statefulset != original
                or target.database.service != service):
            raise ValueError('pool_refresh_database_projection_unqualified')
        selected = successor.resources[_key(original)]
        selected_service = successor.resources[_key(service)]
        if (selected.get('kind') != 'StatefulSet' or _key(selected) != _key(original)
                or service.get('kind') != 'Service' or service['metadata']['name'] != 'loom-postgres'
                or service['metadata']['namespace'] != successor.config['namespace']
                or selected_service.get('kind') != 'Service' or _key(selected_service) != _key(service)):
            raise ValueError('pool_refresh_database_projection_unqualified')
        revision = selected['spec']['template']['metadata']['annotations']['loom.nebius/configuration-revision']
        if not isinstance(revision, str) or not revision:
            raise ValueError('pool_refresh_database_projection_unqualified')
        expected = copy.deepcopy(original)
        expected['spec']['template']['metadata'].setdefault('annotations', {})[
            'loom.nebius/configuration-revision'] = revision
        from scripts.ops.nebius_staging_pool_attachment import _qualified_workload

        # The source selects this identifier, not different DB settings or routing.
        _qualified_workload(selected, expected)
        _qualified_workload(selected_service, service)
        observed = copy.deepcopy(current)
        for document in (observed, expected):
            annotations = document['metadata'].get('annotations', {})
            annotations.pop('kubectl.kubernetes.io/last-applied-configuration', None)
            if not annotations:
                document['metadata'].pop('annotations', None)
        return observed, expected

    def workload_options(self) -> dict[str, tuple[dict[str, Any], ...]]:
        """Permit only exact manager journal states; preserve all other workloads."""
        try:
            pool = self.qualify()
            options: dict[str, tuple[dict[str, Any], ...]] = {
                key: (self.successor_document(row),) for key, row in pool.completion.workloads.items()}
            options[_key(pool.context.request.manager)] = refresh_manager_options(
                self.request, state=self.state_dir, anchor=self.state_dir.parent / 'anchor')
            return options
        except Exception:
            raise ValueError('pool_refresh_projection_unqualified') from None

    def successor_document(self, original: dict[str, Any]) -> dict[str, Any]:
        """Only source-selected application workloads; never manager/pool authority."""
        successor = self.successor
        if (successor is None or original['kind'] not in {'Deployment', 'CronJob'}
                or original['metadata']['namespace'] not in {
                    successor.config['namespace'], successor.config['execution_namespace']}):
            return copy.deepcopy(original)
        pool = (self.predecessor if isinstance(self.predecessor, CompletedPoolCutover)
            else self.predecessor.pool_baseline)
        if pool is None:
            raise ValueError('pool_refresh_projection_unqualified')
        dormant = pool.context.request.fencing.retirement.dormant_consumers
        if any(_key(original) == _key(row) for consumer in dormant
                for row in (consumer.actuator, consumer.collector)):
            # Explicit history-bound remote roots remain closed even when the
            # ordinary renderer no longer emits their regional configuration.
            return copy.deepcopy(original)
        desired = copy.deepcopy(successor.resources[_key(original)])
        if desired['kind'] == 'Deployment':
            if desired['spec']['selector'] != original['spec']['selector']:
                raise ValueError
            before_template, after_template = (row['spec']['template'] for row in (original, desired))
        else:
            before_template, after_template = (row['spec']['jobTemplate']['spec']['template'] for row in (original, desired))
        old, new = before_template['spec'], after_template['spec']
        if old.get('serviceAccountName', 'default') != new.get('serviceAccountName', 'default'):
            raise ValueError
        from scripts.ops.nebius_staging_pool_attachment import _exact_pod

        # Credential and DB bindings remain rooted in the retained workload for
        # both long-lived consumers and periodic writers.
        for field in ('containers', 'initContainers'):
            before = {c['name']: {e['name']: e for e in c.get('env', [])
                if 'valueFrom' in e or e['name'].endswith('_DB_URL')} for c in old.get(field, [])}
            after = {c['name']: {e['name']: e for e in c.get('env', [])
                if 'valueFrom' in e or e['name'].endswith('_DB_URL')} for c in new.get(field, [])}
            if not _exact_pod(before, after, (field,)):
                raise ValueError
        old_volumes, new_volumes = old.get('volumes', []), new.get('volumes', [])
        recovery = 'recovery-platform-config'
        added = [volume for volume in new_volumes if volume.get('name') == recovery]
        if added and not any(volume.get('name') == recovery for volume in old_volumes):
            # A qualified ordinary release may add the controller's read-only
            # recovery configuration. Its existing credential/storage volumes
            # remain frozen; this is not permission to add arbitrary sources.
            volume = {'name': recovery, 'configMap': {'name': 'loom-platform-config', 'items': [
                {'key': 'profile.json', 'path': 'profile.json'},
                {'key': 'environment.json', 'path': 'environment.json'}]}}
            mount = {'name': recovery, 'mountPath': '/var/run/loom-platform', 'readOnly': True}
            containers = new.get('containers', [])
            if (original['kind'] != 'Deployment' or original['metadata']['name'] != 'loom-control-plane'
                    or original['metadata']['namespace'] != successor.config['namespace']
                    or len(added) != 1 or not _exact_pod(added[0], volume, ('volumes', '*'))
                    or len(containers) != 1 or containers[0]['name'] != 'loom-control-plane'
                    or not _exact_pod([row for row in containers[0].get('volumeMounts', [])
                        if row.get('name') == recovery], [mount], ('containers', '*', 'volumeMounts'))
                    or any(row.get('name') == recovery for container in new.get('initContainers', [])
                        for row in container.get('volumeMounts', []))
                    or any(row.get('name') == recovery
                        for field in ('containers', 'initContainers') for container in old.get(field, [])
                        for row in container.get('volumeMounts', []))):
                raise ValueError
            new_volumes = [volume for volume in new_volumes if volume.get('name') != recovery]
        if (not _exact_pod(old_volumes, new_volumes, ('volumes',))
                or not _exact_pod(old.get('imagePullSecrets', []), new.get('imagePullSecrets', []), ('imagePullSecrets',))):
            raise ValueError
        if desired['kind'] == 'Deployment':
            # Preserve the already-frozen restart marker; the live template must
            # still match its exact value, including through ordinary releases.
            restart = 'kubectl.kubernetes.io/restartedAt'
            annotations = before_template['metadata'].get('annotations', {})
            if restart in annotations:
                after_template['metadata'].setdefault('annotations', {}).setdefault(restart,
                    copy.deepcopy(annotations[restart]))
        desired['metadata']['uid'] = _uid(original)
        return desired

    def matches_workload(self, actual: dict[str, Any], expected: dict[str, Any], original: dict[str, Any]) -> bool:
        if self.successor is None or self.successor_document(original) == original:
            return _matches(actual, expected, _uid(original))
        from scripts.ops.nebius_staging_pool_attachment import _qualified_workload

        try:
            if _uid(actual) != _uid(original) or _key(actual) != _key(original):
                return False
            _qualified_workload(expected, actual)
            if expected['kind'] == 'Deployment':
                templates = [row['spec']['template'] for row in (actual, expected)]
            else:
                templates = [row['spec']['jobTemplate']['spec']['template'] for row in (actual, expected)]
            if templates[0]['metadata'] != templates[1]['metadata']:
                return False
            _snapshot(actual)
            return True
        except Exception:
            return False
