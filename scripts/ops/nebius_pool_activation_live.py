"""Activation/recovery on the closed parent's retained protected authority.

No public shortcut, new credentials, arbitrary SQL or automatic write retry.
The anchored activation journal owns ordering and all uncertain outcomes.
"""
from __future__ import annotations

import copy
from collections.abc import Callable
from typing import Any

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_activation_stage import activation_record
from scripts.ops.nebius_pool_gateway_authority import (
    gateway_review_namespaces,
    review_gateway_rules,
)
from scripts.ops.nebius_pool_gateway_retirement import (
    _gateway_record,
    qualify_gateway_retirement_drain,
)
from scripts.ops.nebius_pool_legacy_reopening import (
    _reopening_record,
    observe_legacy_reopening,
    qualify_legacy_reopening,
)
from scripts.ops.nebius_pool_legacy_restart import _restart_record, qualify_legacy_restart
from scripts.ops.nebius_pool_machine_database import MachineRetirementState
from scripts.ops.nebius_pool_machine_retirement import (
    _machine_record,
    qualify_machine_retirement_drain,
)
from scripts.ops.nebius_pool_migration import PoolGuardTarget
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_pool_role_restoration import _role_record, qualify_role_restoration
from scripts.ops.nebius_pool_shutdown import _shutdown_record
from scripts.ops.nebius_pool_startup import (
    _startup_record,
    _startup_workload_state,
    closed_startup_documents,
)
from scripts.ops.nebius_pool_startup_fence import (
    _fence_record,
    _fence_sources,
    marked_startup_document,
    observe_recovery_workloads,
    startup_fence_patches,
)
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI
from scripts.ops.nebius_pool_template_restoration import (
    RecoveryDrainPending,
    _template_record,
    qualify_template_restoration,
    template_restoration_exists,
)


class HTTPSPoolActivationAPI(HTTPSPoolStartupAPI):
    def verify_retained(self) -> None:
        """Recover with active work or unhealthy successors, never weaker scope."""
        try:
            self._scope()
            if closed_startup_documents(self.request, state_dir=self.state, anchor_dir=self.anchor) != (self.closed, self.targets):
                raise ValueError
            parent = self.parent
            migration = self.request.fencing.retirement.migration
            if parent.guards.request != migration:
                raise ValueError
            parent.history.qualify_binding(migration, self.request.manager)
            parent.qualify_writer_bindings()
            # Do not call parent.preflight: its initial idle/backlog checks are
            # invalid after intake opens. These retained entry checks still
            # qualify private inputs, exact backends and physical provider scope.
            parent.checks.preflight(self.request)
            self._qualify_retained_resources()
            for guard in migration.guards:
                if parent.guards.runtime_role(guard, 'inspect') != {'status': 'qualified'}:
                    raise ValueError
            observe_recovery_workloads(self.request, self, state=self.state, anchor=self.anchor)
            parent.qualify_writer_bindings()
            self._scope()
        except Exception:
            raise ValueError('pool_activation_retained_scope_unqualified') from None

    def qualify_runtime(self) -> None:
        self.qualify_database_runtimes()
        self.qualify_gateway_authority()

    def pool_state(self) -> str:
        self._scope()
        return self.parent.history.activation_pool('observe')

    def _guard(self, participant: str) -> PoolGuardTarget:
        self._scope()
        target, = (row for row in self.request.fencing.retirement.migration.guards if str(row.participant_id) == participant)
        return target

    def guard_state(self, participant: str) -> str:
        return self.parent.guards.activation_guard(self._guard(participant), 'observe')

    def _recovery_fence(self) -> dict[str, Any]:
        _, startup = _startup_record(self.request, state=self.state, anchor=self.anchor,
            closed=self.closed, targets=self.targets)
        _, record = _fence_record(self.request, state=self.state, anchor=self.anchor,
            closed=self.closed, targets=self.targets, startup=startup)
        if (record is None or any(row['phase'] != 'fenced' for row in record['workloads'].values())
                or self.pool_state() != 'fenced' or any(self.guard_state(str(row.participant_id)) != 'fenced'
                    for row in self.request.fencing.retirement.migration.guards)):
            raise ValueError('pool_recovery_fence_unconfirmed')
        return record

    def recovery_drained(self) -> bool:
        """Fresh closed-intake drain across both journals, not shutdown authority."""
        try:
            # Stage and mutation boundaries own the complete retained-authority
            # inventory. Drain reads stay fresh, but must not recursively repeat
            # those whole-cluster scans for every ledger observation.
            self._scope()
            before = self._recovery_fence()
            # Check every participant even when the global ledger is still busy.
            # No status is persisted, and no callback may mutate the journals.
            results = [self.parent.history.recovery_pool_drained()]
            results.extend(self.parent.guards.recovery_participant_drained(row)
                for row in self.request.fencing.retirement.migration.guards)
            self._scope()
            if self._recovery_fence() != before or any(type(value) is not bool for value in results):
                raise ValueError
            return all(results)
        except Exception:
            raise ValueError('pool_recovery_drain_unconfirmed') from None

    def _stop_patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool,
                    record_intent: Callable[[dict[str, Any]], None] | None = None) -> bool:
        try:
            from scripts.ops.nebius_pool_manager_image_history import (
                IMAGE_MARKER,
                original_recovery_image,
            )

            self._scope()
            closed, originals, targets, _, record = _shutdown_record(self.request, state=self.state, anchor=self.anchor)
            image = original_recovery_image(self.request, state=self.state, anchor=self.anchor)
            version = before['metadata']['resourceVersion']
            options = (originals[key],)
            if image is not None and image.record is not None and key == _key(self.request.manager):
                if image.record['phases']['isolate']['phase'] == 'intent':
                    options = image.documents[:2]
            item = {} if record is None else record['workloads'][key]
            item = item.get('isolation_stop', item)
            if (closed != self.closed or record is None or key not in targets
                    or originals[key] == targets[key] or not any(_matches(before, row, _uid(closed[key])) for row in options)
                    or _stable(desired) != targets[key]
                    or not isinstance(version, str) or not 0 < len(version) <= 128
                    or item != {'phase': 'prepared', 'before_resource_version': None}
                    or (not preview and record_intent is None)):
                raise ValueError
            fence = self._recovery_fence()
            if self.recovery_drained() is not True:
                raise ValueError
            if not preview:
                # Ledger reads can race changed authority. A dry-run or prior
                # stage observation is never the actual stop's final proof.
                self.verify_retained()
            if self._recovery_fence() != fence:
                raise ValueError
            if _shutdown_record(self.request, state=self.state, anchor=self.anchor)[-1] != record:
                raise ValueError
            # Controller status updates can advance resourceVersion during drain.
            # Reobserve only before dispatch, preserving UID and stable metadata/spec.
            fresh = self.read_workload(key)
            if not _matches(fresh, before, _uid(closed[key])):
                raise ValueError
            before = fresh
            version = before['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            if not preview:
                assert record_intent is not None
                record_intent(before)
                current = _shutdown_record(self.request, state=self.state, anchor=self.anchor)[-1]
                expected = copy.deepcopy(record)
                expected_item = expected['workloads'][key]
                expected_item = expected_item.get('isolation_stop', expected_item)
                expected_item.update(phase='intent', before_resource_version=version)
                if current != expected:
                    raise ValueError
            field = 'suspend' if targets[key]['kind'] == 'CronJob' else 'replicas'
            patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(closed[key])},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'replace', 'path': '/spec/' + field, 'value': targets[key]['spec'][field]}]
            if image is not None and key == _key(self.request.manager) and IMAGE_MARKER in before['metadata'].get('annotations', {}):
                patches.append({'op': 'remove', 'path': '/metadata/annotations/loom.nebius~1manager-image-repair'})
            with self.parent.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                return _patch_result(response, desired=desired, uid=_uid(closed[key]))
        except Exception:
            raise ValueError('pool_shutdown_update_unconfirmed') from None

    def preview_stop(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._stop_patch(key, before, desired, preview=True) else None

    def stop_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any], *,
                      record_intent: Callable[[dict[str, Any]], None] | None = None) -> bool:
        return self._stop_patch(key, before, desired, preview=False, record_intent=record_intent)

    def successor_drained(self, key: str, desired: dict[str, Any]) -> bool:
        """Fresh process drain of the anchored closed successor or restored spec."""
        try:
            def observation():
                if not template_restoration_exists(self.request, state=self.state, anchor=self.anchor):
                    return _shutdown_record(self.request, state=self.state, anchor=self.anchor), None
                current = _startup_workload_state(self.request, state_dir=self.state, anchor_dir=self.anchor)
                if current is None or current[1] is None:
                    raise ValueError
                return current[1], current[0]

            shutdown, options = observation()
            closed, _, targets, _, record = shutdown
            if (closed != self.closed or record is None or key not in targets
                    or record['workloads'][key]['phase'] != 'stopped' or _stable(desired) != targets[key]):
                raise ValueError
            current = self.read_workload(key)
            choices = (targets[key],) if options is None else options[key]
            expected, = (row for row in choices if _matches(current, row, _uid(closed[key])))
            namespace = str(current['metadata']['namespace'])
            path = ('/apis/apps/v1/namespaces/' + namespace + '/replicasets' if current['kind'] == 'Deployment'
                else '/apis/batch/v1/namespaces/' + namespace + '/jobs')
            children = self.parent._request('GET', path + '?limit=1000')
            pods = self.parent._request('GET', '/api/v1/namespaces/' + namespace + '/pods?limit=1000')
            if children is None or pods is None or _stable(self.read_workload(key)) != _stable(current):
                raise ValueError
            after, current_options = observation()
            if after[-1] != record or current_options != options:
                raise ValueError
            return qualify_closed_workload_drain(original=closed[key], desired=expected,
                current=current, children=children, pods=pods)
        except Exception:
            raise ValueError('pool_successor_drain_unconfirmed') from None

    def machine_authority(self) -> MachineRetirementState:
        self._scope()
        state = self.parent.history.machine_retirement('observe')
        self._scope()
        return state

    def retire_machines(self) -> None:
        try:
            self._scope()
            _, _, record = _machine_record(self.request, state=self.state, anchor=self.anchor)
            if record is None or record['phase'] != 'intent':
                raise ValueError
            if qualify_machine_retirement_drain(self.request, self, state=self.state, anchor=self.anchor) is not None:
                raise ValueError
            if _machine_record(self.request, state=self.state, anchor=self.anchor)[-1] != record:
                raise ValueError
            if self.parent.history.machine_retirement('revoke') != 'revoked':
                raise ValueError
            self._scope()
            if _machine_record(self.request, state=self.state, anchor=self.anchor)[-1] != record:
                raise ValueError
        except Exception:
            raise ValueError('pool_machine_retirement_update_unconfirmed') from None

    def read_gateway_authority(self, key: str) -> dict[str, Any]:
        try:
            self._scope()
            originals, _, _, _ = _gateway_record(self.request, state=self.state, anchor=self.anchor)
            original = originals[key]
            actual = self.parent.resources.get_resource(self.parent.documents[key])
            if (actual is None or _key(actual) != key or _uid(actual) != _uid(original)
                    or any(actual.get(field) != original[field] for field in ('apiVersion', 'kind'))
                    or not isinstance(actual['metadata'].get('resourceVersion'), str)
                    or not 0 < len(actual['metadata']['resourceVersion']) <= 128):
                raise ValueError
            _snapshot(actual)
            return actual
        except Exception:
            raise ValueError('pool_gateway_authority_read_unqualified') from None

    def _gateway_role_patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> bool:
        try:
            self._scope()
            originals, targets, _, record = _gateway_record(self.request, state=self.state, anchor=self.anchor)
            version = before['metadata']['resourceVersion']
            if (record is None or key not in targets or not _matches(before, originals[key], _uid(originals[key]))
                    or _stable(desired) != _stable(targets[key])
                    or not isinstance(version, str) or not 0 < len(version) <= 128
                    or record['roles'][key] != {'phase': 'prepared' if preview else 'intent',
                        'before_resource_version': None if preview else version}):
                raise ValueError
            # Dry-run validates the fixed CAS; the actual mutation independently
            # proves current authority and drain immediately before dispatch.
            if not preview and qualify_gateway_retirement_drain(self.request, self, state=self.state, anchor=self.anchor) is not None:
                raise ValueError
            if _gateway_record(self.request, state=self.state, anchor=self.anchor)[-1] != record:
                raise ValueError
            patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(originals[key])},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/rules', 'value': before['rules']},
                {'op': 'replace', 'path': '/rules', 'value': targets[key]['rules']}]
            path = self.parent._approved(self.parent.documents[key]) + '/' + originals[key]['metadata']['name']
            with self.parent.client.stream('PATCH', path + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                return _patch_result(response, desired=desired, uid=_uid(originals[key]))
        except Exception:
            raise ValueError('pool_gateway_role_update_unconfirmed') from None

    def preview_gateway_role(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._gateway_role_patch(key, before, desired, preview=True) else None

    def restrict_gateway_role(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._gateway_role_patch(key, before, desired, preview=False)

    def qualify_gateway_retired(self) -> None:
        """Prove effective read-only rights, not just the fixed Role manifests."""
        try:
            if qualify_gateway_retirement_drain(self.request, self, state=self.state, anchor=self.anchor) is not None:
                raise ValueError
            self.qualify_gateway_readonly()
            if qualify_gateway_retirement_drain(self.request, self, state=self.state, anchor=self.anchor) is not None:
                raise ValueError
        except Exception:
            raise ValueError('pool_gateway_retired_authority_unconfirmed') from None

    def qualify_gateway_readonly(self) -> None:
        """Effective permission proof without requiring old workloads stopped.

        The closed restart barrier independently proves the gateway's process
        drain. Existing retirement retains its all-successor-stopped checks.
        """
        try:
            originals, targets, _, record = _gateway_record(self.request, state=self.state, anchor=self.anchor)
            if record is None or any(row['phase'] != 'restricted' for row in record['roles'].values()):
                raise ValueError
            self.verify_retained()
            bindings = [row for resource, kind in (('rolebindings', 'RoleBinding'), ('clusterrolebindings', 'ClusterRoleBinding'))
                for row in inventory_resources(self.parent._request, 'rbac.authorization.k8s.io/v1', resource, kind)]
            migration = self.request.fencing.retirement.migration
            authority = list({**originals, **targets}.values())
            for namespace in gateway_review_namespaces(migration, bindings):
                review_gateway_rules(self.parent.client, manager_namespace=migration.registration.binding.namespace,
                    namespace=namespace, authority=authority)
            self.verify_retained()
            if _gateway_record(self.request, state=self.state, anchor=self.anchor)[-1] != record:
                raise ValueError
        except Exception:
            raise ValueError('pool_gateway_readonly_authority_unconfirmed') from None

    def _legacy_template_patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool,
                               record_intent: Callable[[dict[str, Any]], None] | None = None) -> bool | RecoveryDrainPending:
        try:
            self._scope()
            closed, originals, targets, _, record = _template_record(self.request, state=self.state, anchor=self.anchor)
            version = before['metadata']['resourceVersion']
            if (closed != self.closed or record is None or key not in targets or originals[key] == targets[key]
                    or not _matches(before, originals[key], _uid(closed[key])) or _stable(desired) != targets[key]
                    or not isinstance(version, str) or not 0 < len(version) <= 128
                    or (not preview and record_intent is None)
                    or record['workloads'][key] != {'phase': 'prepared', 'before_resource_version': None}):
                raise ValueError
            # Dry-run validates the fixed CAS; the actual mutation independently
            # proves current authority and drain immediately before dispatch.
            if not preview:
                pending = qualify_template_restoration(self.request, self, state=self.state, anchor=self.anchor)
                if pending is not None:
                    return RecoveryDrainPending(pending)
            if _template_record(self.request, state=self.state, anchor=self.anchor)[-1] != record:
                raise ValueError
            fresh = self.read_workload(key)
            if not _matches(fresh, before, _uid(closed[key])):
                raise ValueError
            before = fresh
            version = before['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            if not preview:
                assert record_intent is not None
                record_intent(before)
                expected = copy.deepcopy(record)
                expected['workloads'][key] = {'phase': 'intent', 'before_resource_version': version}
                if _template_record(self.request, state=self.state, anchor=self.anchor)[-1] != expected:
                    raise ValueError
            patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(closed[key])},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'replace', 'path': '/spec', 'value': targets[key]['spec']}]
            with self.parent.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                return _patch_result(response, desired=desired, uid=_uid(closed[key]))
        except Exception:
            raise ValueError('pool_legacy_template_update_unconfirmed') from None

    def preview_legacy_template(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._legacy_template_patch(key, before, desired, preview=True) else None

    def restore_legacy_template(self, key: str, before: dict[str, Any], desired: dict[str, Any], *,
                              record_intent: Callable[[dict[str, Any]], None] | None = None) -> bool | RecoveryDrainPending:
        return self._legacy_template_patch(key, before, desired, preview=False, record_intent=record_intent)

    def read_legacy_role(self, key: str) -> dict[str, Any]:
        self._scope()
        return self.parent.fencing.read_role(key)

    def _legacy_role_patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool,
                           record_intent: Callable[[dict[str, Any]], None] | None = None) -> bool | RecoveryDrainPending:
        try:
            self._scope()
            originals, reduced, targets, _, record = _role_record(self.request, state=self.state, anchor=self.anchor)
            version = before['metadata']['resourceVersion']
            if (record is None or key not in targets or not _matches(before, reduced[key], _uid(originals[key]))
                    or _stable(desired) != targets[key]
                    or not isinstance(version, str) or not 0 < len(version) <= 128
                    or (not preview and record_intent is None)
                    or record['roles'][key] != {'phase': 'prepared', 'before_resource_version': None}):
                raise ValueError
            # Dry-run validates the fixed CAS; the actual mutation independently
            # proves current authority and drain immediately before dispatch.
            if not preview:
                pending = qualify_role_restoration(self.request, self, state=self.state, anchor=self.anchor)
                if pending is not None:
                    return RecoveryDrainPending(pending)
            if _role_record(self.request, state=self.state, anchor=self.anchor)[-1] != record:
                raise ValueError
            fresh = self.read_legacy_role(key)
            if not _matches(fresh, before, _uid(originals[key])):
                raise ValueError
            before = fresh
            version = before['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            if not preview:
                assert record_intent is not None
                record_intent(before)
                expected = copy.deepcopy(record)
                expected['roles'][key] = {'phase': 'intent', 'before_resource_version': version}
                if _role_record(self.request, state=self.state, anchor=self.anchor)[-1] != expected:
                    raise ValueError
            patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(originals[key])},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/rules', 'value': before['rules']},
                {'op': 'replace', 'path': '/rules', 'value': targets[key]['rules']},
                {'op': 'replace', 'path': '/metadata/annotations', 'value': targets[key]['metadata']['annotations']}
                    if 'annotations' in targets[key]['metadata'] else {'op': 'remove', 'path': '/metadata/annotations'}]
            path = ('/apis/rbac.authorization.k8s.io/v1/namespaces/' + originals[key]['metadata']['namespace']
                + '/roles/' + originals[key]['metadata']['name'])
            with self.parent.client.stream('PATCH', path + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                return _patch_result(response, desired=desired, uid=_uid(originals[key]))
        except Exception:
            raise ValueError('pool_legacy_role_update_unconfirmed') from None

    def preview_legacy_role(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._legacy_role_patch(key, before, desired, preview=True) else None

    def restore_legacy_role(self, key: str, before: dict[str, Any], desired: dict[str, Any], *,
                            record_intent: Callable[[dict[str, Any]], None] | None = None) -> bool | RecoveryDrainPending:
        return self._legacy_role_patch(key, before, desired, preview=False, record_intent=record_intent)

    def _legacy_restart_patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool,
                               record_intent: Callable[[dict[str, Any]], None] | None = None) -> bool | RecoveryDrainPending:
        try:
            self._scope()
            originals, stopped, targets, _, record = _restart_record(self.request, state=self.state, anchor=self.anchor)
            version = before['metadata']['resourceVersion']
            if (record is None or key not in targets or stopped[key] == targets[key]
                    or not _matches(before, stopped[key], _uid(originals[key])) or _stable(desired) != targets[key]
                    or not isinstance(version, str) or not 0 < len(version) <= 128
                    or (not preview and record_intent is None)
                    or record['workloads'][key] != {'phase': 'prepared', 'before_resource_version': None}):
                raise ValueError
            if not preview:
                pending = qualify_legacy_restart(self.request, self, state=self.state, anchor=self.anchor)
                if pending is not None:
                    return RecoveryDrainPending(pending)
            if _restart_record(self.request, state=self.state, anchor=self.anchor)[-1] != record:
                raise ValueError
            fresh = self.read_workload(key)
            if not _matches(fresh, before, _uid(originals[key])):
                raise ValueError
            before = fresh
            version = before['metadata']['resourceVersion']
            if not isinstance(version, str) or not 0 < len(version) <= 128:
                raise ValueError
            if not preview:
                assert record_intent is not None
                record_intent(before)
                expected = copy.deepcopy(record)
                expected['workloads'][key] = {'phase': 'intent', 'before_resource_version': version}
                if _restart_record(self.request, state=self.state, anchor=self.anchor)[-1] != expected:
                    raise ValueError
            field = 'suspend' if before['kind'] == 'CronJob' else 'replicas'
            patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(originals[key])},
                {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
                {'op': 'test', 'path': '/metadata', 'value': before['metadata']},
                {'op': 'test', 'path': '/spec', 'value': before['spec']},
                {'op': 'replace', 'path': '/spec/' + field, 'value': targets[key]['spec'][field]}]
            with self.parent.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                return _patch_result(response, desired=desired, uid=_uid(originals[key]))
        except Exception:
            raise ValueError('pool_legacy_restart_update_unconfirmed') from None

    def preview_legacy_restart(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._legacy_restart_patch(key, before, desired, preview=True) else None

    def restart_legacy_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any], *,
                              record_intent: Callable[[dict[str, Any]], None] | None = None) -> bool | RecoveryDrainPending:
        return self._legacy_restart_patch(key, before, desired, preview=False, record_intent=record_intent)

    def qualify_legacy_runtimes(self) -> None:
        """Fresh runtime proof behind recovery guards, never an opening receipt."""
        try:
            def observe() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
                self.verify_retained()
                record = _restart_record(self.request, state=self.state, anchor=self.anchor)[-1]
                if record is None or any(row['phase'] != 'started' for row in record['workloads'].values()):
                    raise ValueError
                return observe_recovery_workloads(self.request, self, state=self.state, anchor=self.anchor), record

            expected, record = observe()
            if qualify_legacy_restart(self.request, self, state=self.state, anchor=self.anchor) is not None:
                raise ValueError
            self._legacy_runtime_probes(expected)
            if qualify_legacy_restart(self.request, self, state=self.state, anchor=self.anchor) is not None or observe()[1] != record:
                raise ValueError
        except Exception:
            raise ValueError('pool_legacy_runtimes_unqualified') from None

    def _legacy_runtime_probes(self, expected: dict[str, dict[str, Any]]) -> None:
        """Same exact runtime proof under either independently qualified barrier."""
        parent, migration = self.parent, self.request.fencing.retirement.migration
        parent.history.qualify_binding(migration, self.request.manager)
        parent.history.qualify_manager_database(expected=expected[_key(self.request.manager)])
        parent.history.qualify_manager_legacy_settings(expected=expected[_key(self.request.manager)])
        for target in migration.guards:
            binding = target.database
            if (binding is None or binding.actuator_credential_uid is None
                    or binding.actuator_credential_resource_version is None):
                raise ValueError
            participant, = (row for row in migration.registration.spec.participants if row.participant_id == target.participant_id)
            service, = (row for row in self.request.services if row['metadata']['namespace'] == target.namespace)
            actuators = tuple(row for row in self.request.fencing.retirement.actuators
                if row['metadata']['namespace'] == participant.execution_namespace.name)
            for original in (target.controller, service, *actuators):
                actuator = original['metadata']['namespace'] != target.namespace
                parent.guards.qualify_runtime_database(target, original=original, expected=expected[_key(original)],
                    credential_uid=binding.actuator_credential_uid if actuator else binding.credential_uid,
                    credential_resource_version=binding.actuator_credential_resource_version if actuator else binding.credential_resource_version)
                parent.guards.qualify_runtime_legacy_settings(target, original=original, expected=expected[_key(original)])
                if actuator:
                    parent.guards.qualify_runtime_telemetry(target, original=original, expected=expected[_key(original)])

    def pool_recovery_drained(self) -> bool:
        self._scope()
        result = self.parent.history.recovery_pool_drained()
        self._scope()
        if type(result) is not bool:
            raise ValueError('pool_recovery_drain_unconfirmed')
        return result

    def participant_recovery_drained(self, participant: str) -> bool:
        result = self.parent.guards.recovery_participant_drained(self._guard(participant))
        self._scope()
        if type(result) is not bool:
            raise ValueError('pool_participant_recovery_drain_unconfirmed')
        return result

    def qualify_reopening_runtimes(self) -> None:
        """Actual restored runtimes, with only anchored partial-open allowance."""
        try:
            record, _ = observe_legacy_reopening(self.request, self, state=self.state, anchor=self.anchor)
            if record is None or qualify_legacy_reopening(self.request, self, state=self.state, anchor=self.anchor) is not None:
                raise ValueError
            expected = observe_recovery_workloads(self.request, self, state=self.state, anchor=self.anchor)
            self._legacy_runtime_probes(expected)
            if (qualify_legacy_reopening(self.request, self, state=self.state, anchor=self.anchor) is not None
                    or observe_legacy_reopening(self.request, self, state=self.state, anchor=self.anchor)[0] != record):
                raise ValueError
        except Exception:
            raise ValueError('pool_legacy_reopening_runtimes_unqualified') from None

    def release_recovery_guard(self, participant: str) -> None:
        """Only the anchored reopening parent may dispatch this fixed release."""
        try:
            self._scope()
            _, record = _reopening_record(self.request, state=self.state, anchor=self.anchor)
            if (record is None or record['guards'].get(participant) != 'intent'
                    or self.guard_state(participant) != 'fenced'):
                raise ValueError
            target = self._guard(participant)
            self.qualify_reopening_runtimes()
            if (_reopening_record(self.request, state=self.state, anchor=self.anchor)[1] != record
                    or self.guard_state(participant) != 'fenced'):
                raise ValueError
            if self.parent.guards.release_recovery_guard(target) != 'open':
                raise ValueError
            self.verify_retained()
            if (_reopening_record(self.request, state=self.state, anchor=self.anchor)[1] != record
                    or self.guard_state(participant) != 'open'):
                raise ValueError
        except Exception:
            raise ValueError('pool_legacy_guard_release_unconfirmed') from None

    def _write_record(self) -> dict[str, Any]:
        record = activation_record(self.request, state_dir=self.state, anchor_dir=self.anchor)
        if record is None:
            raise ValueError('pool_activation_intent_required')
        self.verify_retained()
        return record

    def _post_write(self, record: dict[str, Any]) -> None:
        self.verify_retained()
        if activation_record(self.request, state_dir=self.state, anchor_dir=self.anchor) != record:
            raise ValueError('pool_activation_intent_changed')

    def open_pool(self) -> None:
        record = self._write_record()
        if record['opening'] != 'intent' or record['cancellation'] != 'prepared':
            raise ValueError('pool_activation_open_intent_required')
        self.qualify_runtime()
        expected = self._started_workloads()
        key = 'Deployment:' + self.request.fencing.retirement.migration.registration.binding.namespace + ':loom-pool-gateway'
        self.parent.history.open_pool(original=self.closed[key], expected=expected[key])
        self._post_write(record)

    def fence_pool(self) -> None:
        record = self._write_record()
        if record['cancellation'] != 'intent' or self.pool_state() not in {'closed', 'global'}:
            raise ValueError('pool_activation_fence_intent_required')
        if self.parent.history.activation_pool('fence') != 'fenced':
            raise ValueError('pool_activation_fence_unconfirmed')
        self._post_write(record)

    def release_guard(self, participant: str) -> None:
        record, target = self._write_record(), self._guard(participant)
        item = record['guards'][participant]
        if (record['opening'] != 'opened' or record['cancellation'] != 'prepared'
                or item != {'release': 'intent', 'fence': 'prepared'}
                or self.pool_state() != 'global' or self.guard_state(participant) != 'held'):
            raise ValueError('pool_activation_release_intent_required')
        if self.parent.guards.activation_guard(target, 'release') != 'open':
            raise ValueError('pool_activation_release_unconfirmed')
        self._post_write(record)

    def fence_guard(self, participant: str) -> None:
        record, target = self._write_record(), self._guard(participant)
        if (record['cancellation'] != 'fenced' or record['guards'][participant]['fence'] != 'intent'
                or self.pool_state() != 'fenced' or self.guard_state(participant) not in {'held', 'open'}):
            raise ValueError('pool_activation_guard_fence_intent_required')
        if self.parent.guards.activation_guard(target, 'fence') != 'fenced':
            raise ValueError('pool_activation_guard_fence_unconfirmed')
        self._post_write(record)

    def _startup_fence_patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> bool:
        try:
            self._scope()
            _, startup = _startup_record(self.request, state=self.state, anchor=self.anchor,
                closed=self.closed, targets=self.targets)
            _, record = _fence_record(self.request, state=self.state, anchor=self.anchor,
                closed=self.closed, targets=self.targets, startup=startup)
            sources, _ = _fence_sources(self.request, state=self.state, anchor=self.anchor,
                closed=self.closed, targets=self.targets, startup=startup)
            if (record is None or key not in record['workloads'] or key not in sources
                    or record['workloads'][key] != {'phase': 'prepared' if preview else 'intent', 'expected': None}
                    or sources[key][0] is None or before['metadata']['resourceVersion'] != sources[key][0]):
                raise ValueError
            operation = self.request.fencing.retirement.migration.registration.spec.operation_id
            if _stable(desired) != _stable(marked_startup_document(sources[key][1][0], operation)):
                raise ValueError
            from scripts.ops.nebius_pool_manager_image_history import (
                load_manager_image_chain,
                manager_image_fence_patches,
            )
            from scripts.ops.nebius_pool_startup_repair import (
                repair_fence_patches,
                startup_repair_exists,
            )

            images = load_manager_image_chain(self.request, state=self.state, anchor=self.anchor)
            if images and key == _key(images[-1].documents[0]):
                patches = manager_image_fence_patches(self.request, before, state=self.state, anchor=self.anchor)
            elif key == _key(self.request.manager) and startup_repair_exists(self.request, state=self.state, anchor=self.anchor):
                patches = repair_fence_patches(self.request, before, state=self.state, anchor=self.anchor)
            else:
                patches = startup_fence_patches(self.closed[key], before, operation)
            self.verify_retained()
            if self.pool_state() != 'fenced' or any(self.guard_state(str(row.participant_id)) != 'fenced'
                    for row in self.request.fencing.retirement.migration.guards):
                raise ValueError
            with self.parent.client.stream('PATCH', self._path(key) + ('?dryRun=All' if preview else ''),
                    json=patches, headers={'Content-Type': 'application/json-patch+json'}) as response:
                return _patch_result(response, desired=desired, uid=_uid(self.closed[key]))
        except Exception:
            raise ValueError('pool_startup_fence_update_unconfirmed') from None

    def preview_startup_fence(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._startup_fence_patch(key, before, desired, preview=True) else None

    def fence_startup(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._startup_fence_patch(key, before, desired, preview=False)
