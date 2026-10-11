"""Receipt-bound process observation through cancellation and staging restoration.

Unhealthy cancelled applications cannot prevent fencing. A root's own unresolved
stop/restart permits pending readiness; completed effects require current drain
or restored health. This read-only adapter never constructs an active child and
does not replace the parent's independent SQL, writer and provider barriers.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_development_runtime_readiness import qualify_started_deployment
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_migration_guard import KubectlPoolGuardAPI
from scripts.ops.nebius_pool_retirement import qualify_closed_workload_drain
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_recovery_census import _receipt_files, _recovery_options
from scripts.ops.nebius_shared_pool_recovery_collector import SharedRecoveryRuntimeReadiness
from scripts.ops.nebius_shared_pool_recovery_fence import _recovery_frame
from scripts.ops.nebius_shared_pool_recovery_restart import _process_snapshot, _project_inputs

from loom.nebius_platform_render import digest

Document = dict[str, Any]


@dataclass(frozen=True, repr=False)
class SharedPoolRecoveryProcessObservation:
    bindings: SharedPoolGuardBindings
    pending_workloads: tuple[str, ...]
    input_digest: str


def observe_shared_pool_recovery_processes(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        kubeconfig: Path, executable: Path) -> SharedPoolRecoveryProcessObservation:
    """Observe only fixed source roots under the current recovery's exact receipts.

The census owns the full-cluster writer inventory, including retired legacy
roots and database workloads. Here the shutdown roster is the captured staging
and installed dev roots plus gateway; no backup or database root is stopped.
"""
    try:
        retained = copy.deepcopy(bindings)
        files = _receipt_files(state_dir)
        frame, choices, _, receipts = _recovery_options(retained, state_dir)
        if files != receipts:
            raise ValueError
        completed = retained.plan.development.runtime
        originals = {**retained.plan.staging.originals,
            **{key: completed.resources[key] for key in (*completed.plan.fixed['workloads'], *completed.plan.originals)},
            _key(retained.plan.development.gateway_original): retained.plan.development.gateway_original}
        if not originals.keys() <= choices.keys():
            raise ValueError

        def child(phase: str, filename: str) -> Document | None:
            path = state_dir / phase / filename
            return _read(path) if path.exists() or path.is_symlink() else None

        stop = child('recovery-stop', 'stop.json')
        restart = child('recovery-restart', 'restart.json')
        restarted_originals, targets = _project_inputs(retained, stop) if restart is not None and stop is not None else ({}, {})
        guards = KubectlPoolGuardAPI(request=retained.migration, kubeconfig=kubeconfig, executable=executable)
        readiness = SharedRecoveryRuntimeReadiness(bindings=retained, guards=guards)
        for target in retained.migration.guards:
            guards._namespaces(target)
        manager = retained.migration.registration.binding
        if _uid(guards._get('namespace', manager.namespace)) != str(manager.namespace_uid):
            raise ValueError

        def roots() -> dict[str, Document]:
            result = {}
            for key, original in originals.items():
                current = guards._get('deployment' if original['kind'] == 'Deployment' else 'cronjob',
                    original['metadata']['name'], original['metadata']['namespace'])
                if not any(option is not None and _matches(current, option, _uid(original)) for option in choices[key]):
                    raise ValueError
                _snapshot(current)
                result[key] = current
            return result

        def identity(root: Document) -> tuple[Any, ...]:
            return (_uid(root), _stable(root), root['metadata'].get('generation'), copy.deepcopy(root.get('status', {})))

        before = roots()
        collections: dict[str, Document] = {}
        paths = {key: (('/apis/apps/v1/namespaces/' + row['metadata']['namespace'] + '/replicasets'
            if row['kind'] == 'Deployment' else '/apis/batch/v1/namespaces/' + row['metadata']['namespace'] + '/jobs'),
            '/api/v1/namespaces/' + row['metadata']['namespace'] + '/pods') for key, row in before.items()}
        for pair in paths.values():
            for path in pair:
                if path not in collections:
                    collections[path] = guards._run(['get', '--raw', path + '?limit=1000'])
        processes = {key: _process_snapshot(row, *(collections[path] for path in paths[key])) for key, row in before.items()}
        pending = []
        backend = None
        staging = retained.plan.staging.attachment.guard
        for key, current in before.items():
            stopping = stop['resources'][key] if stop is not None else None
            starting = restart['resources'].get(key) if restart is not None else None
            if stopping is None or stopping['status'] == 'prepared':
                continue  # Fencing and cancellation must not require healthy applications.
            intent = stopping['status'] == 'intent'
            restoring = starting is not None and starting['status'] != 'prepared'
            if restoring and starting is not None:
                intent = starting['status'] == 'intent'
                desired = targets[key]
                if not _matches(current, desired, _uid(originals[key])):
                    if not intent:
                        raise ValueError
                    pending.append(key)
                    continue  # Only this validated restart intent admits the stopped side.
            elif intent and not _matches(current, stopping['expected'], _uid(originals[key])):
                pending.append(key)
                continue  # Still on the recorded pre-stop side; no healthy app required.
            deployment = current['kind'] == 'Deployment'
            running = current['spec']['replicas'] == 1 if deployment else current['spec']['suspend'] is False
            children, pods = (collections[path] for path in paths[key])
            if running:
                if not restoring or key not in retained.plan.staging.originals:
                    raise ValueError
                ready = (qualify_started_deployment(current=current, children=children, pods=pods,
                    region=retained.plan.staging.attachment.config['region']) if deployment else True)
                if ready:
                    if backend is None:
                        backend = readiness._identity(guards._database(staging))
                    ready = readiness(completed.request, 'recovery-restart', restarted_originals[key], targets[key], current)
            else:
                ready = qualify_closed_workload_drain(original=originals[key], desired=_snapshot(current), current=current,
                    children=children, pods=pods)
            if not ready:
                if not intent:
                    raise ValueError
                pending.append(key)
        if {key: identity(row) for key, row in roots().items()} != {key: identity(row) for key, row in before.items()}:
            raise ValueError
        after = {path: guards._run(['get', '--raw', path + '?limit=1000']) for path in collections}
        if any(_process_snapshot(row, *(after[path] for path in paths[key])) != processes[key] for key, row in before.items()):
            raise ValueError
        if {key: identity(row) for key, row in roots().items()} != {key: identity(row) for key, row in before.items()}:
            raise ValueError
        # All process/configuration reads precede the closing backend barrier.
        if backend is not None and readiness._identity(guards._database(staging)) != backend:
            raise ValueError
        if (bindings != retained or _recovery_frame(retained, state_dir) != frame or _receipt_files(state_dir) != files):
            raise ValueError
        checksum = digest({'handoff': retained.input_digest, 'frame': frame, 'receipts': files,
            'roots': {key: identity(row) for key, row in before.items()}, 'processes': processes,
            'pending': pending, 'database': backend})
        return SharedPoolRecoveryProcessObservation(retained, tuple(pending), checksum)
    except Exception:
        raise ValueError('shared pool recovery process observation unqualified') from None
