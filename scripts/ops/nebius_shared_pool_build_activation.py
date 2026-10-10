"""One recorded build-namespace exception under the protected shared handoff.

The existing policy must deny a restricted-compliant probe before and after
the namespace CAS. Probes are server dry runs only; no Pod, policy or grant is
created here. Whole-pool qualification and closed intake belong to the parent.
"""
from __future__ import annotations

import copy
import json
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_development_pool_registration import development_profile_catalog
from scripts.ops.nebius_development_runtime_install import _read
from scripts.ops.nebius_development_runtime_retained import qualify_build_policy_spec
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_shared_pool_activation import shared_forward_activation_view
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings
from scripts.ops.nebius_shared_pool_operation import FORWARD_PHASES, _files
from scripts.ops.nebius_shared_pool_workloads import _frame

from loom.nebius_platform_render import digest
from loom_execution_actuator.task_image_renderer import TaskImageJobConfig

_PHASE = 'forward-build-policy'
_DENIAL = 'Only sequential prepare, build and publish phases are allowed.'


def build_policy_rejection_probe(*, namespace: str, service_account: str,
        config: TaskImageJobConfig) -> dict[str, Any]:
    """PSA-restricted compliant, but invalid for the fixed native-build policy.

    Extra publish container violates the first policy rule. Using the installed
    account/image and exact profile resources avoids unrelated admission causes.
    A policy-specific denial, never just a 403, is required by the consumer.
    """
    resources = {'cpu': f'{config.cpu_millis}m', 'memory': f'{config.memory_mib}Mi',
        'ephemeral-storage': f'{config.ephemeral_storage_mib}Mi'}
    container = {'image': config.service_image, 'command': ['true'],
        'resources': {'requests': resources, 'limits': resources},
        'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000, 'runAsGroup': 1000,
            'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
            'capabilities': {'drop': ['ALL']}, 'seccompProfile': {'type': 'RuntimeDefault'}}}
    return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {
        'name': 'loom-build-policy-probe-' + uuid4().hex, 'namespace': namespace},
        'spec': {'serviceAccountName': service_account, 'automountServiceAccountToken': False,
            'restartPolicy': 'Never', 'securityContext': {'runAsNonRoot': True, 'runAsUser': 1000,
                'runAsGroup': 1000, 'seccompProfile': {'type': 'RuntimeDefault'}},
            'initContainers': [{'name': name, **copy.deepcopy(container)} for name in ('prepare', 'build')],
            'containers': [{'name': name, **copy.deepcopy(container)} for name in ('publish', 'unapproved')]}}


def _active(bindings: SharedPoolGuardBindings, state: Path, status: str) -> dict[str, Any]:
    frame = _frame(bindings, state.parent)
    manager = bindings.plan.development.runtime.request.database.manager.retained.request.retained
    recovery = Path(manager.operation['anchor_dir']) / 'shared-pool-recovery.json'
    index = FORWARD_PHASES.index(_PHASE)
    if (state.name != _PHASE or status not in {'prepared', 'started'} or frame['outcome'] != 'pending'
            or recovery.exists() or recovery.is_symlink() or frame['forward'][_PHASE]['status'] != status
            or any(frame['forward'][prior]['status'] != 'complete' for prior in FORWARD_PHASES[:index])
            or any(frame['forward'][later]['status'] != 'prepared' for later in FORWARD_PHASES[index + 1:])):
        raise ValueError
    return frame


def _identity(bindings: SharedPoolGuardBindings) -> dict[str, str]:
    plan = bindings.plan.development
    return {'schema': 'loom.nebius-shared-build-activation.v1', 'input_digest': bindings.input_digest,
        'original_sha256': digest(plan.build_namespace_original), 'target_sha256': digest(plan.build_namespace_target),
        'policy_sha256': digest(plan.build_policy)}


def validate_shared_build_activation(*, bindings: SharedPoolGuardBindings, state_dir: Path,
        complete: bool, prepared: bool = False) -> dict[str, Any]:
    frame = _frame(bindings, state_dir.parent)
    identity = _identity(bindings)
    record = _read(state_dir / 'build-policy.json')
    if (state_dir.name != _PHASE or type(complete) is not bool or type(prepared) is not bool
            or (complete and prepared) or set(record) != {*identity, 'status'}
            or any(record[key] != value for key, value in identity.items())
            or record['status'] not in {'prepared', 'intent', 'activated'}
            or (complete and record['status'] != 'activated')
            or (prepared and (record['status'] != 'prepared' or frame['forward'][_PHASE]['status'] != 'prepared'))):
        raise ValueError
    return record


def prepare_shared_build_activation(*, bindings: SharedPoolGuardBindings, state_dir: Path) -> None:
    frame = _active(bindings, state_dir, 'prepared')
    with private_state._locked_state(state_dir):
        path = state_dir / 'build-policy.json'
        if not path.exists() and not path.is_symlink():
            private_state._atomic_json(path, {**_identity(bindings), 'status': 'prepared'})
        validate_shared_build_activation(bindings=bindings, state_dir=state_dir, complete=False, prepared=True)
        if _active(bindings, state_dir, 'prepared') != frame:
            raise ValueError


def shared_forward_build_namespace_view(*, bindings: SharedPoolGuardBindings, state_dir: Path
        ) -> tuple[dict[str, Any], dict[str, str], dict[str, tuple[dict[str, Any], ...]]]:
    """Receipt-bound namespace choices, not an arbitrary inventory override.

    Includes parent history and receipt bytes so a caller can bracket a remote
    observation. This projects only the namespace; the combined runtime view
    separately qualifies gateway successors. Recovery remains excluded.
    """
    opening = shared_forward_activation_view(bindings=bindings, state_dir=state_dir)
    frame = opening.frame
    plan = bindings.plan.development
    status, files = 'prepared', {}
    child = state_dir / _PHASE
    phase = frame['forward'][_PHASE]['status']
    if phase != 'prepared' or child.exists() or child.is_symlink():
        # An empty prepared directory is a valid interrupted local preparation.
        files = _files(child, allow_empty=phase == 'prepared')
        if files or phase != 'prepared':
            status = validate_shared_build_activation(bindings=bindings, state_dir=child,
                complete=phase == 'complete', prepared=phase == 'prepared')['status']
        if _files(child, allow_empty=phase == 'prepared') != files:
            raise ValueError
    before, after = copy.deepcopy(plan.build_namespace_original), copy.deepcopy(plan.build_namespace_target)
    after['metadata']['uid'] = _uid(before)
    choices = (before,) if status == 'prepared' else (before, after) if status == 'intent' else (after,)
    if shared_forward_activation_view(bindings=bindings, state_dir=state_dir) != opening:
        raise ValueError
    return frame, files, {_key(before): choices}


class HTTPSSharedBuildActivation(ManagementKubernetesTransport):
    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str, ssl_context: ssl.SSLContext,
            qualify_runtime: Callable[[SharedPoolGuardBindings, bool], None], token: str | None = None):
        self.bindings = copy.deepcopy(bindings)
        manager = self.bindings.plan.development.runtime.request.database.manager.retained.request.retained
        self.state = Path(manager.operation['state_dir']).parent / 'shared-pool-handoff' / _PHASE
        runtime = self.bindings.plan.development.runtime.request
        if (not callable(qualify_runtime)
                or api_server.rstrip('/') != runtime.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('shared build activation connection differs')
        _active(self.bindings, self.state, 'started')
        self.identity, self.qualify_runtime = _identity(self.bindings), qualify_runtime
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def _scope(self, *, writing: bool = False) -> None:
        frame = _active(self.bindings, self.state, 'started')
        child = validate_shared_build_activation(bindings=self.bindings, state_dir=self.state, complete=False)
        self.qualify_runtime(self.bindings, writing)
        current = self._request('GET', '/api/v1/namespaces/kube-system')
        if (current is None or current.get('apiVersion') != 'v1' or current.get('kind') != 'Namespace'
                or current['metadata'].get('name') != 'kube-system'
                or _uid(current) != self.bindings.migration.registration.binding.kube_system_uid
                or _identity(self.bindings) != self.identity
                or _active(self.bindings, self.state, 'started') != frame
                or validate_shared_build_activation(bindings=self.bindings, state_dir=self.state, complete=False) != child):
            raise ValueError

    def read_namespace(self) -> dict[str, Any]:
        self._scope()
        original = self.bindings.plan.development.build_namespace_original
        current = self._request('GET', '/api/v1/namespaces/' + original['metadata']['name'])
        if current is None or _uid(current) != _uid(original):
            raise ValueError
        _snapshot(current)
        return current

    def policy_ready(self) -> bool:
        self._scope()
        plan = self.bindings.plan.development

        def policies() -> dict[str, dict[str, Any]]:
            result = {}
            for key, expected in plan.build_policy.items():
                plural = ('validatingadmissionpolicies' if expected['kind'] == 'ValidatingAdmissionPolicy'
                    else 'validatingadmissionpolicybindings')
                current = self._request('GET', '/apis/admissionregistration.k8s.io/v1/' + plural + '/' + expected['metadata']['name'])
                if current is None or not _matches(current, expected, _uid(expected)):
                    raise ValueError
                qualify_build_policy_spec(expected, current)
                result[key] = current
            return result

        before = policies()
        policy, = (row for row in before.values() if row['kind'] == 'ValidatingAdmissionPolicy')
        status, generation = policy.get('status', {}), policy['metadata'].get('generation')
        checking = status.get('typeChecking')
        if isinstance(checking, dict) and checking.get('expressionWarnings'):
            raise ValueError
        if (not isinstance(checking, dict) or type(generation) is not int or generation < 1
                or type(status.get('observedGeneration')) is not int or status['observedGeneration'] < generation):
            return False
        catalog = development_profile_catalog(plan.runtime.request.database.manager.retained.request)
        profile = catalog.task_images[0] if catalog.task_images else catalog.application_images[0]
        namespace = plan.build_namespace_original['metadata']['name']
        probe = build_policy_rejection_probe(namespace=namespace, service_account=profile.target.service_account_name,
            config=profile.settings.job_config())
        with self.client.stream('POST', '/api/v1/namespaces/' + namespace + '/pods?dryRun=All', json=probe) as response:
            if (response.status_code not in {200, 201, 403}
                    or response.headers.get('content-encoding', 'identity').lower() != 'identity'):
                raise ValueError
            raw = bytearray()
            for chunk in response.iter_bytes(chunk_size=16384):
                if len(raw) + len(chunk) > 65536:
                    raise ValueError
                raw.extend(chunk)
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise ValueError
            denied = response.status_code == 403
            if denied and (result.get('apiVersion') != 'v1' or result.get('kind') != 'Status'
                    or result.get('status') != 'Failure' or result.get('reason') != 'Forbidden' or result.get('code') != 403
                    or policy['metadata']['name'] not in result.get('message', '') or _DENIAL not in result.get('message', '')):
                raise ValueError
        if policies() != before:
            raise ValueError
        self._scope()
        return denied

    def activate(self, before: dict[str, Any]) -> bool:
        plan = self.bindings.plan.development
        original, desired = plan.build_namespace_original, plan.build_namespace_target
        version = before['metadata']['resourceVersion']
        if not _matches(before, original, _uid(original)) or not isinstance(version, str) or not 0 < len(version) <= 128:
            raise ValueError
        self._scope(writing=True)
        if _read(self.state / 'build-policy.json')['status'] != 'intent':
            raise ValueError
        if not self.policy_ready():
            return False  # Known pre-dispatch wait; no PATCH has been attempted.
        patches = [{'op': 'test', 'path': '/metadata/uid', 'value': _uid(original)},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': version},
            {'op': 'test', 'path': '/metadata/labels', 'value': original['metadata']['labels']},
            {'op': 'replace', 'path': '/metadata/labels', 'value': desired['metadata']['labels']},
            {'op': 'add', 'path': '/metadata/annotations', 'value': desired['metadata']['annotations']}]
        with self.client.stream('PATCH', '/api/v1/namespaces/' + original['metadata']['name'], json=patches,
                headers={'Content-Type': 'application/json-patch+json'}) as response:
            return _patch_result(response, desired=desired, uid=_uid(original))


def advance_shared_build_activation(*, bindings: SharedPoolGuardBindings, api: HTTPSSharedBuildActivation,
        state_dir: Path) -> bool:
    try:
        if api.bindings != bindings or api.state != state_dir:
            raise ValueError
        _active(bindings, state_dir, 'started')
        with private_state._locked_state(state_dir):
            path = state_dir / 'build-policy.json'
            record = validate_shared_build_activation(bindings=bindings, state_dir=state_dir, complete=False)
            plan = bindings.plan.development

            def save(status: str) -> None:
                if _read(path) != record:
                    raise ValueError
                record['status'] = status
                private_state._atomic_json(path, record)

            current = api.read_namespace()
            if not api.policy_ready():
                return False
            if record['status'] == 'prepared':
                if not _matches(current, plan.build_namespace_original, _uid(plan.build_namespace_original)):
                    raise ValueError
                save('intent')
                try:
                    accepted = api.activate(current)
                except Exception:
                    accepted = True  # Unknown outcome permits only readback.
                if accepted is False:
                    save('prepared')
                    return False
                if accepted is not True:
                    raise ValueError
                current = api.read_namespace()
            if not _matches(current, plan.build_namespace_target, _uid(plan.build_namespace_original)):
                raise ValueError
            if not api.policy_ready():
                return False
            if not _matches(api.read_namespace(), plan.build_namespace_target, _uid(plan.build_namespace_original)):
                raise ValueError
            if record['status'] != 'activated':
                save('activated')
            if validate_shared_build_activation(bindings=bindings, state_dir=state_dir, complete=True) != record:
                raise ValueError
            return True
    except Exception:
        raise ValueError('shared build activation unconfirmed; preserve retained evidence') from None
