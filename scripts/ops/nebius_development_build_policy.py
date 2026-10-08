"""Closed-catalog native build admission; emits no PSA change or write authority.

Install and verify this policy before relaxing the exclusive build namespace's
Pod Security label. A policy projection alone is not that installation proof.
"""
from __future__ import annotations

import json
import re
from typing import Any

from scripts.ops.nebius_development_actuator_runtime import prepare_actuator_runtime
from scripts.ops.nebius_development_runtime_setup import DevelopmentDatabaseRuntime

from loom_service.pool_management.profiles import PoolProfileCatalog


def _literal(value: Any) -> str:
    return json.dumps(value, separators=(',', ':'))


def _mount(name: str, path: str, *, readonly: bool = False) -> str:
    return (f"(m.name == {_literal(name)} && m.mountPath == {_literal(path)} && "
        + ("has(m.readOnly) && m.readOnly" if readonly else "(!has(m.readOnly) || !m.readOnly)") + ")")


def _mounts(variable: str, allowed: list[str]) -> str:
    return (f"size({variable}.volumeMounts) == {len(allowed)} && {variable}.volumeMounts.all(m, "
        "!has(m.subPath) && !has(m.subPathExpr) && !has(m.mountPropagation) && (" + ' || '.join(allowed) + '))')


def _profile(profile: Any, workload: str) -> str:
    config, target = profile.settings.job_config(), profile.target
    arch = profile.cpu_arch if workload == 'task' else profile.recipe.cpu_arch
    selector = target.node_selector or {}
    if (not selector.get('nebius.com/node-group-id')
            or selector.get('loom.nebius/node-os') != 'linux'
            or selector.get('loom.nebius/node-arch') != ('amd64' if arch == 'x86_64' else 'arm64')):
        raise ValueError('native build placement is not architecture-qualified')
    tests = [f"object.metadata.labels['app.kubernetes.io/component'] == '{workload}-image-builder'",
        f"object.metadata.annotations['loom.openai.com/target-id'] == {_literal(target.target_id)}",
        f"variables.p.serviceAccountName == {_literal(target.service_account_name)}",
        f"variables.p.nodeSelector == {_literal(selector)}",
        f"variables.prepare.image == {_literal(config.service_image)}",
        f"variables.publish.image == {_literal(config.service_image)}",
        f"variables.build.image == {_literal(config.buildkit_image)}"]
    if target.runtime_class_name is None:
        tests.extend(('!has(variables.p.runtimeClassName)', '!has(variables.p.overhead)'))
    else:
        tests.append(f'variables.p.runtimeClassName == {_literal(target.runtime_class_name)}')
        overhead = profile.runtime_class_overhead
        if overhead is None:
            raise ValueError('native build runtime overhead is not qualified')
        tests.append("has(variables.p.overhead) && variables.p.overhead.all(k, k in ['cpu','memory','ephemeral-storage'])")
        for resource, quantity in (('cpu', f'{overhead.cpu_millis}m'), ('memory', f'{overhead.memory_mib}Mi'),
                ('ephemeral-storage', f'{overhead.storage_mib}Mi')):
            tests.append(f"quantity('{resource}' in variables.p.overhead ? variables.p.overhead['{resource}'] : '0') "
                f"== quantity('{quantity}')")
    tolerations = list(target.tolerations) + [{'key': 'node.kubernetes.io/' + kind,
        'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 300} for kind in ('not-ready', 'unreachable')]
    expressions = []
    for tolerance in tolerations:
        expressions.append('(' + ' && '.join(
            f't.{key} == {_literal(tolerance[key])}' if key in tolerance else f'!has(t.{key})'
            for key in ('key', 'operator', 'value', 'effect', 'tolerationSeconds')) + ')')
    tests.append('(!has(variables.p.tolerations) || variables.p.tolerations.all(t, ' + ' || '.join(expressions) + '))')
    for resource, quantity in (('cpu', f'{config.cpu_millis}m'), ('memory', f'{config.memory_mib}Mi'),
            ('ephemeral-storage', f'{config.ephemeral_storage_mib}Mi')):
        tests.append(f"variables.all.all(c, quantity(c.resources.requests['{resource}']) == quantity('{quantity}') && "
            f"quantity(c.resources.limits['{resource}']) == quantity('{quantity}'))")
    for phase, wrapper in (('prepare', ['--install-runtime', '--']), ('publish', ['--'])):
        command = [*wrapper, 'python', '-I', '-B', '-m', f'loom_execution_actuator.{workload}_image_runtime',
            phase, '--claim', '/loom/claim/claim.json']
        tests.append(f"size(variables.{phase}.command) == {3 + len(command)} && "
            f"variables.{phase}.command[0] == '/usr/local/bin/loom-build-deadline' && "
            + ' && '.join(f'variables.{phase}.command[{index + 3}] == {_literal(value)}'
                for index, value in enumerate(command)))
        mounts = [_mount('claim', '/loom/claim', readonly=True), _mount('build', '/loom/build', readonly=phase == 'publish'),
            _mount(phase + '-tmp', '/tmp'), _mount('source' if phase == 'prepare' else 'registry',
                '/var/run/loom-task-build/' + ('source' if phase == 'prepare' else 'registry'), readonly=True)]
        if phase == 'prepare':
            mounts.append(_mount('deadline-runtime', '/loom/deadline-runtime'))
        if config.cache_secret_name is not None:
            mounts.append(_mount('cache', '/var/run/loom-task-build/cache', readonly=True))
        tests.append(_mounts('variables.' + phase, mounts))
    flags = '--root /scratch/state --oci-worker-no-process-sandbox --oci-worker-snapshotter=' + config.snapshotter
    tests.append("variables.build.env.map(e, [e.name, e.value]) == " + _literal([
        ['TMPDIR', '/scratch/tmp'], ['DOCKER_CONFIG', '/scratch/docker-config'],
        ['XDG_RUNTIME_DIR', '/scratch/runtime'], ['BUILDKITD_FLAGS', flags]]))
    volumes = ["(v.name == 'claim' && has(v.configMap) && "
        "v.configMap.name.matches('^loom-pool-[0-9a-f]{32}$') && v.configMap.defaultMode == 292 && "
        "(!has(v.configMap.optional) || !v.configMap.optional) && !has(v.configMap.items))"]
    for name, size in (('build', config.ephemeral_storage_mib), ('builder-tmp', config.ephemeral_storage_mib),
            ('prepare-tmp', config.ephemeral_storage_mib // 4), ('publish-tmp', config.ephemeral_storage_mib // 4),
            ('deadline-runtime', 8)):
        volumes.append(f"(v.name == '{name}' && has(v.emptyDir) && "
            f"has(dyn(v.emptyDir).sizeLimit) && quantity(dyn(v.emptyDir).sizeLimit) == quantity('{size}Mi') && "
            "(!has(v.emptyDir.medium) || v.emptyDir.medium == ''))")
    for role, name in (('source', config.source_secret_name), ('registry', config.registry_secret_name),
            ('cache', config.cache_secret_name)):
        if name is None:
            continue
        keys = (['credentials.json'] if config.registry_auth_kind == 'nebius' else ['config.json']) if role == 'registry' else ['access-key', 'secret-key']
        volumes.append(f"(v.name == '{role}' && has(v.secret) && v.secret.secretName == {_literal(name)} && "
            "v.secret.defaultMode == 288 && (!has(v.secret.optional) || !v.secret.optional) && "
            f"v.secret.items.map(i, [i.key, i.path]) == {_literal([[key, key] for key in keys])} && "
            'v.secret.items.all(i, !has(i.mode)))')
    tests.append(f'size(variables.p.volumes) == {len(volumes)} && variables.p.volumes.all(v, ' + ' || '.join(volumes) + ')')
    return '(' + ' && '.join('(' + test + ')' for test in tests) + ')'


def build_policy_documents(namespace: str, profiles: PoolProfileCatalog) -> tuple[dict[str, Any], ...]:
    """Constrain actual pool-wrapped task and application Pods to closed profiles."""
    profiles = PoolProfileCatalog.model_validate_json(profiles.model_dump_json())
    builds = [(profile, 'task') for profile in profiles.task_images] + [
        (profile, 'application') for profile in profiles.application_images]
    if (re.fullmatch(r'[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?', namespace) is None or not builds
            or any(profile.target.namespace != namespace for profile, _ in builds)):
        raise ValueError('native build policy namespace differs from closed profiles')
    name = namespace + '-native-build-v1'
    pod = "variables.p"
    common = (
        "c.securityContext.runAsNonRoot && c.securityContext.runAsUser == 1000 && c.securityContext.runAsGroup == 1000 && "
        "c.securityContext.readOnlyRootFilesystem && !c.securityContext.privileged && "
        "c.securityContext.capabilities.drop == ['ALL'] && "
        "(!has(c.securityContext.procMount) || c.securityContext.procMount == 'Default') && "
        "!has(c.securityContext.seLinuxOptions) && !has(c.securityContext.windowsOptions) && "
        "(!has(c.envFrom) || size(c.envFrom) == 0) && c.env.all(e, !has(e.valueFrom)) && "
        "(!has(c.args) || size(c.args) == 0) && !has(c.lifecycle) && !has(c.startupProbe) && "
        "!has(c.readinessProbe) && !has(c.livenessProbe) && !has(c.restartPolicy) && "
        "(!has(c.ports) || size(c.ports) == 0) && (!has(c.volumeDevices) || size(c.volumeDevices) == 0) && "
        "(!has(c.stdin) || !c.stdin) && (!has(c.tty) || !c.tty) && "
        "c.terminationMessagePath == '/dev/termination-log' && c.terminationMessagePolicy == 'File' && "
        "!has(c.resources.claims) && size(c.resources.requests) == 3 && size(c.resources.limits) == 3 && "
        "c.command[1] == '--deadline-at' && timestamp(c.command[2]) == timestamp(variables.prepare.command[2])"
    )
    validations = [
        (f"size({pod}.initContainers) == 2 && {pod}.initContainers[0].name == 'prepare' && "
         f"{pod}.initContainers[1].name == 'build' && size({pod}.containers) == 1 && {pod}.containers[0].name == 'publish' && "
         f"(!has({pod}.ephemeralContainers) || size({pod}.ephemeralContainers) == 0)", 'Only sequential prepare, build and publish phases are allowed.'),
        (" && ".join(f'has({pod}.{field}) && !{pod}.{field}' for field in (
            'automountServiceAccountToken', 'enableServiceLinks', 'shareProcessNamespace')) + ' && ' +
         ' && '.join(f'(!has({pod}.{field}) || !{pod}.{field})' for field in ('hostNetwork', 'hostPID', 'hostIPC')),
         'Native builds cannot inherit service tokens, host or shared process namespaces.'),
        (f"{pod}.restartPolicy == 'Never' && {pod}.dnsPolicy == 'ClusterFirst' && {pod}.schedulerName == 'default-scheduler' && "
         f"(!has({pod}.priorityClassName) || {pod}.priorityClassName == '') && "
         f"(!has({pod}.priority) || {pod}.priority == 0) && "
         f"(!has({pod}.preemptionPolicy) || {pod}.preemptionPolicy == 'PreemptLowerPriority') && "
         f"(request.operation != 'CREATE' || !has({pod}.nodeName) || {pod}.nodeName == '') && "
         + ' && '.join(f'!has({pod}.{field})' for field in ('hostAliases', 'dnsConfig', 'resourceClaims', 'hostUsers')),
         'Build scheduling or host configuration differs.'),
        (f"{pod}.securityContext.runAsNonRoot && {pod}.securityContext.runAsUser == 1000 && "
         f"{pod}.securityContext.runAsGroup == 1000 && {pod}.securityContext.fsGroup == 1000 && "
         f"{pod}.securityContext.seccompProfile.type == 'RuntimeDefault' && "
         + ' && '.join(f'!has({pod}.securityContext.{field})' for field in ('sysctls', 'seLinuxOptions', 'windowsOptions', 'supplementalGroups')),
         'Build Pod identity differs.'),
        (f"variables.all.all(c, {common})", 'Build phases require bounded non-root credential-isolated execution.'),
        ("[variables.prepare, variables.publish].all(c, !c.securityContext.allowPrivilegeEscalation && "
         "(!has(c.securityContext.capabilities.add) || size(c.securityContext.capabilities.add) == 0) && "
         "c.securityContext.seccompProfile.type == 'RuntimeDefault' && c.securityContext.appArmorProfile.type == 'RuntimeDefault' && "
         "c.env.map(e, [e.name, e.value]) == [['TMPDIR','/tmp'],['DOCKER_CONFIG','/tmp/docker-config']])",
         'Trusted phases cannot relax isolation or load untrusted environment.'),
        ("variables.build.securityContext.allowPrivilegeEscalation && "
         "variables.build.securityContext.capabilities.add == ['SETUID','SETGID'] && "
         "variables.build.securityContext.seccompProfile.type == 'Unconfined' && "
         "variables.build.securityContext.appArmorProfile.type == 'Unconfined' && size(variables.build.command) == 7 && "
         "variables.build.command[0] == '/loom/deadline-runtime/loom-build-deadline' && "
         "variables.build.command[3] == '--' && variables.build.command[4] == 'sh' && variables.build.command[5] == '-c' && "
         + _mounts('variables.build', [_mount('build', '/loom/build'), _mount('builder-tmp', '/scratch'),
             _mount('builder-tmp', '/tmp'), _mount('deadline-runtime', '/loom/deadline-runtime', readonly=True)]),
         'The rootless builder may use only its private scratch and read-only deadline runtime.'),
        (' || '.join(_profile(profile, workload) for profile, workload in builds),
         'Build image, command, placement, resources or credential references differ from the closed catalog.'),
    ]
    policy = {'apiVersion': 'admissionregistration.k8s.io/v1', 'kind': 'ValidatingAdmissionPolicy',
        'metadata': {'name': name}, 'spec': {'failurePolicy': 'Fail', 'matchConstraints': {'resourceRules': [{
            'apiGroups': [''], 'apiVersions': ['v1'], 'operations': ['CREATE', 'UPDATE'],
            'resources': ['pods', 'pods/ephemeralcontainers', 'pods/resize'], 'scope': 'Namespaced'}]},
        'variables': [{'name': 'p', 'expression': 'object.spec'},
            {'name': 'prepare', 'expression': 'variables.p.initContainers[0]'},
            {'name': 'build', 'expression': 'variables.p.initContainers[1]'},
            {'name': 'publish', 'expression': 'variables.p.containers[0]'},
            {'name': 'all', 'expression': 'variables.p.initContainers + variables.p.containers'}],
        'validations': [{'expression': expression, 'message': message, 'reason': 'Forbidden'}
            for expression, message in validations]}}
    binding = {'apiVersion': 'admissionregistration.k8s.io/v1', 'kind': 'ValidatingAdmissionPolicyBinding',
        'metadata': {'name': name}, 'spec': {'policyName': name, 'validationActions': ['Deny'],
            'matchResources': {'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': namespace}}}}}
    return policy, binding


def prepare_build_policy(request: DevelopmentDatabaseRuntime) -> tuple[dict[str, Any], ...]:
    """Requalify the frozen predecessor before deriving any build exception."""
    try:
        prepare_actuator_runtime(request)
        spec = request.manager.retained.request.registration.spec
        participant, = spec.participants
        documents = build_policy_documents(participant.build_namespace.name, spec.profiles)
        for document in documents:
            document['metadata']['labels'] = {
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/development-runtime-operation': str(request.operation_id)}
        return documents
    except Exception:
        raise ValueError('development build policy unqualified') from None
