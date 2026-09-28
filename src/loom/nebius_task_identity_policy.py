"""Target-scoped Pod admission for private root sandboxes.

Baseline PSS still applies. This policy restores stricter runtime controls and
permits only the two native private sandboxes to use root and installation caps.
Installing these documents does not establish runtime qualification.
"""

from __future__ import annotations

import json
from typing import Any

from loom.sandbox_identity import ROOT_INSTALL_CAPABILITIES

PSS_VERSION = "v1.33"


def validate_identity_policy(config: dict[str, Any]) -> bool:
    policy = config.get("task_identity_policy")
    if policy is None:
        return False
    if policy != {
        "mode": "private-root-v1", "target_id": config["target_id"],
        "execution_namespace": config["execution_namespace"],
    }:
        raise ValueError("task identity policy must bind the exact execution target and namespace")
    if (config.get("regional_execution_targets")
            or config.get("schema_version") == "loom.nebius-managed-environment.v1"):
        raise ValueError("task identity policy is qualified only for a single independent target")
    return True


def identity_namespace_labels() -> dict[str, str]:
    return {
        "pod-security.kubernetes.io/enforce": "baseline",
        "pod-security.kubernetes.io/enforce-version": PSS_VERSION,
        "pod-security.kubernetes.io/warn": "restricted",
        "pod-security.kubernetes.io/warn-version": PSS_VERSION,
        "pod-security.kubernetes.io/audit": "restricted",
        "pod-security.kubernetes.io/audit-version": PSS_VERSION,
    }


def _guest_private_expression() -> str:
    return (
        "has(c.restartPolicy) && c.restartPolicy == 'Always' && "
        "size(c.command) in [19,20] && "
        "c.command[0] == '/loom/runtime/guest/bin/loom-guest-runtime' && "
        "c.command[1] == '--payload' && c.command[2] == '/loom/runtime/guest' && "
        "c.command[3] == '--root' && c.command[4] == '/' && "
        "c.command[5] == '--state' && c.command[6] == '/loom/guest-state/incarnation' && "
        "c.command[7] == '--socket' && c.command[8] == '/loom/sandboxes/' + c.name + '/sandbox.sock' && "
        "c.command[9] == '--memory-mib' && c.command[11] == '--storage-mib' && "
        "c.command[13] == '--cpu-millis' && c.command[15] == '--max-transfer-bytes' && "
        "c.command[17] == '--exec-timeout-seconds' && "
        "[10,12,14,16,18].all(i, c.command[i].matches('^[1-9][0-9]*$')) && "
        "int(c.command[10]) >= 512 && int(c.command[10]) <= 1048576 && "
        "int(c.command[12]) >= 128 && int(c.command[12]) <= 1048576 && "
        "int(c.command[14]) >= 1000 && int(c.command[14]) <= 128000 && "
        "int(c.command[16]) <= 10737418240 && int(c.command[18]) <= 86400 && "
        "(size(c.command) == 19 || c.command[19] == '--nested-docker') && "
        "(!has(c.args) || size(c.args) == 0) && !has(c.lifecycle) && "
        "(!has(c.envFrom) || size(c.envFrom) == 0) && (!has(c.env) || c.env.all(e, !has(e.valueFrom))) && "
        "has(c.startupProbe) && has(c.startupProbe.exec) && has(c.readinessProbe) && has(c.readinessProbe.exec) && "
        "c.startupProbe.exec.command == ['/loom/bin/loom-sandbox-runtime','--check-socket','/loom/sandboxes/' + c.name + '/sandbox.sock'] && "
        "c.readinessProbe.exec.command == c.startupProbe.exec.command && "
        "has(c.securityContext.readOnlyRootFilesystem) && c.securityContext.readOnlyRootFilesystem && "
        "has(c.securityContext.runAsNonRoot) && !c.securityContext.runAsNonRoot && "
        "has(c.securityContext.runAsUser) && c.securityContext.runAsUser == 0 && "
        "has(c.securityContext.runAsGroup) && c.securityContext.runAsGroup == 0 && "
        "has(c.securityContext.capabilities.add) && c.securityContext.capabilities.add == ['DAC_OVERRIDE'] && "
        "size(c.volumeMounts) == 6 && c.volumeMounts.all(m, "
        "!has(m.subPathExpr) && !has(m.mountPropagation) && "
        "((m.name == c.name + '-socket' && m.mountPath == '/loom/sandboxes/' + c.name && !has(m.subPath)) || "
        "(m.name == c.name + '-guest-state' && m.mountPath == '/loom/guest-state' && !has(m.subPath)) || "
        "(m.name == 'runtime' && m.mountPath == '/loom/runtime' && !has(m.subPath) && has(m.readOnly) && m.readOnly) || "
        "(m.name == 'runtime' && m.mountPath == '/loom/bin/loom-sandbox-runtime' && "
        "has(m.subPath) && m.subPath == 'loom-sandbox-runtime' && has(m.readOnly) && m.readOnly) || "
        "(m.name == c.name + '-socket' && has(m.subPath) && "
        "((m.mountPath == '/etc/hosts' && m.subPath == 'network/hosts') || "
        "(m.mountPath == '/etc/resolv.conf' && m.subPath == 'network/resolv.conf')))))"
    )


def identity_policy_documents(namespace: str, target_id: str, *, guest_target_id: str | None = None) -> list[dict[str, Any]]:
    name = namespace + "-private-root-v1"
    # Pod defaults below explicitly constrain the inherited identity. A
    # container may inherit runAsNonRoot, but cannot override it to false.
    common = (
        "has(c.securityContext) && "
        "has(c.securityContext.allowPrivilegeEscalation) && !c.securityContext.allowPrivilegeEscalation && "
        "(!has(c.securityContext.privileged) || !c.securityContext.privileged) && "
        "has(c.securityContext.capabilities) && has(c.securityContext.capabilities.drop) && "
        "'ALL' in c.securityContext.capabilities.drop && "
        "(!has(c.securityContext.procMount) || c.securityContext.procMount == 'Default') && "
        "(!has(c.securityContext.seccompProfile) || c.securityContext.seccompProfile.type == 'RuntimeDefault') && "
        "(!has(c.securityContext.appArmorProfile) || c.securityContext.appArmorProfile.type == 'RuntimeDefault') && "
        "!has(c.securityContext.seLinuxOptions) && "
        "(!has(c.volumeDevices) || size(c.volumeDevices) == 0) && "
        "(!has(c.resources.claims) || size(c.resources.claims) == 0) && "
        "(!has(c.resources.requests) || c.resources.requests.all(k, k in ['cpu','memory','ephemeral-storage'])) && "
        "(!has(c.resources.limits) || c.resources.limits.all(k, k in ['cpu','memory','ephemeral-storage']))"
    )
    nonroot = (
        "(!has(c.securityContext.runAsNonRoot) || c.securityContext.runAsNonRoot) && "
        "(!has(c.securityContext.runAsUser) || c.securityContext.runAsUser > 0) && "
        "(!has(c.securityContext.capabilities.add) || size(c.securityContext.capabilities.add) == 0)"
    )
    private = (
        "has(c.restartPolicy) && c.restartPolicy == 'Always' && "
        "size(c.command) == 5 && c.command[0] == '/loom/bin/loom-sandbox-runtime' && "
        "c.command[1] == '--socket' && c.command[2] == '/loom/sandboxes/' + c.name + '/sandbox.sock' && "
        "c.command[3] == '--exec-timeout-seconds' && c.command[4].matches('^[0-9]+$') && "
        "(!has(c.args) || size(c.args) == 0) && !has(c.lifecycle) && "
        "(!has(c.envFrom) || size(c.envFrom) == 0) && "
        "(!has(c.env) || c.env.all(e, !has(e.valueFrom))) && "
        "size(c.volumeMounts) == 4 && c.volumeMounts.all(m, "
        "!has(m.subPathExpr) && !has(m.mountPropagation) && "
        "((m.name == c.name + '-socket' && m.mountPath == '/loom/sandboxes/' + c.name && !has(m.subPath)) || "
        "(m.name == 'runtime' && m.mountPath == '/loom/bin/loom-sandbox-runtime' && "
        "has(m.subPath) && m.subPath == 'loom-sandbox-runtime' && has(m.readOnly) && m.readOnly) || "
        "(m.name == c.name + '-socket' && has(m.subPath) && "
        "((m.mountPath == '/etc/hosts' && m.subPath == 'network/hosts') || "
        "(m.mountPath == '/etc/resolv.conf' && m.subPath == 'network/resolv.conf'))))) && "
        "has(c.securityContext.runAsNonRoot) && "
        "((has(c.securityContext.runAsUser) && c.securityContext.runAsUser == 0) ? "
        "(!c.securityContext.runAsNonRoot && has(c.securityContext.runAsGroup) && "
        "c.securityContext.runAsGroup >= 0 && c.securityContext.runAsGroup <= 2147483647 && "
        "has(c.securityContext.capabilities.add) && "
        f"size(c.securityContext.capabilities.add) == {len(ROOT_INSTALL_CAPABILITIES)} && "
        f"{json.dumps(list(ROOT_INSTALL_CAPABILITIES))}.all(k, k in c.securityContext.capabilities.add)) : "
        "(c.securityContext.runAsNonRoot && (!has(c.securityContext.runAsUser) || c.securityContext.runAsUser > 0) && "
        "(!has(c.securityContext.capabilities.add) || size(c.securityContext.capabilities.add) == 0)))"
    )
    fixture = (
        "has(c.restartPolicy) && c.restartPolicy == 'Always' && "
        "has(c.securityContext.runAsUser) && c.securityContext.runAsUser == 65532 && "
        "has(c.securityContext.runAsGroup) && c.securityContext.runAsGroup == 65532 && "
        "has(c.securityContext.runAsNonRoot) && c.securityContext.runAsNonRoot && "
        "has(c.securityContext.readOnlyRootFilesystem) && c.securityContext.readOnlyRootFilesystem && "
        "(!has(c.volumeMounts) || size(c.volumeMounts) == 0) && "
        "(!has(c.envFrom) || size(c.envFrom) == 0) && "
        "(!has(c.env) || size(c.env) == 0) && !has(c.lifecycle) && "
        "has(c.startupProbe) && has(c.readinessProbe) && "
        "has(c.resources.requests) && has(c.resources.limits) && "
        "['cpu','memory','ephemeral-storage'].all(k, k in c.resources.requests && k in c.resources.limits)"
    )
    if guest_target_id is not None:
        private = f"(variables.isGuest ? ({_guest_private_expression()}) : ({private}))"
    target_condition = (" == " + json.dumps(target_id) if guest_target_id is None
                        else " in " + json.dumps([target_id, guest_target_id]))
    validations = [
        ("!has(object.spec.hostNetwork) || !object.spec.hostNetwork", "Host networking is forbidden."),
        ("!has(object.spec.hostPID) || !object.spec.hostPID", "Host PID is forbidden."),
        ("!has(object.spec.hostIPC) || !object.spec.hostIPC", "Host IPC is forbidden."),
        ("!has(object.spec.shareProcessNamespace) || !object.spec.shareProcessNamespace", "Shared PID is forbidden."),
        ("has(object.spec.securityContext) && object.spec.securityContext.runAsNonRoot && "
         "object.spec.securityContext.runAsUser > 0 && object.spec.securityContext.runAsGroup > 0 && "
         "object.spec.securityContext.seccompProfile.type == 'RuntimeDefault' && "
         "(!has(object.spec.securityContext.sysctls) || size(object.spec.securityContext.sysctls) == 0) && "
         "!has(object.spec.securityContext.seLinuxOptions) && "
         "(!has(object.spec.securityContext.appArmorProfile) || "
         "object.spec.securityContext.appArmorProfile.type == 'RuntimeDefault')", "Trusted Pod defaults must remain restricted."),
        ("!has(object.metadata.annotations) || object.metadata.annotations.all(k, "
         "!k.startsWith('container.apparmor.security.beta.kubernetes.io/') || "
         "object.metadata.annotations[k] == 'runtime/default')", "Unconfined AppArmor is forbidden."),
        ("!has(object.spec.volumes) || object.spec.volumes.all(v, "
         "has(v.configMap) || has(v.downwardAPI) || has(v.emptyDir) || has(v.ephemeral) || "
         "has(v.persistentVolumeClaim) || has(v.projected) || has(v.secret))", "Only restricted volume types are allowed."),
        (f"variables.allContainers.all(c, {common})", "Container security and resource boundaries must remain restricted."),
        (f"variables.ordinary.all(c, {nonroot})", "Only private native sandboxes may run as root or add capabilities."),
        (f"variables.private.all(c, {private})", "Private sandbox command, identity, capabilities or mounts are invalid."),
        (f"variables.fixtures.all(c, {fixture})", "Fixture identity, mounts, lifecycle or resources are invalid."),
        ("variables.regular.all(c, !c.name.startsWith('fixture-'))", "Fixtures must be native init sidecars."),
        ("size(variables.fixtures) == 0 || (size(variables.fixtures) == 1 && "
         "(size(variables.private) == 1 || size(variables.private) == 2) && "
         "(!has(object.spec.hostAliases) || size(object.spec.hostAliases) == 0))",
         "One fixture requires a private sandbox and private hostname resolution."),
        ("variables.regular.all(c, !(c.name in ['task-sandbox','verifier-sandbox']))", "Private sandboxes must be native init sidecars."),
        ("size(variables.private) == 0 || (object.spec.serviceAccountName == 'loom-execution-attempt' && "
         "has(object.spec.automountServiceAccountToken) && !object.spec.automountServiceAccountToken && "
         "has(object.metadata.annotations) && object.metadata.annotations['loom.openai.com/target-id']"
         + target_condition + " && object.spec.volumes.filter(v, "
         "v.name in ['runtime','task-sandbox-socket','verifier-sandbox-socket']).all(v, has(v.emptyDir)) && "
         "!has(object.spec.resourceClaims))", "Private sandboxes require the target-bound execution Pod shape."),
    ]
    policy: dict[str, Any] = {
        "apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
        "metadata": {"name": name},
        "spec": {
            "failurePolicy": "Fail",
            "matchConstraints": {"resourceRules": [{
                "apiGroups": [""], "apiVersions": ["v1"], "operations": ["CREATE", "UPDATE"],
                "resources": ["pods", "pods/ephemeralcontainers"], "scope": "Namespaced",
            }]},
            "variables": [
                {"name": "init", "expression": "has(object.spec.initContainers) ? object.spec.initContainers : []"},
                {"name": "ephemeral", "expression": "has(object.spec.ephemeralContainers) ? object.spec.ephemeralContainers : []"},
                {"name": "regular", "expression": "object.spec.containers + variables.ephemeral"},
                {"name": "allContainers", "expression": "variables.regular + variables.init"},
                {"name": "private", "expression": "variables.init.filter(c, c.name in ['task-sandbox','verifier-sandbox'])"},
                {"name": "fixtures", "expression": "variables.init.filter(c, c.name.startsWith('fixture-'))"},
                {"name": "ordinary", "expression": "variables.allContainers.filter(c, !(c.name in ['task-sandbox','verifier-sandbox']))"},
            ],
            "validations": [{"expression": expression, "message": message} for expression, message in validations],
        },
    }
    if guest_target_id is not None:
        policy["spec"]["variables"].append({
            "name": "isGuest", "expression": "has(object.metadata.annotations) && "
            "'loom.openai.com/target-id' in object.metadata.annotations && "
            "object.metadata.annotations['loom.openai.com/target-id'] == " + json.dumps(guest_target_id),
        })
        policy["spec"]["validations"].append({
            # The built-in OpenAPI quantity reference is absent from CEL's
            # inferred EmptyDir type. Dynamic selection preserves the runtime
            # presence check without a static undefined-field warning.
            "expression": "!variables.isGuest || (size(variables.private) == 2 && size(variables.fixtures) == 0 && "
            "object.spec.volumes.filter(v, v.name in ['task-sandbox-guest-state','verifier-sandbox-guest-state']).all(v, "
            "has(v.emptyDir) && has(dyn(v.emptyDir).sizeLimit) && (!has(v.emptyDir.medium) || v.emptyDir.medium == '')))",
            "message": "Guest targets require two private guests with bounded disk state.",
        })
    binding = {
        "apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
        "metadata": {"name": name},
        "spec": {"policyName": name, "validationActions": ["Deny"], "matchResources": {
            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": namespace}},
        }},
    }
    return [policy, binding]
