"""Separate bootstrap authority for shared-data personal application lifecycle.

The protected installer must verify admission enforcement before granting the
bootstrap binding. Rendering neither installs privileges nor creates the manager
ServiceAccount. Legacy environment authority remains a separate identity.
"""
from __future__ import annotations

import json
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_environment_contract import _LABEL, _PROVIDER_ID

APPLICATION_INSTALLATION_LABEL = "loom.nebius/application-installation"
_ACCOUNT = "loom-application-provisioner"
_UUID = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
_FENCE = "loom-application-retired"


def application_pod_fence(binding: ApplicationNamespaceAuthorityV1, registration: ApplicationRegistrationV1,
                          *, operation_id: UUID) -> dict[str, Any]:
    """Close new Pod admission, not existing processes, credentials or requests.

    A lifecycle provider must journal UID/resourceVersion and prove retirement
    before removing this fence with exact delete preconditions on resume/update.
    """
    binding = ApplicationNamespaceAuthorityV1.model_validate(binding.model_dump())
    row = ApplicationRegistrationV1.model_validate(registration.model_dump())
    if (row.cluster_id != binding.cluster_id or row.data_environment_id != binding.data_environment_id
            or row.application_namespace == binding.shared_namespace or operation_id.int == 0
            or row.deployment_generation > 2**63 - 1):
        raise ValueError("application Pod fence differs from protected identity")
    return {"apiVersion": "v1", "kind": "ResourceQuota", "metadata": {
        "name": _FENCE, "namespace": row.application_namespace,
        "labels": {APPLICATION_INSTALLATION_LABEL: str(binding.installation_id),
                   "loom.nebius/data-environment-id": str(row.data_environment_id),
                   "loom.nebius/application-id": str(row.application_id), "loom.nebius/incarnation": str(row.incarnation)},
        "annotations": {"loom.nebius/deployment-generation": str(row.deployment_generation),
                        "loom.nebius/operation-id": str(operation_id)},
    }, "spec": {"hard": {"pods": "0"}}}


class ApplicationNamespaceAuthorityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["loom.nebius-application-authority.v1"] = "loom.nebius-application-authority.v1"
    installation_id: UUID
    namespace: str = Field(pattern=r"^loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?$", max_length=53)
    cluster_id: str = Field(pattern=_PROVIDER_ID)
    data_environment_id: UUID
    shared_namespace: str = Field(pattern="^" + _LABEL + "$")

    @model_validator(mode="after")
    def _identities(self) -> Self:
        if self.installation_id.int == 0 or self.data_environment_id.int == 0:
            raise ValueError("application authority requires non-nil identities")
        if self.namespace == self.shared_namespace:
            raise ValueError("shared data and management namespaces must be separate")
        return self

    @property
    def name(self) -> str:
        return "loom-applications-" + self.installation_id.hex


def _subject(binding: ApplicationNamespaceAuthorityV1) -> dict[str, str]:
    return {"kind": "ServiceAccount", "name": _ACCOUNT, "namespace": binding.namespace}


def application_namespace_binding(binding: ApplicationNamespaceAuthorityV1, namespace: str) -> dict[str, Any]:
    return {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": {
        "name": binding.name, "namespace": namespace,
    }, "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                   "name": binding.name + "-resources"}, "subjects": [_subject(binding)]}


def render_application_shared_observer(binding: ApplicationNamespaceAuthorityV1) -> list[dict[str, Any]]:
    """Protected opt-in: three named policy reads, no shared writes or list.

    This is not part of the manager's bootstrap path. Only the protected shared
    installer can grant it; the manager cannot create this shared Role/Binding.
    """
    binding = ApplicationNamespaceAuthorityV1.model_validate(binding.model_dump())
    metadata = {"name": binding.name + "-observe", "namespace": binding.shared_namespace,
                "labels": {APPLICATION_INSTALLATION_LABEL: str(binding.installation_id)}}
    return [{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role", "metadata": metadata,
             "rules": [{"apiGroups": ["networking.k8s.io"], "resources": ["networkpolicies"],
                        "verbs": ["get"], "resourceNames": [binding.name + "-" + purpose
                            for purpose in ("postgres", "control-plane", "gateway")]}]},
            {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding", "metadata": metadata,
             "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": metadata["name"]},
             "subjects": [_subject(binding)]}]


def _owned_namespace(binding: ApplicationNamespaceAuthorityV1, obj: str) -> str:
    labels = obj + ".metadata.labels"
    name = obj + ".metadata.name"
    required = {
        APPLICATION_INSTALLATION_LABEL: str(binding.installation_id),
        "loom.nebius/data-environment-id": str(binding.data_environment_id),
        "pod-security.kubernetes.io/enforce": "restricted",
    }
    identity_keys = ("loom.nebius/application-id", "loom.nebius/incarnation")
    conditions = [f"{obj} != null", f"has({labels})"]
    for key in (*required, *identity_keys):
        conditions.append(f"{json.dumps(key)} in {labels}")
    conditions.extend(f"{labels}[{json.dumps(key)}] == {json.dumps(value)}" for key, value in required.items())
    for key in identity_keys:
        conditions.extend((f"{labels}[{json.dumps(key)}].matches({json.dumps(_UUID)})",
                           f"{labels}[{json.dumps(key)}] != '00000000-0000-0000-0000-000000000000'"))
    conditions.extend((
        f"!('loom.nebius/environment-id' in {labels})",
        f"!('loom.nebius/namespace-installation' in {labels})",
        f"{name}.matches('^loom-dev-[a-z0-9]([-a-z0-9]{{0,52}}[a-z0-9])?$')",
        f"!({name} in {json.dumps(['loom-dev-dev', 'loom-dev-staging', 'loom-dev-prod', 'loom-dev-shared', binding.shared_namespace])})",
    ))
    return " && ".join(conditions)


def _policy(binding: ApplicationNamespaceAuthorityV1, suffix: str, *, group: str,
            resource: str, operations: list[str], expression: str) -> list[dict[str, Any]]:
    name = binding.name + "-" + suffix
    subject = "system:serviceaccount:" + binding.namespace + ":" + _ACCOUNT
    return [{"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicy",
             "metadata": {"name": name}, "spec": {
                 "failurePolicy": "Fail",
                 "matchConstraints": {"resourceRules": [{"apiGroups": [group], "apiVersions": ["v1"],
                                                         "operations": operations, "resources": [resource]}]},
                 "matchConditions": [{"name": "application-manager-subject",
                                      "expression": "request.userInfo.username == " + json.dumps(subject)}],
                 "validations": [{"expression": expression, "message": "application " + suffix + " boundary",
                                  "reason": "Forbidden"}],
             }},
            {"apiVersion": "admissionregistration.k8s.io/v1", "kind": "ValidatingAdmissionPolicyBinding",
             "metadata": {"name": name}, "spec": {"policyName": name, "validationActions": ["Deny"]}}]


def render_application_authority(binding: ApplicationNamespaceAuthorityV1) -> list[dict[str, Any]]:
    binding = ApplicationNamespaceAuthorityV1.model_validate(binding.model_dump())
    docs = _policy(binding, "namespaces", group="", resource="namespaces", operations=["CREATE"],
                   expression=_owned_namespace(binding, "object"))
    binding_rule = (
        _owned_namespace(binding, "namespaceObject") + " && "
        f"object.metadata.name == {json.dumps(binding.name)} && "
        "object.roleRef.apiGroup == 'rbac.authorization.k8s.io' && object.roleRef.kind == 'ClusterRole' && "
        f"object.roleRef.name == {json.dumps(binding.name + '-resources')} && "
        "has(object.subjects) && size(object.subjects) == 1 && object.subjects[0].kind == 'ServiceAccount' && "
        "(!has(object.subjects[0].apiGroup) || object.subjects[0].apiGroup == '') && "
        f"object.subjects[0].namespace == {json.dumps(binding.namespace)} && object.subjects[0].name == '{_ACCOUNT}'"
    )
    docs += _policy(binding, "bindings", group="rbac.authorization.k8s.io", resource="rolebindings",
                    operations=["CREATE", "UPDATE"], expression=binding_rule)
    # TokenRequest RBAC alone cannot prevent legacy token issuance: the token
    # controller also fills kubernetes.io/service-account-token Secrets. The
    # three application credential bundles are ordinary Opaque Secrets only.
    docs += _policy(binding, "secrets", group="", resource="secrets", operations=["CREATE", "UPDATE"],
                    expression=_owned_namespace(binding, "namespaceObject") +
                    " && (!has(object.type) || object.type == 'Opaque')")
    annotations = "object.metadata.annotations"
    generation = annotations + "['loom.nebius/deployment-generation']"
    operation = annotations + "['loom.nebius/operation-id']"
    old_annotations = "oldObject.metadata.annotations"
    old_generation = old_annotations + "['loom.nebius/deployment-generation']"
    old_operation = old_annotations + "['loom.nebius/operation-id']"
    fence_rule = " && ".join([
        _owned_namespace(binding, "namespaceObject"), f"object.metadata.name == '{_FENCE}'",
        "has(object.metadata.labels)", *[
            f"{json.dumps(key)} in object.metadata.labels && "
            f"object.metadata.labels[{json.dumps(key)}] == namespaceObject.metadata.labels[{json.dumps(key)}]"
            for key in (APPLICATION_INSTALLATION_LABEL, "loom.nebius/data-environment-id",
                        "loom.nebius/application-id", "loom.nebius/incarnation")
        ],
        # Kubernetes's strict type checker omits this Quantity-valued map from
        # ResourceQuotaSpec. Inspect its dynamic wire shape, still fail-closed
        # and exact; real API tests cover both the policy and quota enforcement.
        "has(dyn(object.spec).hard) && size(dyn(object.spec).hard) == 1 && "
        "'pods' in dyn(object.spec).hard && dyn(object.spec).hard['pods'] == '0'",
        "(!has(object.spec.scopes) || size(object.spec.scopes) == 0) && !has(object.spec.scopeSelector)",
        f"has({annotations}) && 'loom.nebius/deployment-generation' in {annotations} && 'loom.nebius/operation-id' in {annotations}",
        f"{generation}.matches('^[1-9][0-9]{{0,18}}$') && int({generation}) > 0",
        f"{operation}.matches({json.dumps(_UUID)}) && {operation} != '00000000-0000-0000-0000-000000000000'",
        f"(oldObject == null || (int({generation}) >= int({old_generation}) && "
        f"(int({generation}) > int({old_generation}) || {operation} == {old_operation})))",
    ])
    docs += _policy(binding, "pod-fence", group="", resource="resourcequotas", operations=["CREATE", "UPDATE"],
                    expression=fence_rule)
    lifecycle = ["get", "create", "patch", "delete"]
    rules = [
        {"apiGroups": [""], "resources": ["secrets", "services", "serviceaccounts"], "verbs": lifecycle},
        {"apiGroups": ["apps"], "resources": ["deployments"], "verbs": lifecycle},
        {"apiGroups": ["networking.k8s.io"], "resources": ["ingresses", "networkpolicies"], "verbs": lifecycle},
        {"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list", "watch", "delete"]},
        {"apiGroups": ["apps"], "resources": ["replicasets"], "verbs": ["get", "list"]},
        {"apiGroups": [""], "resources": ["resourcequotas"], "verbs": ["create"]},
        {"apiGroups": [""], "resources": ["resourcequotas"], "verbs": ["get", "patch", "delete"],
         "resourceNames": [_FENCE]},
    ]
    docs += [{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
              "metadata": {"name": binding.name + "-resources"}, "rules": rules},
             {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
              "metadata": {"name": binding.name + "-bootstrap"}, "rules": [
                  {"apiGroups": [""], "resources": ["namespaces"], "verbs": ["get", "create"]},
                  {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["rolebindings"], "verbs": ["get", "create"]},
                  {"apiGroups": ["rbac.authorization.k8s.io"], "resources": ["clusterroles"],
                   "resourceNames": [binding.name + "-resources"], "verbs": ["bind"]},
              ]},
             {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
              "metadata": {"name": binding.name + "-bootstrap"}, "subjects": [_subject(binding)],
              "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole",
                          "name": binding.name + "-bootstrap"}}]
    for doc in docs:
        doc["metadata"]["labels"] = {APPLICATION_INSTALLATION_LABEL: str(binding.installation_id)}
    return docs
