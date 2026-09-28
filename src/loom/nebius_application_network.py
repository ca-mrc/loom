"""Shared-side ingress rendered only for the protected development installer.

The personal manager cannot apply these shared-namespace policies. Namespace
labels are protected installation identity, not a sandbox for hostile API code.
"""
from __future__ import annotations

from typing import Any

from loom.nebius_application_authority import (
    APPLICATION_INSTALLATION_LABEL,
    ApplicationNamespaceAuthorityV1,
)
from loom.nebius_application_contract import SharedDevelopmentBindingV1
from loom.nebius_environment_contract import FoundationBinding


def render_application_shared_access(
    authority: ApplicationNamespaceAuthorityV1, shared: SharedDevelopmentBindingV1,
    foundation: FoundationBinding,
) -> list[dict[str, Any]]:
    """Admit API Pods from this installation/data binding, on three exact ports."""
    authority = ApplicationNamespaceAuthorityV1.model_validate(authority.model_dump())
    shared = SharedDevelopmentBindingV1.model_validate(shared.model_dump())
    foundation = FoundationBinding.model_validate(foundation.model_dump())
    shared.validate_foundation(foundation)
    if (authority.cluster_id != shared.cluster_id or authority.data_environment_id != shared.data_environment_id
            or authority.shared_namespace != shared.platform_namespace):
        raise ValueError("shared network authority differs from protected development binding")
    return application_shared_network_policies(authority)


def application_shared_network_policies(authority: ApplicationNamespaceAuthorityV1) -> list[dict[str, Any]]:
    """Exact protected policy shape for both installation and live observation."""
    authority = ApplicationNamespaceAuthorityV1.model_validate(authority.model_dump())
    return [{"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": {
        "name": authority.name + "-" + purpose, "namespace": authority.shared_namespace,
    }, "spec": {
        "podSelector": {"matchLabels": {"app": app}}, "policyTypes": ["Ingress"],
        "ingress": [{"from": [{
            "namespaceSelector": {
                "matchLabels": {APPLICATION_INSTALLATION_LABEL: str(authority.installation_id),
                                "loom.nebius/data-environment-id": str(authority.data_environment_id),
                                "pod-security.kubernetes.io/enforce": "restricted"},
                "matchExpressions": [{"key": key, "operator": "Exists"} for key in (
                    "loom.nebius/application-id", "loom.nebius/incarnation")],
            },
            "podSelector": {"matchLabels": {"app": "loom-service"}},
        }], "ports": [{"protocol": "TCP", "port": port}]}],
    }} for purpose, app, port in (("postgres", "loom-postgres", 5432),
        ("control-plane", "loom-control-plane", 8080), ("gateway", "loom-llm-gateway", 9100))]
