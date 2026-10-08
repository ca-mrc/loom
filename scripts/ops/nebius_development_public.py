"""Fixed additive shared-dev HTTPS resources; no controller or workload mutation."""
from __future__ import annotations

from typing import Any

from loom.nebius_application_authority import APPLICATION_INSTALLATION_LABEL
from loom_service.environment_management.deployment import ManagementDeployment


def render_development_public(deployment: ManagementDeployment) -> list[dict[str, Any]]:
    deployment = ManagementDeployment.model_validate(deployment.model_dump())
    installation = deployment.installation
    foundation, app = installation.foundation, installation.applications
    config = foundation.platform_config
    host = config['public_host']
    if (deployment.namespace != 'loom-nebius-management-dev' or installation.provider_runtime is not None
            or foundation.namespace_authority is not None or config['namespace'] != 'loom-dev'
            or config['environment'] != 'development' or app is None or app.shared.platform_namespace != 'loom-dev'
            or host.partition('.')[2] != foundation.public_dns_zone):
        raise ValueError('shared public route requires independent development and a wildcard-covered host')
    labels = {APPLICATION_INSTALLATION_LABEL: str(deployment.installation_id), 'environment': 'development'}
    return [
        {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
            'metadata': {'name': 'loom-development-public', 'namespace': 'loom-dev', 'labels': labels.copy()},
            'spec': {'podSelector': {'matchExpressions': [
                {'key': 'app', 'operator': 'In', 'values': ['loom-service', 'loom-web']}]},
                'policyTypes': ['Ingress'], 'ingress': [{'from': [{
                    'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': foundation.ingress_namespace}},
                    'podSelector': {'matchLabels': {'app.kubernetes.io/name': foundation.ingress_controller_label}},
                }], 'ports': [{'protocol': 'TCP', 'port': 8080}, {'protocol': 'TCP', 'port': 8090}]}]}},
        {'apiVersion': 'networking.k8s.io/v1', 'kind': 'Ingress',
            'metadata': {'name': 'loom-development', 'namespace': 'loom-dev', 'labels': labels.copy()},
            'spec': {'ingressClassName': foundation.ingress_class_name, 'tls': [{'hosts': [host]}],
                'rules': [{'host': host, 'http': {'paths': [
                    {'path': path, 'pathType': 'Prefix', 'backend': {
                        'service': {'name': name, 'port': {'number': port}}}}
                    for path, name, port in (('/api', 'loom-service', 8090), ('/', 'loom-web', 8080))
                ]}}]}},
    ]
