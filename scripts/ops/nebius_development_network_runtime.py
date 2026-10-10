"""Dev-only runtime networking, without activation or build/runtime authority.

The protected parent must journal these fixed documents before starting workers.
Keep existing foundation and personal ingress; never rewrite staging policies.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from scripts.ops.nebius_development_actuator_runtime import prepare_actuator_runtime
from scripts.ops.nebius_development_runtime_setup import DevelopmentDatabaseRuntime

from loom.nebius_platform_render import _network_policy

_ROOT = Path(__file__).resolve().parents[2]


def prepare_network_runtime(request: DevelopmentDatabaseRuntime) -> tuple[dict[str, Any], ...]:
    """Allow actuator DB and task broker traffic for the exact retained dev pool.

    Native tasks reach only DNS and the credential/LLM gateway; they do not need
    direct API, controller or database access. Observer/actuator egress remains
    outside the task policy so their authenticated manager/cloud/Kubernetes reads
    keep working. Labels are protected installation identity, not user input.
    """
    try:
        prepare_actuator_runtime(request)
        spec = request.manager.retained.request.registration.spec
        participant, = spec.participants
        namespace = participant.execution_namespace.name
        documents = [document for document in yaml.safe_load_all(
            (_ROOT / 'deploy/k8s/nebius-execution-actuator.yaml').read_text())
            if document and document['kind'] == 'NetworkPolicy']
        if {document['metadata']['name'] for document in documents} != {
                'loom-execution-attempt-default-deny', 'loom-execution-attempt-egress'}:
            raise ValueError
        for document in documents:
            document['metadata']['namespace'] = namespace
            for rule in document['spec'].get('egress', []):
                for peer in rule.get('to', []):
                    labels = peer.get('namespaceSelector', {}).get('matchLabels', {})
                    if labels.get('kubernetes.io/metadata.name') == 'loom':
                        labels['kubernetes.io/metadata.name'] = 'loom-dev'
        for purpose, app, port, selector in (
            ('postgres', 'loom-postgres', 5432, {'app.kubernetes.io/name': 'loom-execution-actuator'}),
            ('gateway', 'loom-llm-gateway', 9100, {'app.kubernetes.io/component': 'execution-unit'}),
        ):
            peer = {'namespaceSelector': {'matchLabels': {
                'kubernetes.io/metadata.name': namespace,
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/pool': str(spec.pool_id)}}, 'podSelector': {'matchLabels': selector}}
            documents.append(_network_policy('development-runtime-' + purpose, 'loom-dev',
                {'matchLabels': {'app': app}}, [{'from': [peer], 'ports': [{'protocol': 'TCP', 'port': port}]}]))
        for document in documents:
            document['metadata'].setdefault('labels', {}).update({
                'loom.nebius/management-installation': str(spec.installation_id),
                'loom.nebius/development-runtime-operation': str(request.operation_id)})
        return tuple(documents)
    except Exception:
        raise ValueError('development runtime network unqualified') from None
