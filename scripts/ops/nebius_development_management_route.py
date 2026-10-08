"""Read-only shared ingress qualification for an independent dev-manager route.

No staging application configuration, TLS key, retained management state, public
selector mutation or guard operation is consumed. Fresh preflight does not require
a certificate that the installer has not delivered yet; final proof does.
"""
from __future__ import annotations

import ipaddress
import json
import re
import ssl
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import dns.name
import dns.resolver
from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops import nebius_certificates as certificates
from scripts.ops.nebius_development_management_foundation import HTTPSRetainedDevelopmentFoundation
from scripts.ops.nebius_development_management_install import DevelopmentManagementRequest
from scripts.ops.nebius_development_public import render_development_public
from scripts.ops.nebius_ingress_probe import _connect, probe_management, probe_shared_development
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_install import ManagementInstallError
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_platform_render import digest
from loom_service.environment_management.deployment import ManagementDeployment, render_management


class DevelopmentManagementRouteSettings(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    namespace_uid: UUID
    service_uid: UUID
    controller_uid: UUID
    config_uid: UUID
    ingress_class_uid: UUID
    service_spec_digest: str = Field(pattern=r'^sha256:[0-9a-f]{64}$')
    controller_spec_digest: str = Field(pattern=r'^sha256:[0-9a-f]{64}$')
    config_data_digest: str = Field(pattern=r'^sha256:[0-9a-f]{64}$')

    @field_validator('namespace_uid', 'service_uid', 'controller_uid', 'config_uid', 'ingress_class_uid')
    @classmethod
    def non_nil(cls, value: UUID) -> UUID:
        if not value.int:
            raise ValueError('shared ingress identity must be non-nil')
        return value


def qualify_dns_address(host: str, address: str) -> None:
    """Normal recursive routing must resolve directly to the qualified allocation."""
    resolver = dns.resolver.Resolver()
    for kind in ('A', 'AAAA'):
        answer = resolver.resolve(host, kind, lifetime=10, search=False, raise_on_no_answer=False)
        if answer.canonical_name != dns.name.from_text(host) or list(map(str, answer)) != ([address] if kind == 'A' else []):
            raise ManagementInstallError('development management DNS route differs')


def qualify_tls_address(host: str, address: str, fingerprint: str | None = None) -> None:
    if fingerprint is not None:
        probe_management(address=address, port=443, hostname=host, fingerprint=fingerprint)
    else:
        # Existing wildcard renewal is independent. Public trust + hostname and
        # the exact qualified address suffice; never pin a stale wildcard leaf.
        with _connect(address, 443, host):
            pass


class HTTPSDevelopmentManagementRoute(ManagementKubernetesTransport):
    error_type = ManagementInstallError

    def __init__(self, *, settings: DevelopmentManagementRouteSettings,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        self.settings = settings
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def _resource(self, path: str, *, kind: str, name: str, uid: str,
                  namespace: str | None = None) -> dict[str, Any]:
        row = self._request('GET', path)
        if (row is None or row.get('kind') != kind or row['metadata'].get('name') != name
                or row['metadata'].get('uid') != uid or row['metadata'].get('namespace') != namespace
                or row['metadata'].get('deletionTimestamp') or row['metadata'].get('ownerReferences')):
            raise ManagementInstallError('development shared ingress identity differs')
        return row

    def _observe(self, request: DevelopmentManagementRequest, *, installed: bool) -> str:
        expected = render_management(request.deployment, candidate=request.candidate, profile=request.profile,
            repo_root=Path(__file__).resolve().parents[2]).files['70-public.yaml'][0]
        return self._observe_bound(deployment=request.deployment, kube_system_uid=request.binding.kube_system_uid,
            expected=expected, installed=installed)

    def _observe_bound(self, *, deployment: ManagementDeployment, kube_system_uid: str,
                       expected: dict[str, Any], installed: bool, shared_public: bool = False) -> str:
        foundation = deployment.installation.foundation
        namespace, host = foundation.ingress_namespace, deployment.public_host
        route_namespace, route_name = deployment.namespace, 'loom-management'
        if shared_public:
            wanted = render_development_public(deployment)[1]
            if expected != wanted:
                raise ValueError()
            route_namespace, route_name = 'loom-dev', 'loom-development'
            host = foundation.platform_config['public_host']
        if (deployment.namespace != 'loom-nebius-management-dev'
                or expected['metadata'].get('namespace') != route_namespace
                or expected['metadata'].get('name') != route_name
                or foundation.platform_config['kubernetes_api_server'].rstrip('/') != self.api_server.rstrip('/')):
            raise ValueError()
        self._resource('/api/v1/namespaces/kube-system', kind='Namespace', name='kube-system',
            uid=kube_system_uid)
        self._resource('/api/v1/namespaces/' + namespace, kind='Namespace', name=namespace,
            uid=str(self.settings.namespace_uid))
        service = self._resource('/api/v1/namespaces/' + namespace + '/services/loom-web',
            kind='Service', name='loom-web', namespace=namespace, uid=str(self.settings.service_uid))
        controller = self._resource('/apis/apps/v1/namespaces/' + namespace + '/deployments/loom-shared-ingress',
            kind='Deployment', name='loom-shared-ingress', namespace=namespace, uid=str(self.settings.controller_uid))
        config = self._resource('/api/v1/namespaces/' + namespace + '/configmaps/loom-shared-ingress',
            kind='ConfigMap', name='loom-shared-ingress', namespace=namespace, uid=str(self.settings.config_uid))
        ingress_class = self._resource('/apis/networking.k8s.io/v1/ingressclasses/' + foundation.ingress_class_name,
            kind='IngressClass', name=foundation.ingress_class_name, uid=str(self.settings.ingress_class_uid))
        if (digest(service['spec']) != self.settings.service_spec_digest
                or digest(controller['spec']) != self.settings.controller_spec_digest
                or digest(config['data']) != self.settings.config_data_digest
                or ingress_class['spec'] != {'controller': 'traefik.io/ingress-controller'}
                or service['spec']['type'] != 'LoadBalancer'
                or service['spec']['selector'] != {'app': 'loom-shared-ingress'}
                or not HTTPSRetainedDevelopmentFoundation._ready(controller)):
            raise ValueError()
        port, = service['spec']['ports']
        if port['port'] != 443 or port['targetPort'] != 8443 or port.get('protocol', 'TCP') != 'TCP':
            raise ValueError()
        template = controller['spec']['template']
        if (template['metadata']['labels'].get('app') != 'loom-shared-ingress'
                or template['metadata']['labels'].get('app.kubernetes.io/name') != foundation.ingress_controller_label
                or not any(volume.get('configMap', {}).get('name') == 'loom-shared-ingress'
                           for volume in template['spec']['volumes'])):
            raise ValueError()
        static, routes = json.loads(config['data']['traefik.json']), json.loads(config['data']['routes.yaml'])
        provider = static['providers']['kubernetesIngress']
        if (provider['ingressClass'] != foundation.ingress_class_name
                or provider.get('namespaces') or provider.get('labelSelector')):
            raise ValueError()
        # A TCP passthrough rule wins over an HTTP Ingress. The retained shared
        # controller has one exact standalone SNI route, never a wildcard catchall.
        for router in routes.get('tcp', {}).get('routers', {}).values():
            match = re.fullmatch(r'HostSNI\(`([a-z0-9.-]+)`\)', router['rule'])
            if match is None or match[1] == host:
                raise ValueError()
        address, = service['status']['loadBalancer']['ingress']
        if set(address) - {'ip', 'ipMode'}:
            raise ValueError()
        ip = ipaddress.IPv4Address(address['ip'])
        if not ip.is_global or ip.is_multicast or str(ip) != address['ip']:
            raise ValueError()
        found = False
        for row in inventory_resources(self._request, 'networking.k8s.io/v1', 'ingresses', 'Ingress'):
            own = (row['metadata'].get('namespace'), row['metadata'].get('name')) == (route_namespace, route_name)
            if own:
                if row.get('spec') != expected['spec'] or row['metadata'].get('labels') != expected['metadata']['labels']:
                    raise ValueError()
                if row['metadata'].get('deletionTimestamp') or row['metadata'].get('ownerReferences'):
                    raise ValueError()
                if 'uid' in expected['metadata'] and (_uid(row) != _uid(expected) or _snapshot(row) != _snapshot(expected)):
                    raise ValueError()
                found = True
            for rule in row.get('spec', {}).get('rules', []):
                name = rule.get('host', '')
                if not own and (not name or name == host or (name.startswith('*.') and host.partition('.')[2] == name[2:])):
                    raise ValueError()
        if installed and not found:
            raise ValueError()
        return str(ip)

    def qualify_retained(self, *, deployment: ManagementDeployment, kube_system_uid: str,
                         expected: dict[str, Any]) -> str:
        """Read-only renewal route proof, independent of expired/default TLS."""
        try:
            _uid(expected)
            address = self._observe_bound(deployment=deployment, kube_system_uid=kube_system_uid,
                expected=expected, installed=True)
            qualify_dns_address(deployment.public_host, address)
            if self._observe_bound(deployment=deployment, kube_system_uid=kube_system_uid,
                    expected=expected, installed=True) != address:
                raise ValueError()
            return address
        except Exception:
            raise ManagementInstallError('development management retained route unqualified') from None

    def _qualify(self, request: DevelopmentManagementRequest, *, installed: bool, shared_candidate: str | None = None) -> None:
        try:
            address = self._observe(request, installed=installed)
            shared = None
            if request.shared_public_route:
                shared = render_development_public(request.deployment)[1]
                if self._observe_bound(deployment=request.deployment, kube_system_uid=request.binding.kube_system_uid,
                        expected=shared, installed=installed, shared_public=True) != address:
                    raise ValueError()
                if installed and (shared_candidate is None or re.fullmatch(r'[0-9a-f]{40}', shared_candidate) is None):
                    raise ValueError()
            host, zone = request.deployment.public_host, request.deployment.installation.foundation.public_dns_zone
            if len(zone) > 250 or request.tls_material.public_host != host:
                raise ValueError()
            report = certificates.validate_management_certificate(request.tls_material.chain.encode(),
                request.tls_material.key.encode(), management_host=host)
            wildcard_probe = uuid4().hex[:min(32, 252 - len(zone))] + '.' + zone
            for selected in (wildcard_probe, host):
                qualify_dns_address(selected, address)
            qualify_tls_address(wildcard_probe, address)
            if installed:
                qualify_tls_address(host, address, report['fingerprint_sha256'])
            if shared is not None:
                shared_host = request.deployment.installation.foundation.platform_config['public_host']
                qualify_dns_address(shared_host, address)
                qualify_tls_address(shared_host, address)
                if installed:
                    assert shared_candidate is not None
                    probe_shared_development(address=address, port=443, hostname=shared_host, candidate=shared_candidate)
                if self._observe_bound(deployment=request.deployment, kube_system_uid=request.binding.kube_system_uid,
                        expected=shared, installed=installed, shared_public=True) != address:
                    raise ValueError()
            if self._observe(request, installed=installed) != address:
                raise ValueError()
        except Exception:
            raise ManagementInstallError('development management public route unqualified') from None

    def preflight(self, request: DevelopmentManagementRequest) -> None:
        self._qualify(request, installed=False)

    def verify_public(self, request: DevelopmentManagementRequest, *, shared_candidate: str | None = None) -> None:
        self._qualify(request, installed=True, shared_candidate=shared_candidate)
