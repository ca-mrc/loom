"""Deliver one immutable exact-host certificate in the independent dev manager.

The protected caller supplies qualified issuer output. No DNS/controller mutation
or certificate renewal is performed by this fixed create/replay stage.
"""
from __future__ import annotations

import base64
import hashlib
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from scripts.ops import nebius_certificates as certificates
from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_management_material import ManagementBinding
from scripts.ops.nebius_management_stage import (
    HTTPSManagementStageAPI,
    ManagementStageAPI,
    ManagementStageError,
    _stage_fixed_documents,
)
from scripts.ops.nebius_management_supplied import _defaulted
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.nebius_platform_render import digest


@dataclass(frozen=True, repr=False)
class ManagementTLSMaterial:
    public_host: str
    chain: str
    key: str


def management_tls_secret_name(installation_id: str, material: ManagementTLSMaterial) -> str:
    generation = hashlib.sha256(material.chain.encode()).hexdigest()
    return 'loom-management-tls-' + hashlib.sha256((installation_id + ':' + generation).encode()).hexdigest()[:40]


def _documents(material: ManagementTLSMaterial, binding: ManagementBinding) -> dict[str, dict[str, Any]]:
    try:
        if binding.namespace != 'loom-nebius-management-dev':
            raise ValueError()
        certificates.validate_management_certificate(material.chain.encode(), material.key.encode(),
                                                      management_host=material.public_host)
        document = {'apiVersion': 'v1', 'kind': 'Secret', 'type': 'kubernetes.io/tls', 'immutable': True,
            'metadata': {'namespace': binding.namespace, 'name': management_tls_secret_name(binding.installation_id, material),
                'labels': {'loom.nebius/management-installation': binding.installation_id}},
            'data': {'tls.crt': base64.b64encode(material.chain.encode()).decode(),
                     'tls.key': base64.b64encode(material.key.encode()).decode()}}
        return {_key(document): document}
    except Exception:
        raise ManagementStageError('development management TLS material unqualified') from None


class HTTPSManagementTLSAPI(HTTPSManagementStageAPI):
    def __init__(self, *, material: ManagementTLSMaterial, binding: ManagementBinding,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        self.documents, self.binding = _documents(material, binding), binding
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)


def deliver_management_tls(*, material: ManagementTLSMaterial, binding: ManagementBinding,
                           api: ManagementStageAPI, state_dir: Path) -> dict[str, Any]:
    documents = _documents(material, binding)
    return _stage_fixed_documents(documents=documents, revision=digest(documents), phase='development-management-tls',
        binding=binding, api=api, state_dir=state_dir, default_document=_defaulted)
