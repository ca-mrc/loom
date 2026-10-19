"""Read-only foundation readiness, independent of staging and legacy handoff.

This is data/application readiness only. It neither installs pool consumers nor
proves sole-writer ownership, schema compatibility or operational task execution.
The protected issuer must combine those separate proofs before granting intake.
"""
from __future__ import annotations

import asyncio
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlencode

import httpx
from pydantic import AwareDatetime, BaseModel, ConfigDict
from scripts.ops.nebius_database_readiness import qualify_database_backend, qualify_database_pod
from scripts.ops.nebius_environment_foundation import (
    FoundationCloud,
    FoundationObservationV2,
    FoundationReferenceV2,
    _load,
    _read,
    reconcile_foundation,
)
from scripts.ops.nebius_ingress_stage import _uid

from loom_service.environment_management.candidates import _json
from loom_service.environment_management.kubernetes_provider import (
    KubernetesEnvironmentProvider,
    _contains,
)
from loom_service.pool_management.capacity import digest


class FoundationReadinessV2(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    schema_version: Literal['loom.foundation-readiness.v2'] = 'loom.foundation-readiness.v2'
    foundation: FoundationObservationV2
    verified_at: AwareDatetime
    origin: str
    database_pod_uid: str
    database_pvc_uid: str
    database_volume_name: str


async def _public(http: httpx.AsyncClient, origin: str, token: str) -> dict[str, Any]:
    # A standalone request does not inherit operator Authorization, cookies or
    # client auth. Redirects cannot forward this environment's singleton token.
    request = httpx.Request('GET', origin + '/api/v1/health/ready',
        headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/json', 'Accept-Encoding': 'identity'},
        extensions={'timeout': {'connect': 10.0, 'read': 30.0, 'write': 10.0, 'pool': 10.0}})
    async with asyncio.timeout(30):
        response = await http.send(request, stream=True, auth=None, follow_redirects=False)
        try:
            if response.status_code != 200 or response.headers.get('content-encoding', 'identity') != 'identity':
                raise ValueError('foundation_public_unavailable')
            body = bytearray()
            async for part in response.aiter_bytes():
                body.extend(part)
                if len(body) > 16384:
                    raise ValueError('foundation_public_unqualified')
            return _json(bytes(body))
        finally:
            await response.aclose()


async def observe_foundation(*, state_dir: Path, reference: FoundationReferenceV2,
        kubernetes: KubernetesEnvironmentProvider, cloud: FoundationCloud,
        public_http: httpx.AsyncClient) -> FoundationReadinessV2:
    """Re-observe completed intents, DB/Service lineage and exact public origin.

    Transport clients are created by the protected host with verified TLS and
    scoped credentials; a public runner cannot supply a client, state path or
    desired document. Provider failures have a single secret-free outward code.
    """
    try:
        before = await reconcile_foundation(state_dir=state_dir, reference=reference,
            kubernetes=kubernetes, cloud=cloud, observe_only=True)
        plan = _load(state_dir, reference)
        namespace = reference.identity.application_namespace
        documents = {doc['kind']: doc for doc in plan['files']['20-database.yaml']}
        base = '/api/v1/namespaces/' + namespace

        async def resource(kind: str, path: str) -> dict[str, Any]:
            actual = await kubernetes._request('GET', path)
            wanted = documents[kind]
            key = f"k8s:{kind}:{namespace}:{wanted['metadata']['name']}"
            if actual is None or _uid(actual) != before.identities[key] or not _contains(actual, wanted):
                raise ValueError('foundation_database_binding_unqualified')
            return actual

        database = await resource('StatefulSet', '/apis/apps/v1/namespaces/' + namespace + '/statefulsets/loom-postgres')
        service = await resource('Service', base + '/services/loom-postgres')
        pvc = await resource('PersistentVolumeClaim', base + '/persistentvolumeclaims/data-loom-postgres-0')
        if (pvc.get('status', {}).get('phase') != 'Bound' or not pvc['spec'].get('volumeName')
                or pvc['metadata'].get('deletionTimestamp')):
            raise ValueError('foundation_database_storage_unqualified')
        pods = await kubernetes._request('GET', base + '/pods?' + urlencode({'labelSelector': 'app=loom-postgres', 'limit': 2}))
        endpoints = await kubernetes._request('GET', '/apis/discovery.k8s.io/v1/namespaces/' + namespace + '/endpointslices?'
            + urlencode({'labelSelector': 'kubernetes.io/service-name=loom-postgres', 'limit': 3}))
        if pods is None or endpoints is None:
            raise ValueError('foundation_database_backend_unavailable')
        # These freshly read controller objects have already matched the fixed
        # intent and its retained UIDs. The helpers now bind Pod/endpoint ancestry
        # to exactly those controllers, including all address families.
        pod = qualify_database_pod(namespace=namespace, database=database, service=service,
            retained_database=database, retained_service=service, listing=pods)
        qualify_database_backend(namespace=namespace, service=service, pod=pod, listing=endpoints)
        material = _read(state_dir / 'material.json')
        if digest(material) != before.identities['credentials:material']:
            raise ValueError('foundation_material_conflict')
        token = tomllib.loads(material['loom-admin-secret']['secrets.toml'])['admin']['token']
        origin = 'https://' + reference.identity.public_host
        public = await _public(public_http, origin, token)
        if public != {'status': 'ready', 'postgres': 'ready', 'object_store': 'ready',
                'environment': reference.identity.kind, 'namespace': namespace, 'blockers': []}:
            raise ValueError('foundation_public_binding_unqualified')
        after = await reconcile_foundation(state_dir=state_dir, reference=reference,
            kubernetes=kubernetes, cloud=cloud, observe_only=True)
        if (before.identities != after.identities or before.namespace_uids != after.namespace_uids
                or not timedelta(0) <= datetime.now(UTC) - before.observed_at <= timedelta(minutes=5)):
            raise ValueError('foundation_readiness_stale')
        return FoundationReadinessV2(foundation=before, verified_at=datetime.now(UTC), origin=origin,
            database_pod_uid=_uid(pod), database_pvc_uid=_uid(pvc), database_volume_name=pvc['spec']['volumeName'])
    except Exception:
        raise ValueError('foundation_readiness_unqualified') from None
