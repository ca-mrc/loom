"""Qualify collector-only cloud material without granting provider authority."""
from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

from cryptography.hazmat.primitives import serialization
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops.nebius_management_cloud_scope import (
    _pages,
    _ProviderId,
    _read,
    _require,
    _resource,
)

from loom_service.environment_management.candidates import _json


class DevelopmentCollectorCloudScope(BaseModel):
    model_config = ConfigDict(extra='forbid', frozen=True, hide_input_in_errors=True)
    tenant_id: _ProviderId
    region: str = Field(pattern=r'^[a-z]+-[a-z]+[0-9]+$')
    project_id: _ProviderId
    account_id: _ProviderId
    group_id: _ProviderId
    key_id: _ProviderId


def collector_public_key(*, scope: DevelopmentCollectorCloudScope, config: dict[str, Any],
                         credential: bytes) -> bytes:
    """Validate an inline SDK credential and dev binding, not its live authority."""
    from nebius.base.service_account.credentials_file import ServiceAccountCredentials

    try:
        scope = DevelopmentCollectorCloudScope.model_validate(scope.model_dump())
        _require(isinstance(credential, bytes) and 0 < len(credential) <= 1024**2
            and config['namespace'] == 'loom-dev' and config['environment'] == 'development'
            and (config['project_id'], config['quota_parent_id'], config['region']) == (
                scope.project_id, scope.tenant_id, scope.region))
        raw = _json(credential)
        _require(set(raw) == {'subject-credentials'})
        subject = raw['subject-credentials']
        required = {'alg', 'private-key', 'kid', 'iss', 'sub'}
        _require(isinstance(subject, dict) and required <= set(subject) <= required | {'type'}
            and all(isinstance(value, str) for value in subject.values()))
        parsed = ServiceAccountCredentials.from_json(raw).subject_credentials
        _require(parsed.sub == scope.account_id and parsed.kid == scope.key_id)
        return parsed.parse_private_key().public_key().public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
    except Exception:
        raise ValueError('development collector cloud unqualified') from None


async def qualify_collector_cloud(*, sdk: Any, scope: DevelopmentCollectorCloudScope,
        config: dict[str, Any], credential: bytes, clients: dict[str, Any] | None = None,
        now: datetime | None = None) -> dict[str, str]:
    """Prove the retained key's sole group has precisely tenant viewer authority.

    Tenant viewer is the existing Terraform observer contract: quota allowance
    reads are tenant-scoped. No project editor, registry or storage writer grant
    is accepted. The parent still probes the actual pool with this credential
    and qualifies live material around startup; this is no activation receipt.
    """
    try:
        scope = DevelopmentCollectorCloudScope.model_validate(scope.model_dump())
        return await _qualify_runtime_key(sdk=sdk, scope=scope, config=config, credential=credential,
            permit={'resource_id': scope.tenant_id, 'role': 'viewer'}, clients=clients, now=now)
    except Exception:
        raise ValueError('development collector cloud unqualified') from None


async def _qualify_runtime_key(*, sdk: Any, scope: DevelopmentCollectorCloudScope,
        config: dict[str, Any], credential: bytes, permit: dict[str, str],
        clients: dict[str, Any] | None, now: datetime | None) -> dict[str, str]:
    """Shared read-only IAM mechanics; protected callers fix the required permit."""
    from nebius.api.nebius.iam import v1

    try:
        public = collector_public_key(scope=scope, config=config, credential=credential)
        async with asyncio.timeout(120):
            api: dict[str, Any] = clients if clients is not None else {
                'projects': v1.ProjectServiceClient(sdk), 'accounts': v1.ServiceAccountServiceClient(sdk),
                'groups': v1.GroupServiceClient(sdk), 'memberships': v1.GroupMembershipServiceClient(sdk),
                'permits': v1.AccessPermitServiceClient(sdk), 'public_keys': v1.AuthPublicKeyServiceClient(sdk)}
            project = await _read(api['projects'].get, v1.GetProjectRequest(id=scope.project_id))
            _resource(project, scope.project_id, scope.tenant_id)
            _require(project['status']['container_state'] == 'ACTIVE'
                and project['status']['suspension_state'] == 'NONE'
                and project['spec']['region'] == project['status']['region'] == scope.region)
            account = await _read(api['accounts'].get, v1.GetServiceAccountRequest(id=scope.account_id))
            _resource(account, scope.account_id, scope.project_id)
            _require(account['status']['active'] is True)
            groups = await _pages(api['memberships'].list_member_of, v1.ListMemberOfRequest, subject_id=scope.account_id)
            _require(len(groups) == 1)
            _resource(groups[0], scope.group_id, scope.tenant_id)
            group = await _read(api['groups'].get, v1.GetGroupRequest(id=scope.group_id))
            _resource(group, scope.group_id, scope.tenant_id)
            permits = await _pages(api['permits'].list, v1.ListAccessPermitRequest, parent_id=scope.group_id)
            _require(len(permits) == 1)
            _resource(permits[0], permits[0]['metadata']['id'], scope.group_id)
            _require(permits[0]['spec'] == permit)
            key = await _read(api['public_keys'].get, v1.GetAuthPublicKeyRequest(id=scope.key_id))
            _resource(key, scope.key_id, scope.project_id)
            _require(key['spec']['account'] == {'service_account': {'id': scope.account_id}}
                and key['status']['state'] == 'ACTIVE')
            # Existing observer keys are deliberately non-expiring. If an
            # expiry is present, retain the bootstrap safety margin.
            if expiry := key['spec'].get('expires_at'):
                _require(datetime.fromisoformat(expiry.replace('Z', '+00:00'))
                    > (now or datetime.now(UTC)) + timedelta(minutes=10))
            registered = serialization.load_pem_public_key(key['spec']['data'].encode()).public_bytes(
                serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
            _require(registered == public)
        return {'account_id': scope.account_id, 'key_id': scope.key_id,
            'credential_sha256': hashlib.sha256(credential).hexdigest()}
    except Exception:
        raise ValueError('development collector cloud unqualified') from None
