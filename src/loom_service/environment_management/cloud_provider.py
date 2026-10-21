"""Independent bucket/IAM create intents with deterministic replay identities."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Protocol
from uuid import uuid5

from loom_service.environment_management.kubernetes_provider import _contains
from loom_service.environment_management.provider import ProviderBlockedError, ProvisioningContext
from loom_service.environment_management.steps import ProvisioningStep


class NebiusEnvironmentApi(Protocol):
    async def find(self, kind: str, expected: dict[str, Any]) -> dict[str, Any] | None: ...

    async def create(
        self, kind: str, expected: dict[str, Any], *, idempotency_key: str,
    ) -> dict[str, Any]: ...


class NebiusEnvironmentCloudProvider:
    def __init__(self, api: NebiusEnvironmentApi):
        self.api = api

    @staticmethod
    def _dependency(context: ProvisioningContext, purpose: str, action: str) -> str:
        identity = context.identities.get(f"iam:{purpose}:{action}")
        if not identity:
            raise ProviderBlockedError("cloud_dependency_missing")
        return identity

    def intent(self, context: ProvisioningContext, step: ProvisioningStep) -> tuple[str, dict[str, Any]]:
        purpose = step.payload.get("purpose")
        incarnation = context.registration["incarnation"].replace("-", "")
        prefix = "loom-" + incarnation + "-"
        # Missing scope is a legacy frozen intent, not the current installation
        # default. Replays and retained cleanup must not move existing resources.
        parent = context.provisioning_project_id or context.config["project_id"]
        spec: dict[str, Any]
        if step.kind == "object_bucket":
            if purpose not in {"artifacts", "trajectories", "source", "backup"}:
                raise ProviderBlockedError("cloud_bucket_purpose_invalid")
            v2 = context.config.get('schema_version') == 'loom.nebius-environment-platform.v2'
            wanted_name = context.config['buckets'][purpose] if v2 else prefix + purpose
            if step.payload.get("name") != wanted_name:
                raise ProviderBlockedError("cloud_bucket_name_mismatch")
            kind, name = "bucket", step.payload["name"]
            owner = "canonical" if purpose in {"artifacts", "trajectories"} else purpose
            spec = {
                "default_storage_class": "STANDARD", "force_storage_class": True,
                "object_audit_logging": "ALL",
                "versioning_policy": "DISABLED" if purpose == "source" else "ENABLED",
                "bucket_policy": {"rules": [{
                    "group_id": self._dependency(context, owner, "group"),
                    "paths": ["*"], "roles": ["storage.object-editor"],
                }]},
                "lifecycle_configuration": {"rules": [{
                    "id": "abort-incomplete-uploads", "status": "ENABLED",
                    "abort_incomplete_multipart_upload": {"days_after_initiation": 7},
                }]},
            }
        elif step.kind == "credentials" and purpose in {"canonical", "source", "backup"}:
            kind, name = str(step.payload.get("action", "")), prefix + purpose
            if kind == "service_account":
                spec = {"description": "Isolated Loom environment " + purpose + " objects"}
            elif kind == "group":
                parent, spec = context.provisioning_project_id or context.config["quota_parent_id"], {}
            elif kind == "membership":
                parent = self._dependency(context, purpose, "group")
                spec = {"member_id": self._dependency(context, purpose, "service_account")}
            elif kind == "access_key":
                spec = {
                    "account": {"service_account": {"id": self._dependency(context, purpose, "service_account")}},
                    "secret_delivery_mode": "EXPLICIT",
                    "description": "Isolated Loom environment " + purpose + " object credential",
                }
            else:
                raise ProviderBlockedError("cloud_credential_action_invalid")
        else:
            raise ProviderBlockedError("cloud_step_not_supported")
        metadata: dict[str, Any] = {"parent_id": parent, "labels": {
            "loom-environment-id": str(context.lease.environment_id),
            "loom-incarnation": context.registration["incarnation"],
            "loom-operation-id": str(context.lease.operation_id),
        }}
        if kind != "membership":
            metadata["name"] = name
        expected = {"metadata": metadata, "spec": spec}
        digest = hashlib.sha256(json.dumps(expected, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        metadata["labels"]["loom-intent"] = digest[:32]
        return kind, expected

    async def apply(self, context: ProvisioningContext, step: ProvisioningStep, *, create: bool = True) -> str:
        if not create and not context.identities.get(step.key):
            raise ProviderBlockedError("cloud_import_identity_missing")
        kind, expected = self.intent(context, step)
        actual = await self.api.find(kind, expected)
        if actual is None:
            if not create or step.key in context.identities:
                raise ProviderBlockedError("cloud_recorded_resource_missing")
            actual = await self.api.create(kind, expected, idempotency_key=str(uuid5(context.lease.operation_id, step.key)))
        identity = actual.get("metadata", {}).get("id")
        if (not isinstance(identity, str) or not identity or not _contains(actual, expected)
                or (step.key in context.identities and context.identities[step.key] != identity)):
            raise ProviderBlockedError("cloud_resource_identity_conflict")
        return identity
