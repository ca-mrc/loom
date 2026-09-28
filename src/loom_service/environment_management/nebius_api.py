"""Bounded native Nebius SDK transport for environment-owned IAM and buckets."""

from __future__ import annotations

import json
from typing import Any

from loom_service.environment_management.kubernetes_provider import _contains
from loom_service.environment_management.provider import ProviderBlockedError, ProviderRetryError


class NebiusSdkEnvironmentApi:
    def __init__(self, sdk: Any, *, clients: dict[str, Any] | None = None):
        # The installation owns SDK credentials and shutdown. No ambient login,
        # CLI subprocess, project-wide grants, or Terraform state is involved.
        from nebius.api.nebius.iam import v1, v2
        from nebius.api.nebius.storage import v1 as storage

        self.bindings: dict[str, tuple[Any, str]] = {
            "service_account": (v1, "ServiceAccount"), "group": (v1, "Group"),
            "membership": (v1, "GroupMembership"), "access_key": (v2, "AccessKey"),
            "bucket": (storage, "Bucket"),
        }
        self.clients = clients if clients is not None else {
            kind: getattr(module, name + "ServiceClient")(sdk)
            for kind, (module, name) in self.bindings.items()
        }

    @staticmethod
    def _value(message: Any) -> dict[str, Any]:
        value: dict[str, Any] = json.loads(message.to_json(preserving_proto_field_name=True))
        value.setdefault("spec", {})
        return value

    async def _call(self, method: Any, request: Any, *, key: str | None = None, missing: bool = False) -> Any:
        from grpc import StatusCode
        from nebius.aio.service_error import RequestError

        kwargs: dict[str, Any] = {"timeout": 30, "retries": 0}
        if key is not None:
            kwargs["metadata"] = [("x-idempotency-key", key)]
        try:
            return await method(request, **kwargs)
        except RequestError as exc:
            code = exc.status.code
            if missing and code == StatusCode.NOT_FOUND:
                return None
            if code in {StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED, StatusCode.RESOURCE_EXHAUSTED,
                        StatusCode.ABORTED, StatusCode.ALREADY_EXISTS, StatusCode.INTERNAL, StatusCode.UNKNOWN}:
                raise ProviderRetryError("nebius_request_retry_required") from None
            raise ProviderBlockedError("nebius_request_rejected") from None
        except (TimeoutError, OSError):
            raise ProviderRetryError("nebius_unavailable") from None

    async def find(self, kind: str, expected: dict[str, Any]) -> dict[str, Any] | None:
        module, name = self.bindings[kind]
        client = self.clients[kind]
        metadata = expected["metadata"]
        if kind in {"service_account", "group", "bucket"}:
            request = getattr(module, "Get" + name + "ByNameRequest")(
                parent_id=metadata["parent_id"], name=metadata["name"],
            )
            result = await self._call(client.get_by_name, request, missing=True)
            return self._value(result) if result is not None else None
        # These APIs have no get-by-name. Bound every page and reject duplicate
        # matches; don't silently choose one credential from ambiguous inventory.
        request_class = getattr(module, "List" + name + "sRequest")
        list_resources = client.list_members if kind == "membership" else client.list
        token = ""
        seen: set[str] = set()
        found: dict[str, Any] | None = None
        for _ in range(100):
            page = await self._call(list_resources, request_class(
                parent_id=metadata["parent_id"], page_size=100, page_token=token,
            ))
            for resource in getattr(page, "memberships" if kind == "membership" else "items"):
                row = self._value(resource)
                match = (row["spec"].get("member_id") == expected["spec"]["member_id"] if kind == "membership"
                         else row["metadata"].get("name") == metadata["name"])
                if match:
                    if found is not None:
                        raise ProviderBlockedError("nebius_resource_identity_ambiguous")
                    found = row
            token = page.next_page_token
            if not token:
                return found
            if token in seen:
                break
            seen.add(token)
        raise ProviderBlockedError("nebius_resource_inventory_incomplete")

    async def create(self, kind: str, expected: dict[str, Any], *, idempotency_key: str) -> dict[str, Any]:
        module, name = self.bindings[kind]
        client = self.clients[kind]
        request = getattr(module, "Create" + name + "Request").from_json(json.dumps(expected))
        operation = await self._call(client.create, request, key=idempotency_key)
        try:
            await operation.wait(timeout=30, poll_retries=0)
        except Exception:
            # Creation may have succeeded; never create a new retry identity.
            raise ProviderRetryError("nebius_operation_unconfirmed") from None
        if not operation.successful() or not operation.resource_id:
            raise ProviderBlockedError("nebius_operation_failed")
        resource = await self._call(client.get, getattr(module, "Get" + name + "Request")(id=operation.resource_id))
        return self._value(resource)

    async def access_key_secret(self, identity: str) -> dict[str, str]:
        from nebius.api.nebius.iam.v2 import GetAccessKeySecretRequest

        response = await self._call(self.clients["access_key"].get_secret, GetAccessKeySecretRequest(id=identity))
        if not response.aws_access_key_id or not response.secret:
            raise ProviderBlockedError("nebius_access_key_secret_unavailable")
        return {"access-key": str(response.aws_access_key_id), "secret-key": str(response.secret)}

    def _application_iam(self, kind: str) -> tuple[Any, str]:
        if kind not in {"service_account", "membership", "access_key"}:
            raise ValueError("unsupported application IAM kind")
        return self.bindings[kind]

    async def get_resource(self, kind: str, identity: str) -> dict[str, Any] | None:
        """Read an exact recorded IAM identity, never a bucket or shared group."""
        module, name = self._application_iam(kind)
        result = await self._call(self.clients[kind].get, getattr(module, "Get" + name + "Request")(id=identity),
                                  missing=True)
        return self._value(result) if result is not None else None

    async def delete_resource(self, kind: str, identity: str, *, idempotency_key: str) -> None:
        """One DELETE attempt; caller must journal and qualify exact ownership first."""
        module, name = self._application_iam(kind)
        operation = await self._call(self.clients[kind].delete,
            getattr(module, "Delete" + name + "Request")(id=identity), key=idempotency_key, missing=True)
        if operation is None:
            return
        try:
            await operation.wait(timeout=30, poll_retries=0)
        except Exception:
            raise ProviderRetryError("nebius_iam_deletion_unconfirmed") from None
        if not operation.successful():
            raise ProviderBlockedError("nebius_iam_deletion_failed")

    async def revoke_access_key(self, identity: str, expected: dict[str, Any], *, idempotency_key: str) -> None:
        """Delete only a recorded, readback-matching key ID; never a bucket/data."""
        from nebius.api.nebius.iam.v2 import DeleteAccessKeyRequest, GetAccessKeyRequest

        client = self.clients["access_key"]
        current = await self._call(client.get, GetAccessKeyRequest(id=identity), missing=True)
        if current is None:
            return
        actual = self._value(current)
        if actual.get("metadata", {}).get("id") != identity or not _contains(actual, expected):
            raise ProviderBlockedError("cloud_resource_identity_conflict")
        operation = await self._call(client.delete, DeleteAccessKeyRequest(id=identity), key=idempotency_key, missing=True)
        if operation is not None:
            try:
                await operation.wait(timeout=30, poll_retries=0)
            except Exception:
                raise ProviderRetryError("nebius_key_revocation_unconfirmed") from None
            if not operation.successful():
                raise ProviderBlockedError("nebius_key_revocation_failed")
        if await self._call(client.get, GetAccessKeyRequest(id=identity), missing=True) is not None:
            raise ProviderRetryError("nebius_key_revocation_unconfirmed")
