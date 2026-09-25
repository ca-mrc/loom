"""Internal crash-stable credential material; not credential provisioning.

Trusted lifecycle callers supply a synchronous, side-effect-free factory. Persist
its result BEFORE preparing or issuing external grants or Secret deliveries.
The management SecretStore key is distinct from the shared app keyring it holds.
"""
from __future__ import annotations

import copy
import json
import re
from collections.abc import Callable
from typing import Any, cast
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.nebius_application_material_schema import NebiusApplicationMaterial
from loom.db.nebius_application_operation_schema import NebiusApplicationOperation
from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_application_credentials import application_credential_names
from loom.security.secret_store import LocalEncryptedSecretStore, SecretStoreError, parse_ref
from loom_service.application_management.effects import ApplicationEffectJournal, _secret_targets
from loom_service.application_management.leases import ApplicationLease
from loom_service.environment_management.registry import ManagementError

ApplicationMaterial = dict[str, dict[str, str]]
_MAX_BYTES = 1_048_576


def _identity(operation: NebiusApplicationOperation) -> dict[str, str | int]:
    row = ApplicationRegistrationV1.model_validate(operation.plan_json["registration"])
    if (row.application_id != operation.application_id or row.deployment_generation != operation.deployment_generation
            or row.access_generation != operation.access_generation or row.desired_state != "active"
            or operation.action not in {"create", "update", "resume"}
            or _secret_targets(operation) != set(application_credential_names(row).values())):
        raise ValueError("not a generation-bound active plan")
    return dict(operation_id=str(operation.operation_id), application_id=str(row.application_id),
                incarnation=str(row.incarnation), access_generation=row.access_generation,
                data_environment_id=str(row.data_environment_id))


def _namespace(identity: dict[str, str | int]) -> str:
    return (f"nebius-application/{identity['application_id']}/{identity['incarnation']}"
            f"/g{identity['access_generation']}")


def _validate(value: Any, operation: NebiusApplicationOperation) -> ApplicationMaterial:
    if type(value) is not dict or set(value) != _secret_targets(operation):
        raise ValueError("invalid material targets")
    for bundle in value.values():
        if type(bundle) is not dict or not 1 <= len(bundle) <= 64:
            raise ValueError("invalid material bundle")
        for key, item in bundle.items():
            if (type(key) is not str or re.fullmatch(r"[-._a-zA-Z0-9]{1,253}", key) is None
                    or type(item) is not str or not item or len(item) > _MAX_BYTES):
                raise ValueError("invalid material entry")
    if len(json.dumps(value, sort_keys=True).encode()) > _MAX_BYTES:
        raise ValueError("material too large")
    return cast(ApplicationMaterial, value)


async def _load(session: AsyncSession, operation: NebiusApplicationOperation,
                row: NebiusApplicationMaterial) -> ApplicationMaterial:
    try:
        identity = _identity(operation)
        if parse_ref(row.secret_ref).namespace != _namespace(identity):
            raise ValueError("wrong material namespace")
        plaintext = await LocalEncryptedSecretStore(session).get(row.secret_ref)
        if len(plaintext.encode()) > _MAX_BYTES + 2048:
            raise ValueError("material too large")
        envelope = json.loads(plaintext)
        if (type(envelope) is not dict or set(envelope) != {"identity", "material"}
                or envelope["identity"] != identity
                or any(type(envelope["identity"][key]) is not type(value) for key, value in identity.items())):
            raise ValueError("wrong material identity")
        return _validate(envelope["material"], operation)
    except (SecretStoreError, ValueError, TypeError, KeyError):
        raise ManagementError("application_material_unavailable", 503) from None


class ApplicationMaterialJournal(ApplicationEffectJournal):
    async def ensure_material(self, lease: ApplicationLease,
                              factory: Callable[[dict[str, Any]], ApplicationMaterial]) -> ApplicationMaterial:
        """First committed generation wins. The factory must perform no I/O.

        A rolled-back first call may run the factory again; no external effect
        may depend on its result before this method returns successfully.
        """
        async with self.session_factory.begin() as session:
            operation, _ = await self._leased(session, lease)
            try:
                identity = _identity(operation)
            except (ValueError, TypeError, KeyError):
                raise ManagementError("invalid_application_material_operation", 422) from None
            existing = await session.get(NebiusApplicationMaterial, operation.operation_id)
            if existing is not None:
                return await _load(session, operation, existing)
            try:
                value = factory(copy.deepcopy(operation.plan_json))
            except Exception:
                raise ManagementError("application_material_generation_failed", 503) from None
            try:
                value = _validate(value, operation)
            except (ValueError, TypeError):
                raise ManagementError("invalid_application_material", 422) from None
            try:
                ref = await LocalEncryptedSecretStore(session).put(namespace=_namespace(identity),
                    value=json.dumps({"identity": identity, "material": value}, sort_keys=True))
            except SecretStoreError:
                raise ManagementError("application_material_unavailable", 503) from None
            session.add(NebiusApplicationMaterial(operation_id=operation.operation_id, secret_ref=ref))
            await session.flush()
            return copy.deepcopy(value)

    async def load_material(self, lease: ApplicationLease, *, operation_id: UUID | None = None) -> ApplicationMaterial:
        """The current lease may read its own earlier material for retirement."""
        async with self.session_factory.begin() as session:
            current, _ = await self._leased(session, lease)
            target = current if operation_id is None else await session.get(NebiusApplicationOperation, operation_id)
            if (target is None or target.application_id != current.application_id
                    or target.deployment_generation > current.deployment_generation):
                raise ManagementError("application_material_forbidden", 403)
            row = await session.get(NebiusApplicationMaterial, target.operation_id)
            if row is None:
                raise ManagementError("application_material_missing", 503)
            return await _load(session, target, row)
