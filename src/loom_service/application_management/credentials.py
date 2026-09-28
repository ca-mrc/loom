"""Compose generation access, never personal databases or shared master keys.

Protected installation supplies the shared CA/keyring and qualifies the IAM groups.
This adapter does not activate Pods, enroll shared users, prove object revocation,
release capacity or mark an application ready.
"""
from __future__ import annotations

import base64
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from cryptography import x509
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError

from loom.nebius_application_contract import ApplicationRegistrationV1
from loom.nebius_application_credentials import application_credential_names
from loom_service.application_management.cloud_effects import ApplicationStorageAccessV1
from loom_service.application_management.cloud_provider import ApplicationCloudProvider
from loom_service.application_management.database import AsyncApplicationDatabaseAccess
from loom_service.application_management.kubernetes import ApplicationKubernetesProvider
from loom_service.application_management.leases import ApplicationLease
from loom_service.application_management.material import (
    ApplicationMaterial,
    ApplicationMaterialJournal,
)
from loom_service.application_management.object_access import ApplicationObjectAccessVerifier
from loom_service.environment_management.provider import ProviderBlockedError, ProviderWaitingError
from loom_service.environment_management.registry import ManagementError


@dataclass(frozen=True, repr=False)
class SharedApplicationCredentials:
    data_environment_id: UUID
    ca_pem: str
    secret_store_master_keys: str
    database_name: str = "loom"


class ApplicationCredentialProvider:
    def __init__(self, registry: ApplicationMaterialJournal, cloud: ApplicationCloudProvider,
                 database: AsyncApplicationDatabaseAccess, *, storage_binding: dict[str, Any],
                 shared: SharedApplicationCredentials):
        self.registry, self.cloud, self.database = registry, cloud, database
        self.storage = ApplicationStorageAccessV1.model_validate(storage_binding)
        self.shared = shared

    def _registration(self, plan: dict[str, Any]) -> ApplicationRegistrationV1:
        try:
            row = ApplicationRegistrationV1.model_validate(plan["registration"])
            if (row.data_environment_id != self.shared.data_environment_id
                    or row.data_environment_id != self.storage.data_environment_id
                    or row.data_environment_id != self.database.data_environment_id
                    or str(row.data_environment_id) != plan["shared"]["data_environment_id"]):
                raise ValueError
            return row
        except (ValueError, TypeError, KeyError, AttributeError):
            raise ProviderBlockedError("application_shared_credentials_invalid") from None

    def _qualify(self, plan: dict[str, Any]) -> ApplicationRegistrationV1:
        row = self._registration(plan)
        try:
            shared = self.shared
            if (not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", shared.database_name)
                    or len(shared.ca_pem) > 65536 or len(shared.secret_store_master_keys) > 8192):
                raise ValueError
            certificates = x509.load_pem_x509_certificates(shared.ca_pem.encode())
            now = datetime.now(UTC)
            if not certificates or "PRIVATE KEY" in shared.ca_pem:
                raise ValueError
            for certificate in certificates:
                if (not certificate.extensions.get_extension_for_class(x509.BasicConstraints).value.ca
                        or not certificate.not_valid_before_utc <= now < certificate.not_valid_after_utc):
                    raise ValueError
            parts = shared.secret_store_master_keys.split(",")
            if not parts or any(len(base64.b64decode(part.strip(), validate=True)) != 32 for part in parts):
                raise ValueError
            return row
        except (ValueError, TypeError, KeyError, AttributeError, x509.ExtensionNotFound, x509.DuplicateExtension):
            raise ProviderBlockedError("application_shared_credentials_invalid") from None

    def _url(self, plan: dict[str, Any], row: ApplicationRegistrationV1, password: str) -> str:
        return URL.create("postgresql", username=f"lap_{row.incarnation.hex}_g{row.access_generation}",
            password=password, host=f"loom-postgres.{plan['shared']['platform_namespace']}.svc",
            port=5432, database=self.shared.database_name, query={
                "sslmode": "verify-full", "sslrootcert": "/var/run/loom-db/ca.crt",
            }).render_as_string(hide_password=False)

    def _material(self, plan: dict[str, Any], row: ApplicationRegistrationV1,
                  password: str, storage: dict[str, str]) -> ApplicationMaterial:
        names = application_credential_names(row)
        return {names["db"]: {"url": self._url(plan, row, password), "ca.crt": self.shared.ca_pem},
                names["storage"]: dict(storage),
                names["auth"]: {"secret-store-master-keys": self.shared.secret_store_master_keys}}

    async def prepare(self, lease: ApplicationLease) -> ApplicationMaterial:
        plan = await self.registry.frozen_plan(lease)
        row = self._qualify(plan)
        binding = self.storage.model_dump(mode="json")
        await self.cloud.create(lease, "account", binding)
        await self.cloud.create(lease, "key", binding)
        storage = await self.cloud.key_material(lease)
        material = await self.registry.ensure_material(lease, lambda frozen: self._material(
            frozen, self._qualify(frozen), secrets.token_urlsafe(48), storage))
        try:
            db = material[application_credential_names(row)["db"]]
            password = make_url(db["url"]).password
            if (password is None or re.fullmatch(r"[A-Za-z0-9_-]{48,128}", password) is None
                    or material != self._material(plan, row, password, storage)):
                raise ValueError
        except (ValueError, KeyError, ArgumentError):
            raise ProviderBlockedError("application_credential_material_conflict") from None
        await self.registry.frozen_plan(lease)
        role = await self.database.grant(lease, password)
        if role != f"lap_{row.incarnation.hex}_g{row.access_generation}":
            raise ProviderBlockedError("application_database_result_invalid")
        await self.cloud.create(lease, "data", binding)
        await self.cloud.create(lease, "source", binding)
        await self.registry.frozen_plan(lease)
        return material

    async def deliver(self, lease: ApplicationLease, kubernetes: ApplicationKubernetesProvider) -> None:
        """Deliver only frozen generation bundles into an already-owned namespace.

        Observations are historical write evidence. The lifecycle coordinator
        still checks live Secrets/resources and every prerequisite before startup.
        """
        material = await self.prepare(lease)
        row = self._qualify(await self.registry.frozen_plan(lease))
        for purpose, name in application_credential_names(row).items():
            await kubernetes.create(lease, "credential:" + purpose, {
                "apiVersion": "v1", "kind": "Secret", "immutable": True, "type": "Opaque",
                "metadata": {"name": name, "namespace": row.application_namespace},
                "data": {key: base64.b64encode(value.encode()).decode() for key, value in material[name].items()},
            })

    async def retire_database(self, lease: ApplicationLease) -> None:
        plan = await self.registry.frozen_plan(lease)
        # Retirement needs exact DB identity, not material suitable for a NEW
        # delivery. An expired CA/keyring must not keep old SQL access alive.
        row = self._registration(plan)
        through = lease.access_generation - (1 if row.desired_state == "active" else 0)
        if through == 0:
            return
        await self.database.revoke(lease, through)
        await self.registry.frozen_plan(lease)
        if not await self.database.drain(lease, through):
            raise ProviderWaitingError("application_database_retirement_pending")
        await self.registry.frozen_plan(lease)

    async def retire_cloud(self, lease: ApplicationLease, verifier: ApplicationObjectAccessVerifier) -> None:
        """Retire prior-generation IAM and prove rejection of delivered keys.

        The lifecycle caller must stop old processes separately. Shared groups,
        buckets, policies and other applications' credentials are never targets.
        """
        current_plan = await self.registry.frozen_plan(lease)
        row = self._registration(current_plan)
        through = lease.access_generation - (1 if row.desired_state == "active" else 0)
        plans: dict[UUID, dict[str, Any]] = {lease.operation_id: current_plan}
        for effect in await self.cloud.registry.cloud_history(lease):
            if effect.operation_id not in plans:
                plans[effect.operation_id] = await self.registry.frozen_plan(lease, operation_id=effect.operation_id)
            source = self._registration(plans[effect.operation_id])
            if source.access_generation <= through and effect.action == "create" and effect.phase == "dispatched":
                await self.cloud.reconcile(lease, effect.operation_id, effect.key)
        history = await self.cloud.registry.cloud_history(lease)
        targets = [effect for effect in history if effect.action == "create" and effect.phase == "observed"
                   and self._registration(plans[effect.operation_id]).access_generation <= through]
        proofs: list[tuple[dict[str, Any], dict[str, str]]] = []
        for effect in targets:
            if effect.kind != "access_key":
                continue
            plan = plans[effect.operation_id]
            try:
                material = await self.registry.load_material(lease, operation_id=effect.operation_id)
            except ManagementError as exc:
                # Group grants require committed material. A key interrupted
                # before that commit was permissionless and never deliverable.
                # Corruption, or material missing after any membership intent,
                # remains an error and cannot silently imply revoked access.
                if exc.code != "application_material_missing" or any(
                    item.operation_id == effect.operation_id and item.kind == "membership" for item in history
                ):
                    raise
            else:
                name = application_credential_names(self._registration(plan))["storage"]
                proofs.append((plan, material[name]))
        # Journal history follows generation/sequence, so reversing it removes
        # memberships before keys before their service account. Existing exact
        # deletion intent is reused even across suspend->destroy transitions.
        for effect in reversed(targets):
            await self.cloud.delete(lease, effect.operation_id, effect.key)
        for plan, storage in proofs:
            await self.registry.frozen_plan(lease)
            await verifier.verify_retired(plan, storage)
        await self.registry.frozen_plan(lease)
