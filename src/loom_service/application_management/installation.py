"""Protected personal application configuration; secrets remain in mounted files."""
from __future__ import annotations

import base64
import os
import re
import ssl
import stat
from pathlib import Path
from typing import Self

from psycopg.conninfo import conninfo_to_dict
from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1
from loom.nebius_application_contract import ApplicationReleaseV1, SharedDevelopmentBindingV1
from loom.nebius_environment_contract import FoundationBinding
from loom_service.application_management.cloud_effects import ApplicationStorageAccessV1
from loom_service.application_management.credentials import SharedApplicationCredentials
from loom_service.environment_management.candidates import _json
from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesConnection


def read_protected_file(path: Path, *, limit: int) -> bytes:
    """Read the opened regular inode, allowing Kubernetes projected symlinks."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o027 or not 0 < info.st_size <= limit:
            raise ValueError("invalid protected file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            value = stream.read(limit + 1)
        if not 0 < len(value) <= limit:
            raise ValueError("invalid protected file")
        return value
    finally:
        os.close(descriptor)


class ApplicationRuntimeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kubernetes: ProjectedKubernetesConnection
    cloud_credentials_file: Path
    database_connection_file: Path
    shared_credentials_file: Path
    concurrency: int = Field(default=4, ge=1, le=16, strict=True)
    poll_seconds: int = Field(default=5, ge=1, le=60, strict=True)


class ApplicationInstallation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    shared: SharedDevelopmentBindingV1
    authority: ApplicationNamespaceAuthorityV1
    releases: tuple[ApplicationReleaseV1, ...] = Field(max_length=1000)
    storage: ApplicationStorageAccessV1
    runtime: ApplicationRuntimeSettings

    @model_validator(mode="after")
    def _bindings(self) -> Self:
        if (self.shared.data_environment_id != self.authority.data_environment_id
                or self.shared.data_environment_id != self.storage.data_environment_id
                or self.shared.cluster_id != self.authority.cluster_id
                or self.shared.platform_namespace != self.authority.shared_namespace
                or len({release.release_id for release in self.releases}) != len(self.releases)):
            raise ValueError("invalid application installation binding")
        return self

    def validate_foundation(self, foundation: FoundationBinding) -> None:
        self.shared.validate_foundation(foundation)
        if foundation.provisioning_project_id != self.storage.project_id:
            raise ValueError("application provisioning project differs from foundation")

    def load_credentials(self) -> tuple[str, SharedApplicationCredentials]:
        """Qualify bounded shared material and a non-ambient verify-full SQL route."""
        try:
            raw = _json(read_protected_file(self.runtime.shared_credentials_file, limit=131072))
            if (set(raw) != {"ca_pem", "database_name", "secret_store_master_keys"}
                    or not all(isinstance(value, str) for value in raw.values())
                    or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", raw["database_name"])
                    or not 0 < len(raw["ca_pem"]) <= 65536 or "PRIVATE KEY" in raw["ca_pem"]
                    or not 0 < len(raw["secret_store_master_keys"]) <= 8192
                    or any(len(base64.b64decode(key.strip(), validate=True)) != 32
                           for key in raw["secret_store_master_keys"].split(","))):
                raise ValueError
            ssl.create_default_context(cadata=raw["ca_pem"])
            dsn = read_protected_file(self.runtime.database_connection_file, limit=16384).decode("utf-8").strip()
            connection = conninfo_to_dict(dsn)
            username, ca_path = connection.get("user"), connection.get("sslrootcert")
            if (set(connection) - {"host", "port", "dbname", "user", "password", "sslmode", "sslrootcert"}
                    or connection.get("host") != f"loom-postgres.{self.shared.platform_namespace}.svc"
                    or connection.get("port") != "5432" or connection.get("dbname") != raw["database_name"]
                    or connection.get("sslmode") != "verify-full" or not connection.get("password")
                    or not isinstance(username, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", username)
                    or not isinstance(ca_path, str)):
                raise ValueError
            # The public CA may be world-readable. It must be the same root
            # delivered to applications, not an unrelated ambient trust store.
            with Path(ca_path).open("rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise ValueError
                ca = stream.read(65537).decode("ascii")
            if ca.strip() != raw["ca_pem"].strip():
                raise ValueError
            return dsn, SharedApplicationCredentials(data_environment_id=self.shared.data_environment_id,
                ca_pem=raw["ca_pem"], database_name=raw["database_name"],
                secret_store_master_keys=raw["secret_store_master_keys"])
        except Exception:
            raise ValueError("invalid_application_runtime_material") from None
