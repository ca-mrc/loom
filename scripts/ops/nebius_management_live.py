"""Connect management installation phases to fixed HTTPS Kubernetes adapters.

The protected entrypoint supplies separately qualified publication, IAM, physical
capacity and installed ingress checks. Those are mandatory dependencies, not
caller-selected flags. This module is not an operator CLI or deployment grant.
"""
from __future__ import annotations

import json
import ssl
import tomllib
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_authority_probe import HTTPSManagementAuthorityProbe
from scripts.ops.nebius_management_authority_stage import (
    HTTPSManagementAuthorityAPI,
    management_authority_ready,
)
from scripts.ops.nebius_management_bootstrap import HTTPSBootstrapAPI
from scripts.ops.nebius_management_evidence import HTTPSManagementEvidenceAPI
from scripts.ops.nebius_management_install import (
    _PHASES,
    ManagementInstallError,
    ManagementInstallRequest,
    render_installation,
)
from scripts.ops.nebius_management_material import ManagementBinding, _digest
from scripts.ops.nebius_management_proofs import ManagementPublicProbe, verify_backup_object
from scripts.ops.nebius_management_stage import (
    HTTPSManagementStageAPI,
    _documents,
    _validate_record,
)
from scripts.ops.nebius_management_supplied import HTTPSSuppliedMaterialAPI

from loom_service.environment_management.deployment import RenderedManagement


class ManagementPrerequisites(Protocol):
    def preflight(self, request: ManagementInstallRequest, rendered: RenderedManagement) -> None: ...
    def public_route(self, request: ManagementInstallRequest) -> None: ...


@contextmanager
def backup_client(request: ManagementInstallRequest) -> Iterator[Any]:
    """Only explicit backup credentials, bound HTTPS origin and no write retry."""
    import boto3
    from botocore.config import Config

    config = request.deployment.installation.foundation.platform_config
    endpoint = urlsplit(config["storage_endpoint"])
    if endpoint.scheme != "https" or not endpoint.hostname or endpoint.username or endpoint.password:
        raise ManagementInstallError("management backup endpoint unqualified")
    credentials = request.material["loom-platform-storage"]
    client = boto3.client("s3", endpoint_url=config["storage_endpoint"], region_name=config["region"],
        aws_access_key_id=credentials["backup-access-key"], aws_secret_access_key=credentials["backup-secret-key"],
        config=Config(retries={"total_max_attempts": 1, "mode": "standard"}, proxies={},
                      connect_timeout=10, read_timeout=30, s3={"addressing_style": "path"}))

    def exact_endpoint(request: Any, **_kwargs: Any) -> None:
        url = request.url.decode() if isinstance(request.url, bytes) else request.url
        target = urlsplit(url)
        if (target.scheme, target.netloc) != (endpoint.scheme, endpoint.netloc):
            raise ManagementInstallError("backup request left the qualified endpoint")

    try:
        client.meta.events.register_first("before-send.s3", exact_endpoint)
        yield client
    finally:
        client.close()


class HTTPSManagementInstallationAPI:
    _public_runtime: Literal['legacy', 'applications'] = 'legacy'

    def __init__(self, *, request: ManagementInstallRequest, api_server: str, ssl_context: ssl.SSLContext,
                 runtime_ca_pem: str | None, checks: ManagementPrerequisites, token: str | None = None):
        self.request, self.rendered, self.checks = request, render_installation(request), checks
        self.diagnostic_stage: str | None = None
        self.api_server, self.ssl_context, self.token = api_server, ssl_context, token
        # Never reuse operator mTLS for a supposed runtime-subject probe. Build a
        # clean trust-only context; the only authentication is its short-lived SA token.
        self.runtime_trust = ssl.create_default_context(cadata=runtime_ca_pem)

    def bootstrap_api(self) -> HTTPSBootstrapAPI:
        return HTTPSBootstrapAPI(binding=self.request.binding, api_server=self.api_server,
                                 ssl_context=self.ssl_context, token=self.token)

    def _binding(self, binding: ManagementBinding) -> None:
        expected = self.request.binding
        if (binding.installation_id, binding.namespace, binding.kube_system_uid) != (
            expected.installation_id, expected.namespace, expected.kube_system_uid,
        ):
            raise ManagementInstallError("management live adapter binding differs")

    def preflight(self, request: ManagementInstallRequest, rendered: RenderedManagement) -> None:
        try:
            self.diagnostic_stage = "render"
            if request != self.request or rendered != self.rendered:
                raise ValueError()
            self.diagnostic_stage = "cluster_identity"
            with self.bootstrap_api() as api:
                api.verify_cluster(request.binding)
            self.diagnostic_stage = "prerequisites"
            self.checks.preflight(request, rendered)
            self.diagnostic_stage = None
        except Exception:
            raise ManagementInstallError("management live prerequisites unavailable") from None

    def resources(self, binding: ManagementBinding, phase: str) -> HTTPSManagementStageAPI:
        self._binding(binding)
        if phase == "authority":
            authority = self.request.deployment.installation.foundation.namespace_authority
            assert authority is not None
            return HTTPSManagementAuthorityAPI(authority=authority, binding=binding, api_server=self.api_server,
                                               ssl_context=self.ssl_context, token=self.token)
        if phase == "supplied":
            return HTTPSSuppliedMaterialAPI(material=self.request.material, binding=binding, api_server=self.api_server,
                                           ssl_context=self.ssl_context, token=self.token)
        filename = _PHASES.get(phase)
        if filename is None:
            raise ManagementInstallError("resource outside connected management installation")
        return HTTPSManagementStageAPI(binding=binding, rendered=self.rendered, phase=filename, api_server=self.api_server,
                                       ssl_context=self.ssl_context, token=self.token)

    def _recorded(self, binding: ManagementBinding, phase: str, state_dir: Path,
                  *, kind: str, name: str) -> dict[str, Any]:
        """Read-only stage identity/configuration check; cannot resume creation."""
        filename = _PHASES[phase]
        assert filename is not None
        if not state_dir.is_dir() or state_dir.is_symlink():
            raise ManagementInstallError("management recorded phase unavailable")
        identity = {"schema": "loom.nebius-management-stage.v1", "binding": asdict(binding),
                    "revision": self.rendered.revision, "phase": filename}
        with private_state._locked_state(state_dir):
            record = json.loads(private_state._private_read(state_dir / "stage.json", limit=4 * 1024 * 1024))
            _validate_record(record, identity, _documents(self.rendered, filename, binding))
            item = record["resources"][kind + ":" + binding.namespace + ":" + name]
            if item["status"] != "created":
                raise ManagementInstallError("management recorded resource incomplete")
            with self.resources(binding, phase) as api:
                api.verify_identity(binding)
                value = api.get_resource(item["desired"])
                if value is None or _uid(value) != item["uid"] or _snapshot(value) != item["observed"]:
                    raise ManagementInstallError("management recorded resource changed")
                return value

    def qualify_authority(self, binding: ManagementBinding, state_dir: Path) -> None:
        try:
            self._binding(binding)
            authority = self.request.deployment.installation.foundation.namespace_authority
            assert authority is not None
            with self.resources(binding, "authority") as api:
                if not management_authority_ready(authority=authority, binding=binding, api=api, state_dir=state_dir):
                    raise ManagementInstallError("management authority propagation pending")
            account = self._recorded(binding, "config", state_dir.parent / "config",
                                     kind="ServiceAccount", name="loom-management-provisioner")
            with HTTPSManagementEvidenceAPI(binding=binding, rendered=self.rendered, api_server=self.api_server,
                                            ssl_context=self.ssl_context, token=self.token) as evidence:
                runtime_token = evidence.runtime_token(service_account_uid=_uid(account))
            with HTTPSManagementAuthorityProbe(authority=authority, service_account_uid=_uid(account),
                                               api_server=self.api_server, ssl_context=self.runtime_trust,
                                               token=runtime_token) as probe:
                if not probe.qualify():
                    raise ManagementInstallError("management authority propagation pending")
        except ManagementInstallError:
            raise
        except Exception:
            raise ManagementInstallError("management runtime authority unavailable") from None

    def verify_backup(self, binding: ManagementBinding, rendered: RenderedManagement, job_uid: str) -> dict[str, Any]:
        try:
            self._binding(binding)
            if rendered != self.rendered:
                raise ValueError()
            with HTTPSManagementEvidenceAPI(binding=binding, rendered=rendered, api_server=self.api_server,
                                            ssl_context=self.ssl_context, token=self.token) as evidence:
                report = evidence.backup_report(job_uid=job_uid)
            deployment = self.request.deployment
            with backup_client(self.request) as client:
                return verify_backup_object(client=client, bucket=deployment.backup_bucket, namespace=binding.namespace,
                    job_uid=job_uid, report=report, max_bytes=deployment.postgres_storage_gi * 1024**3)
        except ManagementInstallError:
            raise
        except Exception:
            raise ManagementInstallError("management backup qualification unavailable") from None

    def verify_public(self, binding: ManagementBinding, rendered: RenderedManagement, material_dir: Path) -> None:
        try:
            self._binding(binding)
            if rendered != self.rendered:
                raise ValueError()
            state = material_dir.parent.parent
            self._recorded(binding, "public", state / "public", kind="Ingress", name="loom-management")
            for kind in ("Service", "Deployment"):
                self._recorded(binding, "service", state / "service", kind=kind, name="loom-service")
            self.checks.public_route(self.request)
            record = json.loads(private_state._private_read(material_dir / "material.json", limit=1024 * 1024))
            initialized = json.loads(private_state._private_read(material_dir / "initialized.json"))
            identity = {"schema": "loom.nebius-management-material.v1", "binding": asdict(binding)}
            if (any(record.get(key) != value for key, value in identity.items()) or record.get("status") != "delivered"
                    or record["material_sha256"] != _digest(record["material"])
                    or initialized != {**identity, "operation_id": record["operation_id"],
                                       "material_sha256": record["material_sha256"]}):
                raise ValueError()
            token = tomllib.loads(record["material"]["loom-admin-secret"]["secrets.toml"])["admin"]["token"]
            with ManagementPublicProbe(host=self.request.deployment.public_host, runtime=self._public_runtime) as probe:
                probe.verify(admin_token=token)
        except ManagementInstallError:
            raise
        except Exception:
            raise ManagementInstallError("management public qualification unavailable") from None
