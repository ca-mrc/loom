"""Read-only qualification for a private shared-development foundation.

Not an installer, capacity reservation, recovery/adoption path or CLI. The protected
caller must still qualify cloud quota and fresh credentials, and journal its fixed
create-only operations. Re-run live checks at that write boundary; a saved report
is not permission to write. No staging namespace, runtime or routing is changed.
"""
from __future__ import annotations

import copy
import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator
from scripts.ops.nebius_development_bootstrap import (
    DevelopmentBootstrapBinding,
    _namespace_document,
)
from scripts.ops.nebius_ingress_stage import _snapshot, _uid
from scripts.ops.nebius_management_capacity import _count, qualify_platform_capacity
from scripts.ops.nebius_management_prerequisites import inventory_resources
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_development_foundation import (
    RenderedDevelopmentFoundation,
    render_development_foundation,
)
from loom.nebius_environment_render import PlatformEnvelope
from loom.nebius_platform_render import canonical, digest, validate_environment
from loom_execution_capacity_collector.kubernetes import _quantity
from loom_service.environment_management.candidates import (
    GitHubCandidateCatalog,
    ProtectedPublication,
)

ROOT = Path(__file__).resolve().parents[2]
_NAMESPACE = "loom-dev"
_CONTROLLERS = (
    ("apps/v1", "deployments", "Deployment"), ("apps/v1", "statefulsets", "StatefulSet"),
    ("apps/v1", "replicasets", "ReplicaSet"), ("apps/v1", "daemonsets", "DaemonSet"),
    ("batch/v1", "jobs", "Job"), ("batch/v1", "cronjobs", "CronJob"),
)


class DevelopmentPreflightError(RuntimeError):
    """Closed diagnostic stage only; never expose credentials or inventory bodies."""


class PreparedDevelopmentSource(BaseModel):
    """Source identity carried inside the protected caller's verified bundle.

    Not an attestation by itself. The fixed gateway must authenticate the bundle
    containing these fields and the installer code before invoking live checks.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)
    source_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_archive_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


def prepare_development_source(source_sha: str) -> PreparedDevelopmentSource:
    """Run in the publishing checkout, never in the gateway's Git-less bundle."""
    from scripts.ops.nebius_candidate import source_archive_digest

    try:
        return PreparedDevelopmentSource(source_sha=source_sha, source_archive_sha256=source_archive_digest(source_sha))
    except Exception:
        raise DevelopmentPreflightError("development foundation preflight failed: source") from None


class DevelopmentPreflightSettings(BaseModel):
    """Protected caller's selection, never accepted from a personal app request."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    publication: ProtectedPublication
    source: PreparedDevelopmentSource
    registry_prefix: str = Field(pattern=r"^cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+$")
    kube_system_uid: UUID
    storage_class_uid: UUID
    storage_parameters: dict[str, str]

    @field_validator("kube_system_uid", "storage_class_uid")
    @classmethod
    def nonzero_uid(cls, value: UUID) -> UUID:
        if not value.int:
            raise ValueError("identity must be nonzero")
        return value


@dataclass(frozen=True, repr=False)
class DevelopmentPreflightResult:
    rendered: RenderedDevelopmentFoundation
    evidence: dict[str, Any]


def _pending_storage(claims: list[dict[str, Any]], controllers: list[dict[str, Any]],
                     planned: list[dict[str, Any]]) -> int:
    """Additional MiB, beyond provider usage, including other pending claims."""
    existing = {(row["metadata"]["namespace"], row["metadata"]["name"]): row for row in claims}
    if len(existing) != len(claims):
        raise ValueError()
    pending = 0
    for claim in claims:
        requested = _quantity(claim["spec"]["resources"]["requests"]["storage"], kind="storage")
        allocated = (_quantity(claim["status"]["capacity"]["storage"], kind="storage")
                     if claim.get("status", {}).get("phase") == "Bound" else 0)
        pending += max(0, requested - allocated)
    future: dict[tuple[str, str], int] = {}
    for row in [*controllers, *planned]:
        if row["kind"] != "StatefulSet":
            continue
        start = row["spec"].get("ordinals", {}).get("start", 0)
        if type(start) is not int or not 0 <= start <= 10000:
            raise ValueError()
        for template in row["spec"].get("volumeClaimTemplates", []):
            size = _quantity(template["spec"]["resources"]["requests"]["storage"], kind="storage")
            for ordinal in range(start, start + _count(row)):
                key = (row["metadata"]["namespace"], f'{template["metadata"]["name"]}-{row["metadata"]["name"]}-{ordinal}')
                if key not in existing:
                    future[key] = max(future.get(key, 0), size)
    return pending + sum(future.values())


class HTTPSDevelopmentPreflight(ManagementKubernetesTransport):
    """Fixed authenticated GETs; no ambient kubeconfig, write, redirect or retry."""

    error_type = DevelopmentPreflightError

    def __init__(self, *, settings: DevelopmentPreflightSettings, api_server: str,
                 ssl_context: ssl.SSLContext, token: str | None = None):
        self.settings = settings
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def _read(self, path: str) -> dict[str, Any] | None:
        return self._request("GET", path)

    def _cluster(self) -> None:
        row = self._read("/api/v1/namespaces/kube-system")
        if (row is None or row.get("apiVersion") != "v1" or row.get("kind") != "Namespace"
                or row["metadata"].get("uid") != str(self.settings.kube_system_uid)
                or row["metadata"].get("name") != "kube-system"
                or row["metadata"].get("deletionTimestamp") or row["metadata"].get("ownerReferences")):
            raise ValueError()

    def _fresh_identity(self) -> None:
        self._cluster()
        if self._read("/api/v1/namespaces/" + _NAMESPACE) is not None:
            raise ValueError()
        self._cluster()

    def _installation_identity(self, installation_id: UUID, config: dict[str, Any]) -> tuple[str, str] | None:
        """Coarse installation/policy check, never substitute for anchored journals."""
        self._cluster()
        row = self._read("/api/v1/namespaces/" + _NAMESPACE)
        self._cluster()
        if row is None:
            return None
        binding = DevelopmentBootstrapBinding(str(installation_id), str(self.settings.kube_system_uid),
            config["db_tls_secret_name"])
        snapshot = _snapshot(row)
        operation = snapshot["metadata"]["annotations"]["loom.nebius/development-bootstrap-operation"]
        label = snapshot["metadata"].get("labels", {}).pop("kubernetes.io/metadata.name", _NAMESPACE)
        spec = snapshot.pop("spec", {})
        if (label != _NAMESPACE or spec not in ({}, {"finalizers": ["kubernetes"]})
                or snapshot != _namespace_document(binding, operation)):
            raise ValueError()
        return _uid(row), operation

    def _inventory(self, api: str, resource: str, kind: str) -> list[dict[str, Any]]:
        def read(method: str, path: str) -> dict[str, Any] | None:
            if method != "GET":
                raise ValueError()
            return self._read(path)
        return inventory_resources(read, api, resource, kind, include_terminal_pods=True)

    def _storage_class(self, config: dict[str, Any]) -> None:
        row = self._read("/apis/storage.k8s.io/v1/storageclasses/" + config["storage_class"])
        if (row is None or row.get("apiVersion") != "storage.k8s.io/v1" or row.get("kind") != "StorageClass"
                or row["metadata"].get("name") != config["storage_class"]
                or row["metadata"].get("uid") != str(self.settings.storage_class_uid)
                or row["metadata"].get("deletionTimestamp") or row["metadata"].get("ownerReferences")
                or row.get("provisioner") != "compute.csi.nebius.com"
                or row.get("parameters", {}) != self.settings.storage_parameters
                or row.get("volumeBindingMode") not in {"Immediate", "WaitForFirstConsumer"}):
            raise ValueError()

    async def inspect(self, *, config: dict[str, Any], keyring: dict[str, Any], github_token: str,
                      http: httpx.AsyncClient) -> DevelopmentPreflightResult:
        """Bind actual publication/source and fresh live inventory, with no writes."""
        return await self._inspect(config=config, keyring=keyring, github_token=github_token, http=http,
            installation_id=None)

    async def inspect_installation(self, *, config: dict[str, Any], keyring: dict[str, Any], github_token: str,
                                   http: httpx.AsyncClient, installation_id: UUID) -> DevelopmentPreflightResult:
        """Requalify an anchored caller, before or after its namespace creation.

        This does not load recovery journals, adopt resources or grant writes.
        The calling installer must validate its independent anchor and every
        retained UID/configuration before proceeding with any phase.
        """
        if not isinstance(installation_id, UUID) or not installation_id.int:
            raise DevelopmentPreflightError("development installation identity invalid")
        return await self._inspect(config=config, keyring=keyring, github_token=github_token, http=http,
            installation_id=installation_id)

    async def _inspect(self, *, config: dict[str, Any], keyring: dict[str, Any], github_token: str,
                       http: httpx.AsyncClient, installation_id: UUID | None) -> DevelopmentPreflightResult:
        stage = "configuration"
        try:
            config, keyring = copy.deepcopy(config), copy.deepcopy(keyring)
            if (config.get("namespace") != _NAMESPACE or config.get("environment") != "development"
                    or config.get("schema_version") != "loom.nebius-platform.v1"
                    or config["kubernetes_api_server"].rstrip("/") != self.api_server.rstrip("/")):
                raise ValueError()
            validate_environment(config)
            stage = "publication"
            selection = self.settings.publication
            catalog = GitHubCandidateCatalog(http, token=github_token, publications=[selection],
                keyring=ImageAdmissionKeyring.from_json(canonical(keyring).decode()), registry_prefix=self.settings.registry_prefix)
            selected = await catalog.resolve(selection.candidate_id)
            stage = "source"
            source_digest = self.settings.source.source_archive_sha256
            if (self.settings.source.source_sha != selection.source_sha
                    or selected.candidate.get("source_archive_sha256") != source_digest):
                raise ValueError()
            stage = "render"
            rendered = render_development_foundation(config, selected.candidate, selected.profile, keyring, repo_root=ROOT)
            namespace_identity = None
            stage = "fresh_identity" if installation_id is None else "installation_identity"
            if installation_id is None:
                self._fresh_identity()
            else:
                namespace_identity = self._installation_identity(installation_id, config)
            stage = "storage_class"
            self._storage_class(config)
            stage = "inventory"
            nodes, pods = self._inventory("v1", "nodes", "Node"), self._inventory("v1", "pods", "Pod")
            controllers = [row for api, resource, kind in _CONTROLLERS for row in self._inventory(api, resource, kind)]
            claims = self._inventory("v1", "persistentvolumeclaims", "PersistentVolumeClaim")
            volumes = self._inventory("v1", "persistentvolumes", "PersistentVolume")
            hpas = self._inventory("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler")
            # The existing accountant does not model legacy RC templates. Do not
            # interpret unstarted unsupported controllers as free capacity.
            if self._inventory("v1", "replicationcontrollers", "ReplicationController"):
                raise ValueError()
            supported_owners = {(api, kind) for api, _, kind in _CONTROLLERS} | {("v1", "Node")}
            for row in [*pods, *controllers]:
                for owner in row["metadata"].get("ownerReferences", []):
                    if owner.get("controller") is True and (owner["apiVersion"], owner["kind"]) not in supported_owners:
                        # A visible child of an unobserved custom controller is
                        # not an orphan with a known future replica envelope.
                        raise ValueError()
            stage = "fresh_resources"
            if installation_id is None and (
                    any(row["metadata"]["namespace"] == _NAMESPACE for row in [*pods, *controllers, *claims, *hpas])
                    or any(row.get("spec", {}).get("claimRef", {}).get("namespace") == _NAMESPACE for row in volumes)):
                raise ValueError()
            # Even with no dev claimRef, Kubernetes can bind a new PVC to an
            # Available old PV instead of provisioning an empty disk. Leave such
            # disks untouched and require separate operator qualification.
            for volume in volumes:
                spec = volume["spec"]
                if spec.get("storageClassName") == config["storage_class"]:
                    reference = spec.get("claimRef", {})
                    if installation_id is not None and reference.get("namespace") == _NAMESPACE:
                        matches = [claim for claim in claims if claim["metadata"]["namespace"] == _NAMESPACE
                            and claim["metadata"]["name"] == reference.get("name")
                            and claim["metadata"].get("uid") == reference.get("uid")
                            and claim["metadata"].get("labels", {}).get("loom.nebius/development-installation") == str(installation_id)]
                        if len(matches) != 1 or volume["metadata"]["name"] != "pvc-" + reference["uid"]:
                            raise ValueError()
                        continue  # Installer separately pins and qualifies its exact PV/disk.
                    if (volume.get("status", {}).get("phase") != "Bound"
                            or not all(reference.get(key) for key in ("namespace", "name", "uid"))):
                        raise ValueError()
            stage = "autoscaling_envelope"
            by_key = {(row["kind"], row["metadata"]["namespace"], row["metadata"]["name"]): row for row in controllers}
            for hpa in hpas:
                target, maximum = hpa["spec"]["scaleTargetRef"], hpa["spec"]["maxReplicas"]
                if (target["apiVersion"] != "apps/v1" or target["kind"] not in {"Deployment", "StatefulSet", "ReplicaSet"}
                        or type(maximum) is not int or not 0 < maximum <= 10000):
                    raise ValueError()
                row = by_key[target["kind"], hpa["metadata"]["namespace"], target["name"]]
                replicas = row["spec"].get("replicas", 1)
                if type(replicas) is not int or not 0 <= replicas <= 10000:
                    raise ValueError()
                row["spec"]["replicas"] = max(replicas, maximum)
            stage = "capacity"
            planned = [row for docs in rendered.files.values() for row in docs
                       if row["kind"] in {"Deployment", "StatefulSet", "Job"}]
            accounting_pods = copy.deepcopy(pods)
            for pod in accounting_pods:
                if pod["metadata"].get("deletionTimestamp"):
                    # Deleting Pods still consume resources, but do not occupy
                    # their controller's future active replica/surge allowance.
                    # Charge them as independent roots in this sizing view only.
                    pod["metadata"].pop("ownerReferences", None)
            capacity = qualify_platform_capacity(nodes=nodes, pods=accounting_pods, controllers=controllers, planned=planned,
                reserve=PlatformEnvelope(0, 0, 0, 0), reserve_pods=0)
            stage = "storage_demand"
            pending_storage_mib = _pending_storage(claims, controllers, planned)
            # No promise about future owners yet: this is only the private
            # foundation's actual Pods, including migration and rolling surge.
            stage = "readback"
            self._storage_class(config)
            if installation_id is None:
                self._fresh_identity()
            elif self._installation_identity(installation_id, config) != namespace_identity:
                raise ValueError()
            evidence: dict[str, Any] = {
                "schema_version": "loom.nebius-development-preflight.v1",
                "status": "fresh_dev_preflight_only" if installation_id is None else "installation_dev_preflight_only",
                "namespace": _NAMESPACE, "kube_system_uid": str(self.settings.kube_system_uid),
                "source_sha": selection.source_sha, "source_archive_sha256": source_digest,
                "publication": selection.model_dump(mode="json"), "revision": rendered.revision,
                "input_digest": digest({"config": config, "keyring": keyring, "settings": self.settings.model_dump(mode="json")}),
                "storage_class_uid": str(self.settings.storage_class_uid),
                "database_storage_mib": rendered.platform_envelope.storage_mib, "capacity": capacity,
                "pending_storage_mib": pending_storage_mib,
                "unverified": ["installer_bundle_authority", "provider_storage_quota", "credential_provenance", "installation", "runtime_isolation",
                               "public_access", "shared_pool_activation", "owner_acceptance"],
            }
            if installation_id is not None:
                evidence.update(installation_id=str(installation_id),
                    namespace_uid=namespace_identity[0] if namespace_identity else None)
            return DevelopmentPreflightResult(rendered, evidence)
        except Exception:
            raise DevelopmentPreflightError("development foundation preflight failed: " + stage) from None
