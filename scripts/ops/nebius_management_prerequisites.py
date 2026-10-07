"""Live read-only prerequisites for the protected management installation."""
from __future__ import annotations

import asyncio
import json
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import UUID, uuid5

import httpx
from pydantic import BaseModel, ConfigDict, Field
from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_dns_publication import qualify_public_routes
from scripts.ops.nebius_ingress_operation import LiveIngressAPI, qualify_dns_target
from scripts.ops.nebius_management_capacity import _count, qualify_platform_capacity
from scripts.ops.nebius_management_cloud_scope import (
    ManagementCloudScope,
    _read,
    qualify_cloud_material,
)
from scripts.ops.nebius_management_install import ManagementInstallRequest, render_installation
from scripts.ops.nebius_management_live import backup_client
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport

from loom.execution_image_admission import ImageAdmissionKeyring
from loom.nebius_environment_contract import new_environment_registration
from loom.nebius_environment_render import PlatformEnvelope, render_environment
from loom_execution_capacity_collector.kubernetes import _quantity
from loom_service.environment_management.candidates import GitHubCandidateCatalog
from loom_service.environment_management.deployment import RenderedManagement


class ManagementPrerequisiteError(RuntimeError):
    """Private credential, artifact and inventory contents must not escape."""


def inventory_resources(request: Callable[[str, str], dict[str, Any] | None],
                        api: str, resource: str, kind: str, *,
                        include_terminal_pods: bool = False) -> list[dict[str, Any]]:
    """Fixed read-only collections; incomplete or unstable pages never qualify."""
    collections = {("v1", "nodes", "Node"), ("v1", "pods", "Pod"),
        ("v1", "persistentvolumes", "PersistentVolume"),
        ("v1", "replicationcontrollers", "ReplicationController"),
        ("v1", "persistentvolumeclaims", "PersistentVolumeClaim"),
        ("networking.k8s.io/v1", "ingresses", "Ingress"),
        ("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler"),
        *(("apps/v1", resource, kind) for resource, kind in (
            ("deployments", "Deployment"), ("statefulsets", "StatefulSet"),
            ("replicasets", "ReplicaSet"), ("daemonsets", "DaemonSet"))),
        ("batch/v1", "jobs", "Job"), ("batch/v1", "cronjobs", "CronJob"),
        *(("rbac.authorization.k8s.io/v1", resource, kind) for resource, kind in (
            ("roles", "Role"), ("clusterroles", "ClusterRole"),
            ("rolebindings", "RoleBinding"), ("clusterrolebindings", "ClusterRoleBinding")))}
    try:
        if (api, resource, kind) not in collections:
            raise ValueError()
        prefix = "/api/v1/" if api == "v1" else "/apis/" + api + "/"
        token, version = "", None
        seen: set[str] = set()
        result: list[dict[str, Any]] = []
        for _ in range(30):
            query = {"limit": "100", **({"continue": token} if token else {})}
            if kind == "Pod" and not include_terminal_pods:
                query["fieldSelector"] = "status.phase!=Succeeded,status.phase!=Failed"
            page = request("GET", prefix + resource + "?" + urlencode(query))
            if (page is None or page.get("apiVersion") != api or page.get("kind") != kind + "List"
                    or not isinstance(page.get("items"), list) or len(page["items"]) > 100
                    or not page.get("metadata", {}).get("resourceVersion")
                    or any(row.get("apiVersion", api) != api or row.get("kind", kind) != kind for row in page["items"])):
                raise ValueError()
            current = page["metadata"]["resourceVersion"]
            if version is not None and current != version:
                raise ValueError()
            version = current
            # Kubernetes typed lists omit TypeMeta from individual items.
            result.extend({"apiVersion": api, "kind": kind, **row} for row in page["items"])
            token = page["metadata"].get("continue", "")
            if not token:
                return result
            if not isinstance(token, str) or token in seen:
                raise ValueError()
            seen.add(token)
        raise ValueError()
    except Exception:
        raise ManagementPrerequisiteError("management resource inventory unqualified") from None


class ManagementPrerequisiteSettings(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    candidate_id: UUID
    cloud: ManagementCloudScope
    storage_class_uid: UUID
    storage_parameters: dict[str, str]
    storage_quota_name: str = Field(min_length=1, max_length=255)
    storage_quota_unit: str = Field(min_length=1, max_length=40)
    backup_quota_name: str = Field(min_length=1, max_length=255)
    backup_quota_unit: str = Field(min_length=1, max_length=40)


class HTTPSManagementPrerequisites(ManagementKubernetesTransport):
    """Private installer reads only; no generic apply or operator command input."""

    error_type = ManagementPrerequisiteError

    def __init__(self, *, settings: ManagementPrerequisiteSettings, ingress: LiveIngressAPI,
                 certificate_config: dict[str, Any], ingress_state: Path, operator_cloud_credentials: Path,
                 api_server: str, ssl_context: ssl.SSLContext, token: str | None = None):
        self.settings, self.ingress, self.certificate_config = settings, ingress, certificate_config
        self.diagnostic_stage: str | None = None
        self.ingress_state, self.operator_cloud_credentials = ingress_state, operator_cloud_credentials
        super().__init__(api_server=api_server, ssl_context=ssl_context, token=token)

    def foundation(self, request: ManagementInstallRequest) -> None:
        try:
            foundation = request.deployment.installation.foundation
            config = foundation.platform_config
            scope, binding = self.settings.cloud, self.ingress.binding
            if (scope.provisioning_project_id != foundation.provisioning_project_id
                    or scope.tenant_id != config["quota_parent_id"] or scope.region != config["region"]
                    or scope.provisioning_project_id == config["project_id"]
                    or binding.kube_system_uid != request.binding.kube_system_uid
                    or binding.namespace != foundation.ingress_namespace
                    or binding.child_domain != foundation.public_dns_zone
                    or binding.management_host != request.deployment.public_host
                    or self.api_server.rstrip("/") != config["kubernetes_api_server"].rstrip("/")):
                raise ValueError()
            current = self.ingress.foundation()
            live = current.platform_config
            # Cutover owns this flag; ordinary protected app publication can
            # advance independently of the initial ingress installation journal.
            config["shared_ingress_enabled"] = live["shared_ingress_enabled"] = False
            if (config != live or current.ingress_class_name != foundation.ingress_class_name
                    or current.ingress_controller_label != foundation.ingress_controller_label
                    or current.ingress_namespace != foundation.ingress_namespace
                    or current.public_dns_zone != foundation.public_dns_zone):
                raise ValueError()
        except Exception:
            raise ManagementPrerequisiteError("management foundation unqualified") from None

    def inventory(self, api: str, resource: str, kind: str) -> list[dict[str, Any]]:
        """Only fixed prerequisite collections, with complete stable pagination."""
        return inventory_resources(self._request, api, resource, kind)

    def public_route(self, request: ManagementInstallRequest) -> None:
        try:
            self.foundation(request)
            host = request.deployment.public_host
            for row in self.inventory("networking.k8s.io/v1", "ingresses", "Ingress"):
                claimed = [rule.get("host", "") for rule in row.get("spec", {}).get("rules", [])]
                if any(name == host or (name.startswith("*.") and host.partition(".")[2] == name[2:]) for name in claimed):
                    if (row["metadata"].get("namespace"), row["metadata"].get("name")) != (request.binding.namespace, "loom-management"):
                        raise ValueError()
            target = qualify_dns_target(api=self.ingress, certificate_config=self.certificate_config,
                                        state_dir=self.ingress_state)
            if target["management_host"] != host or target["child_domain"] != request.deployment.installation.foundation.public_dns_zone:
                raise ValueError()
            qualify_public_routes(target)
            self.foundation(request)
        except Exception:
            raise ManagementPrerequisiteError("management public route unqualified") from None

    def preflight(self, request: ManagementInstallRequest, rendered: RenderedManagement) -> None:
        try:
            self.diagnostic_stage = "render"
            if rendered != render_installation(request):
                raise ValueError()
            self.diagnostic_stage = "foundation"
            self.foundation(request)
            self.diagnostic_stage = "platform_capacity"
            missing_storage = self.platform_capacity(request, rendered)
            self.diagnostic_stage = "publication"
            asyncio.run(self.provider_and_publication(request, missing_storage))
            self.diagnostic_stage = "backup_access"
            with backup_client(request) as objects:
                # Nebius object-only bucket policies allow object listing but
                # not HeadBucket. IAM qualification above pins the bucket and
                # exact policy; prove the supplied credential's object access
                # without adding a bucket/project grant or writing a probe.
                response = objects.list_objects_v2(Bucket=request.deployment.backup_bucket, MaxKeys=1)
                if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
                    raise ValueError()
            self.diagnostic_stage = "public_route"
            self.public_route(request)
            self.diagnostic_stage = None
        except Exception:
            raise ManagementPrerequisiteError("management installation prerequisites unqualified") from None

    def platform_capacity(self, request: ManagementInstallRequest, rendered: RenderedManagement) -> int:
        """Check live scheduler fit and return additional persistent-disk demand."""
        installation = request.deployment.installation
        foundation, budget = installation.foundation, installation.platform_budget
        config = foundation.platform_config
        identity = uuid5(request.deployment.installation_id, "render-only-child-capacity")
        registration = new_environment_registration(foundation, environment_id=identity, incarnation=identity,
            owner_user_id=identity, owner_team_id=identity, slug="preflight")
        child = render_environment(registration, request.candidate, foundation, profile=request.profile,
                                   keyring=installation.keyring, repo_root=Path(__file__).resolve().parents[2])
        # This is the renderer's footprint, not a fixed owner roster. Reserve
        # Pod slots for every child the configured aggregate allowance can fit.
        bounds = vars(child.platform_envelope)
        if any(value <= 0 for value in bounds.values()):
            raise ValueError()
        children = min(getattr(budget, key) // value for key, value in bounds.items())
        child_slots = sum(_count(doc) for docs in child.files.values() for doc in docs
                          if doc["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"})
        self.diagnostic_stage = "resource_inventory"
        nodes, pods = self.inventory("v1", "nodes", "Node"), self.inventory("v1", "pods", "Pod")
        controllers = []
        for api, resource, kind in (("apps/v1", "deployments", "Deployment"), ("apps/v1", "statefulsets", "StatefulSet"),
            ("apps/v1", "replicasets", "ReplicaSet"), ("apps/v1", "daemonsets", "DaemonSet"),
            ("batch/v1", "jobs", "Job"), ("batch/v1", "cronjobs", "CronJob")):
            controllers.extend(self.inventory(api, resource, kind))
        # Account HPA maxima rather than promising capacity from a temporary
        # replica low-water mark. This is an in-memory sizing view, never a patch.
        by_key = {(row["kind"], row["metadata"]["namespace"], row["metadata"]["name"]): row for row in controllers}
        for hpa in self.inventory("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler"):
            target = hpa["spec"]["scaleTargetRef"]
            row = by_key[(target["kind"], hpa["metadata"]["namespace"], target["name"])]
            maximum = hpa["spec"]["maxReplicas"]
            if target["kind"] not in {"Deployment", "StatefulSet", "ReplicaSet"} or type(maximum) is not int or maximum <= 0:
                raise ValueError()
            row["spec"]["replicas"] = max(row["spec"].get("replicas", 1), maximum)
        planned = [row for docs in rendered.files.values() for row in docs
                   if row["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"}]
        self.diagnostic_stage = "platform_capacity"
        qualify_platform_capacity(nodes=nodes, pods=pods, controllers=controllers, planned=planned,
                                  reserve=PlatformEnvelope(**budget.model_dump()), reserve_pods=children * child_slots)
        self.diagnostic_stage = "storage_class"
        storage_class = self._request("GET", "/apis/storage.k8s.io/v1/storageclasses/" + config["storage_class"])
        if (storage_class is None or storage_class.get("kind") != "StorageClass"
                or storage_class["metadata"].get("uid") != str(self.settings.storage_class_uid)
                or storage_class["metadata"].get("name") != config["storage_class"]
                or storage_class["metadata"].get("deletionTimestamp")
                or storage_class.get("provisioner") != "compute.csi.nebius.com"
                or storage_class.get("parameters", {}) != self.settings.storage_parameters
                or storage_class.get("volumeBindingMode") not in {"Immediate", "WaitForFirstConsumer"}):
            raise ValueError()
        self.diagnostic_stage = "persistent_storage"
        claims = self.inventory("v1", "persistentvolumeclaims", "PersistentVolumeClaim")
        pending = 0
        existing = {(row["metadata"]["namespace"], row["metadata"]["name"]): row for row in claims}
        if len(existing) != len(claims):
            raise ValueError()
        for claim in claims:
            requested = _quantity(claim["spec"]["resources"]["requests"]["storage"], kind="storage")
            # Provider usage already includes bound capacity, but an expansion
            # request may not have reached the provider yet. Ignore neither.
            allocated = (_quantity(claim["status"]["capacity"]["storage"], kind="storage")
                         if claim.get("status", {}).get("phase") == "Bound" else 0)
            pending += max(0, requested - allocated)
        future: dict[tuple[str, str], int] = {}
        for row in [*controllers, *planned]:
            if row["kind"] != "StatefulSet":
                continue
            spec = row["spec"]
            start = spec.get("ordinals", {}).get("start", 0)
            if type(start) is not int or start < 0:
                raise ValueError()
            # Includes the in-memory HPA maximum and nonzero start ordinals.
            # Same live/planned identity shares one reservation on replay.
            for template in spec.get("volumeClaimTemplates", []):
                size = _quantity(template["spec"]["resources"]["requests"]["storage"], kind="storage")
                for ordinal in range(start, start + _count(row)):
                    key = (row["metadata"]["namespace"], f'{template["metadata"]["name"]}-{row["metadata"]["name"]}-{ordinal}')
                    if key not in existing:
                        future[key] = max(future.get(key, 0), size)
        return budget.storage_mib + pending + sum(future.values())

    async def provider_and_publication(self, request: ManagementInstallRequest, missing_storage_mib: int) -> None:
        from nebius.api.nebius.quotas import v1
        from nebius.sdk import SDK

        self.diagnostic_stage = "publication"
        async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=30) as http:
            await qualify_management_publication(request=request, candidate_id=self.settings.candidate_id, http=http)
        # The observing operator SDK never becomes runtime material. Its private
        # path is installation-owned and cannot come from a personal deployment.
        self.diagnostic_stage = "cloud_identity"
        before = private_state._private_read(self.operator_cloud_credentials, limit=1024 * 1024)
        sdk = SDK(credentials_file_name=str(self.operator_cloud_credentials), user_agent_prefix="loom-management-installer/1.0")
        try:
            async with asyncio.timeout(180):
                await qualify_cloud_material(sdk=sdk, scope=self.settings.cloud, material=request.material,
                                             bucket_name=request.deployment.backup_bucket,
                                             backup_bytes=request.deployment.postgres_storage_gi * 1024**3)
                self.diagnostic_stage = "provider_quota"
                quotas = v1.QuotaAllowanceServiceClient(sdk)
                for name, unit, service, required in (
                    (self.settings.storage_quota_name, self.settings.storage_quota_unit, "compute", missing_storage_mib * 1024**2),
                    (self.settings.backup_quota_name, self.settings.backup_quota_unit, "storage", request.deployment.postgres_storage_gi * 1024**3),
                ):
                    if unit not in {"byte", "bytes", "B"}:
                        raise ValueError()
                    # Nebius can return an entire cross-region inventory despite
                    # List.page_size, including ID-less default placeholders.
                    # Ask only for each exact regional allowance; never relax the
                    # shared IAM pager or fall back to an ambiguous list result.
                    row = await _read(quotas.get_by_name, v1.GetByNameRequest(
                        parent_id=self.settings.cloud.tenant_id, name=name, region=self.settings.cloud.region))
                    if (not row["metadata"].get("id")
                            or row["metadata"]["parent_id"] != self.settings.cloud.tenant_id
                            or row["metadata"]["name"] != name
                            or row["spec"]["region"] != self.settings.cloud.region
                            or row["status"]["state"] != "STATE_ACTIVE"
                            or row["status"]["usage_state"] not in {"USAGE_STATE_USED", "USAGE_STATE_NOT_USED"}
                            or row["status"]["service"] != service or row["status"]["unit"] != unit):
                        raise ValueError()
                    limit, used = (int(row[part].get(field, 0)) for part, field in (("spec", "limit"), ("status", "usage")))
                    if min(limit, used) < 0 or limit - used < required:
                        raise ValueError()
                if private_state._private_read(self.operator_cloud_credentials, limit=1024 * 1024) != before:
                    raise ValueError()
        finally:
            await sdk.close()


async def qualify_management_publication(*, request: ManagementInstallRequest, candidate_id: UUID,
                                         http: httpx.AsyncClient) -> None:
    try:
        installation = request.deployment.installation
        catalog = GitHubCandidateCatalog(http, token=request.material["loom-management-publications"]["token"],
            publications=list(installation.publications), registry_prefix=installation.registry_prefix,
            keyring=ImageAdmissionKeyring.from_json(json.dumps(installation.keyring)))
        selected = await catalog.resolve(candidate_id)
        if selected.candidate != request.candidate or selected.profile != request.profile:
            raise ValueError()
    except Exception:
        raise ManagementPrerequisiteError("management publication unqualified") from None
