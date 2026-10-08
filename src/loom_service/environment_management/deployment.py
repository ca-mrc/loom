"""Management-only manifests reusing the platform database and backup templates.

This pure renderer grants no cloud/Kubernetes authority and writes no Secrets.
Its caller verifies protected publication and provisions independently scoped
credentials before applying to an ownership-qualified management namespace.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.nebius_environment_contract import _hostname
from loom.nebius_environment_render import PlatformEnvelope, _envelope
from loom.nebius_platform_render import (
    _build_platform,
    _env,
    _mount_secret,
    _namespace,
    _network_policy,
    _obj,
    _peer,
    _secret_env,
    canonical,
    digest,
)
from loom_service.application_management.build_deployment import (
    SOURCE_CREDENTIALS_PATH,
    SOURCE_SPOOL_PATH,
    mount_application_source,
    render_application_build_reader,
    source_spool_mib,
)
from loom_service.environment_management.installation import ManagementInstallation
from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesConnection
from loom_service.pool_management.installation_render import mount_machine_token

_LABEL = "loom.nebius/management-installation"
_CONFIG_PATH = "/var/run/loom-management"
_KUBERNETES_PATH = "/var/run/loom-management-kubernetes"
_CLOUD_PATH = "/var/run/loom-management-cloud"
_APPLICATION_CLOUD_PATH = "/var/run/loom-applications-cloud"
_APPLICATION_SHARED_PATH = "/var/run/loom-applications-shared"


class ManagementDeployment(BaseModel):
    """Protected bootstrap input; fixed overhead is separate from child allowance."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["loom.nebius-management-deployment.v1"]
    installation_id: UUID
    namespace: str = Field(pattern=r"^loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?$", max_length=53)
    public_host: str
    postgres_storage_gi: int = Field(ge=10, le=1024, strict=True)
    backup_bucket: str = Field(pattern=r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$")
    installation: ManagementInstallation
    # The protected pool migration supplies this reference. Omission preserves
    # historical input digests and is not permission to discover a live catalog.
    pool_catalog_operation_id: UUID | None = Field(default=None, exclude_if=lambda value: value is None)
    application_builder_machine_id: UUID | None = Field(default=None, exclude_if=lambda value: value is None)
    # An independently installed manager need not share the controller's default
    # certificate. Omission retains historical input hashes and default routing.
    public_tls_secret_name: str | None = Field(default=None,
        pattern=r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$", max_length=63,
        exclude_if=lambda value: value is None)

    _public_host = field_validator("public_host")(_hostname)

    @model_validator(mode="after")
    def validate_bindings(self) -> ManagementDeployment:
        foundation = self.installation.foundation
        config = foundation.platform_config
        if self.installation_id.int == 0:
            raise ValueError("management installation requires a non-nil identity")
        if self.pool_catalog_operation_id is not None and self.pool_catalog_operation_id.int == 0:
            raise ValueError("pool catalog requires a non-nil operation identity")
        authority = foundation.namespace_authority
        if authority is not None and (authority.installation_id != self.installation_id or authority.namespace != self.namespace):
            raise ValueError("namespace authority differs from management installation")
        if self.namespace in {config["namespace"], config["execution_namespace"],
                              config["execution_namespace"] + "-build", foundation.ingress_namespace}:
            raise ValueError("management requires an independent namespace")
        if (self.public_host == config["public_host"]
                or self.public_host == foundation.public_dns_zone
                or self.public_host.endswith("." + foundation.public_dns_zone)):
            raise ValueError("management host must be separate from existing and child routes")
        if self.backup_bucket in config["buckets"].values():
            raise ValueError("management requires an independent backup bucket")
        application = self.installation.applications
        runtime = application.runtime if application is not None else self.installation.provider_runtime
        if runtime is None:
            raise ValueError("management deployment requires an explicit provider runtime")
        if application is not None and (
                application.authority.installation_id != self.installation_id
                or application.authority.namespace != self.namespace
                or application.runtime.database_connection_file != Path(_APPLICATION_SHARED_PATH + "/manager-dsn")
                or application.runtime.shared_credentials_file != Path(_APPLICATION_SHARED_PATH + "/shared.json")):
            raise ValueError("application management authority or mounted credentials differ")
        source = application.runtime.source_upload if application is not None else None
        build = application.runtime.build if application is not None else None
        if source is not None and (source.credentials_file != Path(SOURCE_CREDENTIALS_PATH + "/credentials.json")
                or source.spool_directory != Path(SOURCE_SPOOL_PATH)):
            raise ValueError("application source runtime requires fixed mounted paths")
        if (build is None) != (self.application_builder_machine_id is None):
            raise ValueError("application builder requires a dedicated machine binding")
        if build is not None and (self.application_builder_machine_id is None
                or self.application_builder_machine_id.int == 0 or self.pool_catalog_operation_id is None
                or build.management_origin.rstrip("/") != "https://" + self.public_host
                or build.bearer_token_file != Path("/var/run/loom-pool-token/token")):
            raise ValueError("application builder runtime differs from protected delivery")
        cloud_path = _APPLICATION_CLOUD_PATH if application is not None else _CLOUD_PATH
        if (runtime.kubernetes.endpoint != config["kubernetes_api_server"].rstrip("/")
                or runtime.kubernetes.ca_file != Path(_KUBERNETES_PATH + "/ca.crt")
                or (runtime.kubernetes.token_file != Path(_KUBERNETES_PATH + "/token")
                    if isinstance(runtime.kubernetes, ProjectedKubernetesConnection)
                    else runtime.kubernetes.credentials_file != Path(_KUBERNETES_PATH + "/credentials.json"))
                or runtime.cloud_credentials_file != Path(cloud_path + "/credentials.json")):
            if application is not None:
                raise ValueError("application management must use the bound cluster and mounted credentials")
            raise ValueError("management provider must use the bound cluster and mounted credentials")
        return self


@dataclass(frozen=True)
class RenderedManagement:
    config: dict[str, Any]
    files: dict[str, list[dict[str, Any]]]
    revision: str
    platform_envelope: PlatformEnvelope


def mount_pool_profiles(pod: dict[str, Any], *, operation_id: UUID) -> None:
    """Mount only the immutable catalog retained by the protected pool operation."""
    container, = pod["containers"]
    pod.setdefault("volumes", []).append({"name": "pool-profiles", "configMap": {
        "name": "loom-pool-profiles-" + operation_id.hex,
        "items": [{"key": "profiles.json", "path": "profiles.json"}],
    }})
    container.setdefault("volumeMounts", []).append({
        "name": "pool-profiles", "mountPath": "/var/run/loom-pool-profiles", "readOnly": True,
    })
    container["env"].append({"name": "LOOM_SVC_POOL_PROFILES_FILE", "value": "/var/run/loom-pool-profiles/profiles.json"})


def render_management(
    deployment: ManagementDeployment, *, candidate: dict[str, Any], profile: dict[str, Any], repo_root: Path,
) -> RenderedManagement:
    """No execution stack, public allocation or existing application mutations."""
    if candidate.get("source_ref") != "refs/heads/dev":
        raise ValueError("management requires a protected dev publication")
    images = candidate.get("images")
    if not isinstance(images, dict) or not isinstance(images.get("service"), dict):
        raise ValueError("management image must be an object in the candidate")
    image = images["service"].get("image_ref", "")
    if (not isinstance(image, str) or re.fullmatch(
            re.escape(deployment.installation.registry_prefix) + r"/[a-z0-9._/-]+@sha256:[0-9a-f]{64}", image,
    ) is None):
        raise ValueError("management image must be digest-pinned in the installation registry")
    foundation = deployment.installation.foundation
    ns = deployment.namespace
    # This is a fresh copy, not a modification of the standalone foundation.
    config = foundation.platform_config
    config.update(namespace=ns, execution_namespace=ns + "-execution",
                  public_host=deployment.public_host, postgres_storage_gi=deployment.postgres_storage_gi,
                  db_tls_secret_name="loom-management-db-tls", public_tls_bootstrap=False)
    config["buckets"]["backup"] = deployment.backup_bucket
    config.pop("task_image_builder", None)
    config.pop("task_identity_policy", None)
    config.pop("guest_execution_target", None)
    config.pop("emulated_auth_execution_target", None)
    # Image capability is not installed execution authority. Retain the approved
    # profile, but inherit neither standalone policy nor execution components.
    revision = digest({"deployment": deployment.model_dump(mode="json"), "candidate": candidate, "profile": profile})
    template_profile = {key: value for key, value in profile.items() if key not in {
        "guest_runtime", "guest_runtime_volume_mib", "guest_max_artifact_bytes", "supports_emulated_pkcs11",
    }}
    templates = _build_platform(config, candidate, template_profile, deployment.installation.keyring,
                                repo_root=repo_root, execution_enabled=False)
    # The shared bootstrap/backup commands consume only these fields. Do not
    # mount the old environment's task/model/storage configuration into management.
    runtime_config = {
        "namespace": ns, "region": config["region"], "storage_endpoint": config["storage_endpoint"],
        "buckets": {"backup": deployment.backup_bucket},
    }
    cm = _obj("ConfigMap", "loom-platform-config", ns)
    cm["data"] = {
        "environment.json": canonical(runtime_config).decode(),
    }
    application = deployment.installation.applications
    if application is None:
        cm["data"]["installation.json"] = deployment.installation.model_dump_json()
    account = _obj("ServiceAccount", "loom-platform", ns)
    account["automountServiceAccountToken"] = False
    ingress_peer = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": foundation.ingress_namespace}},
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": foundation.ingress_controller_label}},
    }
    files = {
        "00-namespaces.yaml": [_namespace(ns)],
        "10-config-network.yaml": [
            cm, account, _network_policy("default-deny-ingress", ns, {}, []),
            _network_policy("management-api", ns, {"matchLabels": {"app": "loom-service"}},
                            [{"from": [ingress_peer], "ports": [{"protocol": "TCP", "port": 8090}]}]),
            _network_policy("postgres-private", ns, {"matchLabels": {"app": "loom-postgres"}},
                            [{"from": [_peer(ns)], "ports": [{"protocol": "TCP", "port": 5432}]}]),
        ],
        "20-database.yaml": templates["20-database.yaml"],
        "30-migrate.yaml": templates["30-migrate.yaml"],
        "40-services.yaml": [doc for doc in templates["40-services.yaml"]
                             if doc["metadata"]["name"] == "loom-service"],
        "80-backup.yaml": templates["80-backup.yaml"],
    }
    management_config = cm
    if application is not None:
        management_config = _obj("ConfigMap", "loom-management-applications-" + revision[7:19], ns)
        management_config.update(immutable=True, data={"installation.json": deployment.installation.model_dump_json()})
        files["10-config-network.yaml"].append(management_config)
    service = next(doc for doc in files["40-services.yaml"] if doc["kind"] == "Deployment")
    pod = service["spec"]["template"]["spec"]
    container = pod["containers"][0]
    container["env"] = [
        *_env({"LOOM_ENV": "development", "LOOM_NAMESPACE": ns, "LOOM_SVC_SERVICE_MODE": "management",
               "LOOM_SVC_BIND_HOST": "0.0.0.0", "LOOM_SVC_BIND_PORT": 8090,
               "LOOM_SVC_PUBLIC_BASE_URL": "https://" + deployment.public_host,
               "LOOM_SVC_AUTH_LOCAL_HTTP": "false", "LOOM_SVC_TEAM_REGISTRATION_OPEN": "false",
               "LOOM_SVC_ADMIN_SECRET_FILE": "/var/run/loom/admin/secrets.toml",
               "LOOM_SVC_ENVIRONMENT_MANAGEMENT_CONFIG_FILE": _CONFIG_PATH + "/installation.json"}),
        _secret_env("LOOM_SVC_DB_URL", "loom-platform-db", "service-url"),
        _secret_env("LOOM_SECRET_STORE_MASTER_KEY", "loom-platform-auth", "secret-store-master-key"),
        _secret_env("LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN", "loom-management-publications", "token"),
    ]
    container["readinessProbe"]["httpGet"]["path"] = "/api/v1/health/ready"
    pod["volumes"].append({"name": "management-config", "configMap": {
        "name": management_config["metadata"]["name"], "items": [{"key": "installation.json", "path": "installation.json"}],
    }})
    container["volumeMounts"].append({"name": "management-config", "mountPath": _CONFIG_PATH, "readOnly": True})
    runtime = application.runtime if application is not None else deployment.installation.provider_runtime
    assert runtime is not None  # Validated by ManagementDeployment.
    if isinstance(runtime.kubernetes, ProjectedKubernetesConnection):
        account_name = "loom-application-provisioner" if application is not None else "loom-management-provisioner"
        provisioner = _obj("ServiceAccount", account_name, ns)
        provisioner["automountServiceAccountToken"] = False
        files["10-config-network.yaml"].append(provisioner)
        pod["serviceAccountName"] = account_name
        pod["volumes"].append({"name": "management-kubernetes", "projected": {
            "defaultMode": 0o440, "sources": [
                {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
                {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
            ],
        }})
        container["volumeMounts"].append({"name": "management-kubernetes", "mountPath": _KUBERNETES_PATH,
                                         "readOnly": True})
    else:
        _mount_secret(pod, "management-kubernetes", "loom-management-kubernetes", _KUBERNETES_PATH)
        pod["volumes"][-1]["secret"]["items"] = [
            {"key": name, "path": name} for name in ("ca.crt", "credentials.json")
        ]
    cloud_name = "loom-applications-cloud-" + revision[7:19] if application is not None else "loom-management-cloud"
    _mount_secret(pod, "management-cloud", cloud_name, _APPLICATION_CLOUD_PATH if application is not None else _CLOUD_PATH)
    pod["volumes"][-1]["secret"]["items"] = [{"key": "credentials.json", "path": "credentials.json"}]
    if application is not None:
        _mount_secret(pod, "application-shared", "loom-applications-shared-" + revision[7:19], _APPLICATION_SHARED_PATH)
        pod["volumes"][-1]["secret"]["items"] = [
            {"key": key, "path": key} for key in ("manager-dsn", "shared.json", "ca.crt")
        ]
    if deployment.pool_catalog_operation_id is not None:
        mount_pool_profiles(pod, operation_id=deployment.pool_catalog_operation_id)
        service["spec"]["strategy"] = {"type": "Recreate"}
    if application is not None and application.runtime.source_upload is not None:
        mount_application_source(pod, settings=application.runtime.source_upload,
            secret_name="loom-applications-source-" + revision[7:19], service_image=image)
        service["spec"]["strategy"] = {"type": "Recreate"}
    if application is not None and application.runtime.build is not None:
        assert deployment.application_builder_machine_id is not None
        mount_machine_token(pod, machine_id=deployment.application_builder_machine_id, service_image=image)
        files["10-config-network.yaml"].extend(render_application_build_reader(application.authority,
            namespace=foundation.platform_config["execution_namespace"] + "-build"))
    migration = files["30-migrate.yaml"][0]
    migration["metadata"]["name"] = "loom-management-migrate-" + revision.removeprefix("sha256:")[:12]
    migration_pod = migration["spec"]["template"]["spec"]
    migration_pod.pop("initContainers", None)
    migration_pod["volumes"] = [v for v in migration_pod["volumes"] if v["name"] in {"platform-config", "db-ca"}]
    migrate = migration_pod["containers"][0]
    migrate["command"] = ["python", "-m", "loom.nebius_platform_bootstrap", "management-database"]
    migrate["env"] = [
        {"name": "LOOM_PLATFORM_CONFIG", "value": "/var/run/loom-platform/environment.json"},
        _secret_env("LOOM_DB_URL", "loom-platform-db", "admin-url"),
        _secret_env("LOOM_DB_SERVICE_PASSWORD", "loom-platform-db", "service-password"),
    ]
    migrate["volumeMounts"] = [v for v in migrate["volumeMounts"] if v["name"] in {"platform-config", "db-ca"}]
    ingress = _obj("Ingress", "loom-management", ns, api="networking.k8s.io/v1")
    ingress["spec"] = {
        "ingressClassName": foundation.ingress_class_name,
        "tls": [{"hosts": [deployment.public_host]}],
        "rules": [{"host": deployment.public_host, "http": {"paths": [{
            "path": "/", "pathType": "Prefix", "backend": {"service": {"name": "loom-service", "port": {"number": 8090}}},
        }]}}],
    }
    if deployment.public_tls_secret_name is not None:
        ingress["spec"]["tls"][0]["secretName"] = deployment.public_tls_secret_name
    files["70-public.yaml"] = [ingress]
    # Include rollout, migration, backup scratch and credential preparation in
    # the fixed overhead. This envelope is not an automatic platform resize.
    for docs in files.values():
        for doc in docs:
            doc["metadata"].setdefault("labels", {})[_LABEL] = str(deployment.installation_id)
            if doc["kind"] not in {"Deployment", "StatefulSet", "Job", "CronJob"}:
                continue
            spec = doc["spec"]
            if doc["kind"] == "CronJob":
                spec = spec["jobTemplate"]["spec"]
            template = spec["template"]
            template["metadata"].setdefault("labels", {})[_LABEL] = str(deployment.installation_id)
            template["metadata"]["annotations"]["loom.nebius/configuration-revision"] = revision
            if doc["kind"] in {"Job", "CronJob"}:
                for volume in template["spec"].get("volumes", []):
                    if volume.get("configMap", {}).get("name") == "loom-platform-config":
                        volume["configMap"]["items"] = [{"key": "environment.json", "path": "environment.json"}]
            size = deployment.postgres_storage_gi * 1024 if doc["kind"] == "CronJob" else 256
            for c in template["spec"].get("initContainers", []) + template["spec"]["containers"]:
                allocated = size
                if (doc is service and c is container and application is not None
                        and application.runtime.source_upload is not None):
                    allocated += source_spool_mib(application.runtime.source_upload)
                c["resources"]["requests"]["ephemeral-storage"] = f"{allocated}Mi"
                c["resources"]["limits"]["ephemeral-storage"] = f"{allocated}Mi"
    return RenderedManagement(runtime_config, files, revision, _envelope(files))
