"""Render personal frontend/API workloads without owning shared data/services.

Pure rendering is not admission. Management must qualify inputs, reserve names,
provision revocable credentials, admit the namespace on the shared side, and
enforce lifecycle/schema fencing before applying or declaring readiness.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.nebius_application_authority import (
    APPLICATION_INSTALLATION_LABEL,
    ApplicationNamespaceAuthorityV1,
    application_namespace_binding,
)
from loom.nebius_application_contract import (
    ApplicationRegistrationV1,
    ApplicationReleaseV1,
    SharedDevelopmentBindingV1,
)
from loom.nebius_application_credentials import application_credential_names
from loom.nebius_environment_contract import FoundationBinding
from loom.nebius_environment_render import PlatformEnvelope, _envelope
from loom.nebius_platform_render import (
    _deployment,
    _env,
    _mount_secret,
    _namespace,
    _network_policy,
    _obj,
    _peer,
    _secret_env,
    _service,
    digest,
)
from loom.nebius_pool_priority import PoolApplicationOriginV1, PoolSubmissionSourceV1


@dataclass(frozen=True)
class RenderedApplication:
    registration: ApplicationRegistrationV1
    files: dict[str, list[dict[str, Any]]]
    platform_envelope: PlatformEnvelope


def render_application(
    registration: ApplicationRegistrationV1, release: ApplicationReleaseV1,
    shared: SharedDevelopmentBindingV1, foundation: FoundationBinding,
    *, authority: ApplicationNamespaceAuthorityV1 | None = None,
) -> RenderedApplication:
    """Render only application-owned objects; retain the shared execution profile."""
    # Also reject unchecked model_copy/model_construct inputs at this boundary.
    row = ApplicationRegistrationV1.model_validate(registration.model_dump())
    release = ApplicationReleaseV1.model_validate(release.model_dump())
    shared = SharedDevelopmentBindingV1.model_validate(shared.model_dump())
    foundation = FoundationBinding.model_validate(foundation.model_dump())
    if foundation.namespace_authority is not None and authority is None:
        raise ValueError("legacy environment namespace authority cannot admit personal applications")
    if authority is not None:
        authority = ApplicationNamespaceAuthorityV1.model_validate(authority.model_dump())
        if (authority.cluster_id != shared.cluster_id or authority.data_environment_id != shared.data_environment_id
                or authority.shared_namespace != shared.platform_namespace):
            raise ValueError("application authority differs from shared binding")
    shared.validate_foundation(foundation)
    if (row.cluster_id != shared.cluster_id or row.data_environment_id != shared.data_environment_id
            or row.release_id != release.release_id
            or row.public_host != row.slug + "." + foundation.public_dns_zone):
        raise ValueError("application differs from protected shared bindings")
    if row.desired_state != "active" or release.schema_revision != shared.schema_revision:
        raise ValueError("only active, exact-schema-compatible applications may render")
    config = foundation.platform_config
    if row.public_host == config["public_host"]:
        raise ValueError("application hostname overlaps shared infrastructure")
    ns, data_ns = row.application_namespace, shared.platform_namespace
    if ns in {data_ns, config["execution_namespace"], config["execution_namespace"] + "-build", foundation.ingress_namespace}:
        raise ValueError("application namespace overlaps shared infrastructure")
    revision = digest({"application": row.model_dump(mode="json"), "release": release.model_dump(mode="json"),
                       "shared": shared.model_dump(mode="json"), "foundation": foundation.model_dump(mode="json")})
    namespace = _namespace(ns)
    namespace["metadata"]["labels"]["loom.nebius/data-environment-id"] = str(shared.data_environment_id)
    files: dict[str, list[dict[str, Any]]] = {"00-namespace.yaml": [namespace]}
    if authority is not None:
        namespace["metadata"]["labels"][APPLICATION_INSTALLATION_LABEL] = str(authority.installation_id)
        files["00-namespace.yaml"].append(application_namespace_binding(authority, ns))
    account = _obj("ServiceAccount", "loom-platform", ns)
    account["automountServiceAccountToken"] = False
    ingress_peer = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": foundation.ingress_namespace}},
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": foundation.ingress_controller_label}},
    }
    policies = [_network_policy("default-deny", ns, {}, [], [])]
    for name, app, port in (("public-api", "loom-service", 8090), ("public-web", "loom-web", 8080)):
        policies.append(_network_policy(name, ns, {"matchLabels": {"app": app}},
                                       [{"from": [ingress_peer], "ports": [{"protocol": "TCP", "port": port}]}]))
    outbound = [{"to": [_peer(data_ns, app)], "ports": [{"protocol": "TCP", "port": port}]}
                for app, port in (("loom-postgres", 5432), ("loom-control-plane", 8080), ("loom-llm-gateway", 9100))]
    outbound.append({
        "to": [_peer("kube-system", None) | {"podSelector": {"matchExpressions": [
            {"key": "k8s-app", "operator": "In", "values": ["kube-dns", "coredns"]},
        ]}}],
        "ports": [{"protocol": protocol, "port": 53} for protocol in ("UDP", "TCP")],
    })
    outbound.append({"to": [{"ipBlock": {"cidr": "0.0.0.0/0", "except": [
        "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "127.0.0.0/8", "169.254.0.0/16",
    ]}}], "ports": [{"protocol": "TCP", "port": 443}]})
    policies.append(_network_policy("application-egress", ns, {"matchLabels": {"app": "loom-service"}}, [], outbound))
    files["10-network.yaml"] = [account, *policies]
    env = _env({
        "LOOM_ENV": "development", "LOOM_NAMESPACE": ns,
        "LOOM_SVC_SERVICE_MODE": "api_only", "LOOM_SVC_BIND_HOST": "0.0.0.0", "LOOM_SVC_BIND_PORT": 8090,
        "LOOM_SVC_PUBLIC_BASE_URL": "https://" + row.public_host,
        "LOOM_SVC_AUTH_LOCAL_HTTP": "false",
        "LOOM_SVC_AUTH_SESSION_AUDIENCE_JSON": row.session_audience.model_dump_json(),
        "LOOM_SVC_POOL_SUBMISSION_SOURCE_JSON": PoolSubmissionSourceV1(
            data_environment_id=row.data_environment_id, kind="application",
            application=PoolApplicationOriginV1(application_id=row.application_id,
                incarnation=row.incarnation, deployment_generation=row.deployment_generation,
                release_id=release.release_id, source_digest=release.source_digest),
        ).model_dump_json(),
        "LOOM_SVC_CONTROL_PLANE_URL": f"http://loom-control-plane.{data_ns}.svc:8080",
        "LOOM_SVC_GATEWAY_URL": f"http://loom-llm-gateway.{data_ns}.svc:9100",
        "LOOM_SVC_MINIO_ENDPOINT": config["storage_endpoint"], "LOOM_SVC_MINIO_REGION": config["region"],
        "LOOM_SVC_ARTIFACTS_BUCKET": config["buckets"]["artifacts"],
        "LOOM_SVC_TRAJECTORIES_BUCKET": config["buckets"]["trajectories"],
        "LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON": shared.runtime_profile_json,
        "LOOM_SVC_TEAM_REGISTRATION_OPEN": "false",
    })
    credentials = application_credential_names(row)
    env += [
        _secret_env("LOOM_SVC_DB_URL", credentials["db"], "url"),
        _secret_env("LOOM_SVC_MINIO_ACCESS_KEY", credentials["storage"], "access-key"),
        _secret_env("LOOM_SVC_MINIO_SECRET_KEY", credentials["storage"], "secret-key"),
        _secret_env("LOOM_SECRET_STORE_MASTER_KEYS", credentials["auth"], "secret-store-master-keys"),
    ]
    api = _deployment("loom-service", ns, release.service_image_ref, 8090, "/api/v1/health", env, revision)
    _mount_secret(api["spec"]["template"]["spec"], "db-ca", credentials["db"], "/var/run/loom-db", ca_only=True)
    web = _deployment("loom-web", ns, release.web_image_ref, 8080, "/", _env({
        "LOOM_FRONTEND_ENVIRONMENT": "development", "LOOM_FRONTEND_ENVIRONMENT_LABEL": "Loom " + row.slug,
        "LOOM_FRONTEND_ROUTE_PATH": "", "LOOM_FRONTEND_API_BASE": "",
        "LOOM_FRONTEND_PUBLIC_ORIGIN": "https://" + row.public_host,
    }), revision, cpu="25m", memory="64Mi")
    web["spec"]["template"]["spec"]["securityContext"].update(runAsUser=101, runAsGroup=101, fsGroup=101)
    for deployment in (api, web):
        pod = deployment["spec"]["template"]
        pod["metadata"]["annotations"]["loom.nebius/application-source"] = release.source_digest
        for container in pod["spec"]["containers"]:
            for limits in ("requests", "limits"):
                container["resources"][limits]["ephemeral-storage"] = "256Mi"
    files["20-application.yaml"] = [api, web, _service("loom-service", ns, 8090), _service("loom-web", ns, 8080)]
    ingress = _obj("Ingress", "loom-web", ns, api="networking.k8s.io/v1")
    ingress["spec"] = {
        "ingressClassName": foundation.ingress_class_name,
        "tls": [{"hosts": [row.public_host]}],
        "rules": [{"host": row.public_host, "http": {"paths": [
            {"path": path, "pathType": "Prefix", "backend": {"service": {"name": name, "port": {"number": port}}}}
            for path, name, port in (("/api", "loom-service", 8090), ("/", "loom-web", 8080))
        ]}}],
    }
    files["30-public.yaml"] = [ingress]
    for group in files.values():
        for doc in group:
            doc["metadata"].setdefault("labels", {}).update({
                "loom.nebius/application-id": str(row.application_id),
                "loom.nebius/incarnation": str(row.incarnation),
            })
    return RenderedApplication(row, files, _envelope(files))
