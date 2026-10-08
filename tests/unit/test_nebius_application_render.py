"""Personal applications reference shared development, never own another stack."""

from __future__ import annotations

import copy
import json
from uuid import UUID, uuid4

import pytest

from loom_service.config import LoomServiceSettings
from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs
from tests.unit.test_service_execution_materialization import _profile

DATA_ID = UUID("20000000-0000-4000-8000-000000000009")


def inputs(platform_inputs, slug="alice"):
    from loom.nebius_application_contract import (
        ApplicationReleaseV1,
        SharedDevelopmentBindingV1,
        new_application_registration,
    )

    config = copy.deepcopy(platform_inputs[0])
    config["environment"] = "development"
    foundation = foundation_from(config)
    shared = SharedDevelopmentBindingV1(
        data_environment_id=DATA_ID, cluster_id=config["cluster_id"],
        platform_namespace=config["namespace"], schema_revision="0159",
        runtime_profile_json=_profile().model_dump_json(),
    )
    release = ApplicationReleaseV1(
        release_id=uuid4(), source_digest="sha256:" + "d" * 64, schema_revision="0159",
        service_image_ref="cr.eu-north1.nebius.cloud/test/personal-api@sha256:" + "e" * 64,
        web_image_ref="cr.eu-north1.nebius.cloud/test/personal-web@sha256:" + "f" * 64,
    )
    registration = new_application_registration(
        foundation, shared, application_id=uuid4(), incarnation=uuid4(),
        owner_user_id=uuid4(), owner_team_id=uuid4(), slug=slug, release_id=release.release_id,
    )
    return registration, release, shared, foundation


def docs(result):
    return [doc for group in result.files.values() for doc in group]


def named(result, kind, name):
    return next(doc for doc in docs(result) if doc["kind"] == kind and doc["metadata"]["name"] == name)


def test_four_plus_fifth_application_own_only_their_web_and_api(platform_inputs):
    from loom.nebius_application_render import render_application

    results = []
    for slug in ("alice", "bob", "carol", "dave", "eve"):
        registration, release, shared, foundation = inputs(platform_inputs, slug)
        before = [value.model_dump_json() for value in (registration, release, shared, foundation)]
        result = render_application(registration, release, shared, foundation)
        results.append(result)
        assert [value.model_dump_json() for value in (registration, release, shared, foundation)] == before
        assert result.platform_envelope.storage_mib == 0
        assert result.platform_envelope.cpu_millis > 0
        assert result.platform_envelope.ephemeral_storage_mib > 0
        assert {doc["metadata"]["name"] for doc in docs(result) if doc["kind"] == "Namespace"} == {"loom-dev-" + slug}
        assert {doc["metadata"]["name"] for doc in docs(result) if doc["kind"] == "Deployment"} == {"loom-web", "loom-service"}
        assert not {"PersistentVolumeClaim", "StatefulSet", "Job", "CronJob", "Secret", "ClusterRole", "ClusterRoleBinding"} & {
            doc["kind"] for doc in docs(result)
        }
        for doc in docs(result):
            if doc["kind"] != "Namespace":
                assert doc["metadata"]["namespace"] == registration.application_namespace
            assert doc["metadata"]["labels"]["loom.nebius/application-id"] == str(registration.application_id)
            if doc["kind"] == "Deployment":
                pod = doc["spec"]["template"]["spec"]
                assert pod["automountServiceAccountToken"] is False
                assert not pod.get("initContainers")
                assert not any("persistentVolumeClaim" in volume for volume in pod.get("volumes", []))
        ingress = named(result, "Ingress", "loom-web")
        assert ingress["spec"]["tls"] == [{"hosts": [slug + ".dev.example.com"]}]
        assert ingress["spec"]["rules"][0]["host"] == slug + ".dev.example.com"
    assert len({result.registration.application_namespace for result in results}) == 5
    assert {result.registration.data_environment_id for result in results} == {DATA_ID}


def test_app_images_and_schema_are_separate_from_shared_executor_profile(platform_inputs):
    from loom.nebius_application_render import render_application

    registration, release, shared, foundation = inputs(platform_inputs)
    result = render_application(registration, release, shared, foundation)
    api = named(result, "Deployment", "loom-service")["spec"]["template"]["spec"]["containers"][0]
    web = named(result, "Deployment", "loom-web")["spec"]["template"]["spec"]["containers"][0]
    assert api["image"] == release.service_image_ref
    assert web["image"] == release.web_image_ref
    env = {item["name"]: item["value"] for item in api["env"] if "value" in item}
    assert env["LOOM_SVC_SERVICE_MODE"] == "api_only"
    assert env["LOOM_ENV"] == "development"
    assert json.loads(env["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"]) == json.loads(shared.runtime_profile_json)
    assert json.loads(shared.runtime_profile_json)["candidate_sha"] == "1" * 40
    assert json.loads(shared.runtime_profile_json)["task_image_ref"] != api["image"]
    expected_namespace = foundation.platform_config["namespace"]
    assert env["LOOM_SVC_CONTROL_PLANE_URL"] == f"http://loom-control-plane.{expected_namespace}.svc:8080"
    assert env["LOOM_SVC_GATEWAY_URL"] == f"http://loom-llm-gateway.{expected_namespace}.svc:9100"
    for purpose in ("artifacts", "trajectories"):
        assert env["LOOM_SVC_" + purpose.upper() + "_BUCKET"] == foundation.platform_config["buckets"][purpose]
    references = {item["name"]: item["valueFrom"]["secretKeyRef"] for item in api["env"] if "valueFrom" in item}
    assert set(references) == {"LOOM_SVC_DB_URL", "LOOM_SVC_MINIO_ACCESS_KEY", "LOOM_SVC_MINIO_SECRET_KEY", "LOOM_SECRET_STORE_MASTER_KEYS"}
    assert references["LOOM_SECRET_STORE_MASTER_KEYS"]["name"] == f"loom-application-auth-{registration.incarnation.hex}-g1"
    settings = LoomServiceSettings(
        _env_file=None, **{key.removeprefix("LOOM_SVC_").lower(): value for key, value in env.items() if key.startswith("LOOM_SVC_")},
        db_url="postgresql+psycopg://personal:placeholder@localhost/loom",
        minio_access_key="placeholder", minio_secret_key="placeholder",
    )
    assert settings.session_audience.application_id == registration.application_id
    assert settings.session_audience.access_generation == registration.access_generation
    assert settings.session_audience.origin == "https://alice.dev.example.com"


def test_old_deployment_cannot_follow_new_access_generation_credentials(platform_inputs):
    from loom.nebius_application_render import render_application

    row, release, shared, foundation = inputs(platform_inputs)
    original = render_application(row, release, shared, foundation)
    original_files = copy.deepcopy(original.files)
    updated = render_application(row.model_copy(update={"deployment_generation": 2, "access_generation": 2}),
                                 release, shared, foundation)
    another = render_application(row.model_copy(update={"incarnation": uuid4()}), release, shared, foundation)
    observed = []
    for rendered, generation in ((original, 1), (updated, 2), (another, 1)):
        pod = named(rendered, "Deployment", "loom-service")["spec"]["template"]["spec"]
        refs = {env["name"]: env["valueFrom"]["secretKeyRef"]["name"] for env in pod["containers"][0]["env"] if "valueFrom" in env}
        suffix = f"-{rendered.registration.incarnation.hex}-g{generation}"
        assert refs == {
            "LOOM_SVC_DB_URL": "loom-application-db" + suffix,
            "LOOM_SVC_MINIO_ACCESS_KEY": "loom-application-storage" + suffix,
            "LOOM_SVC_MINIO_SECRET_KEY": "loom-application-storage" + suffix,
            "LOOM_SECRET_STORE_MASTER_KEYS": "loom-application-auth" + suffix,
        }
        ca = next(volume for volume in pod["volumes"] if volume["name"] == "db-ca")
        assert ca["secret"]["secretName"] == refs["LOOM_SVC_DB_URL"]
        observed.append(set(refs.values()))
    assert not observed[0] & observed[1] and not observed[0] & observed[2]
    assert original.files == original_files


def test_material_generation_must_fit_the_operation_database_identity(platform_inputs):
    from loom.nebius_application_render import render_application

    row, release, shared, foundation = inputs(platform_inputs)
    with pytest.raises(ValueError, match="generation"):
        render_application(row.model_copy(update={"access_generation": 2**63}), release, shared, foundation)


@pytest.mark.parametrize("change", ["schema", "cluster", "data-id", "shared-namespace", "host", "suspended", "release", "staging", "production"])
def test_renderer_rejects_cross_binding_and_inactive_inputs(platform_inputs, change):
    from loom.nebius_application_render import render_application

    row, release, shared, foundation = inputs(platform_inputs)
    if change == "schema":
        release = release.model_copy(update={"schema_revision": "0160"})
    elif change == "cluster":
        row = row.model_copy(update={"cluster_id": "another-cluster"})
    elif change == "data-id":
        row = row.model_copy(update={"data_environment_id": uuid4()})
    elif change == "shared-namespace":
        shared = shared.model_copy(update={"platform_namespace": "loom-prod"})
    elif change == "host":
        row = row.model_copy(update={"public_host": "alice.prod.example.com"})
    elif change == "suspended":
        row = row.model_copy(update={"desired_state": "suspended"})
    elif change == "release":
        row = row.model_copy(update={"release_id": uuid4()})
    else:
        config = foundation.platform_config
        config["environment"] = change
        # The personal renderer must reject non-development foundations even
        # when an input bypasses normal validation through model_copy.
        foundation = foundation.model_copy(update={"platform_config_json": json.dumps(config)})
    with pytest.raises(ValueError):
        render_application(row, release, shared, foundation)


def test_personal_application_rejects_valid_staging_foundation(platform_inputs):
    from loom.nebius_application_render import render_application

    row, release, shared, foundation = inputs(platform_inputs)
    staging = foundation_from({**foundation.platform_config, "environment": "staging"})
    with pytest.raises(ValueError, match="shared development foundation"):
        render_application(row, release, shared, staging)


def test_renderer_rejects_shared_foundation_hostname_collision(platform_inputs):
    from loom.nebius_application_render import render_application
    from loom.nebius_environment_contract import FoundationBinding

    row, release, shared, foundation = inputs(platform_inputs)
    config = foundation.platform_config
    config["public_host"] = "alice.dev.example.com"
    foundation = FoundationBinding.model_validate(
        foundation.model_dump() | {"platform_config_json": json.dumps(config)}
    )
    with pytest.raises(ValueError, match="hostname overlaps shared infrastructure"):
        render_application(row, release, shared, foundation)


def test_renderer_rejects_legacy_environment_namespace_authority(platform_inputs):
    from loom.nebius_application_render import render_application
    from loom.nebius_environment_contract import FoundationBinding

    row, release, shared, foundation = inputs(platform_inputs)
    foundation = FoundationBinding.model_validate(foundation.model_dump() | {
        "namespace_authority": {"installation_id": str(uuid4()), "namespace": "loom-nebius-management"},
    })
    with pytest.raises(ValueError, match="legacy environment namespace authority"):
        render_application(row, release, shared, foundation)


def test_renderer_revalidates_unchecked_foundation_inputs(platform_inputs):
    from loom.nebius_application_render import render_application

    row, release, shared, foundation = inputs(platform_inputs)
    foundation = foundation.model_copy(update={"ingress_class_name": "bad/name"})
    with pytest.raises(ValueError):
        render_application(row, release, shared, foundation)


@pytest.mark.parametrize("slug", ["dev", "staging", "prod", "shared", "Alice", "a/b", "a" * 55])
def test_application_namespace_cannot_claim_shared_or_invalid_names(platform_inputs, slug):
    # Positive control means import/setup failure cannot satisfy the assertion.
    inputs(platform_inputs)
    with pytest.raises(ValueError):
        inputs(platform_inputs, slug)


def test_contracts_reject_mutable_images_nil_ids_and_legacy_fields(platform_inputs):
    row, release, shared, _foundation = inputs(platform_inputs)
    for model, field, value in (
        (row, "application_id", str(UUID(int=0))),
        (row, "application_namespace", "loom-dev-bob"),
        (row, "execution_namespace", "loom-run-foo"),
        (row, "access_generation", True),
        (release, "service_image_ref", "example.com/api:latest"),
        (release, "web_image_ref", "https://example.com/web@sha256:" + "f" * 64),
        (shared, "runtime_profile_json", "{}"),
    ):
        with pytest.raises(ValueError):
            type(model).model_validate(model.model_dump() | {field: value})


def test_ingress_and_egress_are_scoped_without_mutating_shared_network(platform_inputs):
    from loom.nebius_application_render import render_application

    row, release, shared, foundation = inputs(platform_inputs)
    result = render_application(row, release, shared, foundation)
    policies = [doc for doc in docs(result) if doc["kind"] == "NetworkPolicy"]
    assert policies
    assert all(doc["metadata"]["namespace"] == "loom-dev-alice" for doc in policies)
    default = named(result, "NetworkPolicy", "default-deny")
    assert default["spec"]["ingress"] == default["spec"]["egress"] == []
    for name, port in (("public-api", 8090), ("public-web", 8080)):
        ingress = named(result, "NetworkPolicy", name)["spec"]["ingress"]
        assert ingress == [{"from": [{
            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": foundation.ingress_namespace}},
            "podSelector": {"matchLabels": {"app.kubernetes.io/name": foundation.ingress_controller_label}},
        }], "ports": [{"protocol": "TCP", "port": port}]}]
    outbound = named(result, "NetworkPolicy", "application-egress")["spec"]["egress"]
    service_peers = [rule for rule in outbound if any("namespaceSelector" in peer and
                    peer["namespaceSelector"].get("matchLabels", {}).get("kubernetes.io/metadata.name") == shared.platform_namespace
                    for peer in rule.get("to", []))]
    assert {port["port"] for rule in service_peers for port in rule["ports"]} == {5432, 8080, 9100}


def test_application_dns_allows_native_and_disposable_resolvers_only(platform_inputs):
    from loom.nebius_application_render import render_application

    result = render_application(*inputs(platform_inputs))
    policy = named(result, "NetworkPolicy", "application-egress")["spec"]
    assert policy["podSelector"] == {"matchLabels": {"app": "loom-service"}}
    dns_rules = [rule for rule in policy["egress"] if any(port["port"] == 53 for port in rule["ports"])]
    assert dns_rules == [{
        "to": [{
            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
            "podSelector": {"matchExpressions": [
                {"key": "k8s-app", "operator": "In", "values": ["kube-dns", "coredns"]},
            ]},
        }],
        "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}],
    }]
