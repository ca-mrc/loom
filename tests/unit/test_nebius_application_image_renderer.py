"""Application images reuse native isolation without pretending to be Tasks."""
from __future__ import annotations

import json
import shlex
from dataclasses import replace
from uuid import uuid4

import pytest

from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import BUILDKIT_IMAGE, TaskImageJobConfig


@pytest.fixture
def build_inputs():
    from loom.application_image_build import ApplicationImageBuildClaimV1

    claim = ApplicationImageBuildClaimV1.model_validate({
        "build_id": str(uuid4()), "attempt": 2, "upload_id": str(uuid4()),
        "installation_id": str(uuid4()), "owner_user_id": str(uuid4()), "owner_team_id": str(uuid4()),
        "data_environment_id": str(uuid4()), "cluster_id": "cluster-development",
        "source": {"source_digest": "sha256:" + "a" * 64, "archive_sha256": "b" * 64,
            "archive_size_bytes": 10240, "base_commit": "c" * 40},
        "recipe": {"cpu_arch": "x86_64", "schema_revision": "0173",
            "trusted_image_ref": "registry.example/service@sha256:" + "d" * 64,
            "buildkit_image_ref": BUILDKIT_IMAGE},
        "storage_endpoint": "https://storage.eu-north1.nebius.cloud", "storage_region": "eu-north1",
        "source_bucket": "shared-source", "registry_repository": "cr.eu-north1.nebius.cloud/registryid/personal-apps",
    })
    target = ExecutionTargetRuntime(target_id="primary", namespace="shared-builds",
        node_selector={"nebius.com/node-group-id": "node-group"})
    config = TaskImageJobConfig(service_image=claim.recipe.trusted_image_ref,
        source_secret_name="source-reader", registry_secret_name="app-registry-writer", registry_auth_kind="nebius")
    return claim, target, config


def test_application_job_keeps_real_source_identity_and_sequential_credential_isolation(build_inputs):
    from loom_execution_actuator.application_image_renderer import render_application_image_job

    claim, target, config = build_inputs
    before = claim.model_dump_json()
    cm, job = render_application_image_job(claim=claim, target=target, config=config)
    assert job["metadata"]["name"] == f"loom-app-{claim.build_id.hex}-a2"
    assert cm["metadata"] == job["metadata"] and cm["immutable"]
    labels = job["metadata"]["labels"]
    assert labels == {"app.kubernetes.io/component": "application-image-builder",
        "loom.application-build-id": str(claim.build_id), "loom.build-attempt": "2"}
    document = json.loads(cm["data"]["claim.json"])
    assert document["source"]["source_digest"] == "sha256:" + "a" * 64
    assert document["build_id"] == str(claim.build_id) and document["attempt"] == 2
    assert not {"task_config", "task_id", "materialization_key", "id", "lease_epoch"} & document.keys()
    assert [row["name"] for row in document["components"]] == ["service", "web"]
    assert [row["oci_output_path"] for row in document["components"]] == ["oci/0000.tar", "oci/0001.tar"]
    pod = job["spec"]["template"]["spec"]
    prepare, build = pod["initContainers"]
    publish, = pod["containers"]
    assert [prepare["name"], build["name"], publish["name"]] == ["prepare", "build", "publish"]
    assert {mount["name"] for mount in build["volumeMounts"]} == {"build", "builder-tmp"}
    volumes = {volume["name"]: volume for volume in pod["volumes"]}
    assert all("emptyDir" in volumes[mount["name"]] for mount in build["volumeMounts"])
    assert {mount["name"] for mount in prepare["volumeMounts"]} & {"source", "registry", "cache"} == {"source"}
    assert {mount["name"] for mount in publish["volumeMounts"]} & {"source", "registry", "cache"} == {"registry"}
    assert next(mount for mount in publish["volumeMounts"] if mount["name"] == "build")["readOnly"]
    for phase in (prepare, publish):
        assert phase["command"][4] == "loom_execution_actuator.application_image_runtime"
        assert phase["securityContext"]["allowPrivilegeEscalation"] is False
    for field in ("automountServiceAccountToken", "hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace"):
        assert pod[field] is False
    assert job["spec"]["backoffLimit"] == 0 and job["spec"]["parallelism"] == job["spec"]["completions"] == 1
    assert claim.model_dump_json() == before


@pytest.mark.parametrize("arch,platform", [("x86_64", "linux/amd64"), ("arm64", "linux/arm64")])
def test_application_build_uses_both_declared_dockerfiles_and_honest_source_arguments(build_inputs, arch, platform):
    from loom_execution_actuator.application_image_renderer import render_application_image_job

    claim, target, config = build_inputs
    claim = claim.model_copy(update={"recipe": claim.recipe.model_copy(update={"cpu_arch": arch})})
    _, job = render_application_image_job(claim=claim, target=target, config=config)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    builds = [shlex.split(line) for line in script.splitlines() if "buildctl-daemonless.sh build" in line]
    assert len(builds) == 2
    for invocation, filename in zip(builds, ("Dockerfile.service", "Dockerfile.web"), strict=True):
        assert f"filename={filename}" in invocation
        assert "context=/loom/build/context" in invocation and "dockerfile=/loom/build/context/deploy" in invocation
        assert f"platform={platform}" in invocation
        assert "build-arg:LOOM_SOURCE_DIGEST=sha256:" + "a" * 64 in invocation
        assert "build-arg:LOOM_SOURCE_BASE_COMMIT=" + "c" * 40 in invocation
        assert "build-arg:LOOM_BUILD_KIND=personal" in invocation
        assert "build-arg:LOOM_BUILD_SHA=unknown" in invocation
        assert "build-arg:LOOM_BUILD_SHA=" + "c" * 40 not in invocation


@pytest.mark.parametrize("damage", ["image", "buildkit", "snapshotter", "export", "format", "arch", "attempt", "source", "cache"])
def test_application_recipe_or_source_drift_never_renders(build_inputs, damage):
    from loom_execution_actuator.application_image_renderer import render_application_image_job

    claim, target, config = build_inputs
    if damage == "image":
        config = replace(config, service_image="registry.example/service@sha256:" + "e" * 64)
    elif damage == "buildkit":
        config = replace(config, buildkit_image="registry.example/buildkit@sha256:" + "e" * 64)
    elif damage == "snapshotter":
        config = replace(config, snapshotter="native")
    elif damage == "export":
        config = replace(config, export_cache_mode="min")
    elif damage == "format":
        config = replace(config, oci_export_format="directory")
    elif damage == "arch":
        target = replace(target, node_selector={"loom.nebius/node-arch": "arm64"})
    elif damage == "attempt":
        claim = claim.model_copy(update={"attempt": 0})
    elif damage == "source":
        claim = claim.model_copy(update={"source": claim.source.model_copy(update={"archive_sha256": "wrong"})})
    else:
        config = replace(config, cache_secret_name="unbound-cache")
    with pytest.raises(ValueError):
        render_application_image_job(claim=claim, target=target, config=config)


def test_recipe_digest_binds_options_and_platform_independently_of_owner_and_attempt(build_inputs):
    claim, _, _ = build_inputs
    assert claim.recipe.digest == claim.model_copy(update={"build_id": uuid4(), "attempt": 3}).recipe.digest
    for field, value in (("cpu_arch", "arm64"), ("schema_revision", "0174"), ("snapshotter", "native")):
        assert claim.recipe.digest != claim.recipe.model_copy(update={field: value}).digest
    assert claim.source_key == "application-sources/v1/sha256/" + "b" * 64 + ".tar"


def test_application_job_name_fits_kubernetes_at_maximum_attempt(build_inputs):
    from loom_execution_actuator.application_image_renderer import render_application_image_job

    claim, target, config = build_inputs
    claim = claim.model_copy(update={"attempt": 2**63 - 1})
    _, job = render_application_image_job(claim=claim, target=target, config=config)
    assert len(job["metadata"]["name"]) <= 63


@pytest.mark.parametrize("path", ["/absolute", "../outside", "a/../b", "a//b", "a\\b", "a/./b"])
def test_shared_component_paths_remain_bounded_and_task_names_remain_task_specific(path):
    from loom.native_image_build import NativeImageBuildComponentV1
    from loom.task_image_build_plan import TaskImageBuildComponentV1

    with pytest.raises(ValueError):
        NativeImageBuildComponentV1(name="service", dockerfile_path=path, context_path=".", oci_output_path="oci/0000.tar")
    with pytest.raises(ValueError):
        TaskImageBuildComponentV1(name="service", dockerfile_path="deploy/Dockerfile.service", context_path=".",
                                  oci_output_path="oci/0000.tar")
