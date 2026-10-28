from __future__ import annotations

import copy
import json
import os
import resource
import shlex
import subprocess
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from loom.task_image_build_plan import TaskImageBuildComponentV1
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import (
    BUILDKIT_IMAGE,
    MIN_TASK_IMAGE_EPHEMERAL_STORAGE_MIB,
    TaskImageJobConfig,
    render_task_image_job,
    task_image_job_name,
)


def _component_script(script: str) -> str:
    creator = shlex.split(script.split("exec ", 1)[1])[-1]
    return shlex.split(creator.split("exec ", 1)[1])[-1]


@pytest.fixture
def inputs():
    return {
        "materialization_id": uuid4(),
        "lease_epoch": 3,
        "claim": {
            "cpu_arch": "x86_64",
            "source_bucket": "task-source",
            "cache_bucket": "image-cache",
            "registry_repository": "registry.example/tasks",
        },
        "components": (
            TaskImageBuildComponentV1(
                name="task",
                dockerfile_path="env/Dockerfile",
                context_path="env",
                oci_output_path="oci/0000.tar",
            ),
            TaskImageBuildComponentV1(
                name="sidecar:database",
                dockerfile_path="database/Dockerfile.db",
                context_path="database",
                oci_output_path="oci/0001.tar",
            ),
        ),
        "target": ExecutionTargetRuntime(
            target_id="primary",
            namespace="builds",
            node_selector={"loom.pool": "native"},
            tolerations=(
                {
                    "key": "loom.pool",
                    "value": "native",
                    "operator": "Equal",
                    "effect": "NoSchedule",
                },
            ),
        ),
        "config": TaskImageJobConfig(
            service_image="registry.example/service@sha256:" + "a" * 64,
            source_secret_name="source-reader",
            registry_secret_name="registry-writer",
            cache_secret_name="cache-access",
            max_processes=384,
        ),
    }


def test_job_serializes_credential_phases_and_never_exposes_auth_to_build(inputs) -> None:
    configmap, job = render_task_image_job(**inputs)
    pod = job["spec"]["template"]["spec"]
    prepare, builder = pod["initContainers"]
    (publish,) = pod["containers"]
    assert [prepare["name"], builder["name"], publish["name"]] == ["prepare", "build", "publish"]
    assert not any("restartPolicy" in container for container in pod["initContainers"])
    volumes = {volume["name"]: volume for volume in pod["volumes"]}
    builder_volumes = {mount["name"] for mount in builder["volumeMounts"]}
    assert builder_volumes == {"build", "builder-tmp", "builder-subids"}
    assert {
        mount["mountPath"] for mount in builder["volumeMounts"] if mount["name"] == "builder-tmp"
    } == {"/scratch", "/tmp"}
    assert all("emptyDir" in volumes[name] for name in builder_volumes - {"builder-subids"})
    assert {mount["name"] for mount in prepare["volumeMounts"]} & {
        "source",
        "cache",
        "registry",
    } == {"source", "cache"}
    assert {mount["name"] for mount in publish["volumeMounts"]} & {
        "source",
        "cache",
        "registry",
    } == {"cache", "registry"}
    for phase in (prepare, publish):
        assert phase["command"] == [
            "python",
            "-I",
            "-B",
            "-m",
            "loom_execution_actuator.task_image_runtime",
            phase["name"],
            "--claim",
            "/loom/claim/claim.json",
        ]
        for mount in phase["volumeMounts"]:
            if mount["name"] in {"source", "cache", "registry", "claim"}:
                assert mount["readOnly"]
        assert phase["terminationMessagePath"] == "/dev/termination-log"
    assert next(mount for mount in publish["volumeMounts"] if mount["name"] == "build")["readOnly"]
    assert configmap["immutable"]
    payload = json.loads(configmap["data"]["claim.json"])
    assert payload["id"] == str(inputs["materialization_id"])
    assert payload["lease_epoch"] == 3 and len(payload["components"]) == 2
    assert volumes["registry"]["secret"]["items"] == [{"key": "config.json", "path": "config.json"}]
    assert {item["key"] for item in volumes["source"]["secret"]["items"]} == {
        "access-key",
        "secret-key",
    }
    assert not any("envFrom" in phase for phase in (prepare, builder, publish))



def test_builder_owner_id_files_are_fixed_readonly_and_do_not_expose_claim(inputs) -> None:
    configmap, job = render_task_image_job(**inputs)
    assert configmap["data"]["subuid"] == configmap["data"]["subgid"] == "user:100000:262144\nroot:1:262144\n"
    pod = job["spec"]["template"]["spec"]
    prepare, builder = pod["initContainers"]
    publish, = pod["containers"]
    volume = next(row for row in pod["volumes"] if row["name"] == "builder-subids")
    assert volume == {"name": "builder-subids", "configMap": {
        "name": configmap["metadata"]["name"], "defaultMode": 0o444,
        "items": [{"key": key, "path": key} for key in ("subuid", "subgid")]}}
    assert [row for row in builder["volumeMounts"] if row["name"] == "builder-subids"] == [
        {"name": "builder-subids", "mountPath": "/etc/" + key, "subPath": key, "readOnly": True}
        for key in ("subuid", "subgid")]
    assert all(row["name"] != "claim" for row in builder["volumeMounts"])
    assert all(row["name"] != "builder-subids" for phase in (prepare, publish) for row in phase["volumeMounts"])
    assert builder["image"] == BUILDKIT_IMAGE
    assert builder["securityContext"]["runAsUser"] == builder["securityContext"]["runAsGroup"] == 1000


def test_native_build_preserves_arguments_target_and_task_deadline(inputs) -> None:
    inputs["claim"]["task_config"] = {
        "task": {"id": "native/build-options", "name": "Native build options"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "environment": {
            "os": "linux",
            "dockerfile": "env/Dockerfile",
            "docker_build_context": "env",
            "docker_build_args": {"VALUE": "a b; $(touch /tmp/never)"},
            "docker_build_target": "selected",
            "build_timeout_sec": 73,
        },
    }
    _, job = render_task_image_job(**inputs)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][2]
    script = _component_script(script)
    builds = [
        shlex.split(line)[1:] for line in script.splitlines() if "buildctl-daemonless.sh build" in line
    ]
    assert "build-arg:VALUE=a b; $(touch /tmp/never)" in builds[0]
    assert "target=selected" in builds[0]
    assert not any(value.startswith(("build-arg:", "target=")) for value in builds[1])
    assert job["spec"]["activeDeadlineSeconds"] == inputs["config"].active_deadline_seconds
    assert builds[0][:6] == ["timeout", "-s", "TERM", "-k", "10", "73"]
    assert builds[0][6:10] == ["sh", "-c", 'trap "exit 124" TERM; "$@" & wait "$!"', "loom-build"]


def test_job_bounds_resources_and_only_builder_has_rootless_exceptions(inputs) -> None:
    _, job = render_task_image_job(**inputs)
    spec = job["spec"]
    pod = spec["template"]["spec"]
    assert spec["backoffLimit"] == 0 and spec["parallelism"] == spec["completions"] == 1
    assert spec["activeDeadlineSeconds"] == inputs["config"].active_deadline_seconds
    assert pod["restartPolicy"] == "Never"
    for field in (
        "automountServiceAccountToken",
        "shareProcessNamespace",
        "hostNetwork",
        "hostPID",
        "hostIPC",
        "enableServiceLinks",
    ):
        assert pod[field] is False
    assert not any("hostPath" in volume or "projected" in volume for volume in pod["volumes"])
    containers = [*pod["initContainers"], *pod["containers"]]
    for container in containers:
        security = container["securityContext"]
        assert security["readOnlyRootFilesystem"] and security["runAsNonRoot"]
        assert security["runAsUser"] == security["runAsGroup"] == 1000
        assert security["capabilities"]["drop"] == ["ALL"]
        assert container["resources"]["requests"] == container["resources"]["limits"]
        assert set(container["resources"]["limits"]) == {"cpu", "memory", "ephemeral-storage"}
        if container["name"] == "build":
            assert security["allowPrivilegeEscalation"]
            assert security["capabilities"]["add"] == ["SETUID", "SETGID"]
            assert (
                security["seccompProfile"] == security["appArmorProfile"] == {"type": "Unconfined"}
            )
        else:
            assert not security["allowPrivilegeEscalation"]
            assert "add" not in security["capabilities"]
            assert security["seccompProfile"] == {"type": "RuntimeDefault"}
    sizes = {
        volume["name"]: int(volume["emptyDir"]["sizeLimit"].removesuffix("Mi"))
        for volume in pod["volumes"]
        if "emptyDir" in volume
    }
    assert sizes == {"build": 16384, "builder-tmp": 16384, "prepare-tmp": 4096, "publish-tmp": 4096}
    # A build can use >7 GiB scratch plus a small output without a volume
    # eviction. Aggregate Pod accounting still enforces the same 16 GiB.
    assert 8192 < sizes["builder-tmp"]
    assert 8192 + 1024 < inputs["config"].ephemeral_storage_mib
    assert all(container["resources"]["limits"]["ephemeral-storage"] == "16384Mi"
               for container in pod["initContainers"] + pod["containers"])
    assert sizes["build"] > 512 + 1024 + 1024 + 3072
    assert sizes["prepare-tmp"] > 1024 and sizes["publish-tmp"] > 3072
    private_tmp = {
        next(
            mount["name"]
            for mount in container["volumeMounts"]
            if mount["mountPath"] in {"/tmp", "/scratch"}
        )
        for container in containers
    }
    assert private_tmp == {"prepare-tmp", "builder-tmp", "publish-tmp"}
    assert pod["nodeSelector"] == {
        **inputs["target"].node_selector,
        "loom.nebius/node-os": "linux",
        "loom.nebius/node-arch": "amd64",
    }
    assert pod["tolerations"] == list(inputs["target"].tolerations)


def test_build_commands_bound_descendants_after_private_creator_namespace(inputs) -> None:
    _, job = render_task_image_job(**inputs)
    builder = job["spec"]["template"]["spec"]["initContainers"][1]
    script = builder["command"][-1]
    assert builder["image"] == BUILDKIT_IMAGE
    outer = shlex.split(script.split("exec ", 1)[1])
    assert outer[:4] == ["rootlesskit", "--state-dir=/scratch/scope/a", "sh", "-ec"]
    assert "ulimit" not in script.split("exec ", 1)[0]
    creator = outer[4]
    assert creator.startswith("ulimit -Su 384\nulimit -Hu 384\nexec ")
    inner = shlex.split(creator.split("exec ", 1)[1])
    assert inner[:5] == ["rootlesskit", "--state-dir=/scratch/scope/b", "--copy-up=/run", "sh", "-ec"]
    script = inner[5]
    assert "/scratch/scope" not in script
    assert script.count("buildctl-daemonless.sh build") == 2
    for fragment in (
        "context=/loom/build/context/env",
        "dockerfile=/loom/build/context/env",
        "filename=Dockerfile",
        "context=/loom/build/context/database",
        "filename=Dockerfile.db",
        "platform=linux/amd64",
        "dest=/loom/build/oci/0000.tar",
        "dest=/loom/build/oci/0001.tar",
        "/loom/build/cache-in/0/index.json",
        "/loom/build/cache-in/1/index.json",
    ):
        assert fragment in script
    environment = {entry["name"]: entry["value"] for entry in builder["env"]}
    assert (
        environment["TMPDIR"] == "/scratch/tmp"
        and environment["DOCKER_CONFIG"] == "/scratch/docker-config"
    )
    assert "--oci-worker-no-process-sandbox" in environment["BUILDKITD_FLAGS"]
    assert "--oci-worker-snapshotter=native" in environment["BUILDKITD_FLAGS"]
    assert "/var/run/loom-task-build" not in script
    assert not any(flag in script for flag in ("--secret", "--ssh", "push=true", "--allow"))
    assert '"loom_task_image_stage":"solve"' in script
    assert '"loom_task_image_stage":"oci_export"' in script
    assert '"loom_task_image_stage":"cleanup"' in script
    assert '"included_in":"solve"' in script


def test_build_script_stage_markers_include_duration_shell(inputs) -> None:
    _, job = render_task_image_job(**inputs)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    assert "solve_started=$(date +%s)" in script
    assert "duration_ms" in script
    assert script.index('"loom_task_image_stage":"solve"') < script.index(
        "buildctl-daemonless.sh"
    )


def test_buildkit_snapshotter_overlayfs_is_explicit_opt_in(inputs) -> None:
    inputs["config"] = replace(inputs["config"], snapshotter="overlayfs")
    _, job = render_task_image_job(**inputs)
    builder = next(
        container
        for container in job["spec"]["template"]["spec"]["initContainers"]
        if container["name"] == "build"
    )
    environment = {entry["name"]: entry["value"] for entry in builder["env"]}
    assert "--oci-worker-snapshotter=overlayfs" in environment["BUILDKITD_FLAGS"]
    assert "--oci-worker-snapshotter=native" not in environment["BUILDKITD_FLAGS"]


def test_installed_task_image_settings_default_to_compatible_snapshotter() -> None:
    from loom_execution_actuator.task_image_settings import NativeTaskImageSettings

    settings = NativeTaskImageSettings(
        namespace="task-builds",
        service_image="registry.example/service@sha256:" + "a" * 64,
        storage_endpoint="https://storage.example",
        storage_region="eu-north1",
        source_bucket="task-source",
        registry_repository="registry.example/tasks",
    )
    assert settings.job_config().snapshotter == "native"
    assert settings.model_copy(update={"snapshotter": "overlayfs"}).job_config().snapshotter == "overlayfs"


def test_task_image_job_config_rejects_unknown_snapshotter() -> None:
    with pytest.raises(ValueError, match="snapshotter"):
        TaskImageJobConfig(
            service_image="registry.example/service@sha256:" + "a" * 64,
            source_secret_name="loom-task-build-source",
            registry_secret_name="loom-task-build-registry",
            snapshotter="aufs",  # type: ignore[arg-type]
        )


def test_export_cache_mode_min_wires_into_build_script(inputs) -> None:
    inputs["config"] = replace(inputs["config"], export_cache_mode="min")
    _, job = render_task_image_job(**inputs)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    assert "mode=min" in script
    assert "mode=max" not in script


def test_task_image_job_config_rejects_unknown_export_cache_mode() -> None:
    with pytest.raises(ValueError, match="export_cache_mode"):
        TaskImageJobConfig(
            service_image="registry.example/service@sha256:" + "a" * 64,
            source_secret_name="loom-task-build-source",
            registry_secret_name="loom-task-build-registry",
            export_cache_mode="balanced",  # type: ignore[arg-type]
        )


def test_oci_export_directory_rewrites_dest_and_claim(inputs) -> None:
    inputs["config"] = replace(inputs["config"], oci_export_format="directory")
    configmap, job = render_task_image_job(**inputs)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    assert "tar=false" in script
    assert "type=oci,dest=/loom/build/oci/0000,tar=false" in script
    assert "/loom/build/oci/0000.tar" not in script
    claim = json.loads(configmap["data"]["claim.json"])
    assert claim["components"][0]["oci_output_path"] == "oci/0000"


def test_task_image_job_config_rejects_unknown_oci_export_format() -> None:
    with pytest.raises(ValueError, match="oci_export_format"):
        TaskImageJobConfig(
            service_image="registry.example/service@sha256:" + "a" * 64,
            source_secret_name="loom-task-build-source",
            registry_secret_name="loom-task-build-registry",
            oci_export_format="squashfs",  # type: ignore[arg-type]
        )


def test_dockerfile_paths_cannot_inject_shell_commands(inputs) -> None:
    path = "nested/space ' ;$(touch BAD)/Dockerfile"
    inputs["components"] = (
        TaskImageBuildComponentV1(
            name="task", dockerfile_path=path, context_path=".", oci_output_path="oci/0000.tar"
        ),
    )
    _, job = render_task_image_job(**inputs)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    command_line = next(line for line in _component_script(script).splitlines() if line.startswith("if timeout"))
    assert "dockerfile=/loom/build/context/nested/space ' ;$(touch BAD)" in shlex.split(
        command_line
    )


def test_render_is_deterministic_and_does_not_mutate_inputs(inputs) -> None:
    before = copy.deepcopy(inputs)
    first = render_task_image_job(**inputs)
    assert render_task_image_job(**inputs) == first and inputs == before
    first[1]["spec"]["template"]["spec"]["nodeSelector"]["loom.pool"] = "changed"
    assert inputs["target"].node_selector == {"loom.pool": "native"}
    assert render_task_image_job(**inputs)[0]["metadata"]["name"] == task_image_job_name(
        inputs["materialization_id"], 3
    )
    assert task_image_job_name(inputs["materialization_id"], 4) != task_image_job_name(
        inputs["materialization_id"], 3
    )
    assert len(task_image_job_name(uuid4(), 2**63 - 1)) <= 63


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_processes", 0),
        ("cpu_millis", True),
        ("active_deadline_seconds", -1),
        ("ephemeral_storage_mib", 8),
        ("source_secret_name", "bad/name"),
        ("service_image", "registry.example/image:latest"),
    ],
)
def test_renderer_rejects_unbounded_or_invalid_configuration(inputs, field, value) -> None:
    with pytest.raises(ValueError):
        replace(inputs["config"], **{field: value})


@pytest.mark.parametrize(
    "damage", ["epoch", "materialization", "architecture", "components", "path"]
)
def test_renderer_rejects_claim_identity_and_path_drift(inputs, damage) -> None:
    if damage == "epoch":
        inputs["claim"]["lease_epoch"] = 4
    elif damage == "materialization":
        inputs["claim"]["id"] = str(uuid4())
    elif damage == "architecture":
        inputs["claim"]["cpu_arch"] = "unknown"
    elif damage == "components":
        inputs["components"] = ()
    else:
        inputs["components"] = (
            inputs["components"][0].model_copy(update={"dockerfile_path": "../Dockerfile"}),
        )
    with pytest.raises(ValueError):
        render_task_image_job(**inputs)


def test_identity_rejects_nil_materialization_and_nonpositive_epoch() -> None:
    with pytest.raises(ValueError):
        task_image_job_name(UUID(int=0), 1)
    with pytest.raises(ValueError):
        task_image_job_name(uuid4(), 0)


def test_component_limit_and_target_architecture_fail_before_job_creation(inputs) -> None:
    inputs["components"] = tuple(
        TaskImageBuildComponentV1(
            name=f"sidecar:component{index}",
            dockerfile_path="Dockerfile",
            context_path=".",
            oci_output_path=f"oci/{index:04d}.tar",
        )
        for index in range(9)
    )
    with pytest.raises(ValueError, match="bounded Dockerfile components"):
        render_task_image_job(**inputs)
    inputs["components"] = inputs["components"][:1]
    inputs["target"] = replace(inputs["target"], node_selector={"kubernetes.io/arch": "arm64"})
    with pytest.raises(ValueError, match="architecture conflicts"):
        render_task_image_job(**inputs)


def test_each_component_cleans_only_private_daemon_data_before_next_phase(inputs) -> None:
    _, job = render_task_image_job(**inputs)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    cleanup = "TMPDIR=/scratch/cleanup rootlesskit rm -rf -- /scratch/state /scratch/tmp /scratch/runtime /scratch/docker-config"
    assert script.count(cleanup) == len(inputs["components"])
    for segment in script.split("if timeout")[1:]:
        success, failure = segment.split("else", 1)
        assert "then" in success and cleanup in success
        assert "rootlesskit rm" not in failure
    for line in script.splitlines():
        if "rm -rf" in line:
            assert "/loom/build" not in line
            assert "*" not in line
    assert script.index(cleanup) < script.rindex("if timeout")


def test_optional_cache_does_not_consume_builder_output_space(inputs) -> None:
    inputs["config"] = replace(inputs["config"], cache_secret_name=None)
    _, job = render_task_image_job(**inputs)
    pod = job["spec"]["template"]["spec"]
    script = pod["initContainers"][1]["command"][-1]
    assert "--export-cache" not in script and "--import-cache" not in script
    assert not any(volume["name"] == "cache" for volume in pod["volumes"])


def test_minimum_budget_rejects_guaranteed_phase_space_exhaustion(inputs) -> None:
    with pytest.raises(ValueError, match="at least 16 GiB"):
        replace(inputs["config"], ephemeral_storage_mib=MIN_TASK_IMAGE_EPHEMERAL_STORAGE_MIB - 1)
    inputs["config"] = replace(inputs["config"], ephemeral_storage_mib=32768)
    _, job = render_task_image_job(**inputs)
    pod = job["spec"]["template"]["spec"]
    volumes = {volume["name"]: volume for volume in pod["volumes"]}
    assert volumes["publish-tmp"]["emptyDir"]["sizeLimit"] == "8192Mi"
    assert volumes["build"]["emptyDir"]["sizeLimit"] == "32768Mi"
    assert pod["containers"][0]["resources"]["limits"]["ephemeral-storage"] == "32768Mi"


def test_native_registry_key_is_mounted_only_in_publisher(inputs) -> None:
    inputs["config"] = replace(inputs["config"], registry_auth_kind="nebius")
    _, job = render_task_image_job(**inputs)
    pod = job["spec"]["template"]["spec"]
    registry = next(volume for volume in pod["volumes"] if volume["name"] == "registry")
    assert registry["secret"]["items"] == [{"key": "credentials.json", "path": "credentials.json"}]
    assert all(
        not any(mount["name"] == "registry" for mount in phase["volumeMounts"])
        for phase in pod["initContainers"]
    )


@pytest.mark.parametrize("architecture,label", [("x86_64", "amd64"), ("arm64", "arm64")])
def test_native_selectors_match_zero_node_template(inputs, architecture, label) -> None:
    inputs["claim"]["cpu_arch"] = architecture
    inputs["target"] = replace(
        inputs["target"],
        node_selector={
            "loom.pool": "native",
            "kubernetes.io/os": "linux",
            "kubernetes.io/arch": label,
        },
    )
    _, job = render_task_image_job(**inputs)
    selector = job["spec"]["template"]["spec"]["nodeSelector"]
    template_labels = {
        "loom.pool": "native",
        "loom.nebius/node-os": "linux",
        "loom.nebius/node-arch": label,
    }
    assert selector.items() <= template_labels.items()
    assert selector["loom.nebius/node-arch"] == label
    assert inputs["target"].node_selector["kubernetes.io/arch"] == label


@pytest.mark.parametrize(
    "constraint,value",
    [
        ("kubernetes.io/os", "windows"),
        ("kubernetes.io/arch", "arm64"),
        ("loom.nebius/node-os", "windows"),
        ("loom.nebius/node-arch", "arm64"),
    ],
)
def test_native_selector_rejects_conflicting_target_constraints(inputs, constraint, value) -> None:
    inputs["target"] = replace(inputs["target"], node_selector={constraint: value})
    with pytest.raises(ValueError, match="architecture conflicts"):
        render_task_image_job(**inputs)


@pytest.mark.parametrize("export_format", ["archive", "directory"])
@pytest.mark.parametrize("cache_enabled", [False, True])
@pytest.mark.parametrize("component_count", [1, 2])
def test_generated_build_script_is_valid_posix_shell(
    inputs, export_format, cache_enabled, component_count,
) -> None:
    inputs["config"] = replace(
        inputs["config"], oci_export_format=export_format,
        cache_secret_name="cache-access" if cache_enabled else None,
    )
    inputs["components"] = inputs["components"][:component_count]
    _, job = render_task_image_job(**inputs)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    result = subprocess.run(["sh", "-n"], input=script, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("export_format", ["archive", "directory"])
@pytest.mark.parametrize(("build_exit", "expected_exit"), [(0, 0), (42, 1), (124, 124)])
@pytest.mark.parametrize("component_count", [1, 2])
def test_generated_build_script_preserves_outcomes_and_valid_stage_json(
    inputs, tmp_path: Path, export_format: str, build_exit: int, expected_exit: int,
    component_count: int,
) -> None:
    # Execute the generated shell with a controlled external build command.
    # Redirect its two container-local directories into this test's own tree.
    # Keep the caller's process ceiling instead of limiting other test workers.
    process_limit = resource.getrlimit(resource.RLIMIT_NPROC)[0]
    inputs["config"] = replace(
        inputs["config"], oci_export_format=export_format,
        max_processes=process_limit if process_limit > 0 else 1_000_000,
    )
    inputs["components"] = inputs["components"][:component_count]
    _, job = render_task_image_job(**inputs)
    script = job["spec"]["template"]["spec"]["initContainers"][1]["command"][-1]
    script = script.replace("/scratch", str(tmp_path / "scratch"))
    script = script.replace("/loom/build", str(tmp_path / "build"))
    binary_dir = tmp_path / "bin"
    binary_dir.mkdir()
    build = binary_dir / "buildctl-daemonless.sh"
    build.write_text("""#!/bin/sh
set -eu
if [ "$LOOM_TEST_BUILD_EXIT" != 0 ]; then exit "$LOOM_TEST_BUILD_EXIT"; fi
while [ "$1" != --output ]; do shift; done
output=$2
dest=${output#*dest=}
dest=${dest%%,*}
case "$output" in
  *tar=false*) mkdir -p "$dest"; printf payload > "$dest/index.json" ;;
  *) printf payload > "$dest" ;;
esac
""")
    cleanup = binary_dir / "rootlesskit"
    cleanup.write_text('#!/bin/sh\nwhile [ "${1#--}" != "$1" ]; do shift; done\n'
                       'if [ "$1" = sh ]; then shift; exec bash "$@"; fi\nexec "$@"\n')
    for binary in (build, cleanup):
        binary.chmod(0o755)
    result = subprocess.run(
        ["bash", "-c", script], text=True, capture_output=True, timeout=10,
        env={**os.environ, "PATH": str(binary_dir) + os.pathsep + os.environ["PATH"],
             "LOOM_TEST_BUILD_EXIT": str(build_exit)},
    )
    assert result.returncode == expected_exit, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines()]
    solve_ends = [event for event in events
                  if event["loom_task_image_stage"] == "solve" and event["event"] == "end"]
    for event in solve_ends:
        assert isinstance(event["duration_ms"], int) and event["duration_ms"] >= 0
    if build_exit:
        (solve_end,) = solve_ends
        assert solve_end["component_index"] == 0
        assert solve_end["failed"] is True
        assert solve_end["exit"] == build_exit
        assert len(events) == 2
    else:
        assert [event["component_index"] for event in solve_ends] == list(range(component_count))
        assert all("failed" not in event for event in solve_ends)
        exported = [event for event in events if event["loom_task_image_stage"] == "oci_export"]
        assert [event["bytes"] for event in exported] == [7] * component_count
        assert len(events) == 5 * component_count
        assert events[-1]["loom_task_image_stage"] == "cleanup"
        assert events[-1]["event"] == "end"
