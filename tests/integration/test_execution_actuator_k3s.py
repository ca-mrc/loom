from __future__ import annotations

import asyncio
import hashlib
import os
import shlex
import ssl
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml
from urllib3.exceptions import MaxRetryError, SSLError

from loom.db.schema import ServiceExecutionLease
from loom.execution_contract import (
    ImageMaterialization,
    IsolationLevel,
    NetworkAccess,
    VerifierTopology,
    WorkloadRequirementsV1,
)
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProbeV1,
    ProcessPhaseV1,
    SidecarContainerV1,
)
from loom.pipeline.keys import canonical_digest
from loom_execution_actuator.kubernetes_api import InClusterKubernetesJobApi
from loom_execution_actuator.renderer import ExecutionTargetRuntime, render_execution_job
from tests.support.execution_image_admission import signed_image_admission_bundle

pytestmark = pytest.mark.skipif(
    os.environ.get("LOOM_RUN_DISPOSABLE_K3S") != "1",
    reason="set LOOM_RUN_DISPOSABLE_K3S=1 to run the disposable Kubernetes API conformance test",
)


def _lease(namespace: str) -> ServiceExecutionLease:
    now = datetime.now(UTC)
    image_ref = "invalid.local/loom-conformance@sha256:" + "a" * 64
    requirements = WorkloadRequirementsV1(
        operating_system="linux",
        cpu_architecture="x86_64",
        gpu_vendor="none",
        gpu_count=0,
        cpu_millis=100,
        memory_mib=128,
        ephemeral_storage_mib=128,
        isolation_level=IsolationLevel.SHARED_KERNEL,
        network_access=NetworkAccess.GATEWAY_ONLY,
        image_materialization=ImageMaterialization.IMMUTABLE_OCI,
        image_ref=image_ref,
        sidecar_count=0,
        verifier_topology=VerifierTopology.IN_ATTEMPT,
        custom_dns=False,
        extra_hosts=False,
        tmpfs=True,
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )
    runtime_image_ref = "invalid.local/runtime@sha256:" + "b" * 64
    runtime = ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_class_id="linux-amd64-cpu-pod-v1",
        composition="init_payload",
        task_image_ref=image_ref,
        runtime_image_ref=runtime_image_ref,
        runtime_binary_sha256="sha256:" + "c" * 64,
        image_admission=signed_image_admission_bundle((image_ref, runtime_image_ref), now=now),
        task_resources=ContainerResourcesV1(
            cpu_millis=100,
            memory_mib=128,
            ephemeral_storage_mib=128,
        ),
        workspace_mib=128,
        runtime_volume_mib=32,
        main=ProcessPhaseV1(
            role="agent",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=30,
        ),
        verifier_execution="in_attempt",
        verifier=ProcessPhaseV1(
            role="verifier",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=30,
        ),
    )
    requirements_json = requirements.model_dump(mode="json")
    runtime_json = runtime.canonical_payload()
    return ServiceExecutionLease(
        id=uuid4(),
        request_id=uuid4(),
        trial_id=uuid4(),
        team_id=uuid4(),
        attempt=1,
        execution_role="attempt",
        parent_lease_id=None,
        generation=1,
        resource_generation=1,
        execution_class_id="linux-amd64-cpu-pod-v1",
        target_id="disposable-k3s",
        routing_generation=1,
        selected_pool_id="nebius-cpu",
        routing_reason="admin_target_binding",
        routing_decision_sha256="sha256:" + "d" * 64,
        # The Job is suspended before submission, so the conformance test never pulls it.
        workload_requirements_json=requirements_json,
        workload_requirements_sha256=canonical_digest(requirements_json),
        runtime_contract_json=runtime_json,
        runtime_contract_sha256=canonical_digest(runtime_json),
        desired_state="create",
        observed_state="reserved",
        cleanup_state="not_requested",
        provider_scope_key="sha256:" + "c" * 64,
        namespace_name=namespace,
        job_name=f"loom-{uuid4().hex[:12]}-a1-g1-a",
        execution_unit_key=uuid4(),
        deadline_at=now + timedelta(minutes=5),
    )


def _executable_lease(
    namespace: str,
    *,
    task_image_ref: str,
    runtime_image_ref: str,
    runtime_binary_sha256: str,
    prepared_fixture: bool = False,
) -> ServiceExecutionLease:
    now = datetime.now(UTC)
    requirements = WorkloadRequirementsV1(
        operating_system="linux",
        cpu_architecture="x86_64",
        gpu_vendor="none",
        gpu_count=0,
        cpu_millis=100,
        memory_mib=128,
        ephemeral_storage_mib=256,
        isolation_level=IsolationLevel.SHARED_KERNEL,
        network_access=NetworkAccess.NONE,
        image_materialization=ImageMaterialization.IMMUTABLE_OCI,
        image_ref=task_image_ref,
        sidecar_count=1,
        verifier_topology=VerifierTopology.IN_ATTEMPT,
        custom_dns=False,
        extra_hosts=False,
        tmpfs=True,
        privileged=False,
        host_path=False,
        host_network=False,
        nested_containers=False,
        host_devices=False,
        host_specialized=False,
    )
    resources = ContainerResourcesV1(
        cpu_millis=100,
        memory_mib=128,
        ephemeral_storage_mib=256,
    )
    phase = {
        "working_directory": "/workspace",
        "timeout_seconds": 30,
        "environment": {},
    }
    runtime = ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_class_id="linux-amd64-cpu-pod-v1",
        composition="init_payload",
        task_image_ref=task_image_ref,
        runtime_image_ref=runtime_image_ref,
        runtime_binary_sha256=runtime_binary_sha256,
        image_admission=signed_image_admission_bundle((task_image_ref, runtime_image_ref), now=now),
        task_resources=resources,
        workspace_mib=128,
        runtime_volume_mib=32,
        setup=(
            ProcessPhaseV1(
                role="setup",
                argv=("/fixture", "phase", "setup"),
                **phase,
            ),
        ),
        main=ProcessPhaseV1(
            role="agent",
            argv=("/fixture", "phase", "agent"),
            **phase,
        ),
        verifier_execution="in_attempt",
        verifier=ProcessPhaseV1(
            role="verifier",
            argv=("/fixture", "phase", "verifier"),
            **phase,
        ),
        sidecars=(
            SidecarContainerV1(
                role_name="service-sidecar",
                image_ref=task_image_ref,
                argv=("/fixture", "sidecar"),
                resources=ContainerResourcesV1(
                    cpu_millis=50,
                    memory_mib=64,
                    ephemeral_storage_mib=32,
                ),
                startup_probe=ProbeV1(kind="http", port=8080, path="/healthz"),
                readiness_probe=ProbeV1(kind="http", port=8080, path="/readyz"),
            ),
        ),
        max_log_bytes_per_stream=1024 * 1024,
        max_artifact_bytes=16 * 1024 * 1024,
    )
    if prepared_fixture:
        fixture = runtime.sidecars[0].model_copy(update={
            "role_name": "fixture-server", "task_fixture": True,
            "task_image_component": "sidecar:server", "hostname": "fixture.example",
        })
        private = []
        for role in ("task-sandbox", "verifier-sandbox"):
            socket = f"/loom/sandboxes/{role}/sandbox.sock"
            probe = ProbeV1(kind="exec", argv=("/loom/bin/loom-sandbox-runtime", "--check-socket", socket))
            private.append(SidecarContainerV1(
                role_name=role, image_ref=task_image_ref, private_sandbox=True,
                argv=("/loom/bin/loom-sandbox-runtime", "--socket", socket, "--exec-timeout-seconds", "900"),
                resources=resources, startup_probe=probe, readiness_probe=probe,
            ))
        runtime = ExecutionRuntimePlanV1.model_validate({
            **runtime.canonical_payload(), "task_image_materialization_id": str(uuid4()),
            "agent_image_ref": task_image_ref,
            "sidecars": [item.model_dump(mode="json") for item in (fixture, *private)],
        })
    requirements_json = requirements.model_dump(mode="json")
    runtime_json = runtime.canonical_payload()
    return ServiceExecutionLease(
        id=uuid4(),
        request_id=uuid4(),
        trial_id=uuid4(),
        team_id=uuid4(),
        attempt=1,
        execution_role="attempt",
        parent_lease_id=None,
        generation=1,
        resource_generation=1,
        execution_class_id="linux-amd64-cpu-pod-v1",
        target_id="disposable-k3s",
        routing_generation=1,
        selected_pool_id="nebius-cpu",
        routing_reason="admin_target_binding",
        routing_decision_sha256="sha256:" + "d" * 64,
        workload_requirements_json=requirements_json,
        workload_requirements_sha256=canonical_digest(requirements_json),
        runtime_contract_json=runtime_json,
        runtime_contract_sha256=canonical_digest(runtime_json),
        desired_state="create",
        observed_state="reserved",
        cleanup_state="not_requested",
        provider_scope_key="sha256:" + "c" * 64,
        namespace_name=namespace,
        job_name=f"loom-{uuid4().hex[:12]}-a1-g1-a",
        execution_unit_key=uuid4(),
        deadline_at=now + timedelta(minutes=5),
    )


def _start_k3s(*, node_name: str | None = None, ephemeral_storage_floor: str | None = None) -> object:
    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(
            "rancher/k3s@sha256:08fdebd14db9ab7d5ea821d5bfa95d02341a6ef886842fcc8d9dfd0e9fa9e0cd"
        )
        .with_exposed_ports(6443)
        .with_command(
            [
                "server",
                "--disable=traefik",
                "--disable=servicelb",
                # Disposable Docker nodes have no cloud integration. K3s's
                # embedded CCM can exit during its own RBAC bootstrap and take
                # the test API down; neither it nor ServiceLB is needed here.
                # Keep ordinary API RBAC, scheduling, CNI and policies enabled.
                "--disable-cloud-controller",
                "--tls-san=127.0.0.1",
                "--write-kubeconfig-mode=644",
                *([] if node_name is None else [f"--node-name={node_name}"]),
                *([] if ephemeral_storage_floor is None else [
                    "--kubelet-arg=eviction-hard=memory.available<100Mi,nodefs.inodesFree<5%,imagefs.inodesFree<5%,"
                    f"nodefs.available<{ephemeral_storage_floor},imagefs.available<{ephemeral_storage_floor}",
                ]),
            ]
        )
        .with_kwargs(privileged=True)
    )
    container.start()
    return container


def _load_client(container: object) -> tuple[object, object, object]:
    from kubernetes import client, config

    deadline = time.monotonic() + 90
    # K3s PID 1 evacuates the root cgroup and enables subtree controllers before
    # its startup banner. Docker exec during that window inserts another root
    # process and can make initialization fail with EBUSY. Observe Docker logs
    # (outside the container) before the first kubeconfig exec, with one deadline.
    while time.monotonic() < deadline:
        stdout, stderr = container.get_logs()
        if b"Starting k3s v" in stdout + stderr:
            break
        time.sleep(1)
    else:
        raise AssertionError("disposable k3s bootstrap did not reach cgroup initialization")
    last_error = "kubeconfig unavailable"
    while time.monotonic() < deadline:
        result = container.exec(["cat", "/etc/rancher/k3s/k3s.yaml"])
        if result.exit_code == 0:
            payload = result.output.decode("utf-8")
            mapped_port = container.get_exposed_port(6443)
            payload = payload.replace("https://127.0.0.1:6443", f"https://127.0.0.1:{mapped_port}")
            config.load_kube_config_from_dict(yaml.safe_load(payload))
            core = client.CoreV1Api()
            batch = client.BatchV1Api()
            try:
                core.get_api_resources(_request_timeout=5)
            except Exception as exc:  # API server is not ready yet.
                last_error = str(exc)
            else:
                # Discovery can serve before the namespace bootstrap controller.
                # Callers bind cluster identity to this namespace's actual UID;
                # wait for creation, never create it or substitute an identity.
                try:
                    core.read_namespace("kube-system", _request_timeout=5)
                except client.exceptions.ApiException as exc:
                    if exc.status != 404:
                        raise
                    last_error = "kube-system namespace bootstrap pending"
                else:
                    # Namespace discovery precedes ServiceCIDR allocator
                    # initialization. On a fresh disposable server, its own
                    # bootstrap Service is proof that allocation has succeeded.
                    # Never probe readiness by retrying a test Service write.
                    try:
                        service = core.read_namespaced_service("kubernetes", "default", _request_timeout=5)
                    except client.exceptions.ApiException as exc:
                        if exc.status != 404:
                            raise
                        last_error = "bootstrap Service allocation pending"
                    else:
                        if service.spec.cluster_ip not in (None, "", "None"):
                            return client, core, batch
                        last_error = "bootstrap Service has no allocated ClusterIP"
        time.sleep(1)
    raise AssertionError(f"disposable k3s did not become ready: {last_error}")


def _docker(*arguments: str) -> str:
    result = subprocess.run(
        ["docker", *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _docker_platform() -> str:
    architecture = _docker("info", "--format", "{{.Architecture}}")
    normalized = {"aarch64": "arm64", "arm64": "arm64", "x86_64": "amd64"}.get(
        architecture,
        architecture,
    )
    return f"linux/{normalized}"


def _build_image(*, tag: str, dockerfile: str, platform: str) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    _docker(
        "build",
        "--platform",
        platform,
        "--file",
        str(repo_root / dockerfile),
        "--tag",
        tag,
        str(repo_root),
    )


@pytest.fixture(scope="module")
def built_images() -> object:
    """Build each disposable-cluster image once per module; every case imports its own copy."""
    suffix = uuid4().hex[:10]
    platform = _docker_platform()
    images = SimpleNamespace(
        platform=platform,
        runtime=f"docker.io/library/loom-runtime-e2e:{suffix}",
        fixture=f"docker.io/library/loom-runtime-fixture:{suffix}",
    )
    try:
        _build_image(tag=images.runtime, dockerfile="deploy/Dockerfile.execution-runtime", platform=platform)
        _build_image(tag=images.fixture, dockerfile="tests/fixtures/execution_runtime_fixture/Dockerfile",
                     platform=platform)
        yield images
    finally:
        for tag in (images.runtime, images.fixture):
            subprocess.run(["docker", "image", "rm", "--force", tag], check=False, capture_output=True)


def _runtime_binary_digest(tag: str, root: Path, platform: str) -> str:
    container_id = _docker("create", "--platform", platform, tag)
    destination = root / "loom-execution-runtime"
    try:
        _docker("cp", f"{container_id}:/loom-execution-runtime", str(destination))
    finally:
        _docker("rm", "--force", container_id)
    return "sha256:" + hashlib.sha256(destination.read_bytes()).hexdigest()


def _import_image(container: object, *, tag: str, root: Path, ordinal: int) -> str:
    archive = root / f"image-{ordinal}.tar"
    _docker("save", "--output", str(archive), tag)
    container_id = container.get_wrapped_container().id
    remote = f"/tmp/loom-image-{ordinal}.tar"
    _docker("cp", str(archive), f"{container_id}:{remote}")
    result = container.exec(["ctr", "images", "import", remote])
    if result.exit_code != 0:
        raise AssertionError(result.output.decode("utf-8", errors="replace"))
    listing = container.exec(["ctr", "images", "ls"])
    if listing.exit_code != 0:
        raise AssertionError(listing.output.decode("utf-8", errors="replace"))
    for line in listing.output.decode("utf-8").splitlines():
        fields = line.split()
        if fields and fields[0] == tag and len(fields) >= 3:
            pinned = tag.rsplit(":", 1)[0] + "@" + fields[2]
            tagged = container.exec(["ctr", "images", "tag", tag, pinned])
            if tagged.exit_code != 0:
                raise AssertionError(tagged.output.decode("utf-8", errors="replace"))
            return pinned
    raise AssertionError(f"imported image {tag} is absent from k3s inventory")


async def _wait_for_dns_pods(core: object, *, timeout: float = 60) -> list[object]:
    deadline = time.monotonic() + timeout
    last_error: MaxRetryError | None = None
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            pods = (
                await asyncio.to_thread(
                    core.list_namespaced_pod,
                    "kube-system",
                    label_selector="k8s-app=kube-dns",
                    _request_timeout=min(5, remaining),
                )
            ).items
        except MaxRetryError as error:
            # A newly started API server can close TLS after discovery succeeds.
            # Only this read-only setup probe tolerates the observed EOF; all
            # authority errors, writes and network-policy assertions fail normally.
            reason = error.reason
            if not (
                isinstance(reason, SSLError)
                and reason.args
                and isinstance(reason.args[0], ssl.SSLEOFError)
            ):
                raise
            last_error = error
        else:
            if pods:
                return pods
            last_error = None
        await asyncio.sleep(min(0.25, max(0, deadline - time.monotonic())))
    raise AssertionError("disposable k3s did not create a CoreDNS Pod") from last_error


def _wait_for_pod(core: object, namespace: str, name: str) -> object:
    deadline = time.monotonic() + 60
    last_phase = "missing"
    while time.monotonic() < deadline:
        pod = core.read_namespaced_pod(name, namespace)
        last_phase = pod.status.phase
        if last_phase == "Running" and any(
            condition.type == "Ready" and condition.status == "True"
            for condition in (pod.status.conditions or [])
        ):
            return pod
        time.sleep(0.25)
    raise AssertionError(
        f"Pod {namespace}/{name} did not become ready: {last_phase}; "
        f"reason={pod.status.reason}; message={pod.status.message}; "
        f"conditions={pod.status.conditions}"
    )


def _pod_probe(core: object, namespace: str, name: str, url: str) -> str:
    from kubernetes.stream import stream

    return str(
        stream(
            core.connect_get_namespaced_pod_exec,
            name,
            namespace,
            command=["/fixture", "probe-report", url],
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False,
        )
    )


async def _wait_for_allowed_peer(
    core: object, namespace: str, name: str, url: str, *, timeout: float = 30
) -> str:
    # Pod Ready does not establish Service DNS/endpoints/dataplane convergence.
    deadline = time.monotonic() + timeout
    while True:
        result = await asyncio.to_thread(_pod_probe, core, namespace, name, url)
        if "exit:0" in result:
            return result
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"allowed peer {namespace}/{name} {url} did not become reachable: {result}")
        await asyncio.sleep(min(0.25, remaining))


def _policy_programming_gaps(
    saved_rules: str, expected: dict[str, tuple[str, tuple[str, ...]]]
) -> list[str]:
    """Observe pinned kube-router installation, independently of traffic outcomes."""
    chains: dict[str, list[list[str]]] = {}
    for line in saved_rules.splitlines():
        if line.startswith("-A "):
            rule = shlex.split(line)
            chains.setdefault(rule[1], []).append(rule[2:])

    def option(rule: list[str], name: str) -> str | None:
        return rule[rule.index(name) + 1] if name in rule else None

    if not any(option(rule, "-j") == "KUBE-ROUTER-FORWARD" for rule in chains.get("FORWARD", [])):
        return ["FORWARD has no kube-router hook"]
    missing = []
    forward = chains.get("KUBE-ROUTER-FORWARD", [])
    for name, (ip, policies) in expected.items():
        candidates = {
            option(rule, "-j") for rule in forward
            if option(rule, "-s") == f"{ip}/32" and (option(rule, "-j") or "").startswith("KUBE-POD-FW-")
        }
        installed = False
        for chain in candidates:
            rules = chains.get(chain, [])
            comments = {
                option(rule, "--comment") for rule in rules
                if (option(rule, "-j") or "").startswith("KUBE-NWPLCY-")
            }
            installed = (
                any(option(rule, "-d") == f"{ip}/32" and option(rule, "-j") == chain for rule in forward)
                and any(option(rule, "-j") == "REJECT" for rule in rules)
                and all(f"run through nw policy {policy}" in comments for policy in policies)
            )
            if installed:
                break
        if not installed:
            missing.append(f"{name} ({ip}): Pod hook/policy attachment not installed")
    return missing


async def _wait_for_policy_programming(
    container: object, expected: dict[str, tuple[str, tuple[str, ...]]], *, timeout: float = 30,
) -> None:
    # Pod Ready does not acknowledge kube-router's asynchronous rule programming.
    # This checks converged policy behavior, not isolation from Pod creation time.
    deadline = time.monotonic() + timeout
    while True:
        result = await asyncio.wait_for(
            asyncio.to_thread(container.exec, ["iptables-save", "-t", "filter"]), timeout=5,
        )
        rules = result.output.decode("utf-8", errors="replace")
        if result.exit_code != 0:
            raise AssertionError(f"cannot inspect disposable k3s policy rules: {rules[-65536:]}")
        gaps = _policy_programming_gaps(rules, expected)
        if not gaps:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"kube-router programming did not converge: {gaps}\nlast filter snapshot:\n{rules[-65536:]}")
        await asyncio.sleep(min(0.25, remaining))


async def test_allowed_peer_waits_for_service_convergence(monkeypatch: pytest.MonkeyPatch) -> None:
    probes = iter(("exit:1 reason:dns", "exit:1 reason:network", "exit:0"))
    monkeypatch.setattr(__name__ + "._pod_probe", lambda *args: next(probes))
    assert (
        await _wait_for_allowed_peer(None, "test", "client", "http://gateway", timeout=2)
        == "exit:0"
    )


async def test_allowed_peer_failure_is_bounded_and_retains_cause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(__name__ + "._pod_probe", lambda *args: "exit:1 reason:timeout")
    with pytest.raises(AssertionError, match="reason:timeout"):
        await _wait_for_allowed_peer(None, "test", "client", "http://gateway", timeout=0.01)


_PROGRAMMED_POLICY = '''-A FORWARD -j KUBE-ROUTER-FORWARD
-A KUBE-ROUTER-FORWARD -s 10.42.0.8/32 -j KUBE-POD-FW-CLIENT
-A KUBE-ROUTER-FORWARD -d 10.42.0.8/32 -j KUBE-POD-FW-CLIENT
-A KUBE-POD-FW-CLIENT -m comment --comment "run through nw policy deny" -j KUBE-NWPLCY-DENY
-A KUBE-POD-FW-CLIENT -m comment --comment "run through nw policy egress" -j KUBE-NWPLCY-EGRESS
-A KUBE-POD-FW-CLIENT -m mark ! --mark 0x10000/0x10000 -j REJECT
'''
_EXPECTED_POLICY = {"client": ("10.42.0.8", ("deny", "egress"))}


@pytest.mark.parametrize("missing", ["FORWARD", "-s 10.42", "-d 10.42", 'policy deny"', 'policy egress"', "-j REJECT"])
def test_policy_readiness_rejects_partial_installation(missing: str) -> None:
    incomplete = "\n".join(line for line in _PROGRAMMED_POLICY.splitlines() if missing not in line)
    assert _policy_programming_gaps(incomplete, _EXPECTED_POLICY)
    assert not _policy_programming_gaps(_PROGRAMMED_POLICY, _EXPECTED_POLICY)


def test_policy_readiness_does_not_accept_another_pods_rules() -> None:
    assert _policy_programming_gaps(_PROGRAMMED_POLICY.replace("10.42.0.8", "10.42.0.9"), _EXPECTED_POLICY)
    assert _policy_programming_gaps(_PROGRAMMED_POLICY, {**_EXPECTED_POLICY, "other": ("10.42.0.9", ("deny",))})


async def test_policy_readiness_waits_for_installation_without_traffic_probes(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshots = iter((b"*filter\nCOMMIT", _PROGRAMMED_POLICY.encode()))

    def inspect(command):
        assert command == ["iptables-save", "-t", "filter"]
        return SimpleNamespace(exit_code=0, output=next(snapshots))

    def no_probe(*args):
        pytest.fail("readiness must not learn by retrying forbidden traffic")

    monkeypatch.setattr(__name__ + "._pod_probe", no_probe)
    await _wait_for_policy_programming(SimpleNamespace(exec=inspect), _EXPECTED_POLICY, timeout=1)


@pytest.mark.parametrize("exit_code", [0, 1])
async def test_policy_readiness_failure_preserves_bounded_rules(exit_code: int) -> None:
    container = SimpleNamespace(exec=lambda command: SimpleNamespace(exit_code=exit_code, output=b"#" * 70000 + b"tail-evidence"))
    with pytest.raises(AssertionError, match="tail-evidence") as error:
        await _wait_for_policy_programming(container, _EXPECTED_POLICY, timeout=0.01)
    assert len(str(error.value)) < 67000


async def test_actuator_api_converges_against_disposable_k3s() -> None:
    from kubernetes import client

    container = _start_k3s()
    try:
        client_module, core, batch = await asyncio.to_thread(_load_client, container)
        namespace = f"loom-actuator-{uuid4().hex[:8]}"
        await asyncio.to_thread(
            core.create_namespace,
            client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)),
        )
        await asyncio.to_thread(
            core.create_namespaced_service_account,
            namespace,
            client.V1ServiceAccount(
                metadata=client.V1ObjectMeta(name="loom-execution-attempt"),
                automount_service_account_token=False,
            ),
        )
        api = InClusterKubernetesJobApi(
            client_module=client_module,
            batch_api=batch,
            core_api=core,
        )
        lease = _lease(namespace)
        target = ExecutionTargetRuntime(
            target_id="disposable-k3s",
            namespace=namespace,
            runtime_class_name="loom-sandbox",
        )
        manifest = render_execution_job(lease, target=target)
        manifest["spec"]["suspend"] = True

        created = await api.create_job(namespace=namespace, manifest=manifest)
        assert created.job_uid
        assert created.normalized_state == "pending"
        assert await api.get_job(namespace=namespace, job_name=lease.job_name) == created
        listed = await api.list_jobs(
            namespace=namespace,
            label_selector="app.kubernetes.io/managed-by=loom-execution-actuator",
        )
        assert [item.job_uid for item in listed.observations] == [created.job_uid]
        assert listed.rejected_count == 0

        watch_task = asyncio.create_task(
            api.watch_jobs(
                namespace=namespace,
                label_selector="app.kubernetes.io/managed-by=loom-execution-actuator",
                resource_version=created.resource_version,
                timeout_seconds=3,
            )
        )
        await asyncio.sleep(0.25)
        await asyncio.to_thread(
            batch.patch_namespaced_job,
            lease.job_name,
            namespace,
            {"metadata": {"annotations": {"loom.openai.com/conformance": "observed"}}},
        )
        watched = await watch_task
        assert watched
        assert watched[-1].job_uid == created.job_uid

        await api.delete_job(
            namespace=namespace,
            job_name=lease.job_name,
            expected_uid=created.job_uid,
            grace_period_seconds=0,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if await api.get_job(namespace=namespace, job_name=lease.job_name) is None:
                break
            await asyncio.sleep(0.25)
        else:
            raise AssertionError("Kubernetes Job did not disappear after exact-UID deletion")
    finally:
        await asyncio.to_thread(container.stop)


@pytest.mark.timeout(180)
async def test_attempt_network_policy_allows_only_dns_and_gateway(built_images: SimpleNamespace) -> None:
    from kubernetes import client, utils

    fixture_tag = built_images.fixture
    container = None
    try:
        with tempfile.TemporaryDirectory(prefix="loom-network-k3s-") as temporary:
            root = Path(temporary)
            container = await asyncio.to_thread(_start_k3s)
            _, core, _ = await asyncio.to_thread(_load_client, container)
            dns_pods = await _wait_for_dns_pods(core)
            await asyncio.to_thread(
                _wait_for_pod,
                core,
                "kube-system",
                dns_pods[0].metadata.name,
            )
            image_ref = await asyncio.to_thread(
                _import_image,
                container,
                tag=fixture_tag,
                root=root,
                ordinal=1,
            )
            attempt_namespace = "loom-nebius-development"
            platform_namespace = "loom"
            for namespace in (attempt_namespace, platform_namespace):
                await asyncio.to_thread(
                    core.create_namespace,
                    client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)),
                )
                await asyncio.to_thread(
                    core.create_namespaced_service_account,
                    namespace,
                    client.V1ServiceAccount(
                        metadata=client.V1ObjectMeta(name="network-fixture"),
                        automount_service_account_token=False,
                    ),
                )

            repo_root = Path(__file__).resolve().parents[2]
            api_client = client.ApiClient()
            attempt_documents = yaml.safe_load_all(
                (repo_root / "deploy/k8s/nebius-execution-actuator.yaml").read_text()
            )
            for document in attempt_documents:
                if document and document.get("kind") == "NetworkPolicy":
                    await asyncio.to_thread(utils.create_from_dict, api_client, document)
            platform_documents = yaml.safe_load_all(
                (repo_root / "deploy/k8s/network-policies.yaml").read_text()
            )
            for document in platform_documents:
                if (
                    document
                    and document.get("kind") == "NetworkPolicy"
                    and document["metadata"]["name"] in {"loom-llm-gateway", "loom-minio"}
                ):
                    await asyncio.to_thread(utils.create_from_dict, api_client, document)

            def pod(name: str, namespace: str, labels: dict[str, str], command: list[str]):
                return client.V1Pod(
                    metadata=client.V1ObjectMeta(
                        name=name,
                        namespace=namespace,
                        labels=labels,
                    ),
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        service_account_name="network-fixture",
                        automount_service_account_token=False,
                        containers=[
                            client.V1Container(
                                name="main",
                                image=image_ref,
                                image_pull_policy="IfNotPresent",
                                command=command,
                            )
                        ],
                    ),
                )

            pods = (
                pod(
                    "gateway",
                    platform_namespace,
                    {"app": "loom-llm-gateway"},
                    ["/fixture", "server", "9100"],
                ),
                pod(
                    "object-store",
                    platform_namespace,
                    {"app": "loom-minio"},
                    ["/fixture", "server", "9000"],
                ),
                pod(
                    "blocked-service",
                    platform_namespace,
                    {"app": "blocked-service"},
                    ["/fixture", "server", "8080"],
                ),
                pod(
                    "execution-client",
                    attempt_namespace,
                    {"app.kubernetes.io/component": "execution-unit"},
                    ["/fixture", "idle"],
                ),
                pod(
                    "execution-server",
                    attempt_namespace,
                    {"app.kubernetes.io/component": "execution-unit"},
                    ["/fixture", "server", "8080"],
                ),
                pod(
                    "probe",
                    attempt_namespace,
                    {"app": "probe"},
                    ["/fixture", "idle"],
                ),
            )
            for item in pods:
                await asyncio.to_thread(
                    core.create_namespaced_pod,
                    item.metadata.namespace,
                    item,
                )
            for name, selector, port in (
                ("gateway", {"app": "loom-llm-gateway"}, 9100),
                ("object-store", {"app": "loom-minio"}, 9000),
                ("blocked-service", {"app": "blocked-service"}, 8080),
            ):
                await asyncio.to_thread(
                    core.create_namespaced_service,
                    platform_namespace,
                    client.V1Service(
                        metadata=client.V1ObjectMeta(name=name),
                        spec=client.V1ServiceSpec(
                            selector=selector,
                            ports=[client.V1ServicePort(port=port, target_port=port)],
                        ),
                    ),
                )
            ready = {
                item.metadata.name: await asyncio.to_thread(
                    _wait_for_pod,
                    core,
                    item.metadata.namespace,
                    item.metadata.name,
                )
                for item in pods
            }
            attempt_policies = ("loom-execution-attempt-default-deny", "loom-execution-attempt-egress")
            expected_policies = {
                "execution-client": attempt_policies,
                "execution-server": attempt_policies,
                "gateway": ("loom-llm-gateway",),
                "object-store": ("loom-minio",),
            }
            await _wait_for_policy_programming(container, {
                name: (ready[name].status.pod_ip, policies) for name, policies in expected_policies.items()
            })
            # A denied connection is meaningful only when the target is serving.
            # Loopback proves this without depending on the policy under test.
            for name, namespace, port in (
                ("object-store", platform_namespace, 9000),
                ("blocked-service", platform_namespace, 8080),
                ("execution-server", attempt_namespace, 8080),
            ):
                await _wait_for_allowed_peer(core, namespace, name, f"http://127.0.0.1:{port}")
            allowed = await _wait_for_allowed_peer(
                core,
                attempt_namespace,
                "execution-client",
                "http://gateway.loom.svc.cluster.local:9100",
            )
            direct_gateway = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                f"http://{ready['gateway'].status.pod_ip}:9100",
            )
            assert "exit:0" in direct_gateway, f"direct Gateway peer was denied: {direct_gateway}"
            assert "exit:0" in allowed, f"DNS Gateway peer was denied: {allowed}"
            object_store = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                "http://object-store.loom.svc.cluster.local:9000",
            )
            assert "exit:0" not in object_store
            blocked = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                "http://blocked-service.loom.svc.cluster.local:8080",
            )
            assert "exit:0" not in blocked
            # Also use Pod IPs: a Service with unprogrammed endpoints must not
            # make the forbidden-peer assertions pass for the wrong reason.
            for name, port in (("object-store", 9000), ("blocked-service", 8080)):
                direct = await asyncio.to_thread(
                    _pod_probe, core, attempt_namespace, "execution-client",
                    f"http://{ready[name].status.pod_ip}:{port}",
                )
                assert "exit:0" not in direct, f"forbidden direct peer {name} was reachable: {direct}"
            public = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "execution-client",
                "http://1.1.1.1:80",
            )
            assert "exit:0" not in public
            execution_ip = ready["execution-server"].status.pod_ip
            ingress = await asyncio.to_thread(
                _pod_probe,
                core,
                attempt_namespace,
                "probe",
                f"http://{execution_ip}:8080",
            )
            assert "exit:0" not in ingress
    except AssertionError as error:
        if container is not None:
            try:
                snapshot = await asyncio.wait_for(
                    asyncio.to_thread(container.exec, ["iptables-save", "-t", "filter"]), timeout=5,
                )
                error.add_note(f"filter snapshot exit={snapshot.exit_code}:\n{snapshot.output[-65536:].decode(errors='replace')}")
            except Exception as diagnostic_error:
                error.add_note(f"filter snapshot unavailable: {type(diagnostic_error).__name__}")
        raise
    finally:
        if container is not None:
            await asyncio.to_thread(container.stop)


@pytest.mark.timeout(360)
@pytest.mark.parametrize("prepared_fixture,termination", [(False, None), (True, "deadline"), (True, "fixture_exit")],
                         ids=["trusted-sidecar", "prepared-fixture-deadline", "prepared-fixture-exit"])
async def test_runtime_executes_task_native_sidecar_and_verifier_without_docker_socket(
    prepared_fixture: bool, termination: str | None, built_images: SimpleNamespace,
) -> None:
    from kubernetes import client

    suffix = uuid4().hex[:10]
    runtime_tag, fixture_tag, platform = built_images.runtime, built_images.fixture, built_images.platform
    container = None
    try:
        with tempfile.TemporaryDirectory(prefix="loom-runtime-k3s-") as temporary:
            root = Path(temporary)
            runtime_binary_sha256 = await asyncio.to_thread(
                _runtime_binary_digest, runtime_tag, root, platform
            )
            container = await asyncio.to_thread(_start_k3s)
            client_module, core, batch = await asyncio.to_thread(_load_client, container)
            runtime_image_ref = await asyncio.to_thread(
                _import_image,
                container,
                tag=runtime_tag,
                root=root,
                ordinal=1,
            )
            task_image_ref = await asyncio.to_thread(
                _import_image,
                container,
                tag=fixture_tag,
                root=root,
                ordinal=2,
            )
            namespace = f"loom-runtime-{suffix}"
            await asyncio.to_thread(
                core.create_namespace,
                client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace)),
            )
            await asyncio.to_thread(
                core.create_namespaced_service_account,
                namespace,
                client.V1ServiceAccount(
                    metadata=client.V1ObjectMeta(name="loom-execution-attempt"),
                    automount_service_account_token=False,
                ),
            )
            await asyncio.to_thread(
                core.create_namespaced_pod,
                namespace,
                client.V1Pod(
                    metadata=client.V1ObjectMeta(
                        name="execution-broker",
                        labels={"app": "execution-broker"},
                    ),
                    spec=client.V1PodSpec(
                        restart_policy="Never",
                        service_account_name="loom-execution-attempt",
                        automount_service_account_token=False,
                        containers=[
                            client.V1Container(
                                name="broker",
                                image=task_image_ref,
                                image_pull_policy="IfNotPresent",
                                command=["/fixture", "broker"],
                            )
                        ],
                    ),
                ),
            )
            await asyncio.to_thread(
                core.create_namespaced_service,
                namespace,
                client.V1Service(
                    metadata=client.V1ObjectMeta(name="execution-broker"),
                    spec=client.V1ServiceSpec(
                        selector={"app": "execution-broker"},
                        ports=[client.V1ServicePort(port=9100, target_port=9100)],
                    ),
                ),
            )
            await asyncio.to_thread(_wait_for_pod, core, namespace, "execution-broker")
            node_api = client.NodeV1Api()
            await asyncio.to_thread(
                node_api.create_runtime_class,
                client.V1RuntimeClass(
                    metadata=client.V1ObjectMeta(name="loom-sandbox"),
                    handler="runc",
                ),
            )
            api = InClusterKubernetesJobApi(
                client_module=client_module,
                batch_api=batch,
                core_api=core,
            )
            lease = _executable_lease(
                namespace,
                task_image_ref=task_image_ref,
                runtime_image_ref=runtime_image_ref,
                runtime_binary_sha256=runtime_binary_sha256, prepared_fixture=prepared_fixture,
            )
            manifest = render_execution_job(
                lease,
                target=ExecutionTargetRuntime(
                    target_id="disposable-k3s",
                    namespace=namespace,
                    runtime_class_name="loom-sandbox",
                    credential_broker_url=(
                        f"http://execution-broker.{namespace}.svc.cluster.local:9100"
                        "/internal/service-execution"
                    ),
                ),
            )
            await api.create_job(namespace=namespace, manifest=manifest)

            deadline = time.monotonic() + 120
            inspect_after = time.monotonic() + 15
            observation = None
            while time.monotonic() < deadline:
                observation = await api.get_job(
                    namespace=namespace,
                    job_name=lease.job_name,
                )
                if observation is not None and observation.normalized_state == "succeeded":
                    break
                if observation is not None and observation.normalized_state in {
                    "failed",
                    "oom_killed",
                    "evicted",
                    "node_lost",
                    "deadline_exceeded",
                    "image_pull_backoff",
                }:
                    failed_pods = await asyncio.to_thread(
                        core.list_namespaced_pod,
                        namespace,
                        label_selector=f"loom.openai.com/lease-id={lease.id}",
                    )
                    details = None
                    logs: dict[str, str] = {}
                    if failed_pods.items:
                        failed_pod = failed_pods.items[0]
                        details = client.ApiClient().sanitize_for_serialization(failed_pod.status)
                        for container_name in (
                            "runtime-materializer",
                            *(item.role_name for item in ExecutionRuntimePlanV1.model_validate(lease.runtime_contract_json).sidecars),
                            "execution",
                        ):
                            try:
                                logs[container_name] = await asyncio.to_thread(
                                    core.read_namespaced_pod_log,
                                    failed_pod.metadata.name,
                                    namespace,
                                    container=container_name,
                                )
                            except Exception as exc:
                                logs[container_name] = f"unavailable: {exc}"
                    raise AssertionError(
                        f"runtime Job failed: {observation} pod={details} logs={logs}"
                    )
                if time.monotonic() >= inspect_after:
                    current_pods = await asyncio.to_thread(
                        core.list_namespaced_pod,
                        namespace,
                        label_selector=f"loom.openai.com/lease-id={lease.id}",
                    )
                    if current_pods.items:
                        current = current_pods.items[0]
                        statuses = [
                            *(current.status.init_container_statuses or []),
                            *(current.status.container_statuses or []),
                        ]
                        failed = [
                            status
                            for status in statuses
                            if status.name in {"runtime-materializer", "execution"}
                            if status.state.terminated is not None
                            and status.state.terminated.exit_code != 0
                        ]
                        blocked = [
                            status
                            for status in statuses
                            if status.state.waiting is not None
                            and status.state.waiting.reason
                            not in {"ContainerCreating", "PodInitializing"}
                        ]
                        if failed or blocked:
                            details = client.ApiClient().sanitize_for_serialization(current.status)
                            raise AssertionError(f"runtime Pod failed closed: {details}")
                    inspect_after = time.monotonic() + 5
                await asyncio.sleep(0.5)
            else:
                current_pods = await asyncio.to_thread(
                    core.list_namespaced_pod,
                    namespace,
                    label_selector=f"loom.openai.com/lease-id={lease.id}",
                )
                details = (
                    client.ApiClient().sanitize_for_serialization(current_pods.items[0].status)
                    if current_pods.items
                    else None
                )
                raise AssertionError(
                    f"runtime Job did not succeed: observation={observation} pod={details}"
                )

            pods = await asyncio.to_thread(
                core.list_namespaced_pod,
                namespace,
                label_selector=f"loom.openai.com/lease-id={lease.id}",
            )
            assert len(pods.items) == 1
            assert observation is not None
            assert observation.termination_summary is not None
            assert observation.termination_summary.output_committed is True
            pod = pods.items[0]
            pod_dict = client.ApiClient().sanitize_for_serialization(pod)
            assert "/var/run/docker.sock" not in str(pod_dict)
            assert "hostPath" not in str(pod_dict)
            expected_roles = (["fixture-server", "task-sandbox", "verifier-sandbox"]
                              if prepared_fixture else ["service-sidecar"])
            assert [item.name for item in pod.spec.init_containers] == ["runtime-materializer", *expected_roles]
            assert all(item.state.terminated is not None for item in pod.status.init_container_statuses)
            if prepared_fixture:
                assert not pod.spec.init_containers[1].volume_mounts
                assert pod.spec.init_containers[1].security_context.run_as_user == 65532
                assert not pod.spec.host_aliases
            logs = await asyncio.to_thread(
                core.read_namespaced_pod_log,
                pod.metadata.name,
                namespace,
                container="execution",
            )
            assert "fixture-phase=setup" in logs
            assert "fixture-phase=agent" in logs
            assert "fixture-phase=verifier" in logs
            if prepared_fixture:
                # A Job deadline must stop this native fixture together with
                # both private sandboxes, even while the controller is idle.
                expired = _executable_lease(namespace, task_image_ref=task_image_ref,
                    runtime_image_ref=runtime_image_ref, runtime_binary_sha256=runtime_binary_sha256,
                    prepared_fixture=True)
                expired_plan = ExecutionRuntimePlanV1.model_validate(expired.runtime_contract_json)
                expired_plan = expired_plan.model_copy(update={
                    "setup": (), "termination_grace_seconds": 2,
                    "main": expired_plan.main.model_copy(update={"argv": ("/fixture", "idle"), "timeout_seconds": 60}),
                })
                if termination == "fixture_exit":
                    expired_plan = expired_plan.model_copy(update={"sidecars": (
                        expired_plan.sidecars[0].model_copy(update={"argv": ("/fixture", "crashing-sidecar")}),
                        *expired_plan.sidecars[1:],
                    )})
                expired.runtime_contract_json = expired_plan.canonical_payload()
                expired.runtime_contract_sha256 = canonical_digest(expired.runtime_contract_json)
                expired.deadline_at = datetime.now(UTC) + timedelta(seconds=25 if termination == "deadline" else 90)
                expired_manifest = render_execution_job(expired, target=ExecutionTargetRuntime(
                    target_id="disposable-k3s", namespace=namespace, runtime_class_name="loom-sandbox",
                    credential_broker_url=f"http://execution-broker.{namespace}.svc.cluster.local:9100/internal/service-execution",
                ))
                await api.create_job(namespace=namespace, manifest=expired_manifest)
                deadline = time.monotonic() + 90
                fixture_started = False
                while time.monotonic() < deadline:
                    observation = await api.get_job(namespace=namespace, job_name=expired.job_name)
                    current = await asyncio.to_thread(core.list_namespaced_pod, namespace,
                        label_selector=f"loom.openai.com/lease-id={expired.id}")
                    for item in current.items:
                        fixture_started |= any(status.name == "fixture-server" and status.state.running is not None
                                               for status in (item.status.init_container_statuses or []))
                    expected_state = "deadline_exceeded" if termination == "deadline" else "failed"
                    if observation is not None and observation.normalized_state == expected_state:
                        break
                    await asyncio.sleep(.5)
                else:
                    raise AssertionError(f"fixture Job did not report {termination}: {observation}")
                expired_pods = await asyncio.to_thread(core.list_namespaced_pod, namespace,
                    label_selector=f"loom.openai.com/lease-id={expired.id}")
                assert fixture_started, "failure test never started its native fixture"
                if termination == "fixture_exit":
                    assert observation.reason in {"SandboxRestarted", "SandboxTerminated"}
                    diagnostic = next(item for item in observation.container_diagnostics if item.name == "fixture-server")
                    ending = diagnostic.previous_termination or diagnostic.current_termination
                    assert ending is not None and ending.exit_code == 73
                # Kubernetes may already have deleted the failed Pod. If it
                # retains the terminal Pod, every native process must be stopped.
                for item in expired_pods.items:
                    statuses = item.status.init_container_statuses
                    assert {status.name for status in statuses} == {"runtime-materializer", *expected_roles}
                    if termination == "deadline":
                        assert all(status.state.terminated is not None for status in statuses)
                assert observation.job_uid is not None
                await api.delete_job(namespace=namespace, job_name=expired.job_name,
                    expected_uid=observation.job_uid, grace_period_seconds=2)
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    remaining = await asyncio.to_thread(core.list_namespaced_pod, namespace,
                        label_selector=f"loom.openai.com/lease-id={expired.id}")
                    if not remaining.items and await api.get_job(namespace=namespace, job_name=expired.job_name) is None:
                        break
                    await asyncio.sleep(.5)
                else:
                    raise AssertionError("fixture Job resources survived UID-bound deletion")
    finally:
        if container is not None:
            await asyncio.to_thread(container.stop)
