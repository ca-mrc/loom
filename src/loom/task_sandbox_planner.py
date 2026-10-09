"""Harness-neutral planner for task-sandbox execution plans (#2296).

Every workspace harness runs as a trusted controller phase that drives one or
two private sandboxes in the task image. This module owns the platform side of
that plan: sandbox sidecars, sockets, probes, identities and resources;
verification topology; the existing QEMU guest extension; frozen network/egress
attachment; and common outputs. The harness specification supplies only its
controller binding and native outputs.

The planner never resolves harness names, network policy or lease scheduling.
Callers pass resolved inputs and receive one immutable `ExecutionRuntimePlanV1`.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal
from uuid import UUID

from loom.execution_contract import effective_guest_capabilities, nebius_guest_execution_class
from loom.execution_image_admission import ExecutionImageAdmissionBundleV1
from loom.execution_requirements import ALL_GUEST_EXECUTION_CAPABILITIES, GuestExecutionCapability
from loom.execution_runtime_contract import (
    TASK_EGRESS_OUTPUT,
    ContainerResourcesV1,
    ExecutionResourceRequestsV1,
    ExecutionRuntimePlanV1,
    GuestExecutionV1,
    ProbeV1,
    ProcessPhaseV1,
    RuntimeHandoffInputV1,
    RuntimeOutputDeclarationV1,
    RuntimeSetupCacheV1,
    RuntimeTaskInputV1,
    SidecarContainerV1,
)
from loom.hosted_harness import (
    HARNESS_SETUP_PHASE,
    MAX_SETUP_CACHE_BYTES,
    SANDBOX_CONTROLLER_MODULE,
    HostedHarnessSpec,
)
from loom.models.networking import NetworkPolicy, WebAllowlist, WebDestination, hosted_http_egress
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.sandbox_identity import resolve_sandbox_identity
from loom.verifier_runtime import resolve_verifier_env_mode

if TYPE_CHECKING:
    from loom.service_execution_materialization import (
        ServiceExecutionInputBindingV1,
        ServiceExecutionRuntimeProfileV1,
    )

SANDBOX_RUNTIME_BINARY = "/loom/bin/loom-sandbox-runtime"
TASK_SANDBOX = "task-sandbox"
VERIFIER_SANDBOX = "verifier-sandbox"
VERIFY_SANDBOX_PHASE = "verify-sandbox"
# Guest disk preparation (30s) and boot (90s) need room to complete before
# Kubernetes treats a healthy slow sandbox as failed.
_GUEST_STARTUP_FAILURE_THRESHOLD = 75
_MIN_SANDBOX_EXEC_LIMIT_SECONDS = 900
# Outputs a deferred verifier produces; everything else belongs to the attempt.
_VERIFIER_OWNED_OUTPUTS = frozenset({
    "diagnostics/verifier-exception.json", "verifier/output.json", "artifacts/verifier/ctrf.json",
})


def guest_capabilities(task: TaskConfig) -> frozenset[GuestExecutionCapability]:
    """Guest capabilities the task declares; any selects the QEMU guest extension."""
    declared = task.environment.execution_requirements
    return ALL_GUEST_EXECUTION_CAPABILITIES.intersection(declared.capabilities if declared else ())


def sandbox_phase_argv(mode: str, module: str = SANDBOX_CONTROLLER_MODULE) -> tuple[str, ...]:
    # Keep Python imports and dependency configuration discovery outside
    # user-controlled task inputs, including dependencies that inspect cwd.
    return ("python", "-I", "-m", module, mode, "--workspace", "/workspace")


def plan_admissions(
    profile: ServiceExecutionRuntimeProfileV1, refs: set[str | None],
) -> ExecutionImageAdmissionBundleV1:
    return ExecutionImageAdmissionBundleV1(
        schema_version=profile.image_admission.schema_version,
        admissions=tuple(
            item for item in profile.image_admission.admissions if item.statement.image_ref in refs
        ),
    )


@dataclass(frozen=True)
class TaskSandboxPlanRequest:
    """Resolved inputs for one task-sandbox plan; nothing here is re-derived."""

    spec: HostedHarnessSpec
    task: TaskConfig
    trial: TrialConfig
    profile: ServiceExecutionRuntimeProfileV1
    binding: ServiceExecutionInputBindingV1
    task_revision_sha256: str
    command_identity: str
    output_paths: tuple[str, ...]
    effective_network_policy: NetworkPolicy
    # Resolved by the caller from the harness spec's controller binding.
    controller_image: str
    task_image_materialization_id: UUID | None = None
    resource_requests: ExecutionResourceRequestsV1 | None = None


@dataclass(frozen=True)
class _Topology:
    guest_execution: GuestExecutionV1 | None
    shared: bool
    retained_services: bool

    @property
    def colocated_verifier(self) -> bool:
        # Guest launch keeps both sandboxes in this pod. Retained services
        # cannot survive the agent pod, so their verifier also grades beside
        # them. Separate grading otherwise defers the verifier until the agent
        # pod is gone (#2212).
        return self.shared or self.guest_execution is not None or self.retained_services

    @property
    def in_place_verifier(self) -> bool:
        return self.shared and self.guest_execution is None

    @property
    def sandbox_roles(self) -> tuple[str, ...]:
        fresh_verifier = self.guest_execution is not None or (self.colocated_verifier and not self.shared)
        return (TASK_SANDBOX, VERIFIER_SANDBOX) if fresh_verifier else (TASK_SANDBOX,)

    @property
    def verifier_output_required(self) -> bool:
        return self.shared or (self.colocated_verifier and self.guest_execution is None)


def _topology(task: TaskConfig, trial: TrialConfig) -> _Topology:
    capabilities = effective_guest_capabilities(task, trial)
    return _Topology(
        guest_execution=(GuestExecutionV1(capabilities=tuple(sorted(capabilities)))
                         if capabilities is not None else None),
        shared=resolve_verifier_env_mode(task, trial) == "shared",
        retained_services=task.environment.service_lifecycle is not None,
    )


def _timeouts(task: TaskConfig, trial: TrialConfig) -> tuple[float, float]:
    agent = (trial.override_agent_timeout_sec or task.agent.timeout_sec) * trial.agent_timeout_multiplier
    verifier = ((trial.override_verifier_timeout_sec or task.verifier.timeout_sec)
                * trial.verifier_timeout_multiplier)
    return agent, verifier


def _sandbox_sidecars(
    request: TaskSandboxPlanRequest, topology: _Topology, resources: ContainerResourcesV1,
) -> list[SidecarContainerV1]:
    from loom.task_fixtures import fixture_sidecars

    task, profile = request.task, request.profile
    env = task.environment
    assert env.docker_image
    agent_timeout, verifier_timeout = _timeouts(task, request.trial)
    setup_timeout = request.spec.setup.timeout_seconds if request.spec.setup is not None else 0
    exec_limit = str(math.ceil(max(_MIN_SANDBOX_EXEC_LIMIT_SECONDS, agent_timeout, verifier_timeout, setup_timeout)))
    sidecars = list(fixture_sidecars(task))
    for role in topology.sandbox_roles:
        socket = f"/loom/sandboxes/{role}/sandbox.sock"
        probe = ProbeV1(kind="exec", argv=(SANDBOX_RUNTIME_BINARY, "--check-socket", socket))
        startup_probe = (probe.model_copy(update={"failure_threshold": _GUEST_STARTUP_FAILURE_THRESHOLD})
                         if topology.guest_execution is not None else probe)
        user = (task.verifier.user if role == VERIFIER_SANDBOX and task.verifier.user is not None
                else env.user)
        sidecars.append(SidecarContainerV1(
            role_name=role, image_ref=env.docker_image,
            argv=(SANDBOX_RUNTIME_BINARY, "--socket", socket, "--exec-timeout-seconds", exec_limit),
            resources=resources, startup_probe=startup_probe, readiness_probe=probe, private_sandbox=True,
            identity=resolve_sandbox_identity(
                user, env.environment.get("HOME"),
                default_uid=profile.run_as_user, default_gid=profile.run_as_group,
            ),
            guest_execution=topology.guest_execution,
        ))
    return sidecars


def _output_declarations(request: TaskSandboxPlanRequest, topology: _Topology) -> tuple[RuntimeOutputDeclarationV1, ...]:
    task = request.task
    env = task.environment
    outputs = [RuntimeOutputDeclarationV1(
        source_path=f".loom/collected/{path}", relative_path=f"artifacts/{path}",
        kind="task_artifact", required=path in task.steps[0].required_artifacts,
    ) for path in request.output_paths]
    native = tuple(
        (item.source_path, item.relative_path, item.kind, item.required) for item in request.spec.native_outputs
    )
    for source, target, kind, required in (
        ("agent/trajectory.jsonl", "trajectory/events.jsonl", "trajectory", True),
        ("agent/usage.json", "accounting/usage.json", "usage", True),
        ("agent/exception.json", "diagnostics/agent-exception.json", "agent_native", False),
        ("verifier/exception.json", "diagnostics/verifier-exception.json", "verifier", False),
        *((("setup/exception.json", "diagnostics/setup-exception.json", "agent_native", False),)
          if request.spec.setup is not None else ()),
        *((("harness-cache/outcome.json", "diagnostics/harness-cache.json", "agent_native", False),)
          if request.spec.setup is not None and request.spec.setup.cacheable else ()),
        *native,
        ("workspace.tar", "artifacts/workspace.tar", "task_artifact", True),
        ("verifier/output.json", "verifier/output.json", "verifier", topology.verifier_output_required),
        ("verifier/ctrf.json", "artifacts/verifier/ctrf.json", "task_artifact", False),
    ):
        outputs.append(RuntimeOutputDeclarationV1(
            source_path=f".loom/{source}", relative_path=target, kind=kind, required=required,
        ))
    if env.service_lifecycle is not None:
        outputs.append(RuntimeOutputDeclarationV1(
            source_path=".loom/service-startup.json", relative_path="diagnostics/service-startup.json",
            kind="task_artifact", required=bool(env.service_lifecycle.startup_command),
        ))
    if env.workspace_reference_files:
        outputs.append(RuntimeOutputDeclarationV1(
            source_path=".loom/workspace-references.json",
            relative_path="artifacts/workspace-references.json", kind="task_artifact", required=True,
        ))
    if env.mutable_paths:
        for name in ("manifest.json", *(f"{index}.tar" for index in range(len(env.mutable_paths)))):
            outputs.append(RuntimeOutputDeclarationV1(
                source_path=f".loom/mutable-paths/{name}",
                relative_path=f"artifacts/mutable-paths/{name}", kind="task_artifact", required=True,
            ))
    # Task egress and setup-only install egress share one audited proxy.
    if hosted_http_egress(request.effective_network_policy) is not None or request.spec.setup is not None:
        outputs.insert(0, TASK_EGRESS_OUTPUT)
    return tuple(outputs)


def _controller_envelope(
    request: TaskSandboxPlanRequest, topology: _Topology, resources: ContainerResourcesV1,
    runtime_volume_mib: int,
) -> tuple[ContainerResourcesV1 | None, ExecutionResourceRequestsV1 | None]:
    profile, storage = request.profile, request.task.environment.storage_mb
    assert storage is not None
    controller = (ContainerResourcesV1(
        cpu_millis=profile.controller_resources.cpu_millis,
        memory_mib=profile.controller_resources.memory_mib,
        ephemeral_storage_mib=storage,
    ) if profile.controller_resources is not None else None)
    requests = request.resource_requests
    if topology.guest_execution is None:
        return controller, requests
    # The shared runtime payload occupies Pod ephemeral storage in addition to
    # both guest disks. Reserve it exactly once, in the controller envelope, so
    # rendering, finance and node-share placement use the same total.
    controller = ContainerResourcesV1.model_validate({
        **(controller or resources).model_dump(), "ephemeral_storage_mib": storage + runtime_volume_mib,
    })
    if requests is not None and requests.controller is not None:
        requests = requests.model_copy(update={"controller": requests.controller.model_copy(update={
            "ephemeral_storage_mib": requests.controller.ephemeral_storage_mib + runtime_volume_mib,
        })})
    return controller, requests


def _setup_egress(spec: HostedHarnessSpec) -> WebAllowlist | None:
    """The spec's install sources, frozen into the plan for setup phases only."""
    if spec.setup is None:
        return None
    return WebAllowlist(destinations=tuple(
        WebDestination(host=source.host, protocol=source.protocol) for source in spec.setup.sources
    ))


def _setup_cache(spec: HostedHarnessSpec) -> RuntimeSetupCacheV1 | None:
    if spec.setup is None or spec.setup.install_root is None:
        return None
    return RuntimeSetupCacheV1(
        identity_sha256=spec.setup.cache_identity(spec.name),
        install_root=spec.setup.install_root, max_bytes=MAX_SETUP_CACHE_BYTES,
    )


def compile_task_sandbox_plan(request: TaskSandboxPlanRequest) -> ExecutionRuntimePlanV1:
    task, trial, profile, spec = request.task, request.trial, request.profile, request.spec
    env = task.environment
    assert spec.workspace, "response-only harnesses never receive a task sandbox"
    assert env.docker_image and env.cpus and env.memory_mb and env.storage_mb
    topology = _topology(task, trial)
    guest = topology.guest_execution
    resources = ContainerResourcesV1(
        cpu_millis=round(env.cpus * 1000), memory_mib=env.memory_mb, ephemeral_storage_mib=env.storage_mb,
    )
    runtime_volume_mib = (profile.guest_runtime_volume_mib or profile.runtime_volume_mib
                          if guest is not None else profile.runtime_volume_mib)
    controller_resources, resource_requests = _controller_envelope(request, topology, resources, runtime_volume_mib)
    agent_timeout, verifier_timeout = _timeouts(task, trial)
    phase_env = {
        "LOOM_TASK_TRIAL_JSON": trial.model_dump_json(exclude_defaults=True),
        "LOOM_TASK_ARTIFACTS_JSON": json.dumps(list(request.output_paths)),
        "LOOM_EFFECTIVE_NETWORK_POLICY_JSON": json.dumps(
            request.effective_network_policy.model_dump(mode="json"), sort_keys=True, separators=(",", ":"),
        ),
    }
    # A frozen agent release may select an older controller than the candidate
    # qualified by the profile. Do not send that image a new input contract.
    if (profile.supports_task_artifact_inputs and request.controller_image == profile.agent_image_ref
            and len(phase_env["LOOM_TASK_ARTIFACTS_JSON"].encode("utf-8")) > ProcessPhaseV1.MAX_ENV_VALUE_BYTES):
        del phase_env["LOOM_TASK_ARTIFACTS_JSON"]
        phase_env["LOOM_TASK_ARTIFACTS_FROM_INPUT"] = "1"

    def phase(role: Literal["setup", "agent", "verifier"], mode: str, timeout: float) -> ProcessPhaseV1:
        return ProcessPhaseV1(
            role=role, argv=sandbox_phase_argv(mode, spec.controller_module),
            working_directory="/app", timeout_seconds=round(timeout), environment=phase_env,
        )

    published_refs: set[str | None] = {request.controller_image, profile.runtime_image_ref}
    if request.task_image_materialization_id is None:
        published_refs.add(env.docker_image)
    return ExecutionRuntimePlanV1(
        effective_network_policy=request.effective_network_policy,
        task_egress=hosted_http_egress(request.effective_network_policy),
        candidate_sha=profile.candidate_sha, task_revision_sha256=request.task_revision_sha256,
        command_identity_sha256=request.command_identity,
        execution_class_id=(nebius_guest_execution_class(
            supports_task_web_egress=profile.supports_task_web_egress,
            supports_emulated_pkcs11="emulated_pkcs11_authentication" in guest.capabilities,
        ).class_id if guest is not None else profile.execution_class_id),
        composition="init_payload", task_image_ref=env.docker_image,
        task_image_materialization_id=request.task_image_materialization_id,
        agent_image_ref=request.controller_image, runtime_image_ref=profile.runtime_image_ref,
        runtime_binary_sha256=profile.runtime_binary_sha256,
        image_admission=plan_admissions(profile, published_refs),
        run_as_user=profile.run_as_user, run_as_group=profile.run_as_group, fs_group=profile.fs_group,
        task_resources=resources,
        resource_requests=resource_requests,
        controller_resources=controller_resources,
        workspace_mib=env.storage_mb,
        runtime_volume_mib=runtime_volume_mib,
        termination_grace_seconds=profile.termination_grace_seconds,
        task_input=RuntimeTaskInputV1(
            manifest_sha256=request.binding.manifest_sha256, file_count=request.binding.file_count,
            total_bytes=request.binding.total_bytes,
        ),
        output_declarations=_output_declarations(request, topology),
        sidecars=tuple(_sandbox_sidecars(request, topology, resources)),
        setup=(
            (phase("setup", HARNESS_SETUP_PHASE, spec.setup.timeout_seconds),) if spec.setup is not None else ()
        ),
        setup_egress=_setup_egress(spec),
        setup_cache=_setup_cache(spec),
        main=phase("agent", spec.controller_phase, agent_timeout),
        verifier_execution="in_attempt" if topology.colocated_verifier else "separate_execution",
        verifier_after_agent_timeout=topology.in_place_verifier,
        in_place_verifier=topology.in_place_verifier,
        verifier=(phase("verifier", VERIFY_SANDBOX_PHASE, verifier_timeout)
                  if topology.colocated_verifier else None),
        max_log_bytes_per_stream=profile.max_log_bytes_per_stream,
        max_artifact_bytes=(profile.guest_max_artifact_bytes or profile.max_artifact_bytes
                            if guest is not None else profile.max_artifact_bytes),
    )


def compile_deferred_verifier_plan(
    agent_plan: ExecutionRuntimePlanV1, task: TaskConfig, *, verifier_timeout_seconds: int,
    handoff_input: RuntimeHandoffInputV1,
) -> ExecutionRuntimePlanV1:
    """Verifier pod that grades a committed workspace after the agent pod is gone.

    It keeps the attempt's prepared image, route and node share, so the freed
    agent capacity is admitted again for the verifier rather than enlarged.
    Only the plan shape; lease scheduling is #2212.
    """
    if agent_plan.execution_role != "attempt" or agent_plan.verifier_execution != "separate_execution":
        raise ValueError("only a separate-execution attempt defers its verifier")
    task_sandbox = next(sidecar for sidecar in agent_plan.sidecars if sidecar.role_name == TASK_SANDBOX)
    user = task.verifier.user if task.verifier.user is not None else task.environment.user
    identity = resolve_sandbox_identity(
        user, task.environment.environment.get("HOME"),
        default_uid=agent_plan.run_as_user, default_gid=agent_plan.run_as_group,
    )
    def verifier_argv(argv: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(
            f"/loom/sandboxes/{VERIFIER_SANDBOX}/sandbox.sock"
            if item == f"/loom/sandboxes/{TASK_SANDBOX}/sandbox.sock" else item
            for item in argv
        )

    # The renderer derives the mounted socket from role_name. Retarget the
    # runtime and both probes together while preserving the frozen attempt.
    verifier_sandbox = task_sandbox.model_copy(update={
        "role_name": VERIFIER_SANDBOX, "identity": identity,
        "argv": verifier_argv(task_sandbox.argv),
        "startup_probe": task_sandbox.startup_probe.model_copy(update={
            "argv": verifier_argv(task_sandbox.startup_probe.argv),
        }),
        "readiness_probe": task_sandbox.readiness_probe.model_copy(update={
            "argv": verifier_argv(task_sandbox.readiness_probe.argv),
        }),
    })
    outputs = [
        item if item == TASK_EGRESS_OUTPUT
        else item.model_copy(update={"required": item.relative_path == "verifier/output.json"})
        for item in agent_plan.output_declarations
        if item.relative_path in _VERIFIER_OWNED_OUTPUTS or item == TASK_EGRESS_OUTPUT
    ]
    requests = agent_plan.resource_requests
    if requests is not None:
        requests = (ExecutionResourceRequestsV1(
            controller=requests.controller, verifier_sandbox=requests.task_sandbox,
        ) if requests.controller is not None or requests.task_sandbox is not None else None)
    # Keep the attempt's trusted controller module; the phase is fixed.
    argv = agent_plan.main.argv
    module = argv[3] if argv[:3] == ("python", "-I", "-m") and len(argv) > 3 else SANDBOX_CONTROLLER_MODULE
    deferred = agent_plan.model_copy(update={
        "execution_role": "verifier",
        "verifier_execution": "skipped",
        "verifier": None,
        "verifier_after_agent_timeout": False,
        "in_place_verifier": False,
        "sidecars": (
            *(sidecar for sidecar in agent_plan.sidecars if not sidecar.private_sandbox and not sidecar.task_fixture),
            verifier_sandbox,
        ),
        "main": agent_plan.main.model_copy(update={
            "role": "verifier",
            "argv": sandbox_phase_argv(VERIFY_SANDBOX_PHASE, module),
            "timeout_seconds": verifier_timeout_seconds,
        }),
        "output_declarations": tuple(outputs),
        "resource_requests": requests,
        "handoff_input": handoff_input,
    })
    return ExecutionRuntimePlanV1.model_validate(deferred.canonical_payload())


__all__ = [
    "HARNESS_SETUP_PHASE",
    "SANDBOX_RUNTIME_BINARY",
    "TASK_SANDBOX",
    "VERIFIER_SANDBOX",
    "VERIFY_SANDBOX_PHASE",
    "TaskSandboxPlanRequest",
    "compile_deferred_verifier_plan",
    "compile_task_sandbox_plan",
    "guest_capabilities",
    "plan_admissions",
    "sandbox_phase_argv",
]
