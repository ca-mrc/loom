"""Typed specifications for hosted (Nebius-native) agent harnesses (#2288).

Each harness declares only what it owns: its execution kind, controller
phase, model use, harness-only features, the sandbox-driver operations it
needs and its native evidence. The common planner owns sandbox construction,
input staging, verifier topology, network egress, resources and output
supervision, and asks the spec instead of branching on an agent name.

This module is imported by the trusted controller image, so it depends on
nothing but the standard library.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Literal

# `response-only`: the model returns text; there is no task sandbox.
# `workspace`: the controller drives a private task sandbox over its driver.
ExecutionKind = Literal["response-only", "workspace"]
ModelUse = Literal["required", "forbidden"]
DriverCapability = Literal["exec", "exec_streaming", "upload", "download"]
# How the materializer validates the harness trace and usage documents.
TraceFormat = Literal["completion-calls", "terminus", "oracle", "codex"]
# Behaviour that only some harnesses implement, independent of topology.
HarnessFeature = Literal["agent_continuation", "pinned_versions", "task_resource_requests"]
# Which trusted image runs the controller phase. `service-runner`: the
# deployed runner image frozen as the plan's task image. `harness-controller`:
# the deployment's digest-pinned controller image (or a pinned version's).
ControllerImage = Literal["service-runner", "harness-controller"]
# The Gateway wire format the harness speaks; None for a model-free harness.
GatewayProtocol = Literal["openai-chat-completions", "openai-responses"]
Readiness = Literal["ready", "unavailable"]

# Trusted controller entry points. The sandbox module also owns the fixed
# `verify-sandbox` phase, so every workspace harness runs through it.
RESPONSE_RUNNER_MODULE = "loom.service_execution_task"
SANDBOX_CONTROLLER_MODULE = "loom.service_execution_sandbox_task"
# The controller phase that runs a harness's pinned install (#2310).
HARNESS_SETUP_PHASE = "setup"

# Operations `ServiceSandboxDriver` implements today. A harness that needs
# more is not natively runnable.
NATIVE_SANDBOX_DRIVER_CAPABILITIES: frozenset[DriverCapability] = frozenset(
    {"exec", "exec_streaming", "upload", "download"},
)
# Operations the QEMU guest sandbox path is qualified for. The guest's
# outer socket proxies the same sandbox API over its RPC channel, and
# supervised processes are qualified through it (#2362).
GUEST_SANDBOX_DRIVER_CAPABILITIES: frozenset[DriverCapability] = frozenset(
    {"exec", "exec_streaming", "upload", "download"},
)


@dataclass(frozen=True)
class NativeOutput:
    """A harness-owned file the runtime captures from the controller's `.loom/`."""

    source_path: str
    relative_path: str
    kind: str
    required: bool


@dataclass(frozen=True)
class InstallSource:
    """An HTTP(S) origin the pinned install may download from during setup."""

    host: str
    protocol: Literal["http", "https"] = "https"


# Bounds for the setup phase's own deadline, separate from the agent's.
MAX_SETUP_TIMEOUT_SECONDS = 1800
# A cached install travels through the sandbox file API, which bounds one
# transfer at 256 MiB.
MAX_SETUP_CACHE_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True, kw_only=True)
class PinnedArchive:
    """A release archive the trusted controller fetches and verifies (#2311).

    The controller downloads it through the setup-only egress proxy, checks
    its npm-style `sha512-<base64>` integrity before any byte reaches the
    sandbox, uploads it and extracts it into `install_root`. Nothing in the
    task image participates except `tar`/`gzip`.
    """

    url: str
    integrity: str
    # Leading path components to drop when extracting (`package/vendor/...`).
    strip_components: int = 0

    def __post_init__(self) -> None:
        import base64
        from urllib.parse import urlsplit

        parts = urlsplit(self.url)
        if parts.scheme != "https" or not parts.hostname or parts.query or parts.fragment:
            raise ValueError("pinned archive requires a plain https URL")
        algorithm, _, digest = self.integrity.partition("-")
        try:
            raw = base64.b64decode(digest, validate=True)
        except ValueError:
            raw = b""
        if algorithm != "sha512" or len(raw) != 64:
            raise ValueError("pinned archive integrity must be sha512-<base64>")
        if not 0 <= self.strip_components <= 8:
            raise ValueError("pinned archive strip_components out of range")

    @property
    def host(self) -> str:
        from urllib.parse import urlsplit

        return urlsplit(self.url).hostname or ""


@dataclass(frozen=True, kw_only=True)
class HarnessSetup:
    """The pinned installation an installed harness runs before its agent (#2310).

    Exactly one of `install` (a command run inside the task sandbox) or
    `archive` (a release fetched and verified by the trusted controller, then
    extracted in the sandbox) runs during the setup phase only. Either may
    reach only `sources`, through the Gateway task-egress proxy; no task
    command or model call runs in setup, and the egress window closes before
    the agent phase.
    """

    install: tuple[str, ...] = ()
    archive: PinnedArchive | None = None
    sources: tuple[InstallSource, ...]
    timeout_seconds: int
    # Absolute sandbox directory the install writes everything into. With
    # `check`, it makes the install cacheable: the directory is archived after
    # a fresh install and restored instead of reinstalling (#2310).
    install_root: str | None = None
    # Command proving an install (fresh or restored) is usable, e.g. `--version`.
    check: tuple[str, ...] = ()
    # Peak sandbox disk the setup needs: installed bytes plus any transient
    # archive. Admission rejects a task whose sandbox cannot hold it (#2362).
    disk_mib: int = 0

    def __post_init__(self) -> None:
        if (self.archive is None) == (not self.install):
            raise ValueError("harness setup declares exactly one of an install command or a pinned archive")
        if self.install and not all(self.install):
            raise ValueError("harness setup requires a non-empty install command")
        if self.archive is not None:
            if self.install_root is None:
                raise ValueError("a pinned archive is extracted into a declared install_root")
            if self.archive.host not in {source.host for source in self.sources if source.protocol == "https"}:
                raise ValueError("pinned archive host must be a declared https install source")
        if not self.sources:
            raise ValueError("harness setup requires declared install sources")
        if not 0 < self.timeout_seconds <= MAX_SETUP_TIMEOUT_SECONDS:
            raise ValueError("harness setup timeout is out of range")
        if not 0 <= self.disk_mib <= 1_048_576:
            raise ValueError("harness setup disk requirement is out of range")
        if (self.install_root is None) != (not self.check):
            raise ValueError("a cacheable harness setup declares both install_root and check")
        if self.install_root is not None and (
            not self.install_root.startswith("/") or self.install_root.rstrip("/") in {"", "/tmp", "/usr", "/home"}
            or ".." in self.install_root.split("/") or "//" in self.install_root
        ):
            raise ValueError("harness setup install_root must be a dedicated absolute directory")

    @property
    def cacheable(self) -> bool:
        return self.install_root is not None

    def cache_identity(self, harness: str) -> str:
        """Digest of everything that determines the installed bytes; part of
        the cache key alongside the team and the exact task image."""
        import hashlib
        import json

        document = {
            "schema_version": "loom.harness-setup-cache-identity.v1",
            "harness": harness, "install": list(self.install),
            **({"archive": [self.archive.url, self.archive.integrity, self.archive.strip_components]}
               if self.archive is not None else {}),
            "sources": [[source.protocol, source.host] for source in self.sources],
            "install_root": self.install_root, "check": list(self.check),
        }
        payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        return "sha256:" + hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class HostedHarnessSpec:
    name: str
    execution_kind: ExecutionKind
    # Trusted controller binding: `python -m <controller_module> <controller_phase>`
    # in `controller_image`. Tasks and trials can never supply these.
    controller_module: str
    controller_phase: str
    controller_image: ControllerImage
    model: ModelUse
    gateway_protocol: GatewayProtocol | None
    trace_format: TraceFormat
    aliases: tuple[str, ...] = ()
    # Hosted support state, independent of product catalog support.
    readiness: Readiness = "ready"
    # Private-solution input policy: stage `solution/**` into this harness's
    # own task sandbox only, and remove it before the snapshot and grading.
    stages_solution: bool = False
    features: frozenset[HarnessFeature] = frozenset()
    required_driver_capabilities: frozenset[DriverCapability] = frozenset()
    native_outputs: tuple[NativeOutput, ...] = ()
    # How the harness gets into the task sandbox; None means it is already
    # there (task image or trusted controller).
    setup: HarnessSetup | None = None
    # Whether a task that declares this harness, with `env_mode: separate`,
    # projects a separate verifier when its requirements are derived without
    # a Trial. Frozen: stored requirement digests depend on it (#2296).
    task_declared_separate_verifier: bool = False
    names: frozenset[str] = field(init=False)

    def __post_init__(self) -> None:
        expected = (
            (RESPONSE_RUNNER_MODULE, "service-runner") if self.execution_kind == "response-only"
            else (SANDBOX_CONTROLLER_MODULE, "harness-controller")
        )
        if (self.controller_module, self.controller_image) != expected:
            raise ValueError(f"{self.name}: {self.execution_kind} harnesses bind {expected}")
        if (self.model == "required") != (self.gateway_protocol is not None):
            raise ValueError(f"{self.name}: a model-backed harness declares exactly one Gateway protocol")
        if self.execution_kind == "response-only" and (
            self.stages_solution or self.required_driver_capabilities or self.native_outputs
        ):
            raise ValueError(f"{self.name}: a response-only harness has no task sandbox")
        if self.execution_kind == "workspace" and not self.required_driver_capabilities:
            raise ValueError(f"{self.name}: a workspace harness must declare its driver operations")
        if self.task_declared_separate_verifier and self.execution_kind != "workspace":
            raise ValueError(f"{self.name}: only a workspace harness has a separate verifier")
        if self.stages_solution and self.model != "forbidden":
            raise ValueError(f"{self.name}: only a model-free harness may receive solution/")
        if self.setup is not None and (
            self.execution_kind != "workspace" or "exec_streaming" not in self.required_driver_capabilities
        ):
            raise ValueError(f"{self.name}: setup installs into a task sandbox through exec_streaming")
        object.__setattr__(self, "names", frozenset({self.name, *self.aliases}))

    @property
    def workspace(self) -> bool:
        return self.execution_kind == "workspace"

    @property
    def natively_runnable(self) -> bool:
        return (
            self.readiness == "ready"
            and self.required_driver_capabilities <= NATIVE_SANDBOX_DRIVER_CAPABILITIES
        )

    def supports(self, feature: HarnessFeature) -> bool:
        return feature in self.features


_SANDBOX_DRIVER: frozenset[DriverCapability] = frozenset({"exec", "upload", "download"})

DIRECT_COMPLETION = HostedHarnessSpec(
    name="direct-completion",
    aliases=("litellm",),
    execution_kind="response-only",
    controller_module=RESPONSE_RUNNER_MODULE,
    controller_phase="direct-completion",
    controller_image="service-runner",
    model="required",
    gateway_protocol="openai-chat-completions",
    trace_format="completion-calls",
)

TERMINUS_2 = HostedHarnessSpec(
    name="terminus-2",
    execution_kind="workspace",
    controller_module=SANDBOX_CONTROLLER_MODULE,
    controller_phase="terminus-2",
    controller_image="harness-controller",
    model="required",
    # Harbor calls the Gateway's OpenAI facade through LiteLLM.
    gateway_protocol="openai-chat-completions",
    trace_format="terminus",
    features=frozenset({"agent_continuation", "pinned_versions", "task_resource_requests"}),
    required_driver_capabilities=_SANDBOX_DRIVER,
    native_outputs=(
        NativeOutput("agent/harbor/trajectory.json", "artifacts/harbor/trajectory.json", "agent_native", True),
        NativeOutput("agent/harbor/recording.cast", "artifacts/harbor/recording.cast", "agent_native", False),
    ),
    # The only harness with this historical projection; keep it that way.
    task_declared_separate_verifier=True,
)

ORACLE = HostedHarnessSpec(
    name="oracle",
    execution_kind="workspace",
    controller_module=SANDBOX_CONTROLLER_MODULE,
    controller_phase="oracle",
    controller_image="harness-controller",
    model="forbidden",
    gateway_protocol=None,
    trace_format="oracle",
    stages_solution=True,
    required_driver_capabilities=_SANDBOX_DRIVER,
)


def _index(specs: Iterable[HostedHarnessSpec]) -> Mapping[str, HostedHarnessSpec]:
    index: dict[str, HostedHarnessSpec] = {}
    phases: set[tuple[ExecutionKind, str]] = set()
    for spec in specs:
        phase = (spec.execution_kind, spec.controller_phase)
        if phase in phases or spec.controller_phase == "verify-sandbox":
            raise ValueError(f"{spec.name}: controller phase {spec.controller_phase!r} is not unique")
        phases.add(phase)
        for name in spec.names:
            if name in index:
                raise ValueError(f"hosted harness name {name!r} is declared twice")
            index[name] = spec
    return MappingProxyType(index)


# Codex CLI runs inside the task sandbox from the pinned, statically linked
# release (#2311). Its home and temp directory stay outside both the task
# workdir and the cached install root; Codex refuses a home under TMPDIR.
CODEX_VERSION = "0.146.0"
CODEX_INSTALL_ROOT = "/tmp/loom-harness/codex"
CODEX_HOME = "/tmp/loom-harness/codex-home"
CODEX_TMPDIR = "/tmp/loom-harness/tmp"
CODEX = HostedHarnessSpec(
    name="codex",
    execution_kind="workspace",
    controller_module=SANDBOX_CONTROLLER_MODULE,
    controller_phase="codex",
    controller_image="harness-controller",
    model="required",
    gateway_protocol="openai-responses",
    trace_format="codex",
    required_driver_capabilities=frozenset({"exec", "exec_streaming", "upload", "download"}),
    native_outputs=(
        NativeOutput("agent/codex/events.jsonl", "artifacts/codex/events.jsonl", "agent_native", True),
        # Codex's own session log; the canonical ATIF is built from it.
        NativeOutput("agent/codex/session.jsonl", "artifacts/codex/session.jsonl", "agent_native", False),
    ),
    setup=HarnessSetup(
        archive=PinnedArchive(
            url=f"https://registry.npmjs.org/@openai/codex/-/codex-{CODEX_VERSION}-linux-x64.tgz",
            integrity="sha512-fswvyGprAPCMiOEue/7MKMk7pCjh9kZIJfJX5i9atmfnmGYbYCcUhZsEH9LEP0+0t5xyPqDbfNXY7NSxIVuXxA==",
            # package/vendor/x86_64-unknown-linux-musl/{bin,codex-path,codex-resources}
            strip_components=3,
        ),
        sources=(InstallSource("registry.npmjs.org"),),
        timeout_seconds=900,
        install_root=CODEX_INSTALL_ROOT,
        check=(f"{CODEX_INSTALL_ROOT}/bin/codex", "--version"),
        # 350 MiB unpacked plus the 131 MiB archive while it is extracted or
        # re-archived for the install cache, plus filesystem overhead (a
        # guest's disk is ext4) and room for Codex's own session files.
        disk_mib=768,
    ),
)

HOSTED_HARNESSES: Mapping[str, HostedHarnessSpec] = _index((DIRECT_COMPLETION, TERMINUS_2, ORACLE, CODEX))

# Names and aliases the native execution path can run today. OpenHands and
# Codex are supported product entries without a hosted spec yet (#2054).
NATIVE_EXECUTION_AGENT_NAMES: frozenset[str] = frozenset(
    name for name, spec in HOSTED_HARNESSES.items() if spec.natively_runnable
)


def hosted_harness(agent_name: str | None) -> HostedHarnessSpec | None:
    """The spec for a selected agent name or alias; None fails closed."""
    return HOSTED_HARNESSES.get(agent_name) if agent_name is not None else None


def is_workspace_harness(agent_name: str | None) -> bool:
    spec = hosted_harness(agent_name)
    return spec is not None and spec.workspace


def harnesses_supporting(feature: HarnessFeature) -> tuple[str, ...]:
    return tuple(sorted({spec.name for spec in HOSTED_HARNESSES.values() if spec.supports(feature)}))


def workspace_controller_phases() -> tuple[str, ...]:
    return tuple(sorted({spec.controller_phase for spec in HOSTED_HARNESSES.values() if spec.workspace}))


__all__ = [
    "CODEX",
    "CODEX_HOME",
    "CODEX_INSTALL_ROOT",
    "CODEX_TMPDIR",
    "CODEX_VERSION",
    "DIRECT_COMPLETION",
    "GUEST_SANDBOX_DRIVER_CAPABILITIES",
    "HARNESS_SETUP_PHASE",
    "HOSTED_HARNESSES",
    "MAX_SETUP_CACHE_BYTES",
    "MAX_SETUP_TIMEOUT_SECONDS",
    "NATIVE_EXECUTION_AGENT_NAMES",
    "NATIVE_SANDBOX_DRIVER_CAPABILITIES",
    "ORACLE",
    "RESPONSE_RUNNER_MODULE",
    "SANDBOX_CONTROLLER_MODULE",
    "TERMINUS_2",
    "ControllerImage",
    "DriverCapability",
    "ExecutionKind",
    "GatewayProtocol",
    "HarnessFeature",
    "HarnessSetup",
    "HostedHarnessSpec",
    "InstallSource",
    "ModelUse",
    "NativeOutput",
    "PinnedArchive",
    "Readiness",
    "TraceFormat",
    "harnesses_supporting",
    "hosted_harness",
    "is_workspace_harness",
    "workspace_controller_phases",
]
