"""Agent catalog — the union of built-in agents and registered
loom-launcher adapters that a service-mode trial may name.

The SPA fetches this catalog via `GET /api/v1/agents` so users pick
from a dropdown instead of typing a name into a text input. The same
list backs request-time validation in routes/trials.py and
routes/batches.py — a typo'd or hostile `agent_name` is rejected at
the API boundary rather than blowing up the worker mid-trial.

Each entry also declares what models it accepts:

- `supported_providers`: tuple of provider names (`"anthropic"`,
  `"openai"`, …) or `("*",)` for "any provider the LLM Gateway can
  route". CLI adapters lock to one provider; generic agents
  (direct-completion, aider, openhands) accept anything.
- `supported_model_sources`: subset of the `ModelSpec.source`
  discriminator. Hosted submissions admit only `"api"`; `local-server`
  and `hf` are retired (#2054). Empty tuple means the agent doesn't take
  a model at all (oracle).
- `product_support`: `"supported"` for the five product entries new
  submissions may select; `"deferred"` (with `deferred_reason`) for the
  rest, which remain listed so historical records stay readable.

Validation at submit time uses these to reject incompatible
(agent_name, agent_model) combos with a 400, rather than letting them
fail mid-trial on the worker. See validate_agent_model_compat below.

Adding a new builtin: append to `_BUILTIN`. Adding a new adapter: the
adapter ships in `loom-launcher` and is registered via
`register_adapter`; the catalog picks it up automatically (with a
generic "any provider, api" support set — adapters
that should restrict can be overridden in `_ADAPTER_OVERRIDES`).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from loom.models.types import ModelSpec

AgentKind = Literal["builtin", "adapter"]
ReadinessStatus = Literal["ready", "unavailable"]
CatalogVisibility = Literal["displayed", "internal"]
ProductSupport = Literal["supported", "deferred"]


@dataclass(frozen=True)
class RuntimeContract:
    """Service-mode runtime requirements for one displayed agent.

    This is declared metadata, not a `which` probe. The service process,
    worker process, and trial sandbox can be different images, so readiness
    has to reflect the product contract for service-mode execution.
    """

    execution: str
    capture: str
    required_executables: tuple[str, ...] = ()
    required_python_modules: tuple[str, ...] = ()
    required_packages: tuple[str, ...] = ()
    endpoint_dialect: str | None = None
    api_key_env: str | None = None
    base_url_env: str | None = None
    model_name_template: str | None = None
    sandbox_network: str = "gateway"
    install_hint: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "execution": self.execution,
            "capture": self.capture,
            "required_executables": list(self.required_executables),
            "required_python_modules": list(self.required_python_modules),
            "required_packages": list(self.required_packages),
            "endpoint_dialect": self.endpoint_dialect,
            "api_key_env": self.api_key_env,
            "base_url_env": self.base_url_env,
            "model_name_template": self.model_name_template,
            "sandbox_network": self.sandbox_network,
            "install_hint": self.install_hint,
        }


def _ready_builtin_contract(
    *,
    execution: str,
    capture: str = "loom-trajectory",
    endpoint_dialect: str | None = None,
    api_key_env: str | None = None,
    base_url_env: str | None = None,
    model_name_template: str | None = None,
) -> RuntimeContract:
    return RuntimeContract(
        execution=execution,
        capture=capture,
        endpoint_dialect=endpoint_dialect,
        api_key_env=api_key_env,
        base_url_env=base_url_env,
        model_name_template=model_name_template,
    )


@dataclass(frozen=True)
class AgentEntry:
    name: str
    needs_model: bool
    kind: AgentKind
    description: str
    aliases: tuple[str, ...] = ()
    supported_providers: tuple[str, ...] = ()
    supported_model_sources: tuple[str, ...] = ()
    runtime_contract: RuntimeContract = RuntimeContract(
        execution="unknown",
        capture="unknown",
    )
    service_mode_ready: bool = True
    readiness_status: ReadinessStatus = "ready"
    readiness_message: str | None = None
    catalog_visibility: CatalogVisibility = "displayed"
    # #320: task-shape capabilities the agent needs. Empty = no hard
    # task-shape requirements (works against any task that exposes a
    # workable agent step). Currently only `solution_solve_sh` exists
    # (oracle needs `solution/solve.sh` in the bundle). Surfaced to
    # the SPA via /api/v1/agents and consumed by the POST /batches
    # preflight to skip incompatible (agent, task) combos before
    # fan-out instead of bubbling agent_error after submit.
    requires_capabilities: frozenset[str] = frozenset()
    # Execution surfaces this runtime exposes to a task. Tasks declare
    # requirements separately in TaskConfig.required_agent_capabilities.
    provides_capabilities: frozenset[str] = frozenset()
    # #2054: whether new hosted submissions may select this entry. Deferred
    # entries stay in the catalog so historical records remain readable.
    product_support: ProductSupport = "supported"
    deferred_reason: str | None = None
    # User-facing name when it differs from the canonical `name` (#2054).
    display_name: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "aliases": list(self.aliases),
            "needs_model": self.needs_model,
            "kind": self.kind,
            "description": self.description,
            "supported_providers": list(self.supported_providers),
            "supported_model_sources": list(self.supported_model_sources),
            "runtime_contract": self.runtime_contract.to_dict(),
            "service_mode_ready": self.service_mode_ready,
            "readiness_status": self.readiness_status,
            "readiness_message": self.readiness_message,
            "catalog_visibility": self.catalog_visibility,
            "requires_capabilities": sorted(self.requires_capabilities),
            "provides_capabilities": sorted(self.provides_capabilities),
            "product_support": self.product_support,
            "deferred_reason": self.deferred_reason,
            "display_name": self.display_name or self.name,
        }

    def readiness_error(self) -> str | None:
        if self.service_mode_ready:
            return None
        msg = self.readiness_message or (
            f"agent {self.name!r} is not ready for service-mode runtime"
        )
        return f"{msg}. See GET /api/v1/agents for runtime setup details."


# #2054: the only entries new hosted submissions may select. Every other
# catalog entry is deferred pending a product decision.
_SUPPORTED_PRODUCT_ENTRIES = frozenset(
    {"oracle", "direct-completion", "terminus-2", "openhands-sdk", "codex"},
)
# Adapters folded into another entry as an alias. `openhands` and
# `openhands-sdk` run the same SDK runner; typed artifact capture and the
# OpenHands export key on `openhands-sdk`, so that name is canonical.
_ADAPTER_ALIASES: dict[str, tuple[str, ...]] = {
    "openhands-sdk": ("openhands",),
}
# The product shows OpenHands under its familiar name.
_ADAPTER_DISPLAY_NAMES: dict[str, str] = {
    "openhands-sdk": "openhands",
}
# Only the provider API path is admitted for new hosted submissions; the
# `local-server` and `hf` sources are retired (#2054).
_HOSTED_MODEL_SOURCES: tuple[str, ...] = ("api",)

# Built-in agents — the worker's `_default_agent_factory` knows these
# names natively (no loom-launcher round trip).
_BUILTIN: tuple[AgentEntry, ...] = (
    AgentEntry(
        name="oracle",
        needs_model=False,
        kind="builtin",
        description=(
            "Runs the task's solution/solve.sh script as ground truth. "
            "Use for canary trials and smoke tests; no LLM call."
        ),
        runtime_contract=_ready_builtin_contract(
            execution="builtin-oracle",
            capture="loom-trajectory",
        ),
        # #320: oracle hard-requires `solution/solve.sh` in the
        # materialized bundle. Tasks that don't ship it (most non-code
        # benchmarks: aime, gpqa, mmlu-pro, bfcl, etc.) are filtered
        # out at POST /batches preflight rather than launched and
        # failed mid-trial with `AgentError: solve.sh ... not found`.
        requires_capabilities=frozenset({"solution_solve_sh"}),
        provides_capabilities=frozenset({"workspace_exec"}),
    ),
    AgentEntry(
        name="direct-completion",
        aliases=("litellm",),
        needs_model=True,
        kind="builtin",
        description=(
            "Direct model completion with response-text artifact projection. "
            "Routes through the LLM Gateway to an OpenAI-compatible "
            "Provider Connection; it does not execute workspace tools."
        ),
        supported_providers=("*",),
        supported_model_sources=_HOSTED_MODEL_SOURCES,
        runtime_contract=_ready_builtin_contract(
            execution="builtin-direct-completion",
            capture="gateway-llm-calls",
            endpoint_dialect="openai_chat",
            api_key_env="LOOM_STEP_TOKEN",
            base_url_env="LOOM_GATEWAY_URL",
            model_name_template="{provider}/{model_id}",
        ),
    ),
    AgentEntry(
        name="terminus-2",
        needs_model=True,
        kind="builtin",
        description=(
            "Harbor Terminus-2 embedded in the worker pool (#744). Runs pinned "
            "harbor.agents.terminus_2 in-process, bridges Driver exec to Harbor "
            "tmux, routes LLM via Gateway step JWT, and persists typed "
            "terminus2_* trajectory events for TB2-compatible export."
        ),
        supported_providers=("*",),
        supported_model_sources=("api",),
        runtime_contract=_ready_builtin_contract(
            execution="builtin-terminus2-harbor",
            capture="typed_events+harbor_artifacts",
            endpoint_dialect="openai_chat",
            api_key_env="LOOM_STEP_TOKEN",
            base_url_env="LOOM_GATEWAY_URL",
            model_name_template="openai/{model_id}",
        ),
        provides_capabilities=frozenset({"workspace_exec"}),
    ),
)
# Note: `claude-code-inbox` was a separate catalog entry for the v0.7
# "in-box" runtime that required `@anthropic-ai/claude-code` pre-baked
# into the sandbox image. Since #317 made the `claude-code` adapter
# install on demand, no task image actually relied on the pre-baked
# path, and the two routed to the same SubprocessAgent code anyway.
# Retired as redundant; use `claude-code` instead.


# Per-adapter overrides for the auto-discovered supported sets. Adapters
# that wrap a CLI bound to one provider get listed here so the SPA
# dropdown only offers compatible models. Adapters NOT in this map fall
# back to the permissive ("*", api) defaults below. Only the `api` source
# remains for hosted submissions (#2054); wire-protocol compatibility is
# checked against the Provider Connection type, not the provider name.
_ADAPTER_OVERRIDES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "claude-code": (("anthropic",), _HOSTED_MODEL_SOURCES),
    "codex": (("*",), _HOSTED_MODEL_SOURCES),
    "gemini-cli": (("google",), _HOSTED_MODEL_SOURCES),
    "kimi-cli": (("moonshot",), _HOSTED_MODEL_SOURCES),
    "qwen-cli": (("alibaba",), _HOSTED_MODEL_SOURCES),
    # terminus-2 is a builtin Harbor-embedded runtime (#744); not a launcher adapter.
    # hello is an internal no-model launcher canary, so it has no model support.
    "hello": ((), ()),
}
_DEFAULT_ADAPTER_SUPPORT: tuple[tuple[str, ...], tuple[str, ...]] = (
    ("*",),
    _HOSTED_MODEL_SOURCES,
)


_ADAPTER_CAPTURE: dict[str, str] = {
    "aider": "log_file",
    "claude-code": "stdout_jsonl",
    "codex": "stdout_jsonl",
    "gemini-cli": "stdout_jsonl",
    "hello": "stdout_jsonl",
    "kimi-cli": "pty",
    "mini-swe-agent": "stdout_jsonl",
    "opencode": "stdout_jsonl",
    "openhands": "stdout_jsonl",
    "openhands-sdk": "stdout_jsonl",
    "qwen-cli": "pty",
    "swe-agent": "log_file",
    # terminus-2: builtin Harbor-embedded runtime — see _BUILTIN.
}


_ADAPTER_REQUIRED_EXECUTABLES: dict[str, tuple[str, ...]] = {
    "aider": ("aider",),
    "claude-code": ("sh", "claude"),
    "codex": ("codex",),
    "gemini-cli": ("gemini",),
    "hello": ("echo",),
    "kimi-cli": ("kimi",),
    "mini-swe-agent": ("mini-swe-agent",),
    "opencode": ("opencode",),
    "qwen-cli": ("qwen",),
    "openhands": ("tmux",),
    "openhands-sdk": ("tmux",),
}


_ADAPTER_REQUIRED_PYTHON_MODULES: dict[str, tuple[str, ...]] = {
    "openhands": (
        "loom_launcher.openhands_sdk_runner",
        "openhands.sdk",
        "openhands.tools.terminal",
    ),
    "openhands-sdk": (
        "loom_launcher.openhands_sdk_runner",
        "openhands.sdk",
        "openhands.tools.terminal",
    ),
    "swe-agent": ("sweagent.run.run_single",),
}


_ADAPTER_REQUIRED_PACKAGES: dict[str, tuple[str, ...]] = {
    "aider": ("aider-chat",),
    "claude-code": ("@anthropic-ai/claude-code",),
    "codex": ("@openai/codex",),
    "gemini-cli": ("@google/gemini-cli",),
    "kimi-cli": ("@moonshot-ai/kimi-code",),
    "mini-swe-agent": ("mini-swe-agent",),
    "opencode": ("opencode-ai",),
    "openhands": ("openhands-sdk", "openhands-tools"),
    "openhands-sdk": ("openhands-sdk", "openhands-tools"),
    "qwen-cli": ("@qwen-code/qwen-code",),
    "swe-agent": ("git+https://github.com/SWE-agent/SWE-agent",),
}


_ADAPTER_RUNTIME_READY: dict[str, bool] = {
    "aider": True,
    "claude-code": True,
    "codex": True,
    "gemini-cli": True,
    "hello": True,
    "kimi-cli": True,
    "mini-swe-agent": True,
    "opencode": True,
    "openhands": True,
    "openhands-sdk": True,
    "qwen-cli": True,
    "swe-agent": True,
}


def _adapter_runtime_contract(adapter: Any) -> RuntimeContract:
    name = str(adapter.name)
    required_executables = _ADAPTER_REQUIRED_EXECUTABLES.get(name, ())
    required_modules = _ADAPTER_REQUIRED_PYTHON_MODULES.get(name, ())
    required_packages = _ADAPTER_REQUIRED_PACKAGES.get(name, ())
    install_hint: str | None
    if name == "hello":
        install_hint = None
    else:
        deps = [
            *(f"executable {e!r}" for e in required_executables),
            *(f"Python module {m!r}" for m in required_modules),
        ]
        dep_text = ", ".join(deps) if deps else "its agent runtime"
        install_hint = (
            f"Provision {dep_text} in every service-mode trial sandbox "
            f"before enabling agent {name!r}."
        )
    return RuntimeContract(
        execution="subprocess-adapter",
        capture=_ADAPTER_CAPTURE.get(name, "unknown"),
        required_executables=required_executables,
        required_python_modules=required_modules,
        required_packages=required_packages,
        endpoint_dialect=str(getattr(adapter, "endpoint_dialect", "unknown")),
        api_key_env=str(getattr(adapter, "api_key_env", "")),
        base_url_env=str(getattr(adapter, "base_url_env", "")),
        model_name_template=str(getattr(adapter, "model_name_template", "")),
        sandbox_network="gateway",
        install_hint=install_hint,
    )


def _adapter_readiness(adapter: Any) -> tuple[bool, ReadinessStatus, str | None]:
    name = str(adapter.name)
    ready = _ADAPTER_RUNTIME_READY.get(name, False)
    if ready:
        # #317 Phase 3c: adapters with install_script are installed
        # into the trial sandbox on demand via the trial-cache layered
        # image; surface that in the readiness message so the SPA
        # doesn't imply the CLI is pre-baked into the task image.
        install_script = getattr(adapter, "install_script", None)
        if install_script:
            return (
                True, "ready",
                (
                    "installs into the trial sandbox on demand "
                    "(layered on top of the task image; cached after "
                    "the first trial of each (image, agent) pair)"
                ),
            )
        return True, "ready", None
    contract = _adapter_runtime_contract(adapter)
    missing = [
        *(f"executable {e!r}" for e in contract.required_executables),
        *(f"Python module {m!r}" for m in contract.required_python_modules),
    ]
    missing_text = ", ".join(missing) if missing else "runtime dependency"
    return (
        False,
        "unavailable",
        (
            f"agent {name!r} requires {missing_text} in the trial sandbox, "
            "and the default service-mode runtime does not provision it"
        ),
    )


def list_agents(*, include_internal: bool = False) -> list[AgentEntry]:
    """Return the user-facing catalog, optionally including internal canaries.

    Adapters are loaded lazily so a deployment that doesn't ship
    `loom-launcher` (rare, but possible) still gets the builtins.

    Internal launcher fixtures are excluded by default. This default is the
    shared presentation boundary for GET /agents, CLI runtime audits, catalog
    provisioning, and compatibility matrices.
    """
    entries: list[AgentEntry] = list(_BUILTIN)
    builtin_names = {e.name for e in _BUILTIN}
    alias_names = {alias for aliases in _ADAPTER_ALIASES.values() for alias in aliases}
    try:
        # Importing `loom_launcher` runs its adapters package, which
        # self-registers every shipped adapter into the registry.
        import loom_launcher  # noqa: F401
        from loom_launcher.registry import all_adapters
    except ImportError:
        return entries
    for adapter in all_adapters():
        if adapter.name in builtin_names or adapter.name in alias_names:
            continue
        visibility = cast(
            CatalogVisibility,
            getattr(adapter, "catalog_visibility", "displayed"),
        )
        if visibility not in {"displayed", "internal"}:
            raise ValueError(
                f"adapter {adapter.name!r} declares invalid "
                f"catalog_visibility {visibility!r}",
            )
        if visibility == "internal" and not include_internal:
            continue
        needs_model = bool(getattr(adapter, "needs_model", True))
        providers, sources = _ADAPTER_OVERRIDES.get(
            adapter.name,
            _DEFAULT_ADAPTER_SUPPORT,
        )
        if not needs_model:
            providers, sources = (), ()
        ready, readiness_status, readiness_message = _adapter_readiness(adapter)
        if adapter.name == "hello":
            description = (
                "Internal launcher contract canary. Runs echo and makes no "
                "model call; not available for user batch submission."
            )
        else:
            description = (
                f"loom-launcher adapter (dialect "
                f"{getattr(adapter, 'endpoint_dialect', 'unknown')}). "
                "Drives the agent's CLI inside the sandbox."
            )
        entries.append(
            AgentEntry(
                name=adapter.name,
                aliases=_ADAPTER_ALIASES.get(adapter.name, ()),
                display_name=_ADAPTER_DISPLAY_NAMES.get(adapter.name),
                needs_model=needs_model,
                kind="adapter",
                description=description,
                supported_providers=providers,
                supported_model_sources=sources,
                runtime_contract=_adapter_runtime_contract(adapter),
                service_mode_ready=ready,
                readiness_status=readiness_status,
                readiness_message=readiness_message,
                catalog_visibility=visibility,
                provides_capabilities=frozenset({"workspace_exec"}),
                **_product_support(adapter.name, visibility),
            ),
        )
    return entries


def _product_support(name: str, visibility: CatalogVisibility) -> dict[str, Any]:
    if name in _SUPPORTED_PRODUCT_ENTRIES:
        return {"product_support": "supported", "deferred_reason": None}
    if visibility == "internal":
        reason = "internal test fixture; not a product entry"
    else:
        reason = "pending a future product decision (#2054)"
    return {"product_support": "deferred", "deferred_reason": reason}


def known_names() -> frozenset[str]:
    """Set of valid `agent_name` values for fast membership check at
    the request boundary. Recomputed per call because adapter
    registration is import-time (idempotent + cheap)."""
    return frozenset(
        name
        for entry in list_agents()
        for name in (entry.name, *entry.aliases)
    )


def get_agent(name: str, *, include_internal: bool = False) -> AgentEntry | None:
    """Look up an entry by name; returns None for unknown agents."""
    for e in list_agents(include_internal=include_internal):
        if e.name == name or name in e.aliases:
            return e
    return None


def resolve_agents(
    names: Iterable[str],
    *,
    include_internal: bool = False,
) -> list[AgentEntry]:
    """Resolve canonical names and aliases, deduplicated in catalog order."""
    catalog = list_agents(include_internal=include_internal)
    by_name = {
        name: entry
        for entry in catalog
        for name in (entry.name, *entry.aliases)
    }
    requested = set(names)
    unknown = sorted(requested - set(by_name))
    if unknown:
        raise ValueError(f"unknown agent(s): {', '.join(unknown)}")
    canonical_names = {by_name[name].name for name in requested}
    return [entry for entry in catalog if entry.name in canonical_names]


def native_execution_error(agent_name: str) -> str | None:
    """Why this agent cannot run on hosted (Nebius native) execution yet.

    Returns None when native execution can run it (or the name is unknown,
    which catalog validation reports separately). The set comes from native
    admission itself, so the catalog, API and admission cannot disagree.
    """
    from loom.service_execution_materialization import NATIVE_EXECUTION_AGENT_NAMES

    agent = get_agent(agent_name, include_internal=True)
    if agent is None or {agent.name, *agent.aliases} & NATIVE_EXECUTION_AGENT_NAMES:
        return None
    runnable = ", ".join(sorted(NATIVE_EXECUTION_AGENT_NAMES))
    return (
        f"agent {agent.display_name or agent.name!r} is not yet runnable on hosted "
        f"(Nebius) execution; currently only {runnable} run there (#2054)"
    )


def selection_agents(
    trial_config: Mapping[str, Any],
    combinations: Sequence[Any],
    indices: set[int] | None = None,
) -> list[tuple[str, str]]:
    """(context, agent_name) per selection of a batch; `combinations` may hold
    `Combination` models or stored dicts. `indices` limits combinations."""
    if combinations:
        out: list[tuple[str, str]] = []
        for i, combo in enumerate(combinations):
            if indices is not None and i not in indices:
                continue
            name = combo.get("agent_name") if isinstance(combo, Mapping) else combo.agent_name
            if isinstance(name, str) and name:
                out.append((f"combinations[{i}]", name))
        return out
    name = trial_config.get("agent_name")
    return [("trial_config", name)] if isinstance(name, str) and name else []


def native_selections_error(
    backend: str | None,
    selections: Iterable[tuple[str, str]],
) -> str | None:
    """First selection hosted Nebius execution cannot run yet, as
    `"<context>: <reason>"`, or None. Other backends are unaffected."""
    from loom.service_execution_backend import NEBIUS_BACKEND

    if backend != NEBIUS_BACKEND:
        return None
    for context, agent_name in selections:
        err = native_execution_error(agent_name)
        if err is not None:
            return f"{context}: {err}"
    return None


def validate_agent_model_compat(
    agent_name: str,
    model: ModelSpec | None,
) -> str | None:
    """Check that `(agent, model)` is a runnable combo.

    Returns None on success, or a human-readable error string on
    rejection. Callers convert non-None returns into a 400 HTTPException.
    """
    agent = get_agent(agent_name)
    if agent is None:
        return f"unknown agent_name {agent_name!r}"
    if agent.product_support != "supported":
        return (
            f"agent {agent_name!r} is not available for new submissions: "
            f"{agent.deferred_reason}"
        )

    if agent.needs_model and model is None:
        return f"agent {agent_name!r} requires a model — got null"
    if not agent.needs_model and model is not None:
        return f"agent {agent_name!r} does not take a model — got {model.provider}/{model.name}"
    if model is None:
        return None

    if model.source not in _HOSTED_MODEL_SOURCES:
        return (
            f"model source {model.source!r} is retired for hosted submissions; "
            "use an OpenAI-compatible Provider Connection"
        )

    if "*" not in agent.supported_providers:
        if model.provider not in agent.supported_providers:
            return (
                f"agent {agent_name!r} supports providers "
                f"{list(agent.supported_providers)}, got "
                f"{model.provider!r}"
            )

    if model.source not in agent.supported_model_sources:
        return (
            f"agent {agent_name!r} supports sources "
            f"{list(agent.supported_model_sources)}, got "
            f"{model.source!r}"
        )

    if model.local_server is not None:
        return f"model.local_server set but source is {model.source!r} (not 'local-server')"

    readiness_err = agent.readiness_error()
    if readiness_err is not None:
        return readiness_err

    return None


__all__ = [
    "AgentEntry",
    "AgentKind",
    "CatalogVisibility",
    "ProductSupport",
    "RuntimeContract",
    "get_agent",
    "known_names",
    "list_agents",
    "native_execution_error",
    "native_selections_error",
    "selection_agents",
    "validate_agent_model_compat",
]
