"""Primitive types and scalar enums shared across Loom models.

Spec §2.3 (Capabilities), §4.1 (Task), §4.2 (Supporting types), §4.5 (TrialState).
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

AgentVersion = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")]

# Scalar fields that match Postgres claim-query semantics exactly.
OS = Literal["linux", "windows"]
GPUVendor = Literal["none", "nvidia"]
CPUArch = Literal["x86_64", "arm64"]
RequiredCPUArch = Literal["x86_64", "arm64", "any"]

# Verifier semantics.
VerifierEnvMode = Literal["shared", "separate"]
# Hosted execution isolation selection (#2314).
IsolationSelection = Literal["auto", "container", "guest"]

# Multi-step aggregation strategy.
MultiStepRewardStrategy = Literal["mean", "min", "weighted", "final"]

# Resource enforcement (trimmed from Harbor's 5 to 3 per spec §2.3).
ResourceMode = Literal["auto", "limit", "guarantee"]

# Capability-axis tag for NetworkPolicy. Matches NetworkPolicy subclass `kind` field.
NetworkPolicyKind = Literal[
    "public", "no-network", "gateway-only", "allowlist", "web-allowlist", "public-web",
]

# Logging level (spec §7.2).
LogLevel = Literal["debug", "info", "warn", "error", "fatal"]

SandboxBackend = Literal["docker", "modal", "fake"]


ModelSource = Literal["api", "local-server", "hf"]
HFExecution = Literal["local-vllm", "inference-api"]


class ModelSpec(BaseModel):
    """Identifies an LLM model the agent should call (spec §4.2).

    `source` discriminates execution path (default "api" preserves the
    existing catalog-backed-API path and keeps old rows valid):

    - **api**: provider's hosted API, routed through the LLM Gateway. The
      only source hosted submissions admit.
    - **local-server** and **hf** (with `hf_execution`): retired for hosted
      execution (#2054). They still parse so historical records stay
      readable; the standalone CLI resolves local models on its own.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    provider: str
    name: str
    source: ModelSource = "api"
    local_server: str | None = None
    hf_execution: HFExecution = "local-vllm"
    tier: str | None = None
    region: str | None = None
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None

    def to_gateway_model_string(self) -> str:
        """Serialize this spec into the string the gateway's chat route
        parses as `provider/name`.

        `api` maps to `<provider>/<name>`. The retired sources keep their
        historical strings (`local/<server>/<name>`, `huggingface/<name>`,
        `local-vllm/<name>`), which the Gateway now rejects (#2054).
        """
        if self.source == "api":
            return f"{self.provider}/{self.name}"
        if self.source == "local-server":
            if not self.local_server:
                raise ValueError(
                    "ModelSpec.source='local-server' requires local_server to be set",
                )
            return f"local/{self.local_server}/{self.name}"
        if self.source == "hf":
            if self.hf_execution == "inference-api":
                return f"huggingface/{self.name}"
            return f"local-vllm/{self.name}"
        raise ValueError(f"unknown ModelSpec.source {self.source!r}")
