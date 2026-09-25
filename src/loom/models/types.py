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

    - **api**: provider's hosted API (Anthropic/OpenAI/Google/HF Inference).
      Routed through the LLM Gateway.
    - **local-server**: pre-configured OpenAI-compatible local server
      (ollama, lm-studio, llama.cpp, vLLM). `local_server` names the
      operator's entry; the worker reads its base_url from config.
    - **hf**: HuggingFace model. `hf_execution` picks how to run it —
      "local-vllm" (default) spawns vLLM on a GPU worker;
      "inference-api" calls HF Inference Endpoints (managed, metered).
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

        Mapping (PR-D):

        - api: `<provider>/<name>` (existing — anthropic/openai/together)
        - local-server: `local/<local_server>/<name>` (gateway reads the
          operator-configured base_url for `local_server` and proxies the
          OpenAI-compatible request)
        - hf + inference-api: `huggingface/<name>` (LiteLLM's HF
          Inference path, requires `HF_TOKEN` on the gateway)
        - hf + local-vllm: `local-vllm/<name>` (worker-spawned vLLM
          subprocess; the gateway returns a clear 501 today and the
          worker handles this path directly in a follow-up)
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
