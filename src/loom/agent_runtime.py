"""Published native agent releases and the immutable binding frozen at submission."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.execution_image_admission import SignedImageAdmissionV1
from loom.models.types import AgentVersion

# These published sources compare the whole /health object to {"ready": True}.
# The sandbox also returns instance_id (#2337). Check immutable source identity,
# not Harbor version or the user-facing label: aliases must have the same result.
# Their shared bridge label, "1.0", also appears on compatible newer sources.
_STRICT_HEALTH_BRIDGE_SOURCES = frozenset({
    "44dbda72dff90fde5c29b094227db6c5ee03389b",
    "6946d8bf6bc8db5d0d3ff0a8e0d47952554b595d",
    "6b96a28c497c9a3ee660e41da188e4b72b1c145b",
    "7912ec076babc2fe51768fd2596b70c7de9e3d31",
    "8186217aaa7f82fe652064cc82c779d0c465d0c4",
    "923f1de6878fc5e909231fdc50c796ce022aa6bb",
    "9754406ca5b8905d4a3c19811ade5987cba63259",
    "ab7647782b8c39caaa25aeaec0e47fb7019433c5",
    "ce08053f89c899c36ce38151487f8ab984446e61",
    "d67e2e0ed095c550040fe4787248a1dbf964d079",
})


class AgentRuntimeBindingV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    agent_name: Literal["terminus-2"] = "terminus-2"
    agent_version: AgentVersion
    runtime_contract: Literal["loom.terminus-controller.v1"]
    agent_image_ref: str = Field(pattern=r"^[^\s@]+@sha256:[0-9a-f]{64}$")
    harbor_version: str = Field(min_length=1, max_length=128)
    harbor_source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    loom_bridge_revision: str = Field(min_length=1, max_length=128)
    publisher_source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")

    def compatibility_error(self) -> str | None:
        if (self.publisher_source_revision in _STRICT_HEALTH_BRIDGE_SOURCES
                or self.loom_bridge_revision in _STRICT_HEALTH_BRIDGE_SOURCES):
            return (
                f"Published agent version {self.agent_name}/{self.agent_version} has an "
                "incompatible Loom bridge: it rejects the sandbox health instance_id field; "
                "choose a compatible published version or explicitly select deployment default."
            )
        return None

    def public_metadata(self) -> dict[str, str]:
        reason = self.compatibility_error()
        return {
            "agent_version": self.agent_version,
            "harbor_version": self.harbor_version,
            "loom_bridge_revision": self.loom_bridge_revision,
            "readiness_status": "unavailable" if reason else "ready",
            "readiness_message": reason or "",
        }


class AgentRuntimeReleaseV1(AgentRuntimeBindingV1):
    schema_version: Literal["loom.agent-runtime-release.v1"]
    image_admission: SignedImageAdmissionV1

    @model_validator(mode="after")
    def matching_image(self) -> AgentRuntimeReleaseV1:
        if self.image_admission.statement.image_ref != self.agent_image_ref:
            raise ValueError("agent release admission subject differs from its image")
        return self

    def binding(self) -> AgentRuntimeBindingV1:
        return AgentRuntimeBindingV1.model_validate(
            self.model_dump(exclude={"schema_version", "image_admission"})
        )
