from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from loom.nebius_kubernetes import NebiusKubernetesConnection, connection_from_fields
from loom.nebius_pool_settings import PoolRuntimeSettings
from loom_execution_actuator.task_image_settings import NativeTaskImageSettings


class ExecutionActuatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_EXECUTION_ACTUATOR_",
        extra="ignore",
    )

    kubernetes_endpoint: str | None = None
    kubernetes_ca_file: Path | None = None
    kubernetes_nebius_credentials_file: Path | None = None

    @model_validator(mode="after")
    def _remote_kubernetes_complete(self) -> "ExecutionActuatorSettings":
        _ = self.kubernetes_connection
        if self.global_pool is not None:
            participant = self.global_pool.participant
            participant.target(self.target_id, "trial")
            if participant.execution_namespace.name != self.namespace:
                raise ValueError("global execution namespace binding differs")
            if self.task_image_builder is not None:
                participant.target(self.target_id, "task_image_build")
                if (participant.build_namespace.name != self.task_image_builder.namespace
                        or self.global_pool.logical_pool_id != self.task_image_builder.pool_id):
                    raise ValueError("global native build binding differs")
        return self

    @property
    def kubernetes_connection(self) -> NebiusKubernetesConnection | None:
        return connection_from_fields(
            self.kubernetes_endpoint,
            self.kubernetes_ca_file,
            self.kubernetes_nebius_credentials_file,
        )

    db_url: str
    global_pool: PoolRuntimeSettings | None = None
    execution_image_admission_public_keys_json: str = '{"schema_version":1,"keys":[]}'
    task_image_builder: NativeTaskImageSettings | None = None
    controller_id: str = Field(min_length=1, max_length=120)
    target_id: str = Field(min_length=1, max_length=80)
    namespace: str = Field(pattern=r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
    pod_identity_audience: str | None = Field(default=None, min_length=1, max_length=255)
    runtime_class_name: str | None = Field(default=None, min_length=1, max_length=63)
    node_selector: dict[str, str] = Field(default_factory=dict)
    tolerations: tuple[dict[str, str], ...] = ()
    service_account_name: str = "loom-execution-attempt"
    credential_broker_url: str = (
        "http://loom-llm-gateway.loom.svc.cluster.local:9100/internal/service-execution"
    )
    poll_seconds: float = Field(default=2.0, ge=0.25, le=60)
    full_reconcile_seconds: float = Field(default=30.0, ge=5, le=300)
    watch_timeout_seconds: int = Field(default=15, ge=5, le=60)
    command_limit: int = Field(default=20, ge=1, le=100)
    command_lease_seconds: int = Field(default=60, ge=5, le=300)
    delete_grace_seconds: int = Field(default=30, ge=0, le=300)
    health_host: str = "0.0.0.0"
    health_port: int = Field(default=8093, ge=1024, le=65535)
