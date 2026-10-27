"""Bounded Compose declarations executed inside a task-owned guest daemon."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom.models.healthcheck import HealthcheckSpec


class ComposeBuild(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    context: str
    dockerfile: str
    args: dict[str, str] = Field(default_factory=dict)
    target: str | None = None
    no_cache: bool = False


class ComposeServiceSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str = Field(pattern=r"^[a-z][a-z0-9-]{0,54}$")
    image: str | None = None
    build: ComposeBuild | None = None
    command: tuple[str, ...] | None = None
    entrypoint: tuple[str, ...] | None = None
    environment: dict[str, str] = Field(default_factory=dict)
    depends_on: dict[str, Literal["service_started", "service_healthy", "service_completed_successfully"]] = Field(default_factory=dict)
    network_service: str | None = None
    ports: tuple[int, ...] = ()
    cpus: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    memory_mb: int | None = Field(default=None, gt=0)
    shm_size_bytes: int | None = Field(default=None, gt=0)
    healthcheck: HealthcheckSpec | None = None
    capabilities: tuple[Literal["SYS_PTRACE"], ...] = ()
    user: str | None = None
    workdir: str | None = None


class NativeComposeSpec(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    source: str
    services: tuple[ComposeServiceSpec, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def valid_topology(self) -> NativeComposeSpec:
        services = {s.name: s for s in self.services}
        if len(services) != len(self.services) or "main" not in services:
            raise ValueError("Compose requires unique services and main")
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(name: str) -> None:
            if name in visiting:
                raise ValueError("Compose service dependency cycle")
            if name in visited:
                return
            if name not in services:
                raise ValueError("Compose dependency names an unknown service")
            visiting.add(name)
            service = services[name]
            dependencies = set(service.depends_on)
            if service.network_service:
                dependencies.add(service.network_service)
            for dependency in dependencies:
                visit(dependency)
                if service.depends_on.get(dependency) == "service_healthy" and services[dependency].healthcheck is None:
                    raise ValueError("healthy Compose dependency requires a healthcheck")
            visiting.remove(name)
            visited.add(name)

        for name in services:
            visit(name)
        return self

    def ordered_services(self) -> tuple[ComposeServiceSpec, ...]:
        services = {s.name: s for s in self.services}
        ordered: dict[str, ComposeServiceSpec] = {}

        def visit(name: str) -> None:
            if name in ordered:
                return
            service = services[name]
            for dependency in (*service.depends_on, *((service.network_service,) if service.network_service else ())):
                visit(dependency)
            ordered[name] = service

        for name in services:
            visit(name)
        return tuple(ordered.values())
