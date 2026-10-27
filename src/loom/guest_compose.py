"""Lower supported native Compose input without losing service topology.

The original YAML remains in the immutable task bundle. No host environment
interpolation, mounts, host networking, devices or Docker sockets are granted.
"""
from __future__ import annotations

import re
import shlex
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import yaml  # type: ignore[import-untyped]

from loom.models.compose import ComposeBuild, ComposeServiceSpec, NativeComposeSpec
from loom.models.healthcheck import HealthcheckSpec

_FIELDS = {"build", "image", "command", "entrypoint", "environment", "depends_on", "network_mode",
           "expose", "healthcheck", "cpus", "mem_limit", "shm_size", "cap_add", "user", "working_dir", "pull_policy"}

# Planner allocations for undeclared sidecars, not upstream task requirements.
SIDECAR_CPUS = 1.0
SIDECAR_MEMORY_MB = 1024
SIDECAR_STORAGE_MB = 2048
GUEST_DOCKER_CPUS = 1.0
GUEST_DOCKER_MEMORY_MB = 1024
GUEST_DOCKER_STORAGE_MB = 4096


@dataclass(frozen=True)
class ComposeResourceEnvelope:
    cpus: float
    memory_mb: int
    storage_mb: int
    policy_cpus: float
    policy_memory_mb: int
    policy_storage_mb: int


def compose_resource_envelope(
    topology: NativeComposeSpec, *, main_cpus: float, main_memory_mb: int, main_storage_mb: int,
) -> ComposeResourceEnvelope:
    """Preserve main requirements and add bounded sidecars plus daemon capacity."""
    main = next(service for service in topology.services if service.name == "main")
    cpus, memory = max(main_cpus, main.cpus or 0), max(main_memory_mb, main.memory_mb or 0)
    policy_cpu, policy_memory, policy_storage = GUEST_DOCKER_CPUS, GUEST_DOCKER_MEMORY_MB, GUEST_DOCKER_STORAGE_MB
    for service in topology.services:
        if service.name == "main":
            continue
        cpus += service.cpus or SIDECAR_CPUS
        memory += service.memory_mb or SIDECAR_MEMORY_MB
        policy_cpu += 0 if service.cpus else SIDECAR_CPUS
        policy_memory += 0 if service.memory_mb else SIDECAR_MEMORY_MB
        policy_storage += SIDECAR_STORAGE_MB
    return ComposeResourceEnvelope(cpus + GUEST_DOCKER_CPUS, memory + GUEST_DOCKER_MEMORY_MB,
        main_storage_mb + policy_storage, policy_cpu, policy_memory, policy_storage)


def _literal(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("Compose values must be literal strings or numbers")
    text = str(value)
    if "\x00" in text or re.search(r"(?<!\$)(?:\$\$)*\$(?:[A-Za-z_]|\{)", text):
        raise ValueError("Compose host environment interpolation is unsupported")
    return text.replace("$$", "$")


def _environment(value: Any) -> dict[str, str]:
    if isinstance(value, list):
        if any(not isinstance(v, str) or "=" not in v for v in value):
            raise ValueError("Compose environment requires explicit values")
        value = dict(v.split("=", 1) for v in value)
    if not isinstance(value, dict):
        raise ValueError("Compose environment must be a mapping or key=value list")
    return {str(key): _literal(item) for key, item in value.items()}


def _argv(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return tuple(shlex.split(_literal(value)))
    if isinstance(value, list) and all(isinstance(v, str) for v in value):
        return tuple(_literal(v) for v in value)
    raise ValueError("Compose command must be a string or argv list")


def _seconds(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value) / 1_000_000_000
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ns|us|ms|s|m|h)", str(value))
    if not match:
        raise ValueError("unsupported Compose duration")
    return float(match[1]) * {"ns": 1e-9, "us": 1e-6, "ms": .001, "s": 1, "m": 60, "h": 3600}[match[2]]


def _bytes(value: Any) -> int:
    match = re.fullmatch(r"([0-9]+)(b|k|kb|m|mb|g|gb)?", str(value).lower())
    if not match:
        raise ValueError("unsupported Compose byte size")
    return int(match[1]) * {None: 1, "b": 1, "k": 1024, "kb": 1024, "m": 1024**2, "mb": 1024**2, "g": 1024**3, "gb": 1024**3}[match[2]]


def _health(value: Any) -> HealthcheckSpec | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) - {"test", "interval", "timeout", "retries", "start_period", "start_interval"}:
        raise ValueError("unsupported Compose healthcheck fields")
    test = value.get("test")
    if isinstance(test, str):
        command = _literal(test)
    elif isinstance(test, list) and len(test) > 1 and test[0] in {"CMD", "CMD-SHELL"}:
        command = shlex.join([_literal(v) for v in test[1:]]) if test[0] == "CMD" else _literal(test[1])
        if test[0] == "CMD-SHELL" and len(test) != 2:
            raise ValueError("Compose shell healthcheck requires one command")
    else:
        raise ValueError("Compose healthcheck requires CMD or CMD-SHELL")
    return HealthcheckSpec(command=command,
        **{target: _seconds(value[key]) for key, target in [("interval", "interval_sec"), ("timeout", "timeout_sec"),
           ("start_period", "start_period_sec"), ("start_interval", "start_interval_sec")] if key in value},
        **({"retries": value["retries"]} if "retries" in value else {}))


def compile_native_compose(bundle: Path, environment: dict[str, Any]) -> NativeComposeSpec:
    sources = environment.get("compose_files", [])
    if len(sources) != 1:
        raise ValueError("native Compose currently requires one authored file")
    source = PurePosixPath(sources[0])
    if source.is_absolute() or ".." in source.parts:
        raise ValueError("Compose source must remain in the task bundle")
    document = yaml.safe_load((bundle / source).read_text())
    if not isinstance(document, dict) or set(document) - {"services", "version", "name"}:
        raise ValueError("unsupported Compose top-level fields")
    declarations = document.get("services")
    if not isinstance(declarations, dict):
        raise ValueError("Compose services must be a mapping")
    declarations = {"main": {}, **declarations}
    services = []
    for name, raw in declarations.items():
        if not isinstance(raw, dict) or set(raw) - _FIELDS:
            raise ValueError(f"unsupported Compose services.{name} fields")
        main = name == "main"
        build = raw.get("build")
        image = raw.get("image", environment.get("docker_image") if main else None)
        if image is not None and (not isinstance(image, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/@+-]*", image)):
            raise ValueError("Compose image must be a literal registry reference")
        if build is None and main and not image:
            build = {"context": str(PurePosixPath(environment.get("docker_build_context", "environment")).relative_to(source.parent)),
                     "dockerfile": str(PurePosixPath(environment.get("dockerfile", "environment/Dockerfile")).relative_to(PurePosixPath(environment.get("docker_build_context", "environment"))))}
        if isinstance(build, str):
            build = {"context": build}
        build_spec = None
        if build is not None:
            if not isinstance(build, dict) or set(build) - {"context", "dockerfile", "args", "target", "no_cache"}:
                raise ValueError("unsupported Compose build fields")
            context = source.parent / _literal(build.get("context", "."))
            dockerfile = context / _literal(build.get("dockerfile", "Dockerfile"))
            for path in (context, dockerfile):
                if path.is_absolute() or ".." in path.parts or not (bundle / path).resolve().is_relative_to(bundle.resolve()):
                    raise ValueError("Compose build input leaves the task bundle")
            if not (bundle / context).is_dir() or not (bundle / dockerfile).is_file():
                raise ValueError("Compose build input is missing")
            build_spec = ComposeBuild(context=str(context), dockerfile=str(dockerfile), args=_environment(build.get("args", {})),
                target=build.get("target"), no_cache=build.get("no_cache", False))
        if image is None and build_spec is None:
            raise ValueError("Compose service requires an image or build")
        dependencies = raw.get("depends_on", {})
        if isinstance(dependencies, list):
            dependencies = dict.fromkeys(dependencies, "service_started")
        if not isinstance(dependencies, dict):
            raise ValueError("Compose depends_on must be a list or mapping")
        converted = {}
        for service, condition in dependencies.items():
            if isinstance(condition, dict):
                if set(condition) != {"condition"}:
                    raise ValueError("unsupported Compose dependency fields")
                condition = condition["condition"]
            converted[service] = condition
        network = raw.get("network_mode")
        if network is not None and (not isinstance(network, str) or not network.startswith("service:")):
            raise ValueError("Compose only permits a declared service network namespace")
        if raw.get("pull_policy", "build" if build_spec else "missing") not in {"build", "missing", "if_not_present"}:
            raise ValueError("unsupported Compose pull policy")
        health = _health(raw.get("healthcheck"))
        services.append(ComposeServiceSpec(name=name, image=image, build=build_spec,
            command=_argv(raw.get("command", ["sh", "-c", "sleep infinity"] if main else None)),
            entrypoint=_argv(raw.get("entrypoint")),
            environment={**(environment.get("environment", {}) if main else {}), **_environment(raw.get("environment", {}))},
            depends_on=converted, network_service=network.removeprefix("service:") if network else None,
            ports=tuple(int(v) for v in raw.get("expose", [])),
            cpus=raw.get("cpus", environment.get("cpus") if main else None),
            memory_mb=(_bytes(raw["mem_limit"]) // 1024**2 if "mem_limit" in raw else environment.get("memory_mb") if main else None),
            shm_size_bytes=_bytes(raw["shm_size"]) if "shm_size" in raw else None,
            healthcheck=health, capabilities=tuple(raw.get("cap_add", [])),
            user=str(raw["user"]) if "user" in raw else None, workdir=raw.get("working_dir")))
    return NativeComposeSpec(source=str(source), services=tuple(services))
