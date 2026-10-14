"""Lower a bounded single-service Compose workbench onto native sandboxes.

The workbench's lifetime and exec streams are owned by the sandbox controller;
stdin_open/tty keep the workbench resident, rather than exposing a host terminal.
Commands, entrypoints, mounts, devices and additional services are not lowered.
Privileged workbenches require a task-owned guest kernel, never host privilege.
"""

import tomllib
from pathlib import Path, PurePosixPath
from typing import Any

import yaml  # type: ignore[import-untyped]


def project_single_service_compose(bundle: Path, environment: dict[str, Any]) -> None:
    environment.pop("single_service_compose", None)
    sources = environment.get("compose_files", [])
    if len(sources) != 1:
        return
    source = sources[0]
    raw = yaml.safe_load((bundle / source).read_text())
    if not isinstance(raw, dict) or set(raw) - {"services", "version", "name"}:
        return
    services = raw.get("services")
    if not isinstance(services, dict) or len(services) != 1:
        return
    service, config = next(iter(services.items()))
    if not isinstance(config, dict) or set(config) - {"build", "privileged", "stdin_open", "tty"}:
        return
    build = config.get("build")
    if isinstance(build, str):
        build = {"context": build}
    if not isinstance(build, dict) or set(build) - {"context", "dockerfile"}:
        return
    for field in ("privileged", "stdin_open", "tty"):
        if field in config and not isinstance(config[field], bool):
            raise ValueError(f"Compose services.{service}.{field} must be boolean")
    context = build.get("context", ".")
    dockerfile = build.get("dockerfile", "Dockerfile")
    for value in (context, dockerfile):
        if not isinstance(value, str) or not value or any(token in value for token in ("\\", "\x00", "$", "://")):
            raise ValueError("native Compose build inputs must be literal bundle-relative paths")
        path = PurePosixPath(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("native Compose build inputs cannot leave their source directory")
    context_path = PurePosixPath(source).parent / context
    dockerfile_path = context_path / dockerfile
    if not (bundle / context_path).is_dir() or not (bundle / dockerfile_path).is_file():
        raise ValueError("native Compose build context or Dockerfile is missing")
    if any(not (bundle / path).resolve().is_relative_to(bundle.resolve()) for path in (context_path, dockerfile_path)):
        raise ValueError("native Compose build inputs leave the task bundle")
    # The Harbor projector fills conventional defaults before inspecting files.
    # Only authored overrides can conflict with the Compose build declaration.
    authored = tomllib.loads((bundle / "task.toml").read_text()).get("environment", {})
    if environment.get("docker_image") or (
        "dockerfile" in authored and environment.get("dockerfile") != dockerfile_path.as_posix()
    ) or (
        "docker_build_context" in authored and environment.get("docker_build_context") != context_path.as_posix()
    ):
        raise ValueError("task environment build inputs conflict with Compose")
    if config.get("privileged"):
        if environment.get("user", "root") not in {"root", "0", "0:0"}:
            raise ValueError("privileged Compose requires root inside an isolated guest")
        requirements = dict(environment.get("execution_requirements") or {})
        capabilities = list(requirements.get("capabilities", []))
        if "isolated_kernel_settings" not in capabilities:
            capabilities.append("isolated_kernel_settings")
        requirements["capabilities"] = capabilities
        environment.update(user="root", execution_requirements=requirements)
    environment.update(
        dockerfile=dockerfile_path.as_posix(), docker_build_context=context_path.as_posix(),
        single_service_compose={"source": source, "service": service,
                                **{field: config.get(field, False) for field in ("privileged", "stdin_open", "tty")}},
    )
