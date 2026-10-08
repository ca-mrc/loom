"""Task bundle compatibility preflight rules.

The rules in this module are intentionally diagnostic: they tell task owners
what to fix in their bundle before Loom fans out expensive worker execution.
They must not silently repair user-owned Dockerfiles or bundle layouts.
"""

from __future__ import annotations

import re
import shlex
import tomllib
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from loom.dockerfile_instructions import DockerfileParseError, dockerfile_instructions
from loom.models.task_compatibility import CompatibilitySeverity as CompatibilitySeverity
from loom.models.task_compatibility import (
    TaskBundleCompatibilityIssue as TaskBundleCompatibilityIssue,
)

_PYTORCH_INDEX_RE = re.compile(r"https?://download\.pytorch\.org/whl(?:/|\b)")
_PYTORCH_INDEX_PACKAGES = frozenset({
    "torch",
    "torchaudio",
    "torchvision",
})
_FROM_NODE_RE = re.compile(
    r"^\s*FROM\s+(?:--platform=\S+\s+)?(?:\S+/)?node:(\d+)(?:\D|$)",
    re.IGNORECASE,
)
_NODESOURCE_MAJOR_RE = re.compile(r"\b(?:setup|node)_(\d+)\.x\b")
_NPM_LATEST_RE = re.compile(r"\bnpm\s+(?:install|i)\b[^\n]*\bnpm@latest\b")
_AMD64_PLATFORM_RE = re.compile(
    r"^\s*FROM\s+--platform=(?P<platform>linux/(?:amd64|x86_64))\b",
    re.IGNORECASE,
)
_AMD64_ASSET_RE = re.compile(
    r"(?P<asset>(?:linux[_-](?:amd64|x86_64)|Linux-x86_64|x86_64\.sh)[^\s;&|<>'\"]*)",
)
_TRAILING_TRUE_RE = re.compile(r"(?:^|\s)\|\|\s*true\s*$")
_WORKDIR_APP_RE = re.compile(r"^\s*WORKDIR\s+/app(?:/|\s|$)", re.IGNORECASE)
_APP_WRITE_RE = re.compile(
    r"(?P<cmd>\btar\b[^&;|]*(?:\s-?[A-Za-z]*f[A-Za-z]*\s+|\s--file(?:=|\s+))/app/"
    r"|>\s*/app/)",
)
_COPY_CONTEXT_TO_APP_RE = re.compile(
    r"^\s*(?:COPY|ADD)\s+(?:--\S+\s+)*\.\s+/app/?\s*$",
    re.IGNORECASE,
)
_APP_ABSOLUTE_REF_RE = re.compile(r"(?<![\w.-])(/app/[^\s;&|<>'\"]+)")
_DNS_MUTATION_TARGETS = (
    "/etc/resolv.conf",
    "/etc/nsswitch.conf",
    "/etc/hosts",
)


def collect_task_dir_compatibility_issues(
    task_dir: Path,
    *,
    task_config: Mapping[str, Any] | None = None,
    target_arches: Iterable[str] | None = None,
) -> list[TaskBundleCompatibilityIssue]:
    """Return structured compatibility issues for every Dockerfile in a task."""

    config = task_config if task_config is not None else _load_task_toml(task_dir)
    effective_target_arches = _target_arches_for_task_dir(
        task_dir,
        task_config=config,
        target_arches=target_arches,
    )
    issues: list[TaskBundleCompatibilityIssue] = []
    for dockerfile in sorted(task_dir.rglob("Dockerfile*")):
        if not dockerfile.is_file():
            continue
        try:
            text = dockerfile.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = dockerfile.read_text()
        relative = dockerfile.relative_to(task_dir)
        found = collect_dockerfile_compatibility_issues(
            text, path=relative, task_dir=task_dir, target_arches=effective_target_arches,
        )
        if _unused_prebuilt_dockerfile(task_dir, relative, config):
            # The image admission boundary validates the actual prebuilt image.
            # Keep source diagnostics, but an unused build recipe cannot block
            # execution of a task that does not build that recipe.
            found = [issue.model_copy(update={
                "severity": CompatibilitySeverity.WARNING,
                "evidence": {**issue.evidence, "build_usage": "unused_prebuilt_recipe"},
            }) for issue in found]
        issues.extend(found)
    return issues


def _unused_prebuilt_dockerfile(
    task_dir: Path, relative: Path, config: Mapping[str, Any],
) -> bool:
    environment = config.get("environment")
    if not isinstance(environment, Mapping) or not environment.get("docker_image"):
        return False
    if relative.as_posix() not in {"Dockerfile", "environment/Dockerfile"}:
        return False
    # An explicit build or Compose definition may still consume this file.
    def declares_build(value: Any) -> bool:
        if isinstance(value, Mapping):
            if value.get("dockerfile") or value.get("compose_files"):
                return True
            return any(declares_build(item) for item in value.values())
        if isinstance(value, (tuple, list)):
            return any(declares_build(item) for item in value)
        return False

    if declares_build(config):
        return False
    return not any((task_dir / "environment" / name).exists() for name in (
        "compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml",
    ))


def validate_task_dir_compatibility(task_dir: Path) -> None:
    """Raise ValueError when a task directory has hard compatibility issues."""

    issues = [
        issue
        for issue in collect_task_dir_compatibility_issues(task_dir)
        if issue.severity == CompatibilitySeverity.ERROR
    ]
    if issues:
        raise ValueError(format_compatibility_issues(issues))


def collect_dockerfile_compatibility_issues(
    text: str,
    *,
    path: Path | None = None,
    task_dir: Path | None = None,
    target_arches: frozenset[str] = frozenset(),
) -> list[TaskBundleCompatibilityIssue]:
    label = path.as_posix() if path is not None else "Dockerfile"
    fixed_node_major: int | None = None
    app_dir_created = False
    copied_context_to_app = False
    seen_app_root_references: set[str] = set()
    issues: list[TaskBundleCompatibilityIssue] = []

    try:
        instructions = dockerfile_instructions(text)
    except DockerfileParseError as exc:
        return [TaskBundleCompatibilityIssue(
            code="TASK_COMPAT_DOCKERFILE_PARSE",
            severity=CompatibilitySeverity.ERROR,
            path=label,
            line=exc.line,
            phase="task_image_build",
            message=str(exc),
            hint="Complete the Dockerfile instruction or heredoc before running compatibility checks.",
        )]

    for instruction in instructions:
        line_no = instruction.line
        stripped = f"{instruction.keyword} {instruction.arguments}".strip()
        upper = stripped.upper()
        if upper.startswith("FROM "):
            issues.extend(
                _platform_compatibility_issues(
                    stripped,
                    target_arches=target_arches,
                    label=label,
                    line_no=line_no,
                ),
            )
            app_dir_created = False
            copied_context_to_app = False
            fixed_node_major = _node_major_from_from_instruction(stripped)
            continue

        if _WORKDIR_APP_RE.search(stripped):
            app_dir_created = True
            continue

        if _COPY_CONTEXT_TO_APP_RE.search(stripped):
            copied_context_to_app = True
            continue

        if not upper.startswith("RUN "):
            continue

        body = stripped[4:].strip()
        if _broad_trailing_true(body):
            issues.append(TaskBundleCompatibilityIssue(
                code="TASK_COMPAT_BROAD_TRAILING_TRUE",
                severity=CompatibilitySeverity.ERROR,
                path=label,
                line=line_no,
                phase="task_image_build",
                message=(
                    "Dockerfile has trailing || true after a multi-command setup "
                    "chain; this can hide deterministic setup failures"
                ),
                hint=(
                    "Scope tolerated failures to the optional command, for example "
                    "by wrapping only that command in parentheses."
                ),
            ))

        for segment in _shell_segments(body):
            issues.extend(_pip_index_issues(segment, label=label, line_no=line_no))
            issues.extend(_dns_mutation_issues(segment, label=label, line_no=line_no))
            issues.extend(
                _amd64_asset_issues(
                    segment,
                    target_arches=target_arches,
                    label=label,
                    line_no=line_no,
                ),
            )
            if match := _NODESOURCE_MAJOR_RE.search(segment):
                fixed_node_major = int(match.group(1))
            if fixed_node_major is not None and _NPM_LATEST_RE.search(segment):
                issues.append(TaskBundleCompatibilityIssue(
                    code="TASK_COMPAT_NPM_LATEST_WITH_FIXED_NODE",
                    severity=CompatibilitySeverity.ERROR,
                    path=label,
                    line=line_no,
                    phase="task_image_build",
                    message=(
                        f"Dockerfile uses moving npm@latest under fixed Node "
                        f"{fixed_node_major}"
                    ),
                    hint=(
                        "Pin an npm major compatible with the selected Node major "
                        "instead of installing npm@latest."
                    ),
                    evidence={"node_major": str(fixed_node_major)},
                ))
            for command, start in _app_write_commands(segment):
                before = segment[:start]
                if app_dir_created or _mkdir_creates_app_root(
                    before,
                    app_dir_created=app_dir_created,
                ):
                    continue
                issues.append(TaskBundleCompatibilityIssue(
                    code="TASK_COMPAT_APP_PARENT_MISSING",
                    severity=CompatibilitySeverity.ERROR,
                    path=label,
                    line=line_no,
                    phase="task_image_build",
                    message=(
                        "Dockerfile writes to /app before creating the /app "
                        f"parent directory: {command}"
                    ),
                    hint=(
                        "Create /app with `RUN mkdir -p /app` before writing "
                        "children under it, or write to a directory created by "
                        "the task image."
                    ),
                    evidence={"command": command},
                ))
            if task_dir is not None and path is not None and copied_context_to_app:
                issues.extend(
                    _app_root_reference_issues(
                        segment,
                        task_dir=task_dir,
                        dockerfile_path=path,
                        label=label,
                        line_no=line_no,
                        seen=seen_app_root_references,
                    ),
                )
            if _mkdir_creates_app_root(
                segment,
                app_dir_created=app_dir_created,
            ):
                app_dir_created = True

    return issues


def _target_arches_for_task_dir(
    task_dir: Path,
    *,
    task_config: Mapping[str, Any] | None,
    target_arches: Iterable[str] | None,
) -> frozenset[str]:
    if target_arches is not None:
        return frozenset(_normalize_arch(arch) for arch in target_arches)
    config = task_config if task_config is not None else _load_task_toml(task_dir)
    environment = config.get("environment") if isinstance(config, Mapping) else None
    if not isinstance(environment, Mapping):
        return frozenset()
    raw_arch = environment.get("cpu_arch")
    if not isinstance(raw_arch, str) or raw_arch in {"", "any"}:
        return frozenset()
    return frozenset({_normalize_arch(raw_arch)})


def _load_task_toml(task_dir: Path) -> Mapping[str, Any]:
    task_toml = task_dir / "task.toml"
    if not task_toml.is_file():
        return {}
    try:
        parsed = tomllib.loads(task_toml.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError, UnicodeDecodeError):
        return {}
    return parsed if isinstance(parsed, Mapping) else {}


def _normalize_arch(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"aarch64", "arm64", "linux/arm64"}:
        return "arm64"
    if normalized in {"amd64", "x86_64", "linux/amd64", "linux/x86_64"}:
        return "x86_64"
    return normalized


def validate_dockerfile_compatibility(
    text: str,
    *,
    path: Path | None = None,
) -> None:
    issues = [
        issue
        for issue in collect_dockerfile_compatibility_issues(text, path=path)
        if issue.severity == CompatibilitySeverity.ERROR
    ]
    if issues:
        raise ValueError(format_compatibility_issues(issues))


def format_compatibility_issues(
    issues: list[TaskBundleCompatibilityIssue],
) -> str:
    lines: list[str] = []
    for issue in issues:
        location = f"{issue.path}:{issue.line}" if issue.line else issue.path
        lines.append(
            f"{issue.code} {location}: {issue.message}. Hint: {issue.hint}",
        )
    return "\n".join(lines)


def _node_major_from_from_instruction(instruction: str) -> int | None:
    if match := _FROM_NODE_RE.search(instruction):
        return int(match.group(1))
    return None


def _platform_compatibility_issues(
    instruction: str,
    *,
    target_arches: frozenset[str],
    label: str,
    line_no: int,
) -> list[TaskBundleCompatibilityIssue]:
    if "arm64" not in target_arches:
        return []
    match = _AMD64_PLATFORM_RE.search(instruction)
    if match is None:
        return []
    platform = match.group("platform").lower()
    return [
        TaskBundleCompatibilityIssue(
            code="TASK_COMPAT_AMD64_PLATFORM",
            severity=CompatibilitySeverity.ERROR,
            path=label,
            line=line_no,
            phase="task_image_build",
            message=(
                f"Dockerfile pins base image platform {platform}, but the task "
                "targets arm64 workers"
            ),
            hint=(
                "Use an arm64 or multi-architecture base image, or change the "
                "task cpu_arch/worker-pool target to x86_64 if the task is "
                "intentionally x86-only."
            ),
            evidence={"target_arch": "arm64", "platform": platform},
        )
    ]


def _amd64_asset_issues(
    segment: str,
    *,
    target_arches: frozenset[str],
    label: str,
    line_no: int,
) -> list[TaskBundleCompatibilityIssue]:
    if "arm64" not in target_arches:
        return []
    match = _AMD64_ASSET_RE.search(segment)
    if match is None:
        return []
    asset = match.group("asset")
    return [
        TaskBundleCompatibilityIssue(
            code="TASK_COMPAT_AMD64_ONLY_ASSET",
            severity=CompatibilitySeverity.ERROR,
            path=label,
            line=line_no,
            phase="task_image_build",
            message=(
                f"Dockerfile downloads or installs amd64-only asset {asset!r} "
                "for an arm64 task"
            ),
            hint=(
                "Select an arm64 artifact based on target architecture, use a "
                "multi-arch package manager source, or mark the task x86_64-only "
                "instead of treating it as an ARM64-compatible bundle."
            ),
            evidence={"target_arch": "arm64", "asset": asset},
        )
    ]


def _pip_index_issues(
    body: str,
    *,
    label: str,
    line_no: int,
) -> list[TaskBundleCompatibilityIssue]:
    issues: list[TaskBundleCompatibilityIssue] = []
    try:
        tokens = shlex.split(body)
    except ValueError:
        return issues
    install_index = _pip_install_index(tokens)
    if install_index is None:
        return issues
    args = tokens[install_index + 1:]
    if not _uses_pytorch_as_sole_index(args):
        return issues
    packages = _install_package_names(args)
    non_index_packages = [
        package for package in packages
        if package not in _PYTORCH_INDEX_PACKAGES
    ]
    if non_index_packages:
        formatted = ", ".join(non_index_packages)
        issues.append(TaskBundleCompatibilityIssue(
            code="TASK_COMPAT_PIP_SOLE_PACKAGE_INDEX",
            severity=CompatibilitySeverity.ERROR,
            path=label,
            line=line_no,
            phase="task_image_build",
            message=(
                "Dockerfile uses a package-specific pip index as the sole "
                f"index while installing non-index package(s): {formatted}"
            ),
            hint=(
                "Use --extra-index-url for package-specific indexes, or keep "
                "PyPI as the primary index for general dependencies."
            ),
            evidence={"packages": formatted},
        ))
    return issues


def _dns_mutation_issues(
    segment: str,
    *,
    label: str,
    line_no: int,
) -> list[TaskBundleCompatibilityIssue]:
    issues: list[TaskBundleCompatibilityIssue] = []
    for target in _DNS_MUTATION_TARGETS:
        if target not in segment:
            continue
        if not _looks_like_mutation(segment, target):
            continue
        issues.append(TaskBundleCompatibilityIssue(
            code="TASK_COMPAT_DNS_MUTATION",
            severity=CompatibilitySeverity.ERROR,
            path=label,
            line=line_no,
            phase="agent_layer_build",
            message=(
                "Dockerfile mutates DNS/NSS configuration before Loom installs "
                "the service-mode agent layer"
            ),
            hint=(
                f"Do not change {target} in the task image before agent setup. "
                "Move network breakage into the task runtime or verifier phase "
                "after the agent is installed. Move DNS breakage into a phase "
                "that runs after Loom has installed the agent layer."
            ),
            evidence={"target": target},
        ))
    return issues


def _looks_like_mutation(segment: str, target: str) -> bool:
    if re.search(rf">\s*{re.escape(target)}(?:\s|$)", segment):
        return True
    try:
        tokens = shlex.split(segment)
    except ValueError:
        return True
    mutating_commands = {
        "cp",
        "mv",
        "install",
        "ln",
        "rm",
        "sed",
        "tee",
        "touch",
        "truncate",
        "chmod",
        "chown",
    }
    command = tokens[0].rsplit("/", 1)[-1] if tokens else ""
    return command in mutating_commands and target in tokens


def _app_root_reference_issues(
    segment: str,
    *,
    task_dir: Path,
    dockerfile_path: Path,
    label: str,
    line_no: int,
    seen: set[str],
) -> list[TaskBundleCompatibilityIssue]:
    issues: list[TaskBundleCompatibilityIssue] = []
    for match in _APP_ABSOLUTE_REF_RE.finditer(segment):
        referenced = match.group(1).rstrip(".,)")
        if referenced in seen or referenced.startswith("/app/environment/"):
            continue
        seen.add(referenced)
        rel = PurePosixPath(referenced.removeprefix("/app/"))
        if not rel.parts:
            continue
        if any(part in {"", ".", ".."} for part in rel.parts):
            continue
        root_path = task_dir.joinpath(*rel.parts)
        if root_path.exists():
            continue
        dockerfile_dir = dockerfile_path.parent
        sibling_rel = dockerfile_dir.joinpath(rel)
        sibling_path = task_dir.joinpath(*sibling_rel.parts)
        if not sibling_path.exists():
            continue
        existing_path = sibling_rel.as_posix()
        issues.append(TaskBundleCompatibilityIssue(
            code="TASK_COMPAT_APP_PATH_MISSING",
            severity=CompatibilitySeverity.ERROR,
            path=label,
            line=line_no,
            phase="task_image_build",
            message=(
                f"Dockerfile references {referenced}, but the bundle only has "
                f"{existing_path}; after `COPY . /app`, that file lands at "
                f"/app/{existing_path}"
            ),
            hint=(
                f"Either place {rel.as_posix()} at the build-context root, "
                f"update the Dockerfile to use /app/{existing_path}, or publish "
                "a bundle whose Docker build context matches the Dockerfile."
            ),
            evidence={
                "referenced_path": referenced,
                "existing_path": existing_path,
            },
        ))
    return issues


def _shell_segments(body: str) -> list[str]:
    return [
        segment.strip()
        for segment in re.split(r"\s*(?:&&|\|\||;)\s*", body)
        if segment.strip()
    ]


def _app_write_commands(segment: str) -> list[tuple[str, int]]:
    commands: list[tuple[str, int]] = []
    for match in _APP_WRITE_RE.finditer(segment):
        commands.append((" ".join(match.group("cmd").split()), match.start()))
    cp_start = _command_start(segment, "cp")
    if cp_start is not None and _cp_writes_app(segment):
        commands.append((" ".join(segment[cp_start:].split()), cp_start))
    return commands


def _command_start(segment: str, command: str) -> int | None:
    match = re.search(rf"(?:^|\s){re.escape(command)}(?:\s|$)", segment)
    if match is None:
        return None
    return match.start() if segment[match.start()] != " " else match.start() + 1


def _cp_writes_app(segment: str) -> bool:
    try:
        tokens = shlex.split(segment)
    except ValueError:
        return False
    if not tokens or tokens[0].rsplit("/", 1)[-1] != "cp":
        return False
    operands = [token for token in tokens[1:] if not token.startswith("-")]
    if len(operands) < 2:
        return False
    destination = operands[-1].rstrip("/")
    return destination == "/app" or destination.startswith("/app/")


def _pip_install_index(tokens: list[str]) -> int | None:
    for index, token in enumerate(tokens[:-1]):
        command = token.rsplit("/", 1)[-1]
        if _is_pip_command(command) and tokens[index + 1] == "install":
            return index + 1
    for index in range(len(tokens) - 3):
        if (
            _is_python_command(tokens[index].rsplit("/", 1)[-1])
            and tokens[index + 1] == "-m"
            and tokens[index + 2] == "pip"
            and tokens[index + 3] == "install"
        ):
            return index + 3
    return None


def _is_pip_command(command: str) -> bool:
    return bool(re.fullmatch(r"pip(?:\d+(?:\.\d+)?)?", command))


def _is_python_command(command: str) -> bool:
    return bool(re.fullmatch(r"python(?:\d+(?:\.\d+)?)?", command))


def _uses_pytorch_as_sole_index(args: list[str]) -> bool:
    has_pytorch_index = False
    has_extra_index = False
    for index, arg in enumerate(args):
        if arg in {"--extra-index-url", "--extra-index"}:
            has_extra_index = True
        elif arg.startswith("--extra-index-url="):
            has_extra_index = True
        elif arg in {"--index-url", "-i"} and index + 1 < len(args):
            has_pytorch_index = bool(_PYTORCH_INDEX_RE.search(args[index + 1]))
        elif arg.startswith("--index-url="):
            has_pytorch_index = bool(_PYTORCH_INDEX_RE.search(arg))
    return has_pytorch_index and not has_extra_index


def _install_package_names(args: list[str]) -> list[str]:
    names: list[str] = []
    skip_next = False
    options_with_values = {
        "--index-url",
        "--extra-index",
        "--extra-index-url",
        "-i",
        "-r",
        "--requirement",
        "-c",
        "--constraint",
        "-f",
        "--find-links",
    }
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg in options_with_values:
            skip_next = True
            continue
        if arg.startswith("-"):
            continue
        if "://" in arg or arg.startswith((".", "/")):
            continue
        if match := re.match(r"([A-Za-z0-9_.-]+)", arg):
            names.append(match.group(1).lower().replace("_", "-"))
    return names


def _mkdir_creates_app_root(segment: str, *, app_dir_created: bool) -> bool:
    try:
        tokens = shlex.split(segment)
    except ValueError:
        return False

    for index, token in enumerate(tokens):
        if token.rsplit("/", 1)[-1] != "mkdir":
            continue
        has_parents = False
        targets: list[str] = []
        remaining = tokens[index + 1:]
        while remaining:
            arg = remaining.pop(0)
            if arg == "--":
                targets.extend(remaining)
                break
            if arg == "--parents":
                has_parents = True
                continue
            if arg.startswith("--"):
                continue
            if arg.startswith("-") and arg != "-":
                if "p" in arg[1:]:
                    has_parents = True
                continue
            targets.append(arg)

        for target in targets:
            normalized = target.rstrip("/") or target
            if normalized == "/app":
                return True
            if normalized.startswith("/app/") and (has_parents or app_dir_created):
                return True
    return False


def _broad_trailing_true(body: str) -> bool:
    match = _TRAILING_TRUE_RE.search(body)
    if not match:
        return False
    before = body[:match.start()].strip()
    if _is_single_optional_git_remote_remove(before):
        return False
    return "&&" in before or ";" in before or _APP_WRITE_RE.search(before) is not None


def _is_single_optional_git_remote_remove(command: str) -> bool:
    command = command.strip()
    if command.startswith("(") and command.endswith(")"):
        command = command[1:-1].strip()
    return bool(re.fullmatch(r"git\s+remote\s+remove\s+origin(?:\s+2>/dev/null)?", command))
