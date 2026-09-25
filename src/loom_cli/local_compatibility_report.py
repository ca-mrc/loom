"""Read-only, whole-input intake diagnostics; no build or runtime qualification."""

from __future__ import annotations

import json
import re
import shlex
import shutil
import tempfile
import tomllib
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from pydantic import ValidationError

from loom.dockerfile_instructions import (
    DockerfileInstruction,
    DockerfileParseError,
    dockerfile_instructions,
)
from loom.execution_architecture import execution_cpu_arch
from loom.execution_requirements import (
    TaskExecutionRequirementsV1,
    execution_requirement_diagnostics,
)
from loom.models.task import EnvironmentConfig, TaskConfig
from loom.models.task_checksum import task_checksum
from loom.mutable_paths import validate_task_workdir
from loom.nebius_terminus_ingest import (
    NEBIUS_TERMINUS_PROFILE,
    UnsupportedComposeEnvironmentError,
    adapt_bundle_for_nebius_terminus,
    preflight_nebius_terminus_admission,
)
from loom.sandbox_identity import resolve_sandbox_identity
from loom.service_execution_materialization import prepare_service_execution_input_manifest
from loom.terminal_bench_normalize import (
    is_terminal_bench_shape,
    normalize_terminal_bench_task_toml,
)


@dataclass
class TaskCompatibilityReport:
    task_id: str
    source_location: str
    status: str = "blocked"
    admission_passed: bool | None = None
    original_requirements: dict[str, Any] = field(default_factory=dict)
    diagnostics: list[dict[str, Any]] = field(default_factory=list)
    changes: list[dict[str, Any]] = field(default_factory=list)

    def add(self, category: str, code: str, reason: str, action: str, *, source: str = "") -> None:
        self.diagnostics.append({
            "category": category,
            "code": code,
            "source_location": source or self.source_location,
            "reason": reason,
            "suggested_action": action,
        })


def render_compatibility_payload(reports: tuple[TaskCompatibilityReport, ...]) -> dict[str, Any]:
    return {
        "schema_version": "loom.local-compatibility-report.v1",
        "runtime_verified": False,
        "blocked_tasks": sum(report.status == "blocked" for report in reports),
        "tasks": [asdict(report) for report in reports],
        "limitations": [
            "Static local checks only; no image build, registry lookup, model calls or runtime execution.",
            "Undeclared requirements in instructions and scripts require task-author review.",
            "Inherited registry-image startup, user and working-directory settings are not inspected by source-only checks.",
            "Passing admission does not establish equivalent task semantics or trajectory delivery.",
        ],
    }


def collect_compatibility_reports(
    *, benchmark_id: str, task_root: Path, task_tomls: tuple[Path, ...],
    execution_profile: str | None,
) -> tuple[TaskCompatibilityReport, ...]:
    reports = []
    for path in task_tomls:
        relative = path.parent.relative_to(task_root)
        task_id = benchmark_id if relative == Path(".") else f"{benchmark_id}/{relative.as_posix()}"
        report = TaskCompatibilityReport(task_id=task_id, source_location=str(path))
        reports.append(report)
        _inspect_task(path, report, execution_profile=execution_profile)
    return tuple(reports)


def _inspect_task(path: Path, report: TaskCompatibilityReport, *, execution_profile: str | None) -> None:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        report.add("package_defect", "invalid_toml", str(exc), "Repair task.toml and rerun validation.")
        return
    report.original_requirements = {
        key: deepcopy(raw[key]) for key in (
            "environment", "agent", "verifier", "steps", "required_agent_capabilities",
            "artifacts", "multi_step", "service_execution",
        )
        if key in raw
    }
    _declared_execution_requirement_diagnostics(raw, report, execution_profile=execution_profile)
    if execution_profile == NEBIUS_TERMINUS_PROFILE:
        _declared_runtime_requirements(raw, report)
    try:
        normalized = normalize_terminal_bench_task_toml(raw, task_id=report.task_id)
        task = TaskConfig.model_validate(normalized)
        execution_cpu_arch(task.environment.cpu_arch)
    except (ValueError, TypeError) as exc:
        reason = str(exc)
        if isinstance(exc, ValidationError):
            reason = "; ".join(
                f"{'.'.join(str(item) for item in error['loc'])}: {error['msg']}"
                for error in exc.errors(include_url=False, include_input=False)
            )
        report.add("package_defect", "invalid_task_config", reason,
                   "Supply the missing Loom intake fields or repair the declared schema; preserve task requirements.")
        return
    _build_context_diagnostics(path.parent, task, report)
    if execution_profile != NEBIUS_TERMINUS_PROFILE:
        if not report.diagnostics:
            report.status = "schema_valid"
        return
    prepared_workdir = (str(task.environment.workdir)
                        if _section(normalized, "environment").get("workdir") is not None else "/app")
    _dockerfile_runtime_requirements(path.parent, task, report,
                                    prepared_workdir=prepared_workdir, harbor_input=is_terminal_bench_shape(raw))
    _dropped_environment_requirements(raw, normalized, report)
    _dropped_runtime_requirements(raw, normalized, report)
    try:
        with tempfile.TemporaryDirectory(prefix="loom-compatibility-") as stage_root:
            staged = Path(stage_root) / "bundle"
            shutil.copytree(path.parent, staged, symlinks=False)
            adapted, _ = adapt_bundle_for_nebius_terminus(staged, normalized)
            _record_changes(normalized, adapted, raw, report)
            _, provenance = prepare_service_execution_input_manifest(
                staged, task_checksum=task_checksum(staged), bucket="validate-local",
                manifest_key=f"{report.task_id}/service-execution-input.json",
            )
            reasons = preflight_nebius_terminus_admission(adapted, provenance)
    except UnsupportedComposeEnvironmentError as exc:
        for relative in exc.relative_paths:
            report.add(
                "unsupported_conversion", "compose_environment_unsupported",
                "Packaged Docker Compose configuration is not converted by this execution profile.",
                "Preserve the supplied service fixtures and main-container overrides. "
                "Qualify their isolated images, mounts, network aliases, health checks, dependencies "
                "and verifier lifecycle through #2050; do not infer a missing external service.",
                source=str(path.parent / relative),
            )
        return
    except (OSError, UnicodeError, ValueError, TypeError, shutil.Error) as exc:
        report.add("unsupported_conversion", "profile_adaptation_failed", str(exc),
                   "Provide a reviewed equivalent bootstrap/image adaptation; do not skip unknown dependencies.",
                   source=str(path.parent))
        return
    report.admission_passed = not reasons
    diagnosed_codes = {item["code"] for item in report.diagnostics}
    for reason in reasons:
        if reason in diagnosed_codes:
            continue
        report.add("runtime_capability", reason, f"Profile admission rejected: {reason}.",
                   "Use a qualified runtime capability or retain this task as blocked.")
    if not report.diagnostics:
        report.status = "converted" if report.changes else "supported"


def _section(raw: dict[str, Any], name: str) -> dict[str, Any]:
    value = raw.get(name)
    return value if isinstance(value, dict) else {}


def _declared_execution_requirement_diagnostics(
    raw: dict[str, Any], report: TaskCompatibilityReport, *, execution_profile: str | None,
) -> None:
    declaration = _section(raw, "environment").get("execution_requirements")
    if declaration is None:
        return
    try:
        requirements = TaskExecutionRequirementsV1.model_validate(declaration)
    except ValidationError:
        # Invalid references or extra fields may contain credentials. Preserve
        # the source and validation paths, but do not copy their values into JSON.
        report.original_requirements["environment"]["execution_requirements"] = {
            "redacted": True, "reason": "Invalid declaration; inspect the source task.toml.",
        }
        return
    if execution_profile != NEBIUS_TERMINUS_PROFILE:
        return
    for item in execution_requirement_diagnostics(requirements):
        report.add(item.category, item.code, item.reason, item.action,
                   source=f"{report.source_location}#environment.execution_requirements.{item.field}")


def _declared_runtime_requirements(raw: dict[str, Any], report: TaskCompatibilityReport) -> None:
    env = _section(raw, "environment")
    agent = _section(raw, "agent")
    verifier = _section(raw, "verifier")

    def add(code: str, key: str, reason: str, issue: int) -> None:
        report.add("runtime_capability", code, reason,
                   f"Preserve the requirement and qualify support through #{issue} before submission.",
                   source=f"{report.source_location}#{key}")

    def readiness(code: str, key: str, reason: str, flag: str) -> None:
        report.add("runtime_capability", code, reason,
                   f"Qualify the selected deployment with {flag}=true before submission; "
                   "this static report cannot verify runtime readiness.",
                   source=f"{report.source_location}#{key}")

    if "user" in env and env["user"] != "agent":
        readiness("task_identity", "environment.user",
                  f"Task user {env['user']!r} is preserved; execution requires a qualified task identity runtime.",
                  "supports_task_identity")
    elif "HOME" in _section(env, "environment"):
        readiness("task_identity", "environment.environment.HOME",
                  "Declared HOME is preserved and requires a qualified task identity runtime.",
                  "supports_task_identity")
    if agent.get("user") is not None:
        add("agent_identity", "agent.user", "Custom agent identity is not admitted by this profile.", 2049)
    if verifier.get("user") is not None:
        readiness("verifier_identity", "verifier.user",
                  "The verifier identity declaration is preserved; execution requires a qualified task identity runtime.",
                  "supports_task_identity")
    if "workdir" in env:
        try:
            validate_task_workdir(env["workdir"])
        except ValueError as exc:
            add("workspace_path", "environment.workdir", str(exc), 2047)
    if "mutable_paths" in env and "mutable_paths" not in EnvironmentConfig.model_fields:
        add("mutable_paths", "environment.mutable_paths", "Declared mutable paths have no supported transfer contract.", 2047)
    if env.get("services") or env.get("sidecars"):
        add("services", "environment.services" if env.get("services") else "environment.sidecars",
            "Declared services need runtime initialization and verifier lifecycle support.", 2050)
    if env.get("service_lifecycle") is not None:
        readiness("service_lifecycle", "environment.service_lifecycle",
                  "The service lifecycle declaration is preserved and requires qualified startup, snapshot and verifier cleanup support.",
                  "service_lifecycle_ready")
    policy = env.get("baseline_network_policy")
    preserved_web = isinstance(policy, dict) and policy.get("kind") == "web-allowlist" and env.get("allow_internet") is not False
    declared_public_web = (
        (isinstance(policy, dict) and policy.get("kind") == "public-web")
        or env.get("allow_internet") is True
        or env.get("network_mode") == "public"
    ) and env.get("allow_internet") is not False and not (isinstance(policy, dict) and policy.get("kind") == "no-network")
    if env.get("allow_internet") is False:
        add("network_policy_change", "environment.allow_internet", "No-network is declared; profile enables Gateway networking.", 2048)
    if preserved_web:
        readiness("runtime_egress", "environment.baseline_network_policy",
                  "The exact HTTP/HTTPS destination allowlist is preserved; runtime enforcement needs deployment qualification.",
                  "supports_task_web_egress")
    elif declared_public_web:
        readiness("runtime_egress", "environment.allow_internet",
                  "Public HTTP and HTTPS stay on the gateway dialer; runtime enforcement needs deployment qualification.",
                  "supports_task_web_egress")
    elif isinstance(policy, dict) and policy.get("kind") != "gateway-only":
        add("runtime_egress", "environment.baseline_network_policy",
            "This network policy has no supported preparation mapping; adaptation would replace it with Gateway networking.", 2048)
    policies = env.get("network_policies_supported")
    if isinstance(policies, list) and policies != ["gateway-only"] and not preserved_web and not declared_public_web:
        add("network_policy_change", "environment.network_policies_supported", "Profile replaces the declared supported policies.", 2048)
    if env.get("cpu_arch", env.get("architecture", "x86_64")) not in ("x86_64", "amd64", "any"):
        add("architecture", "environment.cpu_arch", "Declared architecture differs from this x86_64 execution profile.", 2051)
    if env.get("gpus") or env.get("gpu_vendor", "none") != "none":
        add("devices", "environment.gpus", "Declared GPU capability is not supported by this profile.", 2051)
    if verifier.get("env_mode", verifier.get("environment_mode", "shared")) != "shared":
        add("verifier_environment", "verifier.env_mode", "Profile replaces the declared verifier environment mode.", 2050)
    verifier_args = _section(verifier, "args")
    script = verifier_args.get("script_path")
    if script is not None and script not in ("verifier/run.sh", "/app/verifier/run.sh"):
        report.add("unsupported_conversion", "verifier_entrypoint_changed",
                   f"Profile would replace the declared verifier entrypoint {script!r}.",
                   "Provide a reviewed equivalent verifier bridge that preserves the original entrypoint.",
                   source=f"{report.source_location}#verifier.args.script_path")


def _bare_shell_cmd(value: str) -> bool:
    try:
        command = json.loads(value)
    except json.JSONDecodeError:
        return False
    return (isinstance(command, list) and len(command) == 1 and isinstance(command[0], str)
            and command[0] in {"sh", "/bin/sh", "/usr/bin/sh", "bash", "/bin/bash", "/usr/bin/bash",
                               "zsh", "/bin/zsh", "/usr/bin/zsh"})


def _dockerfile_workdir(
    instructions: tuple[DockerfileInstruction, ...],
) -> tuple[str | None, DockerfileInstruction | None]:
    """Resolve literal source cwd changes; registry defaults and variables stay unknown."""
    stages: dict[str, tuple[str | None, DockerfileInstruction | None]] = {}
    directory: str | None = None
    effective: DockerfileInstruction | None = None
    alias: str | None = None
    for instruction in instructions:
        if instruction.keyword == "FROM":
            words = instruction.arguments.split()
            while words and words[0].startswith("--"):
                words.pop(0)
            if not words:
                return None, None
            directory, effective = stages.get(words[0].lower(), (None, None))
            alias = words[2].lower() if len(words) == 3 and words[1].upper() == "AS" else None
        elif instruction.keyword == "WORKDIR":
            effective = instruction
            value = instruction.arguments.strip()
            if (not re.fullmatch(r"/?(?:[-A-Za-z0-9._]+/)*[-A-Za-z0-9._]+", value)
                    or any(part in {".", ".."} for part in value.split("/"))):
                directory = None
            elif value.startswith("/"):
                directory = value
            else:
                directory = str(PurePosixPath(directory) / value) if directory else None
        if alias is not None:
            stages[alias] = (directory, effective)
    return directory, effective


def _dockerfile_runtime_requirements(
    bundle: Path, task: TaskConfig, report: TaskCompatibilityReport, *, prepared_workdir: str,
    harbor_input: bool = False,
) -> None:
    """Report effective source metadata that preparation/runtime would override.

    Follow local stage inheritance, but never infer registry image metadata or
    execute unreviewed startup scripts. Build-context checks own parse failures.
    """
    if task.environment.dockerfile is None:
        return
    # Preparation accepts its own derived path by returning to this original.
    dockerfile = bundle / str(task.environment.dockerfile).removesuffix(".loom-nebius")
    if not dockerfile.resolve().is_relative_to(bundle.resolve()):
        return
    try:
        instructions = dockerfile_instructions(dockerfile.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        return
    _image_script_private_test_diagnostics(bundle, dockerfile, instructions, prepared_workdir, report)
    directory, workdir_instruction = _dockerfile_workdir(instructions)
    if workdir_instruction is not None and directory != prepared_workdir:
        report.add("unsupported_conversion", "dockerfile_workdir_overridden",
                   f"Final authored WORKDIR {workdir_instruction.arguments!r} "
                   f"resolves to {directory!r}; preparation selects {prepared_workdir!r}.",
                   "Declare the supported original environment.workdir explicitly. Resolve variables or "
                   "inherited registry working directories through image inspection; do not silently relocate task state.",
                   source=f"{dockerfile}:{workdir_instruction.line}")
    stages: dict[str, dict[str, DockerfileInstruction]] = {}
    current: dict[str, DockerfileInstruction] = {}
    local_cmd = False
    for instruction in instructions:
        if instruction.keyword == "FROM":
            words = instruction.arguments.split()
            while words and words[0].startswith("--"):
                words.pop(0)
            if not words:
                return  # Preparation reports malformed FROM instructions.
            current = dict(stages.get(words[0].lower(), {}))
            local_cmd = False
            if len(words) == 3 and words[1].upper() == "AS":
                stages[words[2].lower()] = current
        elif instruction.keyword in {"ENTRYPOINT", "CMD", "USER"}:
            if instruction.keyword == "ENTRYPOINT" and not local_cmd:
                # An authored ENTRYPOINT clears CMD inherited from its base.
                current.pop("CMD", None)
            if instruction.keyword == "CMD":
                local_cmd = True
            current[instruction.keyword] = instruction

    lifecycle = task.environment.service_lifecycle
    for key in ("ENTRYPOINT", "CMD", "USER"):
        effective = current.get(key)
        if effective is None:
            continue
        value = effective.arguments.strip()
        if key != "USER":
            if not value or re.fullmatch(r"\[\s*\]", value):
                continue
            if key == "CMD" and harbor_input and _bare_shell_cmd(value):
                # Pinned Harbor's Docker Compose command replaces image CMD;
                # its Terminus tmux session independently starts Bash. An image
                # ENTRYPOINT remains a separate requirement, checked above.
                report.changes.append({
                    "field": "environment.dockerfile.CMD", "before": value, "after": None,
                    "category": "equivalent_conversion", "source_location": f"{dockerfile}:{effective.line}",
                    "reason": "Harbor replaces the image CMD and Terminus starts Bash independently; "
                              "this bare-shell default requires no startup initializer.",
                })
                continue
            if lifecycle is not None and lifecycle.startup_command:
                continue  # The declaration records the author's reviewed initializer.
            report.add("unsupported_conversion", "dockerfile_startup_overridden",
                       f"Final image {key} {value} is bypassed by the native sandbox runtime; no returning initializer is declared.",
                       "Review the original startup semantics and declare an equivalent returning "
                       "environment.service_lifecycle.startup_command and readiness check when initialization is needed; "
                       "do not execute or drop unreviewed startup behavior.",
                       source=f"{dockerfile}:{effective.line}")
        elif not _prepared_user_matches(value, task):
            report.add("unsupported_conversion", "dockerfile_user_overridden",
                       f"Final image USER {value} is not established by the task identity declaration; preparation selects environment.user instead.",
                       "Review the original image identity and declare its supported environment.user and HOME explicitly; "
                       "named users or UID-only identities may require image metadata inspection. Do not infer a replacement identity.",
                       source=f"{dockerfile}:{effective.line}")


class _ShellOperator(str):
    """An unquoted shell operator, distinct from a quoted word with that value."""


def _shell_commands(source: str) -> list[list[str]]:
    """Keep lexical boundaries before shlex removes quote/comment information.

    Only split literal commands and words; do not evaluate shell expressions.
    Shlex unquotes each word after its role as data or syntax is established.
    """
    commands: list[list[str]] = [[]]
    pending: list[str] = []
    quote = ""
    position = 0

    def word() -> None:
        if pending:
            values = shlex.split("".join(pending), comments=False, posix=True)
            if len(values) != 1:
                raise ValueError("nonliteral shell word")
            commands[-1].append(values[0])
            pending.clear()

    try:
        while position < len(source):
            char = source[position]
            position += 1
            if char == "\\" and quote != "'" and position < len(source):
                following = source[position]
                position += 1
                if following != "\n":
                    pending.extend((char, following))
            elif quote:
                pending.append(char)
                if char == quote:
                    quote = ""
            elif char in "\"'":
                quote = char
                pending.append(char)
            elif char == "#" and not pending:
                while position < len(source) and source[position] != "\n":
                    position += 1
            elif char in "\n;&|":
                word()
                commands.append([])
            elif char in "<>":
                word()
                operator = char
                while position < len(source) and source[position] in "<>":
                    operator += source[position]
                    position += 1
                commands[-1].append(_ShellOperator(operator))
            elif char.isspace():
                word()
            else:
                pending.append(char)
        word()
    except ValueError:
        return []  # Unknown shell forms remain subject to task-author review.
    return [command for command in commands if command]


def _script_private_tests(source: str, bundle: Path) -> set[str]:
    references: set[str] = set()
    for command in _shell_commands(source):
        while command and command[0] in {"if", "then", "else", "!"}:
            command = command[1:]
        if not command:
            continue
        executable = PurePosixPath(command[0]).name
        arguments: list[str] | None = None
        if executable == "pytest":
            arguments = command[1:]
        elif executable in {"python", "python3"} and command[1:3] == ["-m", "pytest"]:
            arguments = command[3:]
        elif executable == "uvx":
            position = 1
            while position < len(command):
                option = command[position]
                if option in {"-p", "--python", "-w", "--with", "--from"}:
                    position += 2
                elif option.startswith(("--python=", "--with=", "--from=")):
                    position += 1
                else:
                    break
            if command[position:position + 1] == ["pytest"]:
                arguments = command[position + 1:]
        if arguments is None:
            continue
        # Only positional pytest selection paths establish this dependency.
        # Option values such as --ignore and -k are not selected test files.
        skip_value = False
        for word in arguments:
            if skip_value:
                skip_value = False
                continue
            if isinstance(word, _ShellOperator):
                skip_value = True
                continue
            if word in {"--ignore", "--ignore-glob", "--deselect", "--confcutdir", "--basetemp",
                        "--rootdir", "--junitxml", "--junit-xml", "--junit-prefix", "--log-file",
                        "-k", "-m", "-c", "-o", "--override-ini"}:
                skip_value = True
                continue
            path = PurePosixPath(word.split("::", 1)[0])
            if not path.is_relative_to("/tests") or ".." in path.parts or "$" in word:
                continue
            local = bundle / "tests" / path.relative_to("/tests")
            if local.resolve().is_relative_to((bundle / "tests").resolve()) and local.is_file():
                references.add(str(path))
    return references


def _image_script_private_test_diagnostics(
    bundle: Path, dockerfile: Path, instructions: tuple[DockerfileInstruction, ...],
    workdir: str, report: TaskCompatibilityReport,
) -> None:
    """Flag literal echo/printf-generated workspace scripts using bundled private tests.

    This is a source diagnostic, not a shell interpreter or image filesystem
    proof. Follow local FROM inheritance; ignore discarded build stages.
    """
    stages: dict[str, list[DockerfileInstruction]] = {}
    current: list[DockerfileInstruction] = []
    for instruction in instructions:
        if instruction.keyword == "FROM":
            words = instruction.arguments.split()
            words = [word for word in words if not word.startswith("--")]
            if not words:
                return
            current = list(stages.get(words[0].lower(), []))
            if len(words) == 3 and words[1].upper() == "AS":
                stages[words[2].lower()] = current
        elif instruction.keyword == "RUN":
            current.append(instruction)
    for instruction in current:
        for command in _shell_commands(instruction.arguments):
            if command[0] not in {"echo", "printf"}:
                continue
            redirects = [i for i, word in enumerate(command) if isinstance(word, _ShellOperator) and word in {">", ">>"}]
            if len(redirects) != 1 or redirects[0] != len(command) - 2:
                continue
            target = PurePosixPath(command[-1])
            if not target.is_absolute() or not target.is_relative_to(workdir) or target.suffix != ".sh" or ".." in target.parts:
                continue
            content = command[1:redirects[0]]
            if command[0] == "printf":
                if not content or content[0] not in {"%s\\n", "%s"}:
                    continue
                separator = "\n" if content[0] == "%s\\n" else ""
                content = content[1:]
            else:
                separator = " "
                if content[:1] == ["-n"]:
                    content = content[1:]
                if content and content[0].startswith("-"):
                    continue  # Escape-interpreting echo variants need shell evaluation.
            for reference in sorted(_script_private_tests(separator.join(content), bundle)):
                report.add(
                    "package_defect", "agent_private_verifier_dependency",
                    f"Image source writes a pytest invocation of bundled private test {reference!r} "
                    f"into workspace script {str(target)!r}.",
                    "Review the public script/private verifier contract and publish an explicit package repair "
                    "that preserves task and scoring semantics. Do not expose private tests to make the script run; "
                    "keep the task blocked until the conflict is resolved (see #1263).",
                    source=f"{dockerfile}:{instruction.line}",
                )


def _prepared_user_matches(source_user: str, task: TaskConfig) -> bool:
    try:
        identity = resolve_sandbox_identity(task.environment.user, task.environment.environment.get("HOME"))
    except ValueError:
        return False
    uid, gid = (identity.run_as_user, identity.run_as_group) if identity else (65532, 65532)
    if source_user in {"root", "0", "0:0"}:
        return (uid, gid) == (0, 0)
    if re.fullmatch(r"[0-9]+:[0-9]+", source_user):
        return tuple(map(int, source_user.split(":"))) == (uid, gid)
    return False  # Named users and implicit primary groups require image inspection.


def _build_context_diagnostics(bundle: Path, task: TaskConfig, report: TaskCompatibilityReport) -> None:
    """Check literal local COPY inputs, without pretending to build an image."""
    env = task.environment
    if env.dockerfile is None:
        return
    dockerfile = bundle / env.dockerfile
    context = bundle / (env.docker_build_context or ".")
    if not dockerfile.resolve().is_relative_to(bundle) or not context.resolve().is_relative_to(bundle):
        report.add("package_defect", "build_path_outside_bundle", "Docker build inputs leave the task bundle.",
                   "Keep the original Dockerfile and build context inside the task package.")
        return
    if not dockerfile.is_file() or not context.is_dir():
        report.add("package_defect", "missing_build_input", "Declared Dockerfile or build context is missing.",
                   "Supply the original Dockerfile and complete build context.", source=str(dockerfile))
        return
    try:
        instructions = dockerfile_instructions(dockerfile.read_text(encoding="utf-8"))
        for instruction in instructions:
            if instruction.keyword not in {"COPY", "ADD"}:
                continue
            arguments = instruction.arguments
            flags = []
            while match := re.match(r"^(--[^\s]+)\s+", arguments):
                flags.append(match.group(1))
                arguments = arguments[match.end():]
            if any(flag.startswith("--from=") for flag in flags):
                continue
            values = json.loads(arguments) if arguments.startswith("[") else shlex.split(arguments)
            if not isinstance(values, list) or len(values) < 2 or not all(isinstance(value, str) for value in values):
                raise ValueError("COPY/ADD requires source paths and a destination")
            for source in values[:-1]:
                # Inline inputs, build-arg expansion and remote sources need
                # Docker evaluation; absence on this filesystem proves nothing.
                if source.startswith("<<") or "$" in source or "://" in source or source.startswith("git@"):
                    continue
                relative = source.lstrip("/")
                if ".." in Path(relative).parts:
                    continue
                exists = (context / relative).exists() if not any(char in relative for char in "*?[") else any(context.glob(relative))
                if not exists:
                    report.add("package_defect", "missing_copy_source",
                               f"{instruction.keyword} source {source!r} is absent from build context {env.docker_build_context or '.'}.",
                               "Restore the original source or publish an explicit reviewed package repair; do not invent an empty directory.",
                               source=f"{dockerfile}:{instruction.line}")
    except (OSError, UnicodeError, ValueError) as exc:
        location = f"{dockerfile}:{exc.line}" if isinstance(exc, DockerfileParseError) else str(dockerfile)
        report.add("package_defect", "invalid_dockerfile", str(exc),
                   "Repair the Dockerfile input syntax and rerun validation.", source=location)


def _dropped_environment_requirements(
    raw: dict[str, Any], normalized: dict[str, Any], report: TaskCompatibilityReport,
) -> None:
    """Do not silently label a lossy Harbor projection as compatible."""
    environment = _section(normalized, "environment")
    recognized = {"architecture", "allow_internet", "env", "services", "memory", "storage"}
    if "mutable_paths" not in EnvironmentConfig.model_fields:
        recognized.add("mutable_paths")  # already reported as a runtime gap
    for key in _section(raw, "environment"):
        if key in environment or key in recognized:
            continue
        report.add("unsupported_conversion", "unmapped_environment_requirement",
                   f"Harbor normalization discards environment.{key}.",
                   "Map this declaration explicitly or provide a reviewed equivalent task package.",
                   source=f"{report.source_location}#environment.{key}")


def _record_changes(
    normalized: dict[str, Any], adapted: dict[str, Any], raw: dict[str, Any],
    report: TaskCompatibilityReport,
) -> None:
    for section in ("environment", "verifier"):
        before = _section(normalized, section)
        after = _section(adapted, section)
        for key in sorted(before.keys() | after.keys()):
            if before.get(key) == after.get(key):
                continue
            report.changes.append({
                "field": f"{section}.{key}", "before": before.get(key), "after": after.get(key),
                "category": (
                    "profile_default" if key not in before else
                    "equivalent_conversion" if (section, key) in {
                        ("environment", "dockerfile"), ("environment", "docker_build_context"),
                    } else "profile_conversion"
                ),
            })
    if normalized.get("steps") != adapted.get("steps"):
        report.changes.append({
            "field": "steps", "before": normalized.get("steps"), "after": adapted.get("steps"),
            "category": "profile_conversion",
        })
        if "steps" in raw or raw.get("artifacts"):
            report.add("unsupported_conversion", "artifact_requirements_changed",
                       "Profile removes declared artifact paths or patterns.",
                       "Preserve artifact requirements through an explicit supported collection contract.")


def _dropped_runtime_requirements(
    raw: dict[str, Any], normalized: dict[str, Any], report: TaskCompatibilityReport,
) -> None:
    for section in ("agent", "verifier"):
        mapped = _section(normalized, section)
        for key in _section(raw, section):
            if key in mapped or (section == "verifier" and key == "environment_mode"):
                continue
            report.add("unsupported_conversion", f"unmapped_{section}_requirement",
                       f"Harbor normalization discards {section}.{key}.",
                       "Map this declaration explicitly while preserving the original requirement.",
                       source=f"{report.source_location}#{section}.{key}")
    if raw.get("required_agent_capabilities") and raw.get("required_agent_capabilities") != normalized.get("required_agent_capabilities"):
        report.add("runtime_capability", "agent_capabilities_unsupported",
                   "Harbor normalization discards the declared required agent capabilities.",
                   "Preserve and qualify the required execution capabilities through #2051.",
                   source=f"{report.source_location}#required_agent_capabilities")
    artifacts = raw.get("artifacts")
    if isinstance(artifacts, list):
        mapped_artifacts = [
            artifact for step in normalized.get("steps", [])
            for artifact in step.get("artifacts", [])
        ]
        if any(artifact not in mapped_artifacts for artifact in artifacts):
            report.add("unsupported_conversion", "unmapped_artifact_requirement",
                       "Harbor normalization discards declared artifact paths.",
                       "Provide a supported artifact collection contract without dropping required outputs.",
                       source=f"{report.source_location}#artifacts")
    for key in ("steps", "multi_step", "service_execution"):
        if key in raw and raw[key] != normalized.get(key):
            report.add("unsupported_conversion", "unmapped_runtime_requirement",
                       f"Harbor normalization changes the declared {key} contract.",
                       "Preserve the original execution contract in a reviewed task conversion.",
                       source=f"{report.source_location}#{key}")
