"""Build-time preparation of the supported Harbor shell bootstrap on Nebius.

Only installer plumbing is relocated. Task setup, assertions, reward handling,
base-image programs, and original source files remain unchanged.
"""

from __future__ import annotations

import json
import re
import shlex
from collections.abc import Collection
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from loom.dockerfile_instructions import dockerfile_instructions
from loom.mutable_paths import validate_task_workdir
from loom.sandbox_identity import SandboxIdentityV1, resolve_sandbox_identity
from loom.task_image_compatibility import classify_terminus_image
from loom.task_image_errors import (
    PACKAGE_GUARD_MESSAGE,
    PACKAGE_GUARD_PREFIX,
    TaskImagePreparationError,
)

OFFLINE_SCRIPT = "verifier/harbor-offline.sh"
# Shared with Harbor image prep. Task-image builds install this set and then
# refuse an image whose sandbox user cannot see bash, tmux, and asciinema.
_TERMINUS_APT_PACKAGES = frozenset({
    "asciinema",
    "bash",
    "ca-certificates",
    "curl",
    "passwd",
    "python3",
    "tmux",
})
_TERMINUS_APK_PACKAGES = frozenset({
    "asciinema",
    "bash",
    "ca-certificates",
    "curl",
    "python3",
    "shadow",
    "tar",
    "tmux",
})
_TERMINUS_TOOL_PROBE = (
    "if ! command -v bash >/dev/null || ! command -v tmux >/dev/null || "
    "! command -v asciinema >/dev/null; then "
    "echo 'task image must preinstall bash, tmux and asciinema' >&2; exit 1; fi"
)
# Leap's tmux package normally relies on systemd-tmpfiles at boot. Task
# containers do not run that service. Preserve authored paths and socket env.
_SUSE_TMUX_RUNTIME_SETUP = (
    "if [ ! -e /run/tmux ] && [ ! -L /run/tmux ]; then "
    "install -d -m 1777 -o 0 -g 0 /run/tmux; fi"
)
# Run after restoring the task USER. A version/binary probe misses an unusable
# default socket directory. -L isolates our server without overriding its root;
# -f avoids executing an authored tmux config during image preparation.
_SUSE_TMUX_STARTUP_PROBE = r'''loom_tmux_probe="loom-preparation-$$" && \
    trap 'timeout 5 tmux -L "$loom_tmux_probe" kill-server >/dev/null 2>&1 || true' EXIT && \
    timeout 10 tmux -f /dev/null -L "$loom_tmux_probe" new-session -d -s loom-preparation 'exec sleep 30' && \
    timeout 5 tmux -L "$loom_tmux_probe" has-session -t =loom-preparation && \
    timeout 5 tmux -L "$loom_tmux_probe" kill-server && trap - EXIT'''
_DOCKERFILE_SUFFIX = ".loom-nebius"
_PACKAGE = re.compile(r"[a-z0-9][a-z0-9+.-]*(?:=[A-Za-z0-9.+:~_-]+)?\Z")
_REQUIREMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*(?:==[A-Za-z0-9][A-Za-z0-9_.+!-]*)?\Z")
_DISTRIBUTION_SUFFIXES = (".whl", ".zip", ".tar", ".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst", ".tgz", ".tbz", ".txz")
_UV_INSTALL = re.compile(
    r"curl -LsSf https://astral\.sh/uv/(?:\d+\.\d+\.\d+/)?install\.sh\s*\|\s*sh\s*\Z",
)
_UV_SOURCE = re.compile(
    r'(?:source\s+(?:"\$HOME/\.local/bin/env"|\$HOME/\.local/bin/env)'
    r'|export PATH="\$HOME/\.local/bin:\$PATH")\s*\Z'
)
_BOOTSTRAP_GUARD = re.compile(
    r"if ! command -v (curl|uv) (?:>/dev/null 2>&1|&> /dev/null); then\Z"
)


def _apt_bootstrap_packages(command: str) -> tuple[str, ...] | None:
    apt = re.fullmatch(
        r"(?:apt-get update(?: -qq)?\s*&&\s*)?"
        r"(?:DEBIAN_FRONTEND=noninteractive\s+)?apt-get install (.+)",
        command,
    )
    if apt is None:
        return None
    # Only conventional apt metadata cleanup is relocated with the installer.
    package_args = re.sub(r"\s*&&\s*rm -rf /var/lib/apt/lists/\*\s*$", "", apt[1])
    words = shlex.split(package_args)
    names = tuple(
        word for word in words if word not in {"-y", "-qq", "--no-install-recommends"}
    )
    if "-y" not in words or not names or any(not _PACKAGE.fullmatch(name) for name in names):
        raise ValueError("nebius-terminus: unsupported apt bootstrap")
    return names


def _apk_bootstrap_packages(command: str) -> tuple[str, ...] | None:
    if not re.match(r"apk\b", command):
        return None
    words = shlex.split(command)
    names = tuple(words[3:])
    if (words[:3] != ["apk", "add", "--no-cache"] or not names
            or any(not _PACKAGE.fullmatch(name) or ".apk" in name for name in names)):
        raise ValueError("nebius-terminus: unsupported apk bootstrap")
    return names


def _unwrap_installer_guards(lines: list[str]) -> list[str]:
    """Unwrap only complete installer-only missing-curl/uv guards.

    All body commands must match before removing either control-flow boundary.
    Nested branches, task setup, and conditional installation of other packages
    cannot be made unconditional by image preparation.
    """
    if any("<<" in line for line in lines if not line.lstrip().startswith("#")):
        # This recognizer cannot distinguish shell commands from heredoc data.
        # Fail closed before inspecting any apparent installer in that data.
        raise ValueError("nebius-terminus: verifier heredoc syntax requires explicit adaptation")
    output: list[str] = []
    position = 0
    while position < len(lines):
        line = lines[position]
        position += 1
        guard = _BOOTSTRAP_GUARD.fullmatch(line.strip())
        if guard is None:
            output.append(line)
            continue
        body: list[str] = []
        while position < len(lines) and lines[position].strip() != "fi":
            body.append(lines[position])
            position += 1
        if position == len(lines):
            raise ValueError("nebius-terminus: unterminated bootstrap guard")
        position += 1
        commands = [
            re.sub(r"\\\r?\n", " ", item).strip()
            for item in body if item.strip() and not item.lstrip().startswith("#")
        ]
        if commands and _apt_bootstrap_packages(commands[0]) == ("curl",):
            commands.pop(0)
            has_curl = True
        else:
            has_curl = False
        valid = (
            has_curl and not commands if guard[1] == "curl" else
            len(commands) == 2
            and _UV_INSTALL.fullmatch(commands[0]) is not None
            and _UV_SOURCE.fullmatch(commands[1]) is not None
        )
        if not valid:
            raise ValueError("nebius-terminus: unsupported bootstrap guard body")
        # Relocating all commands must not leave an enclosing branch or
        # function with an empty body. A skipped installation also returns 0.
        output.append(":\n")
        output.extend(body)
    return output


@dataclass(frozen=True)
class HarborOfflineBootstrap:
    script: str
    apt_packages: tuple[str, ...]
    python_version: str | None
    requirements: tuple[str, ...]
    index_args: tuple[str, ...] = ()
    downloads: tuple[tuple[str, str], ...] = ()
    system_site_packages: bool = False
    apk_packages: tuple[str, ...] = ()
    uses_apt: bool = False


def adapt_harbor_test_script(script: str) -> HarborOfflineBootstrap:
    """Relocate explicit Harbor installer forms, retaining test/setup semantics.

    This is a bounded command recognizer, not a shell evaluator. Unknown shell
    syntax, dependency sources and installers require an explicit adaptation.
    """
    logical_lines: list[str] = []
    pending = ""
    for physical_line in script.splitlines(keepends=True):
        if not pending and physical_line.lstrip().startswith("#"):
            # Backslash-newline does not continue a shell comment. Combining
            # it with the next line could hide executable work in a guard.
            logical_lines.append(physical_line)
            continue
        pending += physical_line
        if not physical_line.rstrip("\r\n").endswith("\\"):
            logical_lines.append(pending)
            pending = ""
    if pending:
        raise ValueError("nebius-terminus: incomplete shell continuation")
    output: list[str] = []
    packages: list[str] = []
    alpine_packages: list[str] = []
    uses_apt = False
    requirements: list[str] = []
    indexes: list[str] = []
    downloads: list[tuple[str, str]] = []
    constants: dict[str, str] = {}
    python_version: str | None = None
    installer_count = source_count = uvx_count = pip_count = pytest_count = 0
    venv: str | None = None
    venv_activated = False

    def requirement(value: str) -> str:
        # Installers interpret bare archive filenames as local sources, even
        # though their spelling also fits a Python distribution name.
        if _REQUIREMENT.fullmatch(value) and (
            "==" in value or not value.lower().endswith(_DISTRIBUTION_SUFFIXES)
        ):
            return value
        # Expand only a literal commit variable, never arbitrary shell expressions.
        for name, commit in constants.items():
            value = value.replace("${" + name + "}", commit)
        if re.fullmatch(r"git\+https://[A-Za-z0-9.-]+/[A-Za-z0-9_./-]+\.git@[0-9a-f]{40}", value):
            return value
        raise ValueError(
            "nebius-terminus: verifier requirements must be package names, exact version pins, "
            "or HTTPS Git sources pinned to full commits"
        )

    def pytest_command(arguments: list[str], *, module: bool = False) -> str:
        if not arguments or any(
            not re.fullmatch(r"[A-Za-z0-9/_.,=:+-]+", arg) for arg in arguments
        ):
            raise ValueError("nebius-terminus: unsupported shell syntax in pytest invocation")
        executable = "/opt/verifier/bin/python -m pytest" if module else "/opt/verifier/bin/pytest"
        return executable + " " + shlex.join(arguments) + "\n"

    for line in _unwrap_installer_guards(logical_lines):
        command = re.sub(r"\\\r?\n", " ", line).strip()
        if not command or command.startswith("#"):
            output.append(line)
            continue
        constant = re.fullmatch(r"([A-Z][A-Z0-9_]*)=['\"]([0-9a-f]{40})['\"]", command)
        if constant:
            constants[constant[1]] = constant[2]
            output.append(line)
            continue
        if re.fullmatch(r"apt-get update(?: -qq)?", command):
            uses_apt = True
            continue
        apt_packages = _apt_bootstrap_packages(command)
        if apt_packages is not None:
            uses_apt = True
            packages.extend(apt_packages)
            continue
        apk_packages = _apk_bootstrap_packages(command)
        if apk_packages is not None:
            # Only a preamble can be relocated without interpreting shell
            # control flow. An APK command inside an uncalled function or false
            # branch must not become unconditional image installation.
            preamble = {":", "set -e", "set -eu", "set -eux", "set -euo pipefail",
                        "set -euxo pipefail", "set -o pipefail"}
            if any(previous.strip() and not previous.lstrip().startswith("#")
                   and previous.strip() not in preamble for previous in output):
                raise ValueError("nebius-terminus: APK bootstrap requires an unconditional preamble")
            alpine_packages.extend(apk_packages)
            continue
        if _UV_INSTALL.fullmatch(command):
            installer_count += 1
            continue
        if _UV_SOURCE.fullmatch(command):
            source_count += 1
            continue
        words = shlex.split(command)
        if words[:2] == ["uv", "venv"]:
            if (
                len(words) != 5
                or words[2] not in {"-p", "--python"}
                or not re.fullmatch(r"\d+\.\d+", words[3])
                or not re.fullmatch(r"[A-Za-z0-9_.-]+", words[4])
                or venv is not None
            ):
                raise ValueError("nebius-terminus: unsupported verifier venv bootstrap")
            python_version, venv = words[3], words[4]
            continue
        if venv is not None and words == ["source", venv + "/bin/activate"]:
            output.append("source /opt/verifier/bin/activate\n")
            venv_activated = True
            continue
        pip_prefix = next(
            (
                prefix
                for prefix in (
                    ("pip", "install"),
                    ("pip3", "install"),
                    ("python", "-m", "pip", "install"),
                    ("python3", "-m", "pip", "install"),
                    ("uv", "pip", "install"),
                )
                if words[: len(prefix)] == list(prefix)
            ),
            None,
        )
        if pip_prefix:
            if pip_prefix[0] == "uv" and not venv_activated:
                raise ValueError("nebius-terminus: uv pip requires the declared verifier venv")
            values = [
                word for word in words[len(pip_prefix) :]
                if word not in {"--break-system-packages", "--no-cache-dir"}
            ]
            if not values:
                raise ValueError("nebius-terminus: empty verifier requirements")
            requirements.extend(requirement(value) for value in values)
            pip_count += 1
            continue
        if words[0] == "uvx":
            uvx_count += 1
            position = 1
            while position + 1 < len(words) and words[position] in {"--index", "--index-strategy"}:
                option, value = words[position : position + 2]
                if (
                    option == "--index"
                    and not re.fullmatch(r"https://[A-Za-z0-9.-]+/[A-Za-z0-9_./-]+", value)
                ) or (
                    option == "--index-strategy"
                    and value not in {"first-index", "unsafe-best-match"}
                ):
                    raise ValueError("nebius-terminus: unsupported verifier package index")
                indexes.extend((option, value))
                position += 2
            if words[position : position + 1] in (["-p"], ["--python"]):
                if position + 1 >= len(words) or not re.fullmatch(r"\d+\.\d+", words[position + 1]):
                    raise ValueError("nebius-terminus: unsupported uvx Python declaration")
                python_version = words[position + 1]
                position += 2
            while position + 1 < len(words) and words[position] in {"-w", "--with"}:
                requirements.append(requirement(words[position + 1]))
                position += 2
            if words[position : position + 1] != ["pytest"]:
                raise ValueError(
                    "nebius-terminus: only the pinned uvx pytest bootstrap is supported"
                )
            output.append(pytest_command(words[position + 1 :]))
            pytest_count += 1
            continue
        pytest_prefix = next(
            (
                prefix
                for prefix in (
                    ("pytest",),
                    ("python", "-m", "pytest"),
                    ("python3", "-m", "pytest"),
                    ("uv", "run", "pytest"),
                )
                if words[: len(prefix)] == list(prefix)
            ),
            None,
        )
        if pytest_prefix:
            if pytest_prefix[0] == "uv" and not venv_activated:
                raise ValueError("nebius-terminus: uv run requires the declared verifier venv")
            output.append(pytest_command(words[len(pytest_prefix) :], module="-m" in pytest_prefix))
            pytest_count += 1
            continue
        if words[:3] == ["curl", "-L", "-o"] and len(words) == 5:
            destination, url = words[3:]
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", destination) or not re.fullmatch(
                r"https://[A-Za-z0-9.-]+/[A-Za-z0-9_./-]+", url
            ):
                raise ValueError("nebius-terminus: unsupported verifier asset download")
            downloads.append((url, destination))
            output.append(f"cp /opt/verifier-assets/{destination} {destination}\n")
            continue
        if re.search(r"\b(?:apt-get|apt|apk|pip|pip3|uv|uvx|curl|wget)\b", command):
            raise ValueError(
                "nebius-terminus: unsupported online bootstrap command in tests/test.sh"
            )
        output.append(line)
    uvx_mode = uvx_count == 1 and pip_count == 0 and venv is None
    pip_mode = pip_count == 1 and uvx_count == 0
    if (
        not (uvx_mode or pip_mode)
        or pytest_count == 0
        or installer_count > 1
        or source_count > 1
        or installer_count != source_count
        or (venv is not None and not venv_activated)
    ):
        raise ValueError(
            "nebius-terminus: tests/test.sh requires a recognized Harbor verifier bootstrap"
        )
    if not any(item.startswith("pytest==") for item in requirements):
        raise ValueError("nebius-terminus: verifier pytest must have an exact version pin")
    return HarborOfflineBootstrap(
        script="".join(output),
        apt_packages=tuple(sorted(set(packages))),
        python_version=python_version,
        requirements=tuple(dict.fromkeys(requirements)),
        index_args=tuple(indexes),
        downloads=tuple(downloads),
        system_site_packages=pip_mode and venv is None,
        apk_packages=tuple(sorted(set(alpine_packages))),
        uses_apt=uses_apt,
    )


def _bundle_path(staged: Path, value: str, *, directory: bool = False) -> Path:
    relative = PurePosixPath(value)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError("nebius-terminus: preparation path must remain inside the bundle")
    path = staged
    if path.is_symlink():
        raise ValueError("nebius-terminus: symlink bundle paths are unsupported")
    for part in relative.parts:
        path /= part
        if path.is_symlink():
            raise ValueError("nebius-terminus: symlink bundle paths are unsupported")
    if path.exists() and (not path.is_dir() if directory else not path.is_file()):
        raise ValueError("nebius-terminus: invalid preparation path type")
    return path


def _without_packaged_openhands_runtime(original: str) -> str:
    """Replace only the complete known foreign-agent packaging convention.

    This runs only in the explicitly selected Terminus preparation profile.
    The original Dockerfile is retained; arbitrary stages or task dependencies
    on the foreign runtime require a separately reviewed adaptation.
    """
    instructions = dockerfile_instructions(original)
    image = "terminalworld-openhands-sdk-cache:1.34.0-py312-musl-v3"
    alias = "terminalworld_openhands_runtime_cache"
    if not any("terminalworld-openhands-sdk-cache" in item.arguments.lower()
               or alias in item.arguments.lower()
               for item in instructions):
        return original
    message = "nebius-terminus: noncanonical OpenHands cache requires explicit adaptation"
    if (len(instructions) < 2 or instructions[0].keyword != "FROM"
            or instructions[0].arguments != f"{image} AS {alias}"
            or instructions[1].keyword != "FROM"
            or sum(item.keyword == "FROM" for item in instructions) != 2):
        raise ValueError(message)
    paths = ("/opt/openhands-python", "/opt/openhands-sdk-venv", "/opt/openhands-musl-loader")
    expected = {f"--from={alias} {path} {path}" for path in paths}
    # Match the scanner's physical LF boundaries; Unicode separators may be
    # ordinary data in a retained shell command or heredoc.
    physical = original.split("\n")
    lines = [line + "\n" for line in physical[:-1]] + [physical[-1]]

    def span(index: int) -> range:
        start = instructions[index].line - 1
        end = instructions[index + 1].line - 1 if index + 1 < len(instructions) else len(lines)
        return range(start, end)

    seen: set[str] = set()
    removed = {0}
    for index, item in enumerate(instructions[1:], 1):
        if item.keyword == "COPY" and item.arguments in expected:
            # Later instructions can depend implicitly on the copied contents,
            # e.g. by copying all of /opt into a task input directory.
            if item.arguments in seen or index < len(instructions) - len(expected):
                raise ValueError(message)
            seen.add(item.arguments)
            removed.add(index)
        else:
            # Headers omit heredoc bodies. Inspect both the complete physical
            # span and the joined header so neither form can hide a dependency.
            source = item.arguments + "\n" + "".join(lines[line] for line in span(index))
            references = re.findall(r'''\bfrom\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s,]+))''',
                                    item.arguments, re.I)
            if (alias in source.lower() or "terminalworld-openhands-sdk-cache" in source.lower()
                    or any(path in source for path in paths)
                    or any("".join(ref).isdigit() for ref in references)):
                raise ValueError(message)
    if seen != expected:
        raise ValueError(message)
    dropped: set[int] = set()
    for index in removed:
        dropped.update(span(index))
    # A comment before a parser directive would disable that directive.
    comment_line = instructions[0].line - 1
    return "".join(
        "# Loom Terminus preparation: omitted the packaged OpenHands runtime cache.\n"
        if index == comment_line else line if index not in dropped else ""
        for index, line in enumerate(lines)
    )


def _arch_package_install() -> str:
    """Add harness packages without upgrading the authored Arch environment.

    A refreshed rolling repository may require an existing package upgrade.
    Reject that transaction before installation instead of silently changing
    task toolchains. Use a separate cache so authored offline inputs survive.
    """
    packages = "bash ca-certificates curl tmux asciinema shadow python tar"
    return f"""loom_prep_cache=$(mktemp -d /tmp/loom-arch-preparation.XXXXXX) && \
    trap 'rm -rf "$loom_prep_cache"' EXIT && \
    chmod 755 "$loom_prep_cache" && \
    pacman -Q > "$loom_prep_cache/installed" && \
    loom_missing='' && \
    for loom_package in {packages}; do \
        loom_existing=$(awk -v package="$loom_package" '$1 == package {{print $2}}' "$loom_prep_cache/installed") || exit 1; \
        if [ -z "$loom_existing" ]; then loom_missing="$loom_missing $loom_package"; fi; \
    done && \
    if [ -n "$loom_missing" ]; then \
    pacman -Sy --noconfirm && \
    pacman -Sp --needed --print-format '%n %v' $loom_missing > "$loom_prep_cache/transaction" && \
    while read -r loom_package loom_version; do \
        [ -n "$loom_package" ] || continue; \
        loom_existing=$(awk -v package="$loom_package" '$1 == package {{print $2}}' "$loom_prep_cache/installed") || exit 1; \
        if [ -n "$loom_existing" ] && [ "$loom_existing" != "$loom_version" ]; then \
            echo "nebius-terminus: Arch harness preparation would change authored package $loom_package ($loom_existing -> $loom_version); use an explicitly reviewed compatible image/repository snapshot" >&2; \
            exit 1; \
        fi; \
    done < "$loom_prep_cache/transaction" && \
    pacman -S --needed --noconfirm --cachedir "$loom_prep_cache" $loom_missing; \
    fi && \
    rm -rf "$loom_prep_cache" && trap - EXIT"""


def _alpine_package_install(packages: tuple[str, ...]) -> str:
    """Fail preparation if APK would change an authored package version.

    Simulation catches dependency upgrades before installation. Rechecking the
    inventory rejects a repository change between resolution and installation.
    Only our temporary evidence is removed; authored APK caches survive.
    """
    inventory = "awk '/^P:/ {name=substr($0,3)} /^V:/ {print name,substr($0,3)}' /lib/apk/db/installed"
    return f"""loom_prep_cache=$(mktemp -d /tmp/loom-alpine-preparation.XXXXXX) && \
    trap 'rm -rf "$loom_prep_cache"' EXIT && \
    {inventory} > "$loom_prep_cache/installed" && \
    apk add --no-cache --simulate {shlex.join(packages)} > "$loom_prep_cache/transaction" && \
    if grep -Eq '\\) (Upgrading|Downgrading|Purging|Reinstalling) ' "$loom_prep_cache/transaction"; then \
        echo 'nebius-terminus: Alpine harness preparation would change authored packages; use an explicitly reviewed compatible image/repository snapshot' >&2; \
        exit 1; \
    fi && \
    apk add --no-cache {shlex.join(packages)} && \
    {inventory} > "$loom_prep_cache/prepared" && \
    while IFS= read -r loom_package; do \
        if ! grep -Fxq "$loom_package" "$loom_prep_cache/prepared"; then \
            echo "nebius-terminus: Alpine preparation changed authored package $loom_package" >&2; exit 1; \
        fi; \
    done < "$loom_prep_cache/installed" && \
    rm -rf "$loom_prep_cache" && trap - EXIT"""


def _classify_terminus_base(original: str) -> tuple[str, bool, bool, tuple[Any, ...]]:
    classified = classify_terminus_image(original)
    return classified.base_image.split("@", 1)[0].removeprefix("docker.io/").removeprefix("library/"), classified.package_manager == "pacman", classified.package_manager == "apk", classified.instructions


def _suse_package_install() -> str:
    """Use Leap's existing repositories; reject solver changes before installing.

    Zypper XML transaction elements, not translated prose, identify removals and
    version changes. A second inventory check covers drift during installation.
    Repository metadata is refreshed once into our private disposable cache.
    """
    command = r'''loom_prep_cache=$(mktemp -d /tmp/loom-suse-preparation.XXXXXX) && \
    trap 'rm -rf "$loom_prep_cache"' EXIT && \
    rpm -qa --qf '%{NAME} %{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}\n' > "$loom_prep_cache/all-installed" && \
    awk '$1 != "gpg-pubkey"' "$loom_prep_cache/all-installed" > "$loom_prep_cache/installed" && \
    awk '{print $1}' "$loom_prep_cache/installed" > "$loom_prep_cache/names" && \
    loom_missing='' && \
    for loom_package in asciinema bash ca-certificates curl shadow python3 tar tmux; do \
        if ! rpm -q "$loom_package" >/dev/null 2>&1; then loom_missing="$loom_missing $loom_package"; fi; \
    done && \
    if [ -n "$loom_missing" ]; then \
        timeout 300 zypper --non-interactive --cache-dir "$loom_prep_cache/cache" refresh && \
        timeout 60 zypper --xmlout --non-interactive --no-refresh --cache-dir "$loom_prep_cache/cache" install --dry-run --no-recommends --no-allow-downgrade --no-allow-vendor-change $loom_missing > "$loom_prep_cache/transaction" && \
        if ! grep -q '<install-summary' "$loom_prep_cache/transaction"; then \
            echo 'nebius-terminus: openSUSE solver returned no transaction summary' >&2; exit 1; \
        fi && \
        if grep -Eq '<to-(upgrade|downgrade|remove|reinstall|change)[[:space:]>\/]' "$loom_prep_cache/transaction"; then \
            echo 'nebius-terminus: openSUSE solver would change authored packages' >&2; exit 1; \
        fi && \
        awk 'BEGIN {RS="<"; FS=">"} $1 ~ /^solvable[[:space:]]/ && $1 ~ /type="package"/ {entry=$1; sub(/^.*[[:space:]]name="/, "", entry); sub(/".*$/, "", entry); print entry}' "$loom_prep_cache/transaction" > "$loom_prep_cache/planned" && \
        while IFS= read -r loom_package; do \
            if grep -Fxq "$loom_package" "$loom_prep_cache/names"; then \
                echo 'nebius-terminus: openSUSE solver would replace an authored package' >&2; exit 1; \
            fi; \
        done < "$loom_prep_cache/planned" && \
        timeout 180 zypper --non-interactive --no-refresh --cache-dir "$loom_prep_cache/cache" install --no-recommends --no-allow-downgrade --no-allow-vendor-change $loom_missing; \
    fi && \
    rpm -qa --qf '%{NAME} %{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}\n' > "$loom_prep_cache/all-prepared" && \
    awk '$1 != "gpg-pubkey"' "$loom_prep_cache/all-prepared" > "$loom_prep_cache/prepared" && \
    while IFS= read -r loom_package; do \
        if ! grep -Fxq "$loom_package" "$loom_prep_cache/prepared"; then \
            echo 'nebius-terminus: openSUSE preparation changed an authored package' >&2; printf '%s\n' "$loom_package" >&2; exit 1; \
        fi; \
    done < "$loom_prep_cache/installed" && \
    rm -rf "$loom_prep_cache" && trap - EXIT'''
    frame = PACKAGE_GUARD_PREFIX + json.dumps(TaskImagePreparationError(
        'image_preparation_package_change_required', PACKAGE_GUARD_MESSAGE,
    ).receipt(), separators=(',', ':'))
    for message in ('openSUSE solver would change authored packages',
                    'openSUSE solver would replace an authored package',
                    'openSUSE preparation changed an authored package'):
        old = "echo 'nebius-terminus: " + message + "'"
        command = command.replace(old, 'echo ' + shlex.quote(frame) + ' >&2; ' + old)
    return command


def _dockerfile_custom_shell(instructions: tuple[Any, ...]) -> bool:
    custom_shell = False
    for instruction in instructions:
        if instruction.keyword != "SHELL":
            continue
        try:
            shell = json.loads(instruction.arguments)
        except ValueError as exc:
            raise ValueError("nebius-terminus: Dockerfile SHELL must be a JSON array") from exc
        if not isinstance(shell, list) or not shell or any(
            not isinstance(part, str) or not part for part in shell
        ):
            raise ValueError("nebius-terminus: Dockerfile SHELL must contain command strings")
        custom_shell = True
    return custom_shell


def _run_instruction(command: str, *, custom_shell: bool) -> str:
    if custom_shell:
        # Override only our build-time command, leaving the authored
        # image's SHELL configuration and preceding RUN behavior intact.
        return "RUN " + json.dumps(["/bin/sh", "-c", command.replace("\\\n", "\n")]) + "\n"
    return "RUN " + command + "\n"


def _fedora_package_install() -> str:
    """Install only missing harness packages; exclude every authored RPM from changes."""
    return r'''loom_prep_cache=$(mktemp -d /tmp/loom-fedora-preparation.XXXXXX) &&     trap 'rm -rf "$loom_prep_cache"' EXIT &&     rpm -qa --queryformat '%{NAME}\n' > "$loom_prep_cache/installed" &&     loom_missing='' &&     for loom_package in bash ca-certificates curl tmux asciinema shadow-utils python3 tar; do         if ! rpm -q "$loom_package" >/dev/null 2>&1; then loom_missing="$loom_missing $loom_package"; fi;     done &&     if [ -n "$loom_missing" ]; then         loom_excluded=$(paste -sd, "$loom_prep_cache/installed") &&         dnf --setopt=install_weak_deps=False --setopt=cachedir="$loom_prep_cache/cache"             --exclude="$loom_excluded" install -y $loom_missing;     fi'''


def _terminus_package_install(
    *, arch: bool, alpine: bool, apt_packages: set[str], apk_packages: set[str], suse: bool = False, fedora: bool = False,
) -> str:
    if fedora:
        return _fedora_package_install()
    if suse:
        return f"{_suse_package_install()} && {_SUSE_TMUX_RUNTIME_SETUP}"
    if arch:
        return _arch_package_install()
    if alpine:
        return _alpine_package_install(tuple(sorted(apk_packages)))
    return (
        "apt-get update -qq && apt-get install -y --no-install-recommends "
        + " ".join(sorted(apt_packages))
    )


def append_terminus_runtime_tools(
    dockerfile: str, *, prepare_harness_dirs: bool = False, build_arg_names: Collection[str] = (),
) -> str:
    """Install the Harbor terminus package set onto an authored task Dockerfile.

    The appended ``RUN`` fails the image build when bash, tmux, or asciinema
    is still missing. Unsupported final bases fail before a build starts.
    `build_arg_names` reserves the caller's frozen build arguments when an
    internal build-only variable is needed to resolve an authored USER.
    """
    final_base, arch, alpine, instructions = _classify_terminus_base(dockerfile)
    suse = final_base == "opensuse/leap:15.5"
    fedora = final_base == "fedora:42"
    custom_shell = _dockerfile_custom_shell(instructions)
    install = _terminus_package_install(
        arch=arch,
        alpine=alpine,
        apt_packages=set(_TERMINUS_APT_PACKAGES),
        apk_packages=set(_TERMINUS_APK_PACKAGES),
        suse=suse, fedora=fedora,
    )
    cleanup = "true" if arch or alpine or suse or fedora else "rm -rf /var/lib/apt/lists/*"
    prepared = (
        dockerfile.rstrip()
        + "\n\n# Loom: terminus runtime tools; original task above.\n"
        + "USER root\n"
        + _run_instruction(
            f"{install} && {cleanup} && {_TERMINUS_TOOL_PROBE}",
            custom_shell=custom_shell,
        )
    )
    last_user = next(
        (
            instruction.arguments.strip()
            for instruction in reversed(instructions)
            if instruction.keyword == "USER"
        ),
        "",
    )
    if prepare_harness_dirs:
        # These are harness input/output roots, never the authored business tree.
        # Keep the image USER (including numeric user:group) and WORKDIR intact.
        roots = "/solution /tests /logs/verifier /loom/verifier"
        owner = shlex.quote(last_user or "root")
        if "$" in last_user:
            # Docker expands USER and ARG defaults using the authored ARG/ENV
            # scope. A quoted shell variable reads that resulting value without
            # re-evaluating command substitutions or changing runtime ENV.
            occupied = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", dockerfile)) | set(build_arg_names)
            name = "LOOM_PREPARATION_OWNER"
            suffix = 0
            while name in occupied:
                suffix += 1
                name = f"LOOM_PREPARATION_OWNER_{suffix}"
            prepared += f"ARG {name}={last_user}\n"
            owner = f'"${name}"'
        checks = " && ".join(f"test ! -L {path}" for path in ("/solution", "/tests", "/logs", "/logs/verifier", "/loom", "/loom/verifier"))
        prepared += _run_instruction(
            f"{checks} && mkdir -p {roots} && chown {owner} {roots} && chmod u+rwx {roots}",
            custom_shell=custom_shell,
        )
    if last_user and last_user not in {"root", "0"}:
        prepared += f"USER {last_user}\n"
    if suse:
        prepared += _run_instruction(_SUSE_TMUX_STARTUP_PROBE, custom_shell=custom_shell)
    return prepared


def _preparation_dockerfile(
    original: str, bootstrap: HarborOfflineBootstrap, workdir: str, identity: SandboxIdentityV1,
) -> str:
    original = _without_packaged_openhands_runtime(original)
    final_base, arch, alpine, instructions = _classify_terminus_base(original)
    suse = final_base == "opensuse/leap:15.5"
    fedora = final_base == "fedora:42"
    if (arch or alpine or suse or fedora) and (bootstrap.apt_packages or bootstrap.uses_apt):
        raise ValueError(
            f"nebius-terminus: {'Arch Linux' if arch else 'openSUSE' if suse else 'Alpine'} images cannot use a Debian package bootstrap; "
            "provide an explicitly reviewed verifier dependency preparation"
        )
    if not alpine and bootstrap.apk_packages:
        raise ValueError("nebius-terminus: APK bootstrap requires an official Alpine final image")
    custom_shell = _dockerfile_custom_shell(instructions)

    def run(command: str) -> str:
        return _run_instruction(command, custom_shell=custom_shell)
    packages = sorted(_TERMINUS_APT_PACKAGES.union(bootstrap.apt_packages))
    requirements = shlex.join((*bootstrap.index_args, *bootstrap.requirements))
    unpinned_tool = bootstrap.python_version is None and not bootstrap.system_site_packages
    if unpinned_tool:
        # Match uvx's tool resolution, including a managed Python when the
        # image interpreter cannot satisfy the tool's Requires-Python metadata.
        # Keep the tool and its interpreter outside the authored task PATH.
        pytest_pin = next(item for item in bootstrap.requirements if item.startswith("pytest=="))
        extras = tuple(
            value for item in bootstrap.requirements if item != pytest_pin
            for value in ("--with", item)
        )
        tool_args = shlex.join((*bootstrap.index_args, *extras, pytest_pin))
        python_setup = (
            "UV_PYTHON_INSTALL_DIR=/opt/verifier-python UV_TOOL_DIR=/opt/verifier-tools "
            "UV_TOOL_BIN_DIR=/opt/verifier-tools/bin loom-nebius-uv tool install "
            f"{tool_args} && ln -s /opt/verifier-tools/pytest /opt/verifier"
        )
    elif bootstrap.python_version is None:
        # Plain pip scripts inherit the image interpreter and its dependencies.
        python_setup = 'loom-nebius-uv venv --python "$(command -v python3)" --system-site-packages /opt/verifier'
    else:
        python_setup = (
            f"UV_PYTHON_INSTALL_DIR=/opt/verifier-python loom-nebius-uv python install {bootstrap.python_version} && "
            f"UV_PYTHON_INSTALL_DIR=/opt/verifier-python loom-nebius-uv venv --python {bootstrap.python_version} /opt/verifier"
        )
    if not unpinned_tool:
        python_setup += f" && loom-nebius-uv pip install --python /opt/verifier/bin/python {requirements}"
    assets = "".join(
        run(f"mkdir -p /opt/verifier-assets && curl --fail --location {shlex.quote(url)} -o /opt/verifier-assets/{name} && chmod 644 /opt/verifier-assets/{name}")
        for url, name in bootstrap.downloads
    )
    uid, gid, home = identity.run_as_user, identity.run_as_group, identity.home
    name = "agent" if uid == 65532 else f"loom-task-{uid}"
    identity_setup = (
        f"(getent group {gid} >/dev/null || groupadd --gid {gid} {name}) && "
        f"(getent passwd {uid} >/dev/null || useradd --uid {uid} --gid {gid} --home-dir {home} {name})"
    )
    workspace_setup = (
        f"mkdir -p {workdir} {home} /tests /logs/verifier /loom/verifier && "
        f"chown -R {uid}:{gid} {workdir} {home} /tests /logs/verifier /loom/verifier"
    )
    if uid != 65532 or gid != 65532 or home != "/home/agent":
        # Preserve authored ownership within the task image for explicit users.
        # Only newly created HOME/workspace roots need initial ownership; the
        # private verifier/harness directories are controlled by preparation.
        workspace_setup = (
            f'for directory in {workdir} {home}; do '
            f'if [ ! -e "$directory" ]; then mkdir -p "$directory" && '
            f'chown {uid}:{gid} "$directory"; fi; done && '
            f"mkdir -p /tests /logs/verifier /loom/verifier && "
            f"chown -R {uid}:{gid} /tests /logs/verifier /loom/verifier"
        )
    # Authored caches may be offline task inputs (for example Poetry wheels).
    # Disable only our uv download cache; never delete the image's HOME cache.
    alpine_packages = _TERMINUS_APK_PACKAGES.union(bootstrap.apk_packages)
    package_install = _terminus_package_install(
        arch=arch,
        alpine=alpine,
        apt_packages=set(packages),
        apk_packages=set(alpine_packages),
        suse=suse, fedora=fedora,
    )
    package_cleanup = "true" if arch or alpine or suse or fedora else "rm -rf /var/lib/apt/lists/*"
    preparation = run(f"""export UV_NO_CACHE=1 && {package_install} && \\
    {python_setup} && \\
    loom-nebius-uv pip freeze --python /opt/verifier/bin/python > /opt/verifier/resolved-requirements.txt && \\
    {identity_setup} && \\
    {workspace_setup} && \\
    {package_cleanup}""")
    return (
        original.rstrip()
        + "\n\n# Loom Nebius: build-only harness/verifier preparation; original task above.\n"
        + "USER root\n"
        + "COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/loom-nebius-uv\n"
        + preparation
        + f"""
{assets}ENV HOME={home}
# Preserve the base image PATH and agent interpreter; verifier uses its own venv.
USER {uid}:{gid}
WORKDIR {workdir}
"""
        + (run(_SUSE_TMUX_STARTUP_PROBE) if suse else "")
    )


def prepare_nebius_terminus_image(staged: Path, environment: dict[str, Any]) -> bool:
    """Write deterministic derived inputs and point the config at their Dockerfile.

    The supplied environment must already use the supported workspace identity.
    This bounded adapter needs the original Dockerfile and Harbor tests/test.sh;
    prebuilt/custom offline images require an explicit reviewed preparation path.
    """
    if environment.get("docker_image") or not environment.get("dockerfile"):
        raise ValueError("nebius-terminus: image preparation requires original Dockerfile sources")
    if environment.get("docker_build_target"):
        raise ValueError(
            "nebius-terminus: selected Dockerfile build targets require explicit adaptation"
        )
    workdir = validate_task_workdir(environment.get("workdir", "/app"))
    source_name = str(environment["dockerfile"])
    if source_name.endswith(_DOCKERFILE_SUFFIX):
        source_name = source_name.removesuffix(_DOCKERFILE_SUFFIX)
    source = _bundle_path(staged, source_name)
    test_script = _bundle_path(staged, "tests/test.sh")
    if not source.is_file() or not test_script.is_file():
        raise ValueError("nebius-terminus: original Dockerfile and tests/test.sh are required")
    bootstrap = adapt_harbor_test_script(test_script.read_text())
    if environment.get("user") == "image":
        raise ValueError(
            "nebius-terminus: legacy offline preparation requires an explicit numeric or root task identity; "
            "image-default USER requires native task-image execution"
        )
    identity = resolve_sandbox_identity(
        environment.get("user", "agent"), (environment.get("environment") or {}).get("HOME"),
    ) or SandboxIdentityV1(run_as_user=65532, run_as_group=65532, home="/home/agent")
    dockerfile = _preparation_dockerfile(source.read_text(), bootstrap, workdir, identity)
    target_name = source_name + _DOCKERFILE_SUFFIX
    target = _bundle_path(staged, target_name)
    offline = _bundle_path(staged, OFFLINE_SCRIPT)
    # Validate both output paths before writing either one.
    _bundle_path(staged, "verifier", directory=True)
    changed = (
        not target.is_file()
        or target.read_text() != dockerfile
        or not offline.is_file()
        or offline.read_text() != bootstrap.script
        or not offline.stat().st_mode & 0o111
    )
    target.write_text(dockerfile)
    offline.parent.mkdir(parents=True, exist_ok=True)
    offline.write_text(bootstrap.script)
    offline.chmod(0o755)
    environment["dockerfile"] = target_name
    return changed
