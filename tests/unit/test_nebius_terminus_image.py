"""Behavioral boundaries for the bounded Harbor image preparation adapter."""

import json
import os
import subprocess
from pathlib import Path

import pytest

from loom.nebius_terminus_image import (
    OFFLINE_SCRIPT,
    _arch_package_install,
    adapt_harbor_test_script,
    append_terminus_runtime_tools,
    prepare_nebius_terminus_image,
)
from loom.service_execution_terminus2 import TASK_IMAGE_TOOLS_REQUIRED

SCRIPT = """#!/bin/bash
apt-get update
apt-get install -y curl primer3
curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh
source "$HOME/.local/bin/env"
# Keep this task-specific preparation and reward logic.
rm *.csv
python3 /tests/gen_large_csv.py input
cp /tests/test.py /app/test.py
uvx \\
  -p 3.13 \\
  -w pytest==8.4.1 \\
  -w pandas==2.3.3 \\
  -w pytest-json-ctrf==0.3.5 \\
  pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA
if [ $? -eq 0 ]; then
  echo 1 > /logs/verifier/reward.txt
else
  echo 0 > /logs/verifier/reward.txt
fi
"""


def bundle(tmp_path: Path) -> dict:
    (tmp_path / "environment").mkdir()
    (tmp_path / "environment/Dockerfile").write_text(
        "FROM python:3.13-slim-bookworm\nWORKDIR /app\nCOPY data /data\n",
    )
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test.sh").write_text(SCRIPT)
    return {
        "dockerfile": "environment/Dockerfile",
        "docker_build_context": "environment",
        "workdir": "/app",
    }


def test_retains_setup_and_reward_semantics_and_extracts_pins() -> None:
    result = adapt_harbor_test_script(SCRIPT)
    assert result.python_version == "3.13"
    assert result.apt_packages == ("curl", "primer3")
    assert result.requirements == ("pytest==8.4.1", "pandas==2.3.3", "pytest-json-ctrf==0.3.5")
    assert (
        "rm *.csv\npython3 /tests/gen_large_csv.py input\ncp /tests/test.py /app/test.py\n"
        in result.script
    )
    assert (
        "/opt/verifier/bin/pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA\n"
        in result.script
    )
    assert result.script.endswith(SCRIPT[SCRIPT.index("if [ $? -eq 0 ]") :])
    assert "apt-get" not in result.script
    assert "uvx" not in result.script


def test_canonical_uv_installer_preserves_pins_test_arguments_and_reward() -> None:
    source = SCRIPT.replace("uv/0.9.5/install.sh", "uv/install.sh")
    expected = adapt_harbor_test_script(SCRIPT)

    actual = adapt_harbor_test_script(source)

    assert actual == expected
    assert actual.python_version == "3.13"
    assert actual.requirements == ("pytest==8.4.1", "pandas==2.3.3", "pytest-json-ctrf==0.3.5")
    assert actual.script.endswith(source[source.index("if [ $? -eq 0 ]") :])
    result = subprocess.run(["bash", "-n"], input=actual.script, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_canonical_uv_installer_in_complete_missing_uv_guard() -> None:
    source = SCRIPT.replace("uv/0.9.5/install.sh", "uv/install.sh").replace(
        "curl -LsSf", "if ! command -v uv >/dev/null 2>&1; then\n  curl -LsSf",
    ).replace('source "$HOME/.local/bin/env"', '  source "$HOME/.local/bin/env"\nfi')

    actual = adapt_harbor_test_script(source)

    assert "command -v" not in actual.script
    assert "install.sh" not in actual.script
    assert actual.requirements == adapt_harbor_test_script(SCRIPT).requirements
    assert actual.script.endswith(source[source.index("if [ $? -eq 0 ]") :])
    result = subprocess.run(["bash", "-n"], input=actual.script, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("installer", [
    "https://astral.sh/uv/install.sh?version=latest", "https://astral.sh/uv/install.sh#fragment",
    "https://astral.sh/uv//install.sh", "http://astral.sh/uv/install.sh",
    "https://astral.sh.example.org/uv/install.sh", "https://astral.sh/uv/latest/install.sh",
])
def test_canonical_uv_installer_does_not_accept_other_endpoints(installer: str) -> None:
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(SCRIPT.replace("https://astral.sh/uv/0.9.5/install.sh", installer))


@pytest.mark.parametrize("suffix", ["; touch /app/ready", " && echo initialized", " --extra"])
def test_canonical_uv_installer_does_not_drop_appended_commands(suffix: str) -> None:
    source = SCRIPT.replace("uv/0.9.5/install.sh | sh", "uv/install.sh | sh" + suffix)
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(source)


def test_preserves_other_shell_continuations() -> None:
    statement = 'printf "%s" \\\n  "hello"\n'
    result = adapt_harbor_test_script(SCRIPT.replace("rm *.csv\n", statement))
    assert statement in result.script


def test_quiet_apt_bootstrap_preserves_packages_and_reward() -> None:
    script = SCRIPT.replace(
        "apt-get update\napt-get install -y curl primer3",
        "apt-get update -qq && apt-get install -y -qq curl primer3 "
        "&& rm -rf /var/lib/apt/lists/*",
    )
    result = adapt_harbor_test_script(script)
    assert result.apt_packages == ("curl", "primer3")
    assert result.script.endswith(SCRIPT[SCRIPT.index("if [ $? -eq 0 ]") :])


def test_pip_no_cache_flag_is_not_a_requirement() -> None:
    result = adapt_harbor_test_script(
        "pip3 install --no-cache-dir pytest==8.3.5 pytest-json-ctrf==0.5.0\n"
        "pytest /tests/test_state.py -rA\n"
        "exit $?\n"
    )
    assert result.requirements == ("pytest==8.3.5", "pytest-json-ctrf==0.5.0")
    assert result.system_site_packages
    assert result.script == "/opt/verifier/bin/pytest /tests/test_state.py -rA\nexit $?\n"


def test_uv_path_activation_relocates_only_installer_path() -> None:
    script = SCRIPT.replace(
        'source "$HOME/.local/bin/env"', 'export PATH="$HOME/.local/bin:$PATH"',
    )
    result = adapt_harbor_test_script(script)
    assert 'export PATH=' not in result.script
    assert "python3 /tests/gen_large_csv.py input\n" in result.script
    assert result.python_version == "3.13"


@pytest.mark.parametrize("flag", ["--allow-unauthenticated", "--force-yes", "--purge"])
def test_apt_behavior_changing_flags_remain_rejected(flag: str) -> None:
    with pytest.raises(ValueError, match="unsupported apt bootstrap"):
        adapt_harbor_test_script(SCRIPT.replace("install -y", f"install -y {flag}"))


def test_custom_path_activation_is_not_silently_removed() -> None:
    script = SCRIPT.replace('source "$HOME/.local/bin/env"', 'export PATH="/task/bin:$PATH"')
    with pytest.raises(ValueError, match="recognized Harbor"):
        adapt_harbor_test_script(script)


@pytest.mark.parametrize("redirect", [">/dev/null 2>&1", "&> /dev/null"])
def test_missing_curl_guard_relocates_only_curl_installer(redirect: str) -> None:
    script = SCRIPT.replace(
        "apt-get update\napt-get install -y curl primer3",
        f"if ! command -v curl {redirect}; then\n"
        "  # Installer-only guard.\n"
        "  apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*\n"
        "fi",
    )
    result = adapt_harbor_test_script(script)
    assert result.apt_packages == ("curl",)
    assert "command -v" not in result.script
    assert result.script.endswith(SCRIPT[SCRIPT.index("if [ $? -eq 0 ]") :])
    assert "python3 /tests/gen_large_csv.py input\n" in result.script


def test_missing_uv_guard_relocates_complete_pinned_installer() -> None:
    script = SCRIPT.replace(
        "apt-get update\napt-get install -y curl primer3",
        "if ! command -v uv &> /dev/null; then\n"
        "  apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*",
    ).replace('source "$HOME/.local/bin/env"', 'source "$HOME/.local/bin/env"\nfi')
    result = adapt_harbor_test_script(script)
    assert result.apt_packages == ("curl",)
    assert "command -v" not in result.script
    assert result.python_version == "3.13"
    assert result.requirements == ("pytest==8.4.1", "pandas==2.3.3", "pytest-json-ctrf==0.3.5")
    assert result.script.endswith(SCRIPT[SCRIPT.index("if [ $? -eq 0 ]") :])


@pytest.mark.parametrize(
    "body",
    [
        "apt-get install -y curl primer3",
        "apt-get install -y curl\ntouch /tmp/task-output",
        "apt-get install -y curl\nelse\ntouch /tmp/task-output",
        "if true; then\napt-get install -y curl\nfi",
        "curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh",
        "# No installation",
    ],
)
def test_guard_with_non_installer_work_is_rejected(body: str) -> None:
    script = "if ! command -v curl >/dev/null 2>&1; then\n" + body + "\nfi\n" + SCRIPT
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(script)


def test_unterminated_installer_guard_is_rejected() -> None:
    script = SCRIPT + "if ! command -v curl >/dev/null 2>&1; then\napt-get install -y curl\n"
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(script)


def test_guard_comment_backslash_cannot_hide_task_work() -> None:
    script = (
        "if ! command -v curl >/dev/null 2>&1; then\n"
        "apt-get install -y curl\n"
        "# A shell comment does not continue onto the next physical line. \\\n"
        "echo TASK_WORK\nfi\n" + SCRIPT
    )
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(script)


@pytest.mark.parametrize("opening", ["cat <<'PAYLOAD'", "cat <<-PAYLOAD", "cat <<PAYLOAD"])
def test_installer_guard_inside_heredoc_requires_explicit_adaptation(opening: str) -> None:
    script = (
        opening + "\n"
        "if ! command -v curl >/dev/null 2>&1; then\n"
        "apt-get install -y curl\nfi\nPAYLOAD\n" + SCRIPT
    )
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(script)


@pytest.mark.parametrize("condition", ["true", "false"])
def test_relocated_guard_preserves_enclosing_branch_semantics(condition: str) -> None:
    script = (
        f"if {condition}; then\n"
        "if ! command -v curl >/dev/null 2>&1; then\n"
        "apt-get install -y curl\nfi\nfi\n"
        "if [ $? -eq 0 ]; then echo TASK_WORK; fi\n"
        "exit 0\n" + SCRIPT
    )
    # Exit before pytest: exercise real shell control flow without executing
    # installers, changing system files, or requiring the verifier environment.
    converted = adapt_harbor_test_script(script).script
    syntax = subprocess.run(["bash", "-n"], input=converted, text=True, capture_output=True)
    assert syntax.returncode == 0, syntax.stderr
    result = subprocess.run(["bash"], input=converted, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "TASK_WORK\n"


@pytest.mark.parametrize(
    "old,new",
    [
        ("pytest==8.4.1", "pytest>=8"),
        ("-p 3.13", "--python latest"),
        ("-w pandas==2.3.3", "--with pandas>=2"),
        ("uv/0.9.5", "uv/latest"),
        ("apt-get install -y curl primer3", "apt-get install -y curl && echo danger"),
        ("rm *.csv", "python3 -m pip install pandas"),
        ("-rA", '-rA "$EXTRA"'),
    ],
)
def test_refuses_unknown_bootstrap_without_silent_fallback(old: str, new: str) -> None:
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(SCRIPT.replace(old, new))


@pytest.mark.parametrize("dependency", ["pandas", "uproot", "GitPython"])
def test_unversioned_auxiliary_requirement_retains_original_verification(dependency: str) -> None:
    source = SCRIPT.replace("pandas==2.3.3", dependency)
    result = adapt_harbor_test_script(source)
    assert result.requirements == ("pytest==8.4.1", dependency, "pytest-json-ctrf==0.3.5")
    assert result.script == adapt_harbor_test_script(SCRIPT).script


def test_plain_pip_accepts_unversioned_auxiliary_requirement() -> None:
    result = adapt_harbor_test_script(
        "pip install pytest==8.4.1 GitPython\n"
        "python -m pytest /tests/test_state.py -rA\n"
    )
    assert result.requirements == ("pytest==8.4.1", "GitPython")
    assert result.system_site_packages
    assert result.script == "/opt/verifier/bin/python -m pytest /tests/test_state.py -rA\n"


@pytest.mark.parametrize("dependency", [
    "https://example.com/package.whl", "git+https://example.com/project.git@main",
    "./local-package", "pandas;echo", "pandas>=2", "${DEPENDENCY}",
    "auxiliary-1.0-py3-none-any.whl", "auxiliary-1.0.tar.gz", "auxiliary-1.0.zip",
])
def test_unversioned_requirement_support_does_not_accept_other_sources(dependency: str) -> None:
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(SCRIPT.replace("pandas==2.3.3", dependency))


def test_requires_original_recognized_bootstrap() -> None:
    with pytest.raises(ValueError, match="recognized Harbor"):
        adapt_harbor_test_script("#!/bin/sh\n/opt/verifier/bin/pytest /tests/test_outputs.py\n")


def test_derivation_preserves_sources_and_base_interpreter_and_is_repeatable(
    tmp_path: Path,
) -> None:
    environment = bundle(tmp_path)
    original = (tmp_path / "environment/Dockerfile").read_bytes()
    assert prepare_nebius_terminus_image(tmp_path, environment)
    assert environment["dockerfile"] == "environment/Dockerfile.loom-nebius"
    assert (tmp_path / "environment/Dockerfile").read_bytes() == original
    assert (tmp_path / "tests/test.sh").read_text() == SCRIPT
    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert derived.startswith(original.decode())
    assert "ghcr.io/astral-sh/uv:0.9.5" in derived
    assert "pandas==2.3.3" in derived
    assert "primer3" in derived
    assert "chown -R 65532:65532 /app /home/agent /tests /logs/verifier /loom/verifier" in derived
    assert "USER 65532:65532\nWORKDIR /app" in derived
    assert "ENV PATH=" not in derived
    assert "COPY tests" not in derived
    assert (tmp_path / OFFLINE_SCRIPT).stat().st_mode & 0o111
    assert not prepare_nebius_terminus_image(tmp_path, environment)
    assert (tmp_path / environment["dockerfile"]).read_text() == derived


@pytest.mark.parametrize("value", ["../outside", "/tmp/outside"])
def test_rejects_path_escape(tmp_path: Path, value: str) -> None:
    environment = bundle(tmp_path)
    environment["dockerfile"] = value
    with pytest.raises(ValueError, match="inside the bundle"):
        prepare_nebius_terminus_image(tmp_path, environment)


@pytest.mark.parametrize("link", ["tests/test.sh", "environment/Dockerfile", "verifier"])
def test_rejects_source_and_output_symlinks(tmp_path: Path, link: str) -> None:
    environment = bundle(tmp_path)
    target = tmp_path / link
    if target.exists():
        target.unlink()
    target.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(ValueError, match="symlink"):
        prepare_nebius_terminus_image(tmp_path, environment)


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"docker_image": "ubuntu:24.04"}, "original Dockerfile"),
        ({"docker_build_target": "base"}, "build targets"),
        ({"workdir": "/loom"}, "workdir"),
    ],
)
def test_rejects_unreviewed_image_modes(tmp_path: Path, changes: dict, match: str) -> None:
    environment = bundle(tmp_path)
    environment.update(changes)
    with pytest.raises(ValueError, match=match):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()


def test_rejects_unknown_base_before_writing_outputs(tmp_path: Path) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text("FROM fedora:40\n")
    with pytest.raises(ValueError, match="Debian/Ubuntu"):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()


@pytest.mark.parametrize("base", ["archlinux:latest", "archlinux:base", "archlinux:base-devel"])
def test_arch_preparation_preserves_source_and_uses_its_package_manager(tmp_path: Path, base: str) -> None:
    environment = bundle(tmp_path)
    original = f"FROM {base} AS original\nRUN touch /authored-input\nFROM original\nWORKDIR /app\n"
    (tmp_path / "environment/Dockerfile").write_text(original)
    script = SCRIPT.replace("apt-get update\napt-get install -y curl primer3\n", "")
    (tmp_path / "tests/test.sh").write_text(script)

    assert prepare_nebius_terminus_image(tmp_path, environment)

    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert derived.startswith(original)
    assert "pacman" in derived and "apt-get" not in derived
    assert "pandas==2.3.3" in derived
    assert "ENV PATH=" not in derived and "COPY tests" not in derived
    assert (tmp_path / "tests/test.sh").read_text() == script
    assert (tmp_path / OFFLINE_SCRIPT).read_text().endswith(script[script.index("if [ $? -eq 0 ]") :])
    assert not prepare_nebius_terminus_image(tmp_path, environment)


def test_arch_rejects_debian_bootstrap_dependencies_before_preparation(tmp_path: Path) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text("FROM archlinux:latest\n")

    with pytest.raises(ValueError, match=r"Arch.*Debian.*bootstrap"):
        prepare_nebius_terminus_image(tmp_path, environment)

    assert not (tmp_path / OFFLINE_SCRIPT).exists()
    assert not (tmp_path / "environment/Dockerfile.loom-nebius").exists()


@pytest.mark.parametrize("transaction,accepted", [("python 3.14\n", True),
                                                ("bash 5.3\npython 3.14\n", True),
                                                ("bash 5.4\npython 3.14\n", False)])
def test_arch_package_transaction_never_changes_authored_packages(
    tmp_path: Path, transaction: str, accepted: bool,
) -> None:
    # Isolate only the package-manager process. Execute the real generated shell
    # guard, including its inventory, transaction parsing and failure ordering.
    manager = tmp_path / "pacman"
    manager.write_text("#!/bin/sh\nset -eu\ncase \"$1\" in\n"
                       "-Q) printf 'bash 5.3\\n' ;;\n"
                       "-Sy) exit 0 ;;\n"
                       "-Sp|-S) for arg; do test \"$arg\" != bash || exit 43; done; "
                       "if [ \"$1\" = -Sp ]; then printf '%s' \"$LOOM_TEST_TRANSACTION\"; "
                       "else touch \"$LOOM_TEST_INSTALL_CALLED\"; fi ;;\n"
                       "*) exit 42 ;;\nesac\n")
    manager.chmod(0o755)
    installed = tmp_path / "install-called"
    env = {**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
           "LOOM_TEST_TRANSACTION": transaction, "LOOM_TEST_INSTALL_CALLED": str(installed)}

    result = subprocess.run(["/bin/sh", "-c", _arch_package_install()],
                            capture_output=True, text=True, env=env)

    assert (result.returncode == 0) is accepted, result.stderr
    assert installed.exists() is accepted
    if not accepted:
        assert "would change authored package bash (5.3 -> 5.4)" in result.stderr


def test_plain_pip_keeps_base_dependencies_and_both_pytest_results(tmp_path: Path) -> None:
    environment = bundle(tmp_path)
    script = """pip install pytest==8.4.1 pytest-json-ctrf==0.3.5 --break-system-packages
python -m pytest --ctrf /logs/verifier/original.json -rA
ORIGINAL_EXIT_CODE=$?
python -m pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA
ADDITIONAL_EXIT_CODE=$?
if [ $ORIGINAL_EXIT_CODE -eq 0 ] && [ $ADDITIONAL_EXIT_CODE -eq 0 ]; then
    echo 1 > /logs/verifier/reward.txt
else
    echo 0 > /logs/verifier/reward.txt
fi
"""
    (tmp_path / "tests/test.sh").write_text(script)
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    offline = (tmp_path / OFFLINE_SCRIPT).read_text()
    assert '--python "$(command -v python3)" --system-site-packages /opt/verifier' in derived
    assert offline.count("/opt/verifier/bin/python -m pytest") == 2
    assert offline.endswith(script[script.index("ADDITIONAL_EXIT_CODE") :])
    assert "pip install" not in offline


def test_combined_apt_and_preinstalled_uv_keep_task_commands() -> None:
    script = SCRIPT.replace(
        "apt-get update\napt-get install -y curl primer3",
        "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y curl primer3",
    )
    script = script.replace("curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh\n", "")
    script = script.replace('source "$HOME/.local/bin/env"\n', "")
    result = adapt_harbor_test_script(script)
    assert result.apt_packages == ("curl", "primer3")
    assert "python3 /tests/gen_large_csv.py input" in result.script
    assert "apt-get" not in result.script


def test_explicit_uv_venv_relocates_activation_and_preserves_python() -> None:
    result = adapt_harbor_test_script("""uv venv -p 3.12 .tb
source .tb/bin/activate
uv pip install pytest==8.4.1 mailman==3.3.8 pytest-json-ctrf==0.3.5
uv run pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA
""")
    assert result.python_version == "3.12"
    assert result.script.startswith("source /opt/verifier/bin/activate\n")
    assert "mailman==3.3.8" in result.requirements
    assert "uv " not in result.script


def test_cpu_index_pinned_git_and_download_are_prepared_offline(tmp_path: Path) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text("FROM python:3.11\nWORKDIR /app\n")
    script = SCRIPT.replace(
        "uvx \\\n",
        """COMMIT_HASH='34bbbfdface3c18e5221aa7de6032d7220c6c6a1'
curl -L -o mobile_sam.pt https://github.com/ChaoningZhang/MobileSAM/raw/master/weights/mobile_sam.pt
uvx --index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match \\
""",
    ).replace(
        "-w pandas==2.3.3", "-w git+https://github.com/ChaoningZhang/MobileSAM.git@${COMMIT_HASH}"
    )
    (tmp_path / "tests/test.sh").write_text(script)
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    offline = (tmp_path / OFFLINE_SCRIPT).read_text()
    assert (
        "--index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match" in derived
    )
    assert ".git@34bbbfdface3c18e5221aa7de6032d7220c6c6a1" in derived
    assert "curl --fail --location" in derived
    assert "cp /opt/verifier-assets/mobile_sam.pt mobile_sam.pt" in offline
    assert "curl " not in offline
    assert "${COMMIT_HASH}" not in derived


@pytest.mark.parametrize(
    "command",
    [
        "pip install pytest>=8",
        "pip install pytest==8.4.1 && touch /tmp/unreviewed",
        "uvx -p 3.13 -w pytest==8.4.1 -w git+https://example.com/repo.git@main pytest /tests/test.py",
        "curl -L -o ../weights https://example.com/weights",
    ],
)
def test_unknown_dependency_sources_or_shell_remain_rejected(command: str) -> None:
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(command + "\npytest /tests/test.py\n")


@pytest.mark.parametrize("version", ["0.7.13", "0.8.22", "0.10.0"])
def test_pinned_uv_bootstraps_preserve_requirements_and_pytest_arguments(version: str) -> None:
    source = SCRIPT.replace("uv/0.9.5", "uv/" + version).replace(
        "-p 3.13", "--python 3.13"
    ).replace("-w ", "--with ")
    assert adapt_harbor_test_script(source) == adapt_harbor_test_script(SCRIPT)


@pytest.mark.parametrize(
    "dockerfile",
    [
        "FROM python:3.9-slim\nWORKDIR /app\nRUN python3 <<'PY'\nfrom datetime import datetime\nPY\n",
        'FROM python:3.9-slim\nCOPY <<-"FIRST" <<SECOND /tmp/\n\tFROM alpine:3.20\n\tFIRST\nSHELL []\nSECOND\n',
        "FROM node:18\nWORKDIR /app\n",
        "FROM node:22-bookworm-slim AS base\nFROM base AS task\n",
        "FROM php:7.1-cli\nWORKDIR /app\n",
        "FROM php:7.4.33-cli-buster\nWORKDIR /app\n",
        "FROM php:8.3-cli-bookworm AS base\nFROM base AS task\n",
        "FROM rootproject/root:6.30.06-ubuntu22.04\nWORKDIR /app\n",
        "FROM rootproject/root:6.24.06-ubuntu20.04\nWORKDIR /app\n",
        "FROM --platform=linux/amd64 \\\n python:3.9-slim AS base\nFROM base AS task\n",
    ],
)
def test_preparation_identifies_final_stage_without_changing_task_python(
    tmp_path: Path, dockerfile: str,
) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text(dockerfile)
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert derived.startswith(dockerfile)
    assert "loom-nebius-uv python install 3.13" in derived
    assert "ENV PATH=" not in derived
    assert (tmp_path / "environment/Dockerfile").read_text() == dockerfile


@pytest.mark.parametrize(
    "dockerfile,match",
    [
        ("FROM python:3.13-slim\nRUN cat <<EOF\nFROM ubuntu:24.04\n", "unterminated heredoc"),
        ("FROM ubuntu:24.04 AS base\nFROM alpine:3.20\n", "Alpine.*Debian.*bootstrap"),
        ("FROM python:3.13-slim\nSHELL bash -c\n", "SHELL"),
        ("ARG BASE=ubuntu:24.04\nFROM ${BASE}\n", "Debian/Ubuntu"),
        ("FROM node:18-alpine\n", "Debian/Ubuntu"),
        ("FROM php:8.3-cli-alpine\n", "Debian/Ubuntu"),
        ("FROM php:cli\n", "Debian/Ubuntu"),
        ("FROM php:8.3-fpm\n", "Debian/Ubuntu"),
        ("FROM custom/php:7.1-cli\n", "Debian/Ubuntu"),
        ("FROM php:8.3-cli-unknown\n", "Debian/Ubuntu"),
        ("FROM rootproject/root:6.30.06-fedora39\n", "Debian/Ubuntu"),
        ("FROM rootproject/root:latest\n", "Debian/Ubuntu"),
    ],
)
def test_ambiguous_or_unsupported_image_preparation_does_not_write_outputs(
    tmp_path: Path, dockerfile: str, match: str,
) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text(dockerfile)
    with pytest.raises(ValueError, match=match):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()
    assert not (tmp_path / "environment/Dockerfile.loom-nebius").exists()


@pytest.mark.parametrize("shell", [["/bin/bash", "-e", "-c"], ["/bin/zsh", "-c"]])
def test_preparation_uses_explicit_sh_without_replacing_authored_shell(tmp_path, shell):
    environment = bundle(tmp_path)
    original = "FROM python:3.13-slim\nSHELL " + json.dumps(shell) + "\nWORKDIR /app\n"
    (tmp_path / "environment/Dockerfile").write_text(original)
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert derived.startswith(original)
    appended = derived[len(original):]
    assert "SHELL " not in appended
    commands = [json.loads(line[4:]) for line in appended.splitlines() if line.startswith("RUN ")]
    assert commands and all(command[:2] == ["/bin/sh", "-c"] for command in commands)
    assert "loom-nebius-uv python install 3.13" in commands[0][2]
    assert (tmp_path / "environment/Dockerfile").read_text() == original


@pytest.mark.parametrize("user,home,expected", [
    ("root", "/root", "0:0"), ("1001:1002", "/home/miles", "1001:1002"),
])
def test_preparation_preserves_declared_user_home_and_missing_task_dependencies(tmp_path, user, home, expected):
    environment = bundle(tmp_path)
    environment.update(user=user, environment={"HOME": home})
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert f"ENV HOME={home}\n" in derived
    assert f"USER {expected}\n" in derived
    assert "php" not in derived and "composer" not in derived and "wget" not in derived
    assert "chown -R 65532:65532 /app" not in derived
    assert "ENV PATH=" not in derived


def test_preparation_rejects_unresolved_named_user_before_writing(tmp_path):
    environment = bundle(tmp_path)
    environment["user"] = "miles"
    with pytest.raises(ValueError, match="unsupported task identity"):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()


def test_apt_metadata_cleanup_is_relocated_with_explicit_packages():
    source = SCRIPT.replace("apt-get update\napt-get install -y curl primer3", 
                            "apt-get update && apt-get install -y curl primer3 && rm -rf /var/lib/apt/lists/*")
    assert adapt_harbor_test_script(source) == adapt_harbor_test_script(SCRIPT)


@pytest.mark.parametrize("suffix", [" && rm -rf /app/*", " && touch /app/setup", " || true"])
def test_apt_cleanup_does_not_hide_arbitrary_task_commands(suffix):
    with pytest.raises(ValueError, match="unsupported apt bootstrap"):
        adapt_harbor_test_script(SCRIPT.replace("apt-get install -y curl primer3", 
                                               "apt-get install -y curl primer3" + suffix))


def test_uvx_without_python_pin_resolves_its_isolated_tool_interpreter(tmp_path):
    environment = bundle(tmp_path)
    script = SCRIPT.replace('  -p 3.13 \\\n', '')
    (tmp_path / "tests/test.sh").write_text(script)
    result = adapt_harbor_test_script(script)
    assert result.python_version is None
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert "loom-nebius-uv tool install" in derived
    assert "UV_PYTHON_INSTALL_DIR=/opt/verifier-python" in derived
    assert "--with pandas==2.3.3" in derived
    assert "--with pytest-json-ctrf==0.3.5" in derived
    assert "pytest==8.4.1" in derived
    assert "ln -s /opt/verifier-tools/pytest /opt/verifier" in derived
    assert '--python "$(command -v python3)"' not in derived
    assert "--system-site-packages" not in derived
    assert "/opt/verifier/bin/pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA" in result.script
    assert result.script.endswith(script[script.index("if [ $? -eq 0 ]"):])


OPENHANDS_STAGE = 'FROM terminalworld-openhands-sdk-cache:1.34.0-py312-musl-v3 AS terminalworld_openhands_runtime_cache\n'
OPENHANDS_PATHS = ('/opt/openhands-python', '/opt/openhands-sdk-venv', '/opt/openhands-musl-loader')
OPENHANDS_COPIES = ''.join(f'COPY --from=terminalworld_openhands_runtime_cache {p} {p}\n' for p in OPENHANDS_PATHS)


@pytest.mark.parametrize("task_setup", ["RUN mkdir /task-data\n", "RUN printf 'alpha\u2028omega' > /task-data\n"])
def test_terminus_derivation_omits_complete_foreign_agent_cache_only(tmp_path, task_setup):
    env = bundle(tmp_path)
    source = tmp_path / 'environment/Dockerfile'
    task_image = 'FROM ubuntu:22.04\n' + task_setup + 'COPY data /task-data\nCMD ["/bin/bash"]\n'
    original = OPENHANDS_STAGE + task_image + OPENHANDS_COPIES
    source.write_text(original)
    assert prepare_nebius_terminus_image(tmp_path, env)
    derived = (tmp_path / env['dockerfile']).read_text()
    assert source.read_text() == original
    from loom.dockerfile_instructions import dockerfile_instructions
    instructions = dockerfile_instructions(derived)
    assert all('terminalworld_openhands_runtime_cache' not in i.arguments for i in instructions)
    assert all('terminalworld-openhands-sdk-cache' not in i.arguments for i in instructions)
    assert task_image in derived
    assert 'OpenHands' in derived and 'Terminus' in derived
    assert not prepare_nebius_terminus_image(tmp_path, env)


@pytest.mark.parametrize('alteration', [
    'cache_run', 'extra_copy', 'changed_destination', 'partial', 'task_dependency',
    'numeric_reference', 'unknown_tag', 'another_stage',
])
def test_noncanonical_foreign_agent_cache_requires_explicit_adaptation(tmp_path, alteration):
    env = bundle(tmp_path)
    source = tmp_path / 'environment/Dockerfile'
    original = OPENHANDS_STAGE + 'FROM ubuntu:22.04\n' + OPENHANDS_COPIES
    if alteration == 'cache_run':
        original = original.replace('FROM ubuntu', 'RUN touch /task-input\nFROM ubuntu')
    if alteration == 'extra_copy':
        original += 'COPY --from=terminalworld_openhands_runtime_cache /task-input /task-input\n'
    if alteration == 'changed_destination':
        original = original.replace('/opt/openhands-python /opt/openhands-python', '/opt/openhands-python /task-input')
    if alteration == 'partial':
        original = original.replace(OPENHANDS_COPIES.splitlines(keepends=True)[0], '')
    if alteration == 'task_dependency':
        original += 'RUN /opt/openhands-python/bin/python /task-setup.py\n'
    if alteration == 'numeric_reference':
        original += 'COPY --from=0 /other /other\n'
    if alteration == 'unknown_tag':
        original = original.replace('1.34.0-py312-musl-v3', 'new-version')
    if alteration == 'another_stage':
        original += 'FROM ubuntu:22.04\n'
    source.write_text(original)
    with pytest.raises(ValueError, match=r'OpenHands.*explicit'):
        prepare_nebius_terminus_image(tmp_path, env)
    assert source.read_text() == original
    assert not (tmp_path / 'environment/Dockerfile.loom-nebius').exists()


def test_foreign_cache_adaptation_retains_parser_directives():
    from loom.dockerfile_instructions import dockerfile_instructions
    from loom.nebius_terminus_image import _without_packaged_openhands_runtime

    directives = '# syntax=docker/dockerfile:1\n# escape=`\n'
    task = 'FROM ubuntu:22.04\nRUN echo hello`\nworld\n'
    result = _without_packaged_openhands_runtime(directives + OPENHANDS_STAGE + task + OPENHANDS_COPIES)
    assert result.startswith(directives)
    assert [(i.keyword, i.arguments) for i in dockerfile_instructions(result)] == [
        (i.keyword, i.arguments) for i in dockerfile_instructions(directives + task)]


@pytest.mark.parametrize('dependency', [
    'COPY --from=TERMINALWORLD_OPENHANDS_RUNTIME_CACHE /other /other\n',
    'COPY --from=terminalworld-openhands-sdk-cache:1.34.0-py312-musl-v3 /other /other\n',
    'COPY --from="0" /other /other\n',
    'COPY <<EOF /entrypoint.sh\n#!/bin/sh\nexec /opt/openhands-python/bin/python /task.py\nEOF\n',
])
def test_hidden_foreign_runtime_dependencies_are_not_removed(dependency):
    from loom.nebius_terminus_image import _without_packaged_openhands_runtime

    original = OPENHANDS_STAGE + 'FROM ubuntu:22.04\n' + dependency + OPENHANDS_COPIES
    with pytest.raises(ValueError, match=r'OpenHands.*explicit'):
        _without_packaged_openhands_runtime(original)


def test_runtime_copies_cannot_precede_implicit_task_dependencies():
    from loom.nebius_terminus_image import _without_packaged_openhands_runtime

    original = OPENHANDS_STAGE + 'FROM ubuntu:22.04\n' + OPENHANDS_COPIES + 'RUN cp -a /opt /task-input\n'
    with pytest.raises(ValueError, match=r'OpenHands.*explicit'):
        _without_packaged_openhands_runtime(original)


def test_apk_bootstrap_preserves_task_commands_and_reward():
    script = SCRIPT.replace('apt-get update\napt-get install -y curl primer3\n',
                            'apk add --no-cache curl\n')
    result = adapt_harbor_test_script(script)
    assert result.apk_packages == ('curl',)
    assert result.apt_packages == ()
    assert 'apk add' not in result.script
    assert 'python3 /tests/gen_large_csv.py input' in result.script
    assert result.script.endswith(script[script.index('if [ $? -eq 0 ]'):])


@pytest.mark.parametrize('command', ['apk add curl', 'apk add --no-cache --allow-untrusted curl',
                                     'apk add --no-cache ./curl.apk', 'apk add --no-cache curl.apk', 'apk add --no-cache curl.apk=1',
                                     'apk add --no-cache curl.apk-1', 'apk upgrade',
                                     'apk add --no-cache curl; touch /solved'])
def test_rejects_unknown_apk_bootstrap(command):
    script = SCRIPT.replace('apt-get update\napt-get install -y curl primer3', command)
    with pytest.raises(ValueError, match='bootstrap'):
        adapt_harbor_test_script(script)


@pytest.mark.parametrize('base', ['alpine:3.20', 'alpine:3.20.3'])
def test_alpine_preparation_preserves_source_and_separate_interpreter(tmp_path, base):
    environment = bundle(tmp_path)
    original = f'FROM {base} AS task\nRUN touch /authored-input\nFROM task\nWORKDIR /app\n'
    (tmp_path / 'environment/Dockerfile').write_text(original)
    script = SCRIPT.replace('apt-get update\napt-get install -y curl primer3\n',
                            'apk add --no-cache curl\n')
    (tmp_path / 'tests/test.sh').write_text(script)
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment['dockerfile']).read_text()
    assert derived.startswith(original)
    assert 'apk add' in derived and 'apt-get' not in derived and 'pacman' not in derived
    assert 'python install 3.13' in derived and 'pandas==2.3.3' in derived
    assert 'ENV PATH=' not in derived and 'COPY tests' not in derived
    assert (tmp_path / 'tests/test.sh').read_text() == script
    assert not prepare_nebius_terminus_image(tmp_path, environment)


@pytest.mark.parametrize('base,manager', [('alpine:3.20', 'apt'), ('ubuntu:24.04', 'apk'),
                                        ('archlinux:latest', 'apk')])
def test_rejects_mismatched_bootstrap_manager(tmp_path, base, manager):
    environment = bundle(tmp_path)
    (tmp_path / 'environment/Dockerfile').write_text(f'FROM {base}\n')
    if manager == 'apk':
        (tmp_path / 'tests/test.sh').write_text(SCRIPT.replace(
            'apt-get update\napt-get install -y curl primer3\n', 'apk add --no-cache curl\n'))
    with pytest.raises(ValueError, match='bootstrap'):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()
    assert not (tmp_path / 'environment/Dockerfile.loom-nebius').exists()


@pytest.mark.parametrize('transaction,drift,accepted', [
    ('(1/2) Installing curl (8.0-r0)', False, True),
    ('(1/2) Upgrading musl (1.2.5-r0 -> 1.2.5-r1)', False, False),
    ('(1/2) Downgrading musl (1.2.5-r0 -> 1.2.4-r1)', False, False),
    ('(1/2) Purging musl (1.2.5-r0)', False, False),
    ('(1/2) Reinstalling musl (1.2.5-r0)', False, False),
    ('(1/2) Installing curl (8.0-r0)', True, False),
])
def test_alpine_transaction_rejects_changes_to_authored_packages(tmp_path, transaction, drift, accepted):
    from loom.nebius_terminus_image import _alpine_package_install
    database = tmp_path / 'installed'
    database.write_text('P:musl\nV:1.2.5-r0\n\n')
    called = tmp_path / 'install-called'
    manager = tmp_path / 'apk'
    manager.write_text('#!/bin/sh\nset -eu\ncase "$*" in\n'
                       '*--simulate*) printf "%s\\n" "$LOOM_TEST_TRANSACTION" ;;\n'
                       '*) touch "$LOOM_TEST_INSTALL_CALLED"; '
                       'if [ "$LOOM_TEST_DRIFT" = 1 ]; then '
                       'printf "P:musl\\nV:1.2.5-r1\\n\\n" > "$LOOM_TEST_DB"; fi ;;\nesac\n')
    manager.chmod(0o755)
    env = {**os.environ, 'PATH': f"{tmp_path}:{os.environ['PATH']}",
           'LOOM_TEST_TRANSACTION': transaction, 'LOOM_TEST_INSTALL_CALLED': str(called),
           'LOOM_TEST_DB': str(database), 'LOOM_TEST_DRIFT': str(int(drift))}
    command = _alpine_package_install(('curl',)).replace('/lib/apk/db/installed', str(database))
    result = subprocess.run(['/bin/sh', '-c', command], capture_output=True, text=True, env=env)
    assert (result.returncode == 0) is accepted, result.stderr
    assert called.exists() is (accepted or drift)
    if not accepted:
        assert 'authored package' in result.stderr


def test_alpine_rejects_apt_update_even_without_apt_packages(tmp_path):
    environment = bundle(tmp_path)
    (tmp_path / 'environment/Dockerfile').write_text('FROM alpine:3.20\n')
    (tmp_path / 'tests/test.sh').write_text(SCRIPT.replace(
        'apt-get install -y curl primer3', 'apk add --no-cache curl'))
    with pytest.raises(ValueError, match=r'Alpine.*Debian.*bootstrap'):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()


def test_leap_preparation_retains_authored_source_identity_and_uses_only_zypper():
    from loom.nebius_terminus_image import _SUSE_TMUX_STARTUP_PROBE
    authored = 'FROM opensuse/leap:15.5\nRUN echo authored > /task-input\nUSER 1001:1001\n'
    derived = append_terminus_runtime_tools(authored)
    assert derived.startswith(authored)
    assert derived.endswith('USER 1001:1001\nRUN ' + _SUSE_TMUX_STARTUP_PROBE + '\n')
    assert 'install -d -m 1777 -o 0 -g 0 /run/tmux' in derived
    assert 'ENV TMUX_TMPDIR' not in derived
    assert 'zypper' in derived and 'apt-get' not in derived
    assert '--dry-run' in derived and '--no-refresh' in derived
    assert 'addrepo' not in derived and 'dist-upgrade' not in derived


def test_leap_offline_preparation_also_probes_startup_as_the_task_user(tmp_path):
    from loom.nebius_terminus_image import _SUSE_TMUX_STARTUP_PROBE
    environment = bundle(tmp_path)
    environment['user'] = '1001:1001'
    environment['environment'] = {'HOME': '/home/task'}
    (tmp_path / 'environment/Dockerfile').write_text('FROM opensuse/leap:15.5\nUSER 1001:1001\n')
    (tmp_path / 'tests/test.sh').write_text('uvx --with pytest==8.4.1 pytest /tests/test_state.py\n')
    assert prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment['dockerfile']).read_text()
    assert 'install -d -m 1777 -o 0 -g 0 /run/tmux' in derived
    assert derived.endswith('USER 1001:1001\nWORKDIR /app\nRUN ' + _SUSE_TMUX_STARTUP_PROBE + '\n')


@pytest.mark.parametrize('existing', ['missing', 'directory', 'file', 'symlink', 'dangling-symlink'])
def test_leap_runtime_directory_setup_preserves_authored_paths(tmp_path, existing):
    from loom.nebius_terminus_image import _SUSE_TMUX_RUNTIME_SETUP
    runtime = tmp_path / 'runtime'
    target = tmp_path / 'target'
    target.mkdir(mode=0o750)
    if existing == 'directory':
        runtime.mkdir(mode=0o750)
    elif existing == 'file':
        runtime.write_text('authored')
    elif 'symlink' in existing:
        runtime.symlink_to(target if existing == 'symlink' else tmp_path / 'absent')
    # The Linux qualification checks real root ownership; this local installer
    # records that requested contract without requiring macOS root privileges.
    called = tmp_path / 'install-called'
    installer = tmp_path / 'install'
    installer.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$LOOM_TEST_INSTALL_CALLED"\n')
    installer.chmod(0o755)
    env = {**os.environ, 'PATH': f"{tmp_path}:{os.environ['PATH']}",
           'LOOM_TEST_INSTALL_CALLED': str(called)}
    command = _SUSE_TMUX_RUNTIME_SETUP.replace('/run/tmux', str(runtime))
    result = subprocess.run(['/bin/sh', '-c', command], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    assert called.exists() is (existing == 'missing')
    if existing == 'missing':
        assert called.read_text().splitlines() == ['-d', '-m', '1777', '-o', '0', '-g', '0', str(runtime)]
    elif existing == 'directory':
        assert runtime.stat().st_mode & 0o7777 == 0o750
    elif existing == 'file':
        assert runtime.read_text() == 'authored'
    else:
        assert runtime.is_symlink()
    assert target.stat().st_mode & 0o7777 == 0o750


@pytest.mark.parametrize('startup_exit,cleanup_timeout,expected_exit', [(0, False, 0), (1, False, 1), (0, True, 124)])
def test_leap_startup_probe_fails_closed_and_cleans_only_its_own_server(tmp_path, startup_exit, cleanup_timeout, expected_exit):
    from loom.nebius_terminus_image import _SUSE_TMUX_STARTUP_PROBE
    calls = tmp_path / 'calls'
    tmux = tmp_path / 'tmux'
    tmux.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$LOOM_TEST_TMUX_CALLS"\n'
                    'case "$*" in *new-session*) exit "$LOOM_TEST_STARTUP_EXIT" ;; esac\n')
    timeout = tmp_path / 'timeout'
    timeout_calls = tmp_path / 'timeout-calls'
    timeout.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$LOOM_TEST_TIMEOUT_CALLS"\n'
                       'if [ "$LOOM_TEST_CLEANUP_TIMEOUT" = 1 ]; then '
                       'case "$*" in *kill-server*) exit 124 ;; esac; fi\nshift\nexec "$@"\n')
    tmux.chmod(0o755)
    timeout.chmod(0o755)
    env = {**os.environ, 'PATH': f"{tmp_path}:{os.environ['PATH']}",
           'LOOM_TEST_TMUX_CALLS': str(calls), 'LOOM_TEST_STARTUP_EXIT': str(startup_exit),
           'LOOM_TEST_TIMEOUT_CALLS': str(timeout_calls), 'LOOM_TEST_CLEANUP_TIMEOUT': str(int(cleanup_timeout))}
    result = subprocess.run(['/bin/sh', '-c', _SUSE_TMUX_STARTUP_PROBE], capture_output=True, text=True, env=env)
    assert result.returncode == expected_exit, result.stderr
    commands = calls.read_text().splitlines()
    label = commands[0].split()[3]
    assert label.startswith('loom-preparation-')
    bounded_commands = timeout_calls.read_text().splitlines()
    assert bounded_commands[0].startswith('10 tmux ')
    assert all(command.startswith('5 tmux ') for command in bounded_commands[1:])
    assert bounded_commands[-1] == f'5 tmux -L {label} kill-server'
    if not cleanup_timeout:
        assert commands[-1] == f'-L {label} kill-server'
    assert any('has-session' in command for command in commands) is (startup_exit == 0)


@pytest.mark.parametrize('transaction,drift,accepted', [
    ('<install-summary><to-install><solvable type="package" name="tmux"/></to-install></install-summary>', False, True),
    ('<install-summary><to-upgrade/></install-summary>', False, False),
    ('<install-summary><to-downgrade/></install-summary>', False, False),
    ('<install-summary><to-remove/></install-summary>', False, False),
    ('<install-summary><to-install><solvable type="package" name="libc"/></to-install></install-summary>', False, False),
    ('<install-summary><to-install/></install-summary>', True, False),
    ('<install-summary><to-install/></install-summary>', 'key-renewal', True),
    ('<stream/>', False, False),
])
def test_leap_solver_and_inventory_reject_authored_package_changes(tmp_path, transaction, drift, accepted):
    from loom.nebius_terminus_image import _suse_package_install
    installed = tmp_path / 'installed'
    installed.write_text('libc 0:1-1.x86_64\ngpg-pubkey 0:old-key.(none)\n')
    called = tmp_path / 'install-called'
    rpm = tmp_path / 'rpm'
    rpm.write_text('#!/bin/sh\ncase "$*" in\n*-qa*) cat "$LOOM_TEST_INSTALLED" ;;\n*) exit 1 ;;\nesac\n')
    manager = tmp_path / 'zypper'
    manager.write_text('#!/bin/sh\nset -eu\ncase "$*" in\n'
                       '*--dry-run*) printf "%s\\n" "$LOOM_TEST_TRANSACTION" ;;\n'
                       '*install*) touch "$LOOM_TEST_INSTALL_CALLED"; '
                       'if [ "$LOOM_TEST_DRIFT" = 1 ]; then '
                       'printf "libc 0:2-1.x86_64\\n" > "$LOOM_TEST_INSTALLED"; '
                       'elif [ "$LOOM_TEST_DRIFT" = 2 ]; then '
                       'printf "libc 0:1-1.x86_64\\ngpg-pubkey 0:renewed-key.(none)\\n" > "$LOOM_TEST_INSTALLED"; fi ;;\nesac\n')
    timeout = tmp_path / 'timeout'
    timeout.write_text('#!/bin/sh\nshift\nexec "$@"\n')
    for executable in (rpm, manager, timeout):
        executable.chmod(0o755)
    env = {**os.environ, 'PATH': f"{tmp_path}:{os.environ['PATH']}",
           'LOOM_TEST_TRANSACTION': transaction, 'LOOM_TEST_INSTALLED': str(installed),
           'LOOM_TEST_INSTALL_CALLED': str(called), 'LOOM_TEST_DRIFT': '2' if drift == 'key-renewal' else str(int(drift))}
    result = subprocess.run(['/bin/sh', '-c', _suse_package_install()], capture_output=True, text=True, env=env)
    assert (result.returncode == 0) is accepted, result.stderr
    assert called.exists() is (accepted or drift)


@pytest.mark.parametrize('wrapped', ['if false; then\napk add --no-cache wget\nfi',
                                   'bootstrap() {\napk add --no-cache wget\n}',
                                   '(\napk add --no-cache wget\n)',
                                   'for value in a; do\napk add --no-cache wget\ndone'])
def test_rejects_apk_in_unreviewed_control_flow_before_writing(tmp_path, wrapped):
    environment = bundle(tmp_path)
    (tmp_path / 'environment/Dockerfile').write_text('FROM alpine:3.20\n')
    (tmp_path / 'tests/test.sh').write_text(wrapped + '\nuvx --with pytest==8.4.1 pytest /tests/test_state.py\n')
    with pytest.raises(ValueError, match=r'APK.*unconditional preamble'):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()
    assert not (tmp_path / 'environment/Dockerfile.loom-nebius').exists()


def test_task_dockerfile_gains_terminus_runtime_tools() -> None:
    prepared = append_terminus_runtime_tools(
        "FROM python:3.12-slim-bookworm\nUSER agent\nWORKDIR /app\n"
    )
    assert "apt-get install -y --no-install-recommends" in prepared
    install = prepared.split("apt-get install", 1)[1]
    for package in ("asciinema", "bash", "ca-certificates", "curl", "tmux"):
        assert package in install
    assert "command -v bash" in prepared
    assert "command -v tmux" in prepared
    assert "command -v asciinema" in prepared
    assert TASK_IMAGE_TOOLS_REQUIRED in prepared
    assert prepared.rstrip().endswith("USER agent")


def test_task_dockerfile_fails_closed_when_the_base_cannot_be_prepared() -> None:
    with pytest.raises(ValueError, match="image preparation supports"):
        append_terminus_runtime_tools("FROM scratch\n")
