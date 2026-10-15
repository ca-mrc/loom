#!/usr/bin/env python3
"""Add an isolated-guest capability to the existing single-job KVM lifecycle."""

from __future__ import annotations

import argparse
import fcntl
import json
import shutil
import sys
from pathlib import Path
from typing import Any

CAPABILITY = "loom-ci-isolated-v1"
MEMORY_CAPABILITY = "loom-ci-guest12g-v1"
MARKER = "/etc/loom-ci-ephemeral-runner"
RUNTIME = Path("/usr/local/lib/loom-ci-runner-expansion")
# Reuse the service image's existing pinned kubectl build. Hosted images already
# provide this CLI; the minimal OLDLAB guest must provide the same executable
# boundary before accepting operator-test jobs.
KUBECTL_IMAGE = "registry.k8s.io/kubectl:v1.36.2@sha256:b0d792e0d8dfb9bb1b922b78b23137e2a34bb6f9667640353a9d2aadd1fd7761"


def guest_tools_script() -> str:
    return (
        "if ! PATH=/usr/local/bin:/usr/bin:/bin command -v kubectl >/dev/null; then\n"
        "  kubectl_container=$(timeout 180 docker create " + KUBECTL_IMAGE + ")\n"
        "  if ! docker cp \"$kubectl_container:/bin/kubectl\" /usr/local/bin/kubectl; then\n"
        "    docker rm \"$kubectl_container\" >/dev/null\n"
        "    exit 1\n"
        "  fi\n"
        "  docker rm \"$kubectl_container\" >/dev/null\n"
        "  chmod 0755 /usr/local/bin/kubectl\n"
        "fi\n"
    )


def guest_script(original: str) -> str:
    boundary = "systemctl is-active --quiet docker\n"
    if original.count(boundary) != 1:
        raise ValueError("installed guest lifecycle has an unsupported Docker-ready boundary")
    marker = (
        'printf \'%s\\n\' \'{"schema_version":1,"provider":"oldlab"}\' > ' + MARKER + "\n"
        "chmod 0444 " + MARKER + "\n"
    )
    return original.replace(boundary, boundary + guest_tools_script() + marker)


def install(base: Any) -> None:
    if getattr(base, "_loom_unified_installed", False):
        return
    original_script = base._slot_run_script
    original_jit = base.GitHubAPI.generate_jit_config
    original_create = base._create_slot

    def slot_script() -> str:
        return guest_script(original_script())

    def generate_jit(self: Any, *, name: str, labels: Any) -> Any:
        return original_jit(
            self,
            name=name,
            labels=tuple(dict.fromkeys((*labels, CAPABILITY, MEMORY_CAPABILITY, "loom-ci-exec"))),
        )

    def create_slot(profile: Any, **kwargs: Any) -> Any:
        if profile.memory_mib_per_slot < 12288:
            raise ValueError("common-queue guests need 12 GiB for the existing 8 GiB sandbox tests")
        return original_create(profile, **kwargs)

    base._slot_run_script = slot_script
    base.GitHubAPI.generate_jit_config = generate_jit
    base._create_slot = create_slot
    base._loom_unified_installed = True


def install_runtime(runtime: Path) -> None:
    if runtime != RUNTIME:
        raise ValueError("unified plugin installs only into the existing OLDLAB pool runtime")
    adapter = runtime / "expand_host.py"
    source = adapter.read_text()
    boundary = 'if __name__ == "__main__":'
    addition = "from ci_runner_unified_host import install as install_unified_guest\ninstall_unified_guest(base)\n\n"
    if boundary not in source:
        raise ValueError("installed pool adapter has an unsupported entrypoint")
    target = runtime / "ci_runner_unified_host.py"
    if Path(__file__).resolve() != target:
        shutil.copyfile(__file__, target)
        target.chmod(0o600)
    if addition not in source:
        backup = adapter.with_suffix(".py.before-unified")
        if backup.exists():
            raise ValueError(
                "an earlier adapter backup exists; inspect before another installation"
            )
        shutil.copy2(adapter, backup)
        temporary = adapter.with_suffix(".py.new")
        temporary.write_text(source.replace(boundary, addition + boundary, 1))
        temporary.chmod(0o600)
        temporary.replace(adapter)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--install-runtime", type=Path, required=True)
    parser.add_argument("--retire-legacy-idle", action="store_true")
    args = parser.parse_args()
    install_runtime(args.install_runtime)
    if args.retire_legacy_idle:
        sys.path.insert(0, str(RUNTIME))
        import adaptive_pool

        base = adaptive_pool.b
        profile = base.load_profile(Path("/etc/loom-ci-runner-pool/profile.toml"))
        base._verify_host(profile)
        api = adaptive_pool.API(
            repository=profile.repository,
            token=base.read_token(Path("/etc/loom-ci-runner-pool/github-token")),
        )
        runner = base.SubprocessCommandRunner()
        retired, retained = [], []
        with (profile.state_root / "adaptive.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            inventory = base._runner_inventory(api, profile)
            for slot, state in base._existing_states(profile).items():
                record = inventory.get(state.runner_id)
                if record and MEMORY_CAPABILITY in {item["name"] for item in record.get("labels", [])}:
                    continue
                if adaptive_pool.retire(profile, state, api, runner):
                    retired.append(slot)
                else:
                    retained.append(slot)
        print(json.dumps({"legacy_idle_retired": retired, "busy_or_racing_retained": retained}))


if __name__ == "__main__":
    main()
