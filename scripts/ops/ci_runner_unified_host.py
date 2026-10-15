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
MARKER = "/etc/loom-ci-ephemeral-runner"
RUNTIME = Path("/usr/local/lib/loom-ci-runner-expansion")


def guest_script(original: str) -> str:
    boundary = "systemctl is-active --quiet docker\n"
    if original.count(boundary) != 1:
        raise ValueError("installed guest lifecycle has an unsupported Docker-ready boundary")
    marker = (
        'printf \'%s\\n\' \'{"schema_version":1,"provider":"oldlab"}\' > ' + MARKER + "\n"
        "chmod 0444 " + MARKER + "\n"
    )
    return original.replace(boundary, boundary + marker)


def install(base: Any) -> None:
    if getattr(base, "_loom_unified_installed", False):
        return
    original_script = base._slot_run_script
    original_jit = base.GitHubAPI.generate_jit_config

    def slot_script() -> str:
        return guest_script(original_script())

    def generate_jit(self: Any, *, name: str, labels: Any) -> Any:
        return original_jit(
            self, name=name, labels=tuple(dict.fromkeys((*labels, CAPABILITY, "loom-ci-exec")))
        )

    base._slot_run_script = slot_script
    base.GitHubAPI.generate_jit_config = generate_jit
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
                if record and CAPABILITY in {item["name"] for item in record.get("labels", [])}:
                    continue
                if adaptive_pool.retire(profile, state, api, runner):
                    retired.append(slot)
                else:
                    retained.append(slot)
        print(json.dumps({"legacy_idle_retired": retired, "busy_or_racing_retained": retained}))


if __name__ == "__main__":
    main()
