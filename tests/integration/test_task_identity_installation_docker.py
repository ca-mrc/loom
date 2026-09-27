"""Real native sandbox installation and fresh-verifier handoff; no network/model."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
from pathlib import Path, PurePosixPath

import httpx
import pytest

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.sandbox_identity import ROOT_INSTALL_CAPABILITIES
from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

pytestmark = [pytest.mark.docker, pytest.mark.timeout(180)]


@pytest.mark.parametrize("custom_shell", [False, True])
@pytest.mark.parametrize("bootstrap", [
    "uvx --with pytest==8.4.1 pytest /tests/test.py\n",
    "uvx -p 3.13 --with pytest==8.4.1 pytest /tests/test.py\n",
    "pip install pytest==8.4.1\npytest /tests/test.py\n",
], ids=["uvx-unpinned", "uvx-pinned", "pip"])
def test_image_preparation_preserves_authored_package_caches(tmp_path, custom_shell, bootstrap):
    """Execute generated preparation; only external package downloads are fixtures."""
    import docker

    from loom.dockerfile_instructions import dockerfile_instructions
    from loom.nebius_terminus_image import prepare_nebius_terminus_image

    (tmp_path / "environment").mkdir()
    (tmp_path / "tests").mkdir()
    source = "FROM python:3.11-slim\n"
    if custom_shell:
        source += 'SHELL ["/bin/bash", "-e", "-c"]\n'
    (tmp_path / "environment/Dockerfile").write_text(source)
    (tmp_path / "tests/test.sh").write_text(bootstrap)
    environment = {"dockerfile": "environment/Dockerfile", "docker_build_context": "environment",
                   "workdir": "/app", "user": "root", "environment": {"HOME": "/root"}}
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    runs = [item.arguments for item in dockerfile_instructions(derived) if item.keyword == "RUN"]
    assert len(runs) == 1
    argv = json.loads(runs[0]) if custom_shell else ["/bin/sh", "-c", runs[0]]
    (tmp_path / "prepare.json").write_text(json.dumps(argv))
    # Keep filesystem and shell effects real. These two network installers are
    # replaced at their executable boundary so the regression runs offline.
    (tmp_path / "apt-get").write_text("#!/bin/sh\nexit 0\n")
    (tmp_path / "loom-nebius-uv").write_text("""#!/bin/sh
set -eu
printf '%s\\n' "${UV_NO_CACHE-unset}" >> /tmp/verifier-cache-policy
case "$1" in
  python) test "$2" = install ;;
  venv) mkdir -p /opt/verifier/bin ;;
  tool) test "$2" = install; mkdir -p "$UV_TOOL_DIR/pytest/bin" ;;
  pip) test "$2" = install || test "$2" = freeze ;;
  *) exit 2 ;;
esac
""")
    for name in ("apt-get", "loom-nebius-uv"):
        (tmp_path / name).chmod(0o755)
    script = """import json, os, pathlib, subprocess
cache = pathlib.Path('/root/.cache')
for name in ('pypoetry/artifacts/wheel', 'pip/wheel', 'uv/task-owned'):
    path = cache / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'original offline dependency')
    path.chmod(0o640)
subprocess.run(json.loads(pathlib.Path('/fixture/prepare.json').read_text()), check=True,
               env={**os.environ, 'PATH': '/fixture:' + os.environ['PATH']})
for path in (cache/'pypoetry/artifacts/wheel', cache/'pip/wheel', cache/'uv/task-owned'):
    assert path.is_file(), f'preparation deleted authored cache: {path}'
    assert path.read_bytes() == b'original offline dependency'
    assert path.stat().st_mode & 0o777 == 0o640
cache_policy = pathlib.Path('/tmp/verifier-cache-policy').read_text().splitlines()
assert cache_policy and set(cache_policy) == {'1'}, cache_policy
assert pathlib.Path('/opt/verifier/bin').is_dir()
assert 'UV_NO_CACHE' not in os.environ
print('authored caches preserved; verifier downloads uncached')
"""
    client = docker.from_env()
    try:
        output = client.containers.run(
            "python:3.11-slim", ["-c", script], entrypoint="python", remove=True,
            network_mode="none", cap_drop=["ALL"], cap_add=list(ROOT_INSTALL_CAPABILITIES),
            security_opt=["no-new-privileges"], mem_limit="256m", nano_cpus=250_000_000,
            environment={"HOME": "/root"},
            volumes={str(tmp_path): {"bind": "/fixture", "mode": "ro"}},
        )
        assert output.strip() == b"authored caches preserved; verifier downloads uncached"
    finally:
        client.close()


@pytest.fixture(scope="module")
def native_binary(tmp_path_factory):
    directory = tmp_path_factory.mktemp("identity-runtime")
    repository = Path(__file__).resolve().parents[2]
    subprocess.run([
        "docker", "run", "--rm", "--network", "none", "--user", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{repository}:/src:ro", "-v", f"{directory}:/output", "-w", "/src",
        "-e", "GOCACHE=/tmp/go-cache", "-e", "CGO_ENABLED=0", "golang:1.26-alpine3.23",
        "go", "build", "-o", "/output/loom-sandbox-runtime", "./cmd/loom-sandbox-runtime",
    ], check=True, timeout=120, capture_output=True)
    return directory / "loom-sandbox-runtime"


@pytest.fixture
async def sandboxes(native_binary, tmp_path, request):
    import docker

    client = docker.from_env()
    containers = []
    drivers = []
    try:
        for name, uid, gid, home in (("agent", 0, 0, "/root"), ("verifier", 0, 0, "/root"),
                                     ("default", 65532, 65532, "/home/agent")):
            directory = tmp_path / name
            directory.mkdir()
            # Model the setgid socket emptyDir with the controller's fsGroup.
            directory.chmod(0o2777)
            container = client.containers.run(
                getattr(request, "param", "python:3.11-slim"), ["--socket", "/socket/sandbox.sock"], detach=True,
                entrypoint="/loom/bin/loom-sandbox-runtime", user=f"{uid}:{gid}",
                network_mode="none", cap_drop=["ALL"],
                cap_add=list(ROOT_INSTALL_CAPABILITIES) if uid == 0 else [],
                security_opt=["no-new-privileges"], environment={"HOME": home},
                volumes={str(native_binary): {"bind": "/loom/bin/loom-sandbox-runtime", "mode": "ro"},
                         str(directory): {"bind": "/socket", "mode": "rw"}},
            )
            containers.append(container)
            driver = ServiceSandboxDriver(
                directory / "sandbox.sock",
                capabilities=Capabilities(os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
                                          dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({"limit"})),
                network_policy=NoNetwork(),
            )
            drivers.append(driver)
            for attempt in range(100):
                try:
                    await driver.start()
                    break
                except (OSError, RuntimeError, httpx.ConnectError):
                    if attempt == 99:
                        raise
                    await asyncio.sleep(0.05)
        yield drivers
    finally:
        for driver in drivers:
            await driver.stop()
        for container in containers:
            container.remove(force=True)
        client.close()


async def test_root_installs_real_deb_and_fresh_verifier_observes_owned_system_state(sandboxes, tmp_path):
    agent, verifier, default = sandboxes
    for driver in (agent, verifier):
        result = await driver.exec("mkdir -p /app && test ! -e /usr/local/share/loom-identity-proof && ! dpkg-query -W loom-identity-proof")
        assert result.return_code == 0, result.stderr
    default_result = await default.exec("id -u; id -g; printf '%s\\n' \"$HOME\"; touch /usr/local/forbidden")
    assert default_result.return_code != 0
    assert default_result.stdout.splitlines() == [b"65532", b"65532", b"/home/agent"]
    setup = r"""set -eu
test "$(id -u)" = 0
test "$HOME" = /root
mkdir -p /tmp/package/DEBIAN /tmp/package/usr/local/share/loom-identity-proof
printf '%s\n' installed-by-agent > /tmp/package/usr/local/share/loom-identity-proof/installed
cat > /tmp/package/DEBIAN/control <<'CONTROL'
Package: loom-identity-proof
Version: 1.0
Architecture: all
Maintainer: Loom test fixture
Description: Local task installation proof
CONTROL
cat > /tmp/package/DEBIAN/postinst <<'SCRIPT'
#!/bin/sh
set -eu
chown -R 1001:1002 /usr/local/share/loom-identity-proof
setpriv --reuid 1001 --regid 1002 --clear-groups /bin/sh -c '
id -u > /usr/local/share/loom-identity-proof/dropped-uid
sleep 3600 >/dev/null 2>&1 &
echo $! > /usr/local/share/loom-identity-proof/child-pid
'
SCRIPT
chmod 0755 /tmp/package/DEBIAN/postinst
dpkg-deb --root-owner-group --build /tmp/package /tmp/loom-identity-proof.deb
dpkg --install /tmp/loom-identity-proof.deb
"""
    installed = await agent.exec(setup)
    assert installed.return_code == 0, installed.stderr
    await agent.stop_processes()
    checked = await agent.exec("test ! -e /proc/$(cat /usr/local/share/loom-identity-proof/child-pid)")
    assert checked.return_code == 0, checked.stderr
    paths = (PurePosixPath("/usr/local/share/loom-identity-proof"), PurePosixPath("/var/lib/dpkg"))
    await export_mutable_paths(agent, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    await import_mutable_paths(verifier, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    checked = await verifier.exec(
        "set -eu; test \"$(dpkg-query -W -f='${Status}' loom-identity-proof)\" = 'install ok installed'; "
        "test \"$(cat /usr/local/share/loom-identity-proof/dropped-uid)\" = 1001; "
        "test \"$(stat -c '%u:%g' /usr/local/share/loom-identity-proof/installed)\" = 1001:1002; "
        "test \"$(cat /usr/local/share/loom-identity-proof/installed)\" = installed-by-agent",
    )
    assert checked.return_code == 0, checked.stderr
    unrelated = await default.exec("test ! -e /usr/local/share/loom-identity-proof && ! dpkg-query -W loom-identity-proof")
    assert unrelated.return_code == 0, unrelated.stderr
    await verifier.stop_processes()


async def test_handoff_preserves_numeric_ownership_when_account_names_differ(sandboxes, tmp_path):
    agent, verifier, _ = sandboxes
    for driver, uid in ((agent, 1201), (verifier, 1301)):
        result = await driver.exec(
            f"groupadd -g {uid} ownercheck && useradd -u {uid} -g {uid} ownercheck && "
            "mkdir -p /app /data"
        )
        assert result.return_code == 0, result.stderr
    result = await agent.exec("echo owned > /data/marker && chown 1201:1201 /data/marker")
    assert result.return_code == 0, result.stderr
    paths = (PurePosixPath("/data"),)
    await export_mutable_paths(agent, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    await import_mutable_paths(verifier, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    result = await verifier.exec("stat -c '%u:%g' /data/marker")
    assert result.return_code == 0, result.stderr
    assert result.stdout.strip() == b"1201:1201"


async def test_private_grading_inputs_do_not_change_the_tasks_file_manifest(sandboxes, tmp_path, monkeypatch):
    from loom.nebius_terminus_ingest import offline_verifier_run_sh_bytes
    from loom.service_execution_sandbox_task import run_verifier
    from loom.trial.workspace_snapshot import _export_workspace_archive
    from tests.unit.test_service_execution_terminus_plan import _inputs

    agent, verifier, _ = sandboxes
    task, trial, _ = _inputs()
    task.verifier.args["script_path"] = "verifier/run.sh"
    controller = tmp_path / "controller"
    (controller / ".loom").mkdir(parents=True)
    (controller / "tests").mkdir()
    (controller / "tests/private-marker").write_text("private grading input")
    (controller / "verifier").mkdir()
    (controller / "verifier/run.sh").write_bytes(offline_verifier_run_sh_bytes())
    (controller / "verifier/harbor-offline.sh").write_text("""set -eu
test "$(cat /tests/private-marker)" = 'private grading input'
test "$PWD" = /app
python - <<'PY'
from pathlib import Path
root = Path('/app')
expected = set((root / 'manifest').read_text().splitlines())
actual = {str(p) for p in root.rglob('*') if p.is_file()}
assert actual == expected, (actual, expected)
assert (root / 'answer').read_text() == 'answer'
PY
echo 1 > /logs/verifier/reward.txt
echo '{}' > /logs/verifier/ctrf.json
""")
    created = await agent.exec(
        "mkdir /app; printf answer > /app/answer; "
        "printf '/app/answer\\n/app/manifest\\n' > /app/manifest"
    )
    assert created.return_code == 0, created.stderr
    await agent.stop_processes()
    await _export_workspace_archive(agent, PurePosixPath("/app"), controller / ".loom/workspace.tar")
    # The fixture checks readiness; the production entrypoint owns its connection.
    await verifier.stop()
    connection = ServiceSandboxDriver(tmp_path / "verifier/sandbox.sock",
                                      capabilities=verifier.capabilities, network_policy=NoNetwork())
    monkeypatch.setattr("loom.service_execution_sandbox_task.sandbox_driver", lambda *_: connection)
    await run_verifier(controller, task, trial)
    result = json.loads((controller / ".loom/verifier/output.json").read_bytes())
    assert result["rewards"] == {"resolved": 1, "passed": 1}


async def test_native_virtualenv_handoff_preserves_external_interpreter_and_aliases(sandboxes, tmp_path):
    agent, verifier, _ = sandboxes
    for driver in (agent, verifier):
        assert (await driver.exec("mkdir -p /app /root/.cache/task")).return_code == 0
    made = await agent.exec(
        "/usr/local/bin/python3.11 -m venv --without-pip /root/.cache/task/venv && "
        "echo cache-state > /root/.cache/task/marker && "
        "find /root/.cache/task/venv/bin -type l -exec readlink {} \\;"
    )
    assert made.return_code == 0, made.stderr
    assert b"/usr/local/bin/python3.11" in made.stdout
    await agent.stop_processes()
    paths = (PurePosixPath("/root/.cache/task"),)
    options = {"workdir": PurePosixPath("/app"),
               "reference_files": (PurePosixPath("/usr/local/bin/python3.11"),)}
    await export_mutable_paths(agent, paths, tmp_path / "snapshot", **options)
    await import_mutable_paths(verifier, paths, tmp_path / "snapshot", **options)
    links = await verifier.exec("find /root/.cache/task/venv/bin -type l -exec readlink {} \\;")
    assert links.return_code == 0 and links.stdout == made.stdout
    executed = await verifier.exec(
        "/root/.cache/task/venv/bin/python -c 'import sys; from pathlib import Path; "
        "assert sys.prefix == \"/root/.cache/task/venv\"; "
        "assert Path(\"/root/.cache/task/marker\").read_text() == \"cache-state\\n\"; print(\"preserved\")'"
    )
    assert executed.return_code == 0 and executed.stdout.strip() == b"preserved"


@pytest.mark.parametrize("mutation_stage", ["before_export", "during_export"])
async def test_mutated_reference_cannot_forge_its_fingerprint_with_task_utilities(sandboxes, tmp_path, mutation_stage):
    from loom.trial.workspace_snapshot import WorkspaceSnapshotError

    agent, verifier, _ = sandboxes
    for driver in (agent, verifier):
        assert (await driver.exec("mkdir -p /app /cache; echo untouched > /cache/baseline")).return_code == 0
    forged = await agent.exec(r"""set -eu
original=$(sha256sum < /usr/local/bin/python3.11)
printf '#!/bin/sh\nprintf "%%s\\n" "%s"\n' "$original" > /usr/local/bin/sha256sum
chmod 0755 /usr/local/bin/sha256sum
ln -s /usr/local/bin/python3.11 /cache/python
""")
    assert forged.return_code == 0, forged.stderr
    mutation = "printf X | dd of=/usr/local/bin/python3.11 bs=1 seek=100 conv=notrunc status=none"
    if mutation_stage == "before_export":
        assert (await agent.exec(mutation)).return_code == 0
    else:
        wrapper = tmp_path / "tar"
        wrapper.write_text('#!/bin/sh\n' + mutation + '\nexec /bin/tar "$@"\n')
        wrapper.chmod(0o755)
        await agent.upload(wrapper, PurePosixPath("/usr/local/bin/tar"))
    await agent.stop_processes()
    paths = (PurePosixPath("/cache"),)
    options = {"workdir": PurePosixPath("/app"),
               "reference_files": (PurePosixPath("/usr/local/bin/python3.11"),)}
    await export_mutable_paths(agent, paths, tmp_path / "snapshot", **options)
    with pytest.raises(WorkspaceSnapshotError, match="reference"):
        await import_mutable_paths(verifier, paths, tmp_path / "snapshot", **options)
    assert (await verifier.exec("cat /cache/baseline")).stdout.strip() == b"untouched"


@pytest.mark.parametrize("drift", [None, "alias", "executable"])
async def test_workspace_venv_preserves_real_image_aliases_and_rejects_drift(sandboxes, tmp_path, drift):
    from loom.trial.workspace import WorkspaceStagingPolicy
    from loom.trial.workspace_references import (
        export_workspace_references,
        import_workspace_with_references,
    )
    from loom.trial.workspace_snapshot import (
        WorkspaceSnapshotError,
        _export_workspace_archive,
        _strip_private_entries,
    )

    agent, verifier, independent = sandboxes
    root = PurePosixPath("/app")
    references = (PurePosixPath("/usr/local/bin/python3"), PurePosixPath("/usr/local/bin/python3.11"))
    aliases = {"/usr/local/bin/python3": "python3.11"}
    policy = WorkspaceStagingPolicy(("tests/**",), ("tests/**",), ())
    created = await agent.exec(
        "mkdir -p /app/tests && echo private > /app/tests/secret && "
        "python3 -m venv /app/venv && echo agent-output > /app/answer"
    )
    assert created.return_code == 0, created.stderr
    assert (await agent.exec("readlink /app/venv/bin/python3")).stdout.strip() == b"/usr/local/bin/python3"
    baseline = await verifier.exec("mkdir -p /app/tests; echo trusted > /app/tests/secret; echo baseline > /app/baseline")
    assert baseline.return_code == 0, baseline.stderr
    await agent.stop_processes()
    archive = tmp_path / "workspace.tar"
    await _export_workspace_archive(agent, root, archive)
    _strip_private_entries(archive, policy)
    await export_workspace_references(agent, archive, root=root, policy=policy,
                                     reference_files=references, reference_symlinks=aliases)
    if drift:
        command = ("ln -sfn /usr/local/bin/python3.11 /usr/local/bin/python3" if drift == "alias"
                   else "chmod 0700 /usr/local/bin/python3.11")
        assert (await verifier.exec(command)).return_code == 0
        with pytest.raises(WorkspaceSnapshotError):
            await import_workspace_with_references(verifier, archive, root, policy=policy,
                                                    reference_files=references, reference_symlinks=aliases)
        result = await verifier.exec("test ! -e /app/venv && test ! -e /app/answer && cat /app/baseline /app/tests/secret")
        assert result.return_code == 0 and result.stdout.splitlines() == [b"baseline", b"trusted"]
    else:
        await import_workspace_with_references(verifier, archive, root, policy=policy,
                                                reference_files=references, reference_symlinks=aliases)
        result = await verifier.exec(
            "set -eu; test ! -e /app/baseline; test \"$(cat /app/tests/secret)\" = trusted; "
            "test \"$(cat /app/answer)\" = agent-output; "
            "test \"$(readlink /app/venv/bin/python3)\" = /usr/local/bin/python3; "
            "test \"$(readlink /usr/local/bin/python3)\" = python3.11; "
            "/app/venv/bin/python -c 'import sys; assert sys.prefix == \"/app/venv\"; print(\"venv-ok\")'"
        )
        assert result.return_code == 0 and result.stdout.strip() == b"venv-ok", result.stderr
    untouched = await independent.exec("test ! -e /app/venv && test ! -e /app/answer")
    assert untouched.return_code == 0
