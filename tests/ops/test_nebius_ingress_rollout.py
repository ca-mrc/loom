"""Protected ingress sends an exact data bundle, never remote shell instructions."""
from __future__ import annotations

import importlib
import io
import json
import os
import subprocess
import zipfile
from pathlib import Path

import pytest
import yaml
from tests.ops.test_nebius_ingress_bootstrap import configuration


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_rollout")


def test_bundle_reproducible_contains_both_first_party_wheels_and_unchanged_supervisor(tmp_path):
    uv = tmp_path / "uv"
    uv.write_bytes(b"approved uv")
    requirements = tmp_path / "requirements.txt"
    requirements.write_bytes(b"httpx==0.28.1\n")
    wheels = tmp_path / "wheels"
    wheels.mkdir()
    for name in ("loom-0.0.0-py3-none-any.whl", "loom_bundle_checksum-0.1.0-py3-none-any.whl"):
        (wheels / name).write_bytes(b"wheel fixture")
    config = configuration(tmp_path)
    content = module().build_bundle(config, uv=uv, requirements=requirements, wheels=wheels)
    assert content == module().build_bundle(config, uv=uv, requirements=requirements, wheels=wheels)
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        assert archive.read("scripts/ops/nebius_certificate_gateway.py") == (
            Path(__file__).resolve().parents[2] / "scripts/ops/nebius_certificate_gateway.py").read_bytes()
        assert len([name for name in archive.namelist() if name.endswith(".whl")]) == 2
        assert json.loads(archive.read("installation.json")) == config
        assert all("credential" not in name and "kubeconfig" not in name for name in archive.namelist())
    (wheels / "loom_bundle_checksum-0.1.0-py3-none-any.whl").unlink()
    with pytest.raises(module().RolloutError):
        module().build_bundle(config, uv=uv, requirements=requirements, wheels=wheels)


@pytest.mark.parametrize("action,command", [("image-intent", "loom-nebius-ingress-image-intent-v1"),
                                             ("install", "loom-nebius-ingress-v1"),
                                             ("dns", "loom-nebius-ingress-dns-v1"),
                                             ("rollback", "loom-nebius-ingress-rollback-v1")])
def test_transport_only_uses_fixed_command_with_host_checking(action, command, monkeypatch):
    report = {"status": "complete" if action == "install" else "rolled_back", "candidate": "b" * 40, "namespace": "loom-dev",
              "installation_id": "18718d96-d389-40b3-a79b-11489924d0d4", "private": "secret"}
    if action == "install":
        report.update(controller_uid="afc90687-cd28-4c9d-a2da-eb08a94b1d5b", secret_uid="9648da86-a4ab-470a-b619-dab7ee4f5d88",
                      fingerprint_sha256="c" * 64)
    elif action == "image-intent":
        report.update(status="image_copy_once", image="cr.eu-north1.nebius.cloud/registry/loom-shared-ingress@" + module().DIGEST)
    elif action == "dns":
        report.update(status="dns_published", service_uid="afc90687-cd28-4c9d-a2da-eb08a94b1d5b",
                      fingerprint_sha256="c" * 64, address="8.8.8.8", zone="example.test",
                      child_domain="dev.example.test", management_host="management.example.test",
                      records=[{"name": "*.dev", "record_id": "record-1", "origin": "uncertain"},
                               {"name": "management", "record_id": "record-2", "origin": "external"}])

    def run(args, **kwargs):
        assert args[-1] == command and args[-2] == "codex@192.0.2.1"
        assert "StrictHostKeyChecking=yes" in args and "IdentitiesOnly=yes" in args
        assert "UserKnownHostsFile=/private/known_hosts" in args
        assert kwargs["input"] == b"exact tooling"
        return subprocess.CompletedProcess(args, 0, json.dumps(report).encode(), b"private logs")

    monkeypatch.setattr(subprocess, "run", run)
    result = module().transfer(b"exact tooling", action=action, target="codex@192.0.2.1",
                               key=Path("/private/key"), known_hosts=Path("/private/known_hosts"))
    assert result == {key: value for key, value in report.items() if key != "private"}


@pytest.mark.parametrize("action,target", [("delete", "codex@192.0.2.1"), ("install", "-oProxyCommand=evil"),
                                            ("install", "codex@host; id")])
def test_bad_transport_input_cannot_start_process(action, target, monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: pytest.fail("unsafe process started"))
    with pytest.raises(module().RolloutError):
        module().transfer(b"data", action=action, target=target, key=Path("/private/key"), known_hosts=Path("/private/known_hosts"))


def test_failed_remote_mutation_is_never_retried_or_exposed(monkeypatch):
    calls = []

    def fail(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, b"private material", b"private logs")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(module().RolloutError) as error:
        module().transfer(b"data", action="install", target="codex@host", key=Path("/private/key"), known_hosts=Path("/private/known_hosts"))
    assert len(calls) == 1
    assert "private material" not in str(error.value)


@pytest.mark.parametrize("case", ["qualified", "critical", "scan_failed", "wrong_digest", "suppressed", "empty_results"])
def test_pinned_scan_rejects_unqualified_or_misbound_image(tmp_path, monkeypatch, case):
    digest = "sha256:3429c14149401de2ac82fc72ddc6a92642332b90deb3012301ff211b9d2d0f18"
    image = "docker.io/library/traefik@" + digest
    report = {"SchemaVersion": 2, "ArtifactName": image, "ArtifactType": "container_image",
              "Results": [{"Target": "fixture", "Class": "os-pkgs", "Type": "alpine",
                           "Vulnerabilities": [{"VulnerabilityID": "CVE-fixture", "Severity": "HIGH"}]}]}
    if case == "critical":
        report["Results"][0]["Vulnerabilities"][0]["Severity"] = "CRITICAL"
    elif case == "wrong_digest":
        report["ArtifactName"] = "traefik:latest"
    elif case == "suppressed":
        report["Results"][0]["ExperimentalModifiedFindings"] = [{"Status": "ignored"}]
    elif case == "empty_results":
        report["Results"] = []
    monkeypatch.setattr(module(), "install_trivy", lambda path, **kw: tmp_path / "trivy")

    def run(args, **kwargs):
        assert args[-1] == image and "--show-suppressed" in args
        assert kwargs["env"] == {"PATH": "/bin:/usr/bin", "LANG": "C.UTF-8"}
        Path(args[args.index("--output") + 1]).write_text(json.dumps(report))
        return subprocess.CompletedProcess(args, 1 if case == "scan_failed" else 0, b"", b"private diagnostic")

    monkeypatch.setattr(subprocess, "run", run)
    if case == "qualified":
        result = module().scan_ingress_image(tmp_path)
        assert result["image"] == image and result["status"] == "scan_qualified"
        assert len(result["report_sha256"]) == 64 and len(result["policy_sha256"]) == 64
    else:
        with pytest.raises(module().RolloutError):
            module().scan_ingress_image(tmp_path)


def test_wheels_build_with_locked_hashed_backend_and_stable_timestamps(tmp_path, monkeypatch):
    calls = []
    import tarfile

    def run(args, **kwargs):
        if args[:2] == ["git", "archive"]:
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode="w"):
                pass
            return subprocess.CompletedProcess(args, 0, output.getvalue(), b"")
        assert "--require-hashes" in args and "--no-create-gitignore" in args
        constraints = Path(args[args.index("--build-constraints") + 1]).read_text()
        assert "setuptools==79.0.1" in constraints
        assert "--hash=sha256:e147c0549f27767ba362f9da434eab9c5dc0045d5304feb602a0af001089fc51" in constraints
        assert kwargs["env"]["SOURCE_DATE_EPOCH"] == "315532800"
        calls.append(args[args.index("--package") + 1])
        return subprocess.CompletedProcess(args, 0, b"", b"")

    monkeypatch.setattr(subprocess, "run", run)
    module().build_wheels(tmp_path, uv=Path("/qualified/uv"))
    assert calls == ["loom", "loom-bundle-checksum"]


@pytest.mark.skipif(not os.environ.get("LOOM_TEST_INGRESS_REQUIREMENTS"), reason="actual reproducible wheel builds")
def test_independent_builds_ignore_source_modes_umask_and_ignored_artifacts(tmp_path, monkeypatch):
    import shutil

    original = module().ROOT
    root = tmp_path / "source"
    root.mkdir()
    for path, value in {
        "pyproject.toml": '[build-system]\nrequires=["setuptools>=69"]\nbuild-backend="setuptools.build_meta"\n'
            '[project]\nname="loom"\nversion="0.0.0"\n[tool.setuptools.packages.find]\nwhere=["src"]\n'
            '[tool.uv.workspace]\nmembers=["packages/checksum"]\n',
        "src/loom/__init__.py": 'VALUE = "reviewed"\n',
        "packages/checksum/pyproject.toml": '[build-system]\nrequires=["setuptools>=69"]\nbuild-backend="setuptools.build_meta"\n'
            '[project]\nname="loom-bundle-checksum"\nversion="0.1.0"\n',
        "packages/checksum/loom_bundle_checksum/__init__.py": 'VALUE = "reviewed"\n',
        ".gitignore": 'build/\n*.egg-info/\n',
    }.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(value)
        target.chmod(0o664)
    shutil.copy2(original / "uv.lock", root / "uv.lock")
    for args in (("init", "--initial-branch=dev"), ("add", "."), ("commit", "-m", "reviewed fixture")):
        subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.test", *args],
                       cwd=root, check=True, capture_output=True)
    monkeypatch.setattr(module(), "ROOT", root)
    uv = Path(shutil.which("uv"))
    builds = []
    for index, mask in enumerate((0o002, 0o077)):
        build = tmp_path / str(index)
        build.mkdir()
        for path in (root / "src").rglob("*.py"):
            path.chmod(0o664 if index == 0 else 0o644)
        previous = os.umask(mask)
        try:
            wheels = module().build_wheels(build, uv=uv)
        finally:
            os.umask(previous)
        builds.append({path.name: path.read_bytes() for path in wheels.iterdir()})
    assert builds[0] == builds[1], "bundle wheel bytes depended on checkout permissions or umask"
    # Setuptools can otherwise include stale ignored files from an earlier build.
    stale = root / "build/lib/loom/stale.py"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text('VALUE = "unreviewed"\n')
    build = tmp_path / "stale-check"
    build.mkdir()
    wheels = module().build_wheels(build, uv=uv)
    assert builds[0] == {path.name: path.read_bytes() for path in wheels.iterdir()}


def cli(tmp_path, monkeypatch, *, operation="install", prepare=False):
    import sys

    config = configuration(tmp_path)
    monkeypatch.setenv("NEBIUS_INGRESS_INSTALLATION_JSON", json.dumps(config))
    monkeypatch.setenv("LOOM_DEPLOY_SSH_TARGET", "codex@192.0.2.1")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KEY_FILE", "/private/key")
    monkeypatch.setenv("LOOM_DEPLOY_SSH_KNOWN_HOSTS_FILE", "/private/known_hosts")
    monkeypatch.setenv("NEBIUS_REGISTRY_SERVICE_ACCOUNT_JSON", '{"private":"registry-credential"}')
    args = ["rollout", "--operation", operation, "--requirements", str(tmp_path / "requirements.txt"),
            "--evidence-dir", str(tmp_path / "evidence")]
    if prepare:
        args += ["--prepare-bundle", str(tmp_path / "approved.zip")]
    monkeypatch.setattr(sys, "argv", args)
    monkeypatch.setattr(module(), "verify_source", lambda c: None)
    monkeypatch.setattr(module().shutil, "which", lambda name: "/qualified/uv")
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(a, 0, b"uv 0.11.26 (x86_64-unknown-linux-gnu)\n", b""))
    monkeypatch.setattr(module(), "build_wheels", lambda *a, **kw: tmp_path / "wheels")
    monkeypatch.setattr(module(), "build_bundle", lambda *a, **kw: b"approved tooling")
    return config


def test_operator_prepares_without_registry_scan_or_remote_operations(tmp_path, monkeypatch, capsys):
    cli(tmp_path, monkeypatch, prepare=True)
    for name in ("scan_ingress_image", "refresh_registry_auth", "mirror_ingress_image", "transfer"):
        monkeypatch.setattr(module(), name, lambda *a, **kw: pytest.fail("prepare performed an external operation"))
    assert module().main() == 0
    assert (tmp_path / "approved.zip").read_bytes() == b"approved tooling"
    assert (tmp_path / "approved.zip").stat().st_mode & 0o077 == 0
    assert json.loads(capsys.readouterr().out)["status"] == "prepared"
    assert module().main() == 1, "preparation overwrote approved bundle"


@pytest.mark.parametrize("action,status", [("rollback", "rolled_back"), ("dns", "dns_published")])
def test_readback_operations_skip_scanner_and_registry_but_use_restricted_transport(tmp_path, monkeypatch, action, status):
    cli(tmp_path, monkeypatch, operation=action)
    for name in ("scan_ingress_image", "refresh_registry_auth", "mirror_ingress_image"):
        monkeypatch.setattr(module(), name, lambda *a, **kw: pytest.fail("rollback depended on image publication"))
    calls = []

    def transfer(content, **kwargs):
        calls.append(kwargs["action"])
        assert content == b"approved tooling"
        return {"status": status}

    monkeypatch.setattr(module(), "transfer", transfer)
    assert module().main() == 0 and calls == [action]


def test_scan_failure_prevents_registry_credentials_use_and_remote_mutation(tmp_path, monkeypatch, capsys):
    cli(tmp_path, monkeypatch)

    def fail(*args, **kwargs):
        raise module().RolloutError("private scan failure")

    monkeypatch.setattr(module(), "scan_ingress_image", fail)
    for name in ("refresh_registry_auth", "mirror_ingress_image", "transfer"):
        monkeypatch.setattr(module(), name, lambda *a, **kw: pytest.fail("unqualified image reached mutation"))
    assert module().main() == 1
    report = json.loads(capsys.readouterr().out)
    assert report == {"status": "blocked", "phase": "image_scan"}


def test_install_scans_then_publishes_before_gateway_cutover(tmp_path, monkeypatch):
    config = cli(tmp_path, monkeypatch)
    phases = []
    monkeypatch.setattr(module(), "scan_ingress_image", lambda *a: phases.append("scan") or {"status": "scan_qualified"})

    def refresh(credentials, prefix, auth):
        assert credentials.read_text() == '{"private":"registry-credential"}'
        assert credentials.stat().st_mode & 0o077 == 0
        assert "NEBIUS_REGISTRY_SERVICE_ACCOUNT_JSON" not in os.environ
        phases.append("auth")

    monkeypatch.setattr(module(), "refresh_registry_auth", refresh)
    monkeypatch.setattr(module(), "mirror_ingress_image", lambda **kw: phases.append("mirror") or {"status": "mirrored"})
    def transfer(*args, action, **kwargs):
        phases.append(action)
        return ({"status": "image_copy_once", "image": config["image"], "installation_id": config["binding"]["installation_id"],
                 "candidate": config["candidate"], "namespace": config["binding"]["namespace"]}
                if action == "image-intent" else {"status": "complete"})
    monkeypatch.setattr(module(), "transfer", transfer)
    assert module().main() == 0
    assert phases == ["scan", "auth", "image-intent", "mirror", "install"]


@pytest.mark.parametrize("copy_completed", [True, False])
def test_new_workflow_invocation_reconciles_uncertain_copy_without_repeating_it(tmp_path, monkeypatch, copy_completed):
    from contextlib import redirect_stdout

    from scripts.ops import nebius_ingress_entry as entry
    from scripts.ops import nebius_ingress_image as images
    from tests.ops.test_nebius_ingress_entry import installed

    gateway = tmp_path / "gateway"
    gateway.mkdir()
    config_path, config = installed(gateway)
    (gateway / "nebius-ingress").mkdir(mode=0o700)
    calls, destination = [], []
    def inspect(image, auth):
        if "docker.io/" not in image and not destination:
            raise images.ImageError("destination unavailable")
    def copy(args, **kwargs):
        assert args[0] == "copy"
        calls.append("copy")
        if copy_completed:
            destination.append(True)
        raise images.ImageError("copy result unknown")
    def refresh(credentials, prefix, auth):
        auth.write_text('{"auths":{"cr.eu-north1.nebius.cloud":{"auth":"aWFtOnNlY3JldA=="}}}')
        auth.chmod(0o600)
    def transfer(content, *, action, **kwargs):
        if action == "install":
            return {"status": "complete"}
        output = io.StringIO()
        with redirect_stdout(output):
            assert entry.main(str(config_path), action) == 0
        return json.loads(output.getvalue())
    monkeypatch.setattr(images, "_inspect", inspect)
    monkeypatch.setattr(images, "_run", copy)
    monkeypatch.setattr(module(), "refresh_registry_auth", refresh)
    monkeypatch.setattr(module(), "scan_ingress_image", lambda *a: {"status": "scan_qualified"})
    monkeypatch.setattr(module(), "transfer", transfer)
    for attempt in ("first-run", "new-run"):
        runner = tmp_path / attempt
        runner.mkdir()
        cli(runner, monkeypatch)
        monkeypatch.setenv("NEBIUS_INGRESS_INSTALLATION_JSON", json.dumps(config))
        assert module().main() == (0 if copy_completed else 1)
    assert calls == ["copy"], "a new Actions runner repeated an uncertain registry mutation"


def test_source_must_match_clean_integrated_commit(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    root.mkdir()

    def git(*args):
        return subprocess.run(["git", "-c", "user.name=Fixture", "-c", "user.email=fixture@example.test", *args],
                              cwd=root, check=True, capture_output=True).stdout.strip().decode()

    git("init", "--initial-branch=dev")
    (root / "source").write_text("reviewed")
    git("add", "source")
    git("commit", "-m", "fixture")
    sha = git("rev-parse", "HEAD")
    git("update-ref", "refs/remotes/origin/dev", sha)
    config = configuration(tmp_path)
    config["source_sha"] = sha
    monkeypatch.setattr(module(), "ROOT", root)
    module().verify_source(config)
    (root / "source").write_text("uncommitted")
    with pytest.raises(module().RolloutError):
        module().verify_source(config)
    git("add", "source")
    git("commit", "-m", "unintegrated")
    with pytest.raises(module().RolloutError):
        module().verify_source(config)
    config["source_sha"] = git("rev-parse", "HEAD")
    with pytest.raises(module().RolloutError):
        module().verify_source(config)


@pytest.mark.skipif(not os.environ.get("LOOM_TEST_INGRESS_REQUIREMENTS"), reason="actual isolated ingress wheel qualification")
def test_actual_bundle_bootstraps_with_both_wheels_and_parent_death_cleanup(tmp_path):
    import shutil

    from scripts.ops.nebius_ingress_bootstrap import command, prepare_release, run_private
    from tests.ops.test_nebius_ingress_bootstrap import qualify_installed_watchdog
    from tests.ops.test_nebius_ingress_entry import installed

    _, config = installed(tmp_path)
    uv = Path(shutil.which("uv"))
    wheels = module().build_wheels(tmp_path, uv=uv)
    content = module().build_bundle(config, uv=uv, requirements=Path(os.environ["LOOM_TEST_INGRESS_REQUIREMENTS"]), wheels=wheels)
    release, _ = prepare_release(content)
    assert json.loads(run_private(command(release, "qualify"), timeout=60)) == {"status": "tooling_qualified"}
    qualify_installed_watchdog(release, release / "venv/bin/python", tmp_path)
    assert not (tmp_path / "nebius-ingress/state").exists()
    # The actual isolated entry can reserve/read back intent without a
    # kubeconfig, certificate config, registry credentials or checkout imports.
    for status in ("image_copy_once", "image_readback_only"):
        receipt = json.loads(run_private(command(release, "image-intent"), timeout=60))
        assert receipt["status"] == status and receipt["image"] == config["image"]


def test_workflow_exposes_only_fixed_protected_ingress_operations_and_dedicated_key():
    workflow = yaml.load((Path(__file__).resolve().parents[2] / ".github/workflows/nebius-rollout.yml").read_text(), Loader=yaml.BaseLoader)
    assert workflow["on"]["workflow_dispatch"]["inputs"]["operation"]["options"] == [
        "rollout", "recover", "inspect", "certificate", "ingress", "ingress-rollback", "ingress-dns",
        "management-preflight", "management-install",
        "management-diagnostic-preflight", "management-diagnostic-install",
        "management-recovery-preflight", "management-recovery-install",
        "management-refresh-preflight", "management-refresh-install",
        "management-pool-preflight", "management-pool-install", "management-pool-rollback",
        "management-pool-repair-preflight", "management-pool-repair-install", "management-pool-repair-rollback",
        "development-preflight", "development-install",
        "development-management-preflight", "development-management-install",
    ]
    job = workflow["jobs"]["ingress"]
    assert job["environment"] == {"name": "nebius-integration", "deployment": "false"}
    assert job["permissions"] == {"contents": "read"}
    assert "concurrency" not in job, "ingress must use the same exclusion as application rollout"
    assert workflow["concurrency"]["cancel-in-progress"] == "false"
    for guard in ("github.repository == 'qianyi-sun/loom'", "github.ref == 'refs/heads/dev'",
                  "github.event_name == 'workflow_dispatch'", "inputs.operation == 'ingress'", "inputs.operation == 'ingress-rollback'",
                  "inputs.operation == 'ingress-dns'"):
        assert guard in job["if"]
    transport = next(step for step in job["steps"] if step.get("name") == "Run the exact ingress operation")
    assert transport["env"]["DEPLOY_SSH_KEY"] == "${{ secrets.NEBIUS_INGRESS_SSH_KEY }}"
    assert transport["env"]["INGRESS_OPERATION"] == "${{ inputs.operation }}"
    assert transport["env"]["NEBIUS_REGISTRY_SERVICE_ACCOUNT_JSON"] == (
        "${{ inputs.operation == 'ingress' && secrets.NEBIUS_REGISTRY_SERVICE_ACCOUNT_JSON || '' }}"
    )
    assert all("NEBIUS_DEPLOY_SSH_KEY" not in value and "NEBIUS_CERTIFICATE_SSH_KEY" not in value
               for value in transport["env"].values())
    artifact = next(step for step in job["steps"] if step.get("uses", "").startswith("actions/upload-artifact@"))
    assert "*.json" in artifact["with"]["path"] and "*.zip" not in artifact["with"]["path"]
