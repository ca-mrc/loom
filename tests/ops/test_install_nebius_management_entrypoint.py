"""Installing one management grant preserves all unrelated gateway authority."""
from __future__ import annotations

import base64
import hashlib
import importlib
import json
import struct
import subprocess
import sys
from pathlib import Path

import pytest
from tests.ops.test_nebius_ingress_bootstrap import archive
from tests.ops.test_nebius_management_gateway import (
    diagnostic_operation,
    operation,
    retirement_operation,
    upgrade_operation,
)


def module():
    return importlib.import_module("scripts.ops.install_nebius_management_entrypoint")


@pytest.fixture(params=["initial", "upgrade", "retirement", "diagnostic"])
def inputs(tmp_path, request):
    (tmp_path / ".loom").mkdir(mode=0o700)
    (tmp_path / ".ssh").mkdir(mode=0o700)
    keys = tmp_path / ".ssh/authorized_keys"
    keys.write_bytes(b'# operator\nrestrict,command="ingress-command" ssh-ed25519 FOREIGN old\n')
    keys.chmod(0o600)
    metadata = {"initial": operation, "upgrade": upgrade_operation, "retirement": retirement_operation,
        "diagnostic": diagnostic_operation}[request.param](tmp_path / ".loom")
    content = archive({"operation.json": json.dumps(metadata).encode(),
        "scripts/ops/nebius_management_gateway.py": b'def authorized_main(digest):\n    return 0\n',
        "scripts/ops/nebius_certificate_gateway.py": b"# supervisor\n"})
    wire = struct.pack(">I", 11) + b"ssh-ed25519" + struct.pack(">I", 32) + b"i" * 32
    key = "ssh-ed25519 " + base64.b64encode(wire).decode()
    return tmp_path / ".loom/nebius-management", keys, key, content, hashlib.sha256(content).hexdigest()


def test_preview_preserves_keys_and_creates_nothing(inputs):
    root, keys, key, content, digest = inputs
    before = keys.read_bytes()
    assert module().install(content, expected_sha256=digest, public_key=key)["status"] == "prepared"
    assert keys.read_bytes() == before and not root.exists()


def test_grant_is_exact_fixed_command_preserves_existing_keys_and_checks_sources(inputs):
    root, keys, key, content, digest = inputs
    before = keys.read_bytes()
    report = module().install(content, expected_sha256=digest, public_key=key, apply=True)
    after = keys.read_bytes()
    assert after.startswith(before) and len(after.splitlines()) == 3
    assert module().install(content, expected_sha256=digest, public_key=key, apply=True) == report
    assert keys.read_bytes() == after and not (root / "state").exists()
    entry = root / "authority" / digest / "entrypoint.py"
    for command, expected in [("loom-nebius-management-preflight-v1", 0), ("loom-nebius-management-install-v1", 0),
        ("loom-nebius-management-install-v1 extra", 126), ("loom-nebius-ingress-v1", 126), ("kubectl apply", 126)]:
        result = subprocess.run([sys.executable, "-I", str(entry)], input=content, capture_output=True,
                                env={"SSH_ORIGINAL_COMMAND": command}, timeout=10)
        assert result.returncode == expected
    (entry.parent / "scripts/ops/nebius_certificate_gateway.py").write_bytes(b'print("modified")')
    result = subprocess.run([sys.executable, "-I", str(entry)], input=content, capture_output=True,
        env={"SSH_ORIGINAL_COMMAND": "loom-nebius-management-install-v1"}, timeout=10)
    assert result.returncode == 126 and b"modified" not in result.stdout


def test_upgrade_grant_preserves_bootstrap_and_does_not_stage_private_inputs(inputs):
    root, keys, key, content, digest = inputs
    root.mkdir(mode=0o700)
    originals = {"inputs.json": b"retained bootstrap inputs", "state/phase.json": b"retained state",
                 "anchor/installation.json": b"retained anchor"}
    for name, value in originals.items():
        path = root / name
        path.parent.mkdir(mode=0o700, exist_ok=True)
        path.write_bytes(value)
    before = keys.read_bytes()
    assert module().install(content, expected_sha256=digest, public_key=key, apply=True)["status"] == "installed"
    assert keys.read_bytes().startswith(before)
    assert all((root / name).read_bytes() == value for name, value in originals.items())
    assert (root / "authority" / digest / "entrypoint.py").is_file()
    assert not (root / "upgrade").exists()


@pytest.mark.parametrize("case", ["unknown_schema", "bootstrap_path", "initial_schema", "other_directory",
                                 "mixed_anchor", "mixed_inputs"])
def test_upgrade_grant_rejects_mixed_operation_paths_before_writes(inputs, case):
    root, keys, key, _, _ = inputs
    metadata = upgrade_operation(root.parent)
    if case == "unknown_schema":
        metadata["schema"] = "loom.nebius-management-unknown.v1"
    elif case == "bootstrap_path":
        for field, name in (("state_dir", "state"), ("anchor_dir", "anchor"), ("inputs_path", "inputs.json")):
            metadata[field] = str(root / name)
    elif case == "initial_schema":
        metadata["schema"] = "loom.nebius-management-operation.v1"
    elif case == "other_directory":
        metadata["state_dir"] = str(root / "other/state")
    elif case == "mixed_anchor":
        metadata["anchor_dir"] = str(root / "anchor")
    else:
        metadata["inputs_path"] = str(root / "inputs.json")
    content = archive({"operation.json": json.dumps(metadata).encode(),
        "scripts/ops/nebius_management_gateway.py": b"# fixed gateway\n",
        "scripts/ops/nebius_certificate_gateway.py": b"# fixed supervisor\n"})
    before = keys.read_bytes()
    with pytest.raises(module().InstallError):
        module().install(content, expected_sha256=hashlib.sha256(content).hexdigest(), public_key=key, apply=True)
    assert keys.read_bytes() == before and not root.exists()


@pytest.mark.parametrize("case", ["digest", "options", "other_grant", "public_keys", "symlink"])
def test_conflicting_or_untrusted_inputs_never_modify_keys(inputs, case, tmp_path):
    _, keys, key, content, digest = inputs
    if case == "digest":
        digest = "0" * 64
    elif case == "options":
        key = 'command="arbitrary" ' + key
    elif case == "other_grant":
        keys.write_text(keys.read_text() + key + "\n")
    elif case == "public_keys":
        keys.chmod(0o644)
    else:
        keys.rename(tmp_path / "saved")
        keys.symlink_to(tmp_path / "saved")
    before = keys.read_bytes()
    with pytest.raises(module().InstallError):
        module().install(content, expected_sha256=digest, public_key=key, apply=True)
    assert keys.read_bytes() == before


def test_operator_cli_is_standalone_without_checkout_imports(inputs, tmp_path):
    root, _, key, content, digest = inputs
    bundle, public = tmp_path / "approved.zip", tmp_path / "key.pub"
    bundle.write_bytes(content)
    public.write_text(key)
    for path in (bundle, public):
        path.chmod(0o600)
    script = Path(__file__).resolve().parents[2] / "scripts/ops/install_nebius_management_entrypoint.py"
    result = subprocess.run([sys.executable, "-I", str(script), "--bundle", str(bundle), "--bundle-sha256", digest,
        "--public-key", str(public)], cwd=tmp_path, capture_output=True, timeout=10)
    assert result.returncode == 0
    assert json.loads(result.stdout)["status"] == "prepared" and not root.exists()
