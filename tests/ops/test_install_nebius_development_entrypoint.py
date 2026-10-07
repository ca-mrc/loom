"""The operator grants a dedicated dev key without replacing other authorities."""
from __future__ import annotations

import base64
import hashlib
import importlib
import os
import struct
import subprocess
import sys
from pathlib import Path

import pytest
from tests.ops.test_nebius_development_gateway import archive, members, operation


def module():
    return importlib.import_module("scripts.ops.install_nebius_development_entrypoint")


@pytest.fixture
def authority(tmp_path):
    root = tmp_path / ".loom"
    root.mkdir(mode=0o700)
    ssh = tmp_path / ".ssh"
    ssh.mkdir(mode=0o700)
    keys = ssh / "authorized_keys"
    keys.write_bytes(b'# operator\nrestrict,command="management-command" ssh-ed25519 FOREIGN old\n')
    keys.chmod(0o600)
    files = members(root)
    # Only the installed wrapper test uses this executable substitute; archive
    # and real gateway behavior are independently exercised in gateway tests.
    files["scripts/ops/nebius_development_gateway.py"] = b'def authorized_main(digest):\n    return 0\n'
    raw = archive(files)
    wire = struct.pack(">I", 11) + b"ssh-ed25519" + struct.pack(">I", 32) + b"i" * 32
    key = "ssh-ed25519 " + base64.b64encode(wire).decode()
    return root, keys, key, raw, hashlib.sha256(raw).hexdigest()


def test_preview_grants_nothing_and_apply_preserves_foreign_keys_on_replay(authority):
    root, keys, key, raw, digest = authority
    before = keys.read_bytes()
    assert module().install(raw, expected_sha256=digest, public_key=key)["status"] == "prepared"
    assert keys.read_bytes() == before and not (root / "nebius-development").exists()
    result = module().install(raw, expected_sha256=digest, public_key=key, apply=True)
    assert result["status"] == "installed"
    after = keys.read_bytes()
    assert after.startswith(before) and len(after.splitlines()) == 3
    assert module().install(raw, expected_sha256=digest, public_key=key, apply=True) == result
    assert keys.read_bytes() == after
    assert not Path(operation(root)["state_dir"]).exists()
    assert not (root / "nebius-management").exists()


def test_installed_entry_allows_only_dev_commands_and_checks_local_code(authority):
    root, _, key, raw, digest = authority
    module().install(raw, expected_sha256=digest, public_key=key, apply=True)
    entry = Path(operation(root)["state_dir"]).parent / "authority" / digest / "entrypoint.py"

    def invoke(command):
        return subprocess.run([sys.executable, "-I", str(entry)], capture_output=True, timeout=10,
            env={"PATH": os.defpath, "SSH_ORIGINAL_COMMAND": command}).returncode

    assert invoke("loom-nebius-development-preflight-v1") == 0
    assert invoke("loom-nebius-development-install-v1") == 0
    for name in ("loom-nebius-management-install-v1", "loom-nebius-pool-rollback-v1", "id"):
        assert invoke(name) == 126
    (entry.parent / "scripts/ops/nebius_development_operation.py").write_bytes(b"modified")
    assert invoke("loom-nebius-development-install-v1") == 126


@pytest.mark.parametrize("damage", ["digest", "reused-key", "symlink-keys", "public-keys"])
def test_bad_authority_preserves_existing_material(authority, damage):
    root, keys, key, raw, digest = authority
    if damage == "digest":
        digest = "0" * 64
    elif damage == "reused-key":
        keys.write_text('restrict,command="staging-command" ' + key + " staging\n")
    elif damage == "symlink-keys":
        previous = keys.with_suffix(".old")
        keys.rename(previous)
        keys.symlink_to(previous)
    else:
        keys.chmod(0o644)
    before = keys.read_bytes()
    with pytest.raises(module().InstallError):
        module().install(raw, expected_sha256=digest, public_key=key, apply=True)
    assert keys.read_bytes() == before and not (root / "nebius-development").exists()
