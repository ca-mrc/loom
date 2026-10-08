"""The completion reader extends only its fixed command; old grants survive."""
from __future__ import annotations

import os
import subprocess
import sys

import pytest
from scripts.ops.install_nebius_legacy_pool_completion import install


def test_gateway_preserves_existing_grants_and_reader_has_no_arbitrary_argv(tmp_path):
    tmp_path.chmod(0o700)
    wrapper = tmp_path / 'github-kubectl.py'
    original = b'print("existing grant")\n'
    wrapper.write_bytes(original)
    wrapper.chmod(0o600)
    source = b'import sys; print("reader", sys.argv[1])\n'
    revision = 'a' * 40
    assert install(tmp_path, source, revision)['status'] == 'installed'
    installed = wrapper.read_bytes()
    assert installed.endswith(original)
    assert (tmp_path / 'legacy-pool-completion' / revision / 'previous-github-kubectl.py').read_bytes() == original
    assert install(tmp_path, source, revision)['status'] == 'unchanged'
    assert wrapper.read_bytes() == installed
    for command, code, output in [
        ('kubectl --kubeconfig retained get pods', 0, 'existing grant'),
        ('loom-nebius-legacy-pool-completion-v1 operation', 0, 'reader operation'),
        ('loom-nebius-legacy-pool-completion-v1 operation extra', 126, ''),
    ]:
        result = subprocess.run([sys.executable, str(wrapper)],
                                env={**os.environ, 'SSH_ORIGINAL_COMMAND': command},
                                capture_output=True, text=True, check=False)
        assert (result.returncode, result.stdout.strip()) == (code, output)
    with pytest.raises(ValueError, match='source differs'):
        install(tmp_path, b'changed', revision)
    assert wrapper.read_bytes() == installed


def test_gateway_rejects_symlink_without_modifying_target(tmp_path):
    tmp_path.chmod(0o700)
    target = tmp_path / 'other'
    target.write_text('original')
    target.chmod(0o600)
    (tmp_path / 'github-kubectl.py').symlink_to(target)
    with pytest.raises(ValueError, match='private and owned'):
        install(tmp_path, b'print(1)', 'a' * 40)
    assert target.read_text() == 'original'
