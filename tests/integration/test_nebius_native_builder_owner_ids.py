"""Real pinned public builder/UID1000; no credentials, host namespaces or daemon mounts."""
from __future__ import annotations

import subprocess
from uuid import uuid4

import pytest

from loom_execution_actuator.task_image_renderer import _BUILDER_SUBIDS, BUILDKIT_IMAGE

pytestmark = [pytest.mark.docker, pytest.mark.timeout(150)]


def test_native_builder_preserves_high_owner_ids_with_readonly_subid_files(tmp_path):
    """Original 65536 fails chown; fixed mapping preserves owners and cleans snapshots."""
    run_id = uuid4().hex
    names = []
    results = []
    for key in ("subuid", "subgid"):
        source = tmp_path / key
        source.write_text(_BUILDER_SUBIDS)
        source.chmod(0o444)
    inner = (
        'mkdir -p /scratch/state/owned; touch /scratch/state/owned/file; '
        'cat /proc/self/uid_map; cat /proc/self/gid_map; '
        'chown -R 197609:197121 /scratch/state/owned; '
        'chmod 700 /scratch/state/owned; '
        'test "$(stat -c %u:%g /scratch/state/owned/file)" = 197609:197121; '
        'stat -c %u:%g /scratch/state/owned/file'
    )
    try:
        for mapped in (False, True):
            name = 'loom-native-owner-ids-' + uuid4().hex
            names.append(name)
            # --net=host below means only the already isolated, offline Docker
            # container network; no Docker host namespace is joined.
            script = (
                'set -eu; test "$(id -u):$(id -g)" = 1000:1000; '
                'test ! -w /etc/subuid; test ! -w /etc/subgid; '
                'ulimit -u 512; rootlesskit --net=host sh -ec "$1"; '
                'rootlesskit --net=host rm -rf /scratch/state; '
                'test ! -e /scratch/state; echo owner-id-cleanup-ok'
            )
            command = [
                'docker', 'run', '--rm', '--name', name, '--network', 'none',
                '--label', 'loom.owner-id-proof=' + run_id,
                '--user', '1000:1000', '--cap-drop', 'ALL', '--cap-add', 'SETUID', '--cap-add', 'SETGID',
                '--security-opt', 'seccomp=unconfined', '--security-opt', 'apparmor=unconfined',
                '--read-only', '--pids-limit', '64', '--memory', '256m', '--cpus', '0.5',
                '--tmpfs', '/scratch:rw,exec,uid=1000,gid=1000,mode=700,size=32m',
                '--tmpfs', '/tmp:rw,exec,mode=1777,size=32m',
                '--env', 'TMPDIR=/scratch', '--env', 'XDG_RUNTIME_DIR=/scratch/runtime',
            ]
            if mapped:
                for key in ('subuid', 'subgid'):
                    command.extend(['--mount', f'type=bind,source={tmp_path / key},target=/etc/{key},readonly'])
            command.extend(['--entrypoint', 'sh', BUILDKIT_IMAGE, '-ec', script, 'owner-id-proof', inner])
            result = subprocess.run(command, text=True, capture_output=True, timeout=60)
            results.append(result)
            if mapped:
                assert result.returncode == 0, result.stderr
                assert '197609:197121' in result.stdout and 'owner-id-cleanup-ok' in result.stdout
                assert '262144' in result.stdout
            else:
                assert result.returncode != 0 and 'Invalid argument' in result.stderr, result.stderr
                assert '65536' in result.stdout
        assert len(results) == 2
    finally:
        for name in names:
            subprocess.run(['docker', 'rm', '--force', name], capture_output=True, timeout=15)
        remaining = subprocess.run(['docker', 'ps', '-aq', '--filter', 'label=loom.owner-id-proof=' + run_id],
                                   text=True, capture_output=True, timeout=15)
        assert remaining.returncode == 0
        assert not remaining.stdout.strip()
