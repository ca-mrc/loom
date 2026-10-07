"""Real filesystem and process boundaries for gateway cache maintenance."""
from __future__ import annotations

import configparser
import importlib
import json
import os
import py_compile
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / 'scripts/ops/nebius_gateway_cleanup.py'
DAY = 86400


@pytest.fixture(autouse=True)
def private_umask():
    previous = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(previous)


def cleaner():
    assert SCRIPT.is_file(), 'scheduled gateway cache cleanup is not implemented'
    return importlib.import_module('scripts.ops.nebius_gateway_cleanup')


def private(path: Path, content: bytes = b'') -> Path:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.write_bytes(content)
    path.chmod(0o600)
    return path


def release(root: Path, *, complete: bool = True, digest: str = 'a') -> tuple[Path, Path]:
    selected = root / 'releases' / (digest * 64)
    source = private(selected / 'venv/lib/python3.12/site-packages/example/module.py', b'value = 42\n')
    cache = private(source.parent / '__pycache__/module.cpython-312.pyc', b'regenerable bytecode')
    os.utime(cache, (time.time() - 10 * DAY,) * 2)
    private(selected / 'operation.json', b'private operation evidence')
    private(root / 'tooling.lock')
    private(root / 'anchor/operation.lock')
    private(root / 'anchor/dispatch/operation.lock')
    if complete:
        private(selected / 'complete', b'complete')
    return selected, cache


@pytest.fixture
def root(tmp_path):
    result = tmp_path / 'nebius-management'
    result.mkdir(mode=0o700)
    return result


def test_default_report_then_apply_then_repeat_preserves_evidence(root):
    selected, cache = release(root)
    records = [private(root / name, b'keep exactly') for name in (
        'inputs.json', 'state/journal.json', 'anchor/recovery.json', 'backups/backup.dump',
        'authority/original/receipt.json')]
    records.extend([selected / 'operation.json', cache.parent.parent / 'module.py', selected / 'complete'])
    before = {path: path.read_bytes() for path in records}
    report = cleaner().clean(root)
    assert report['status'] == 'reported'
    assert report['candidate_files'] == 1
    assert report['candidate_bytes'] == cache.stat().st_size
    assert report['deleted_bytes'] == 0 and cache.exists()
    report = cleaner().clean(root, apply=True)
    assert report['status'] == 'cleaned'
    assert report['deleted_files'] == 1 and report['deleted_bytes'] > 0
    assert not cache.exists() and selected.is_dir()
    assert {path: path.read_bytes() for path in records} == before
    assert cleaner().clean(root, apply=True)['deleted_files'] == 0


@pytest.mark.parametrize('relative', ['', 'upgrade', 'retirement', 'retirement-diagnostic',
    'retirement-recovery', 'refresh/18718d96-d389-40b3-a79b-11489924d0d4',
    'pool-cutover/18718d96-d389-40b3-a79b-11489924d0d4',
    'pool-repair/18718d96-d389-40b3-a79b-11489924d0d4'])
def test_supported_scopes(root, relative):
    _, cache = release(root / relative)
    assert cleaner().clean(root, apply=True)['deleted_files'] == 1
    assert not cache.exists()


def test_incomplete_unknown_and_recent_files_are_retained(root):
    _, incomplete = release(root, complete=False)
    selected, fresh = release(root, digest='b')
    os.utime(fresh, None)
    unknown = private(selected / 'venv/lib/python3.12/site-packages/example/__pycache__/notes.json', b'keep')
    sourceless = private(unknown.with_name('only.cpython-312.pyc'), b'keep')
    other_scope = private(root / 'unrecognized/releases' / ('c' * 64) / '__pycache__/module.cpython-312.pyc')
    assert cleaner().clean(root, apply=True)['deleted_files'] == 0
    assert all(path.exists() for path in (incomplete, fresh, unknown, sourceless, other_scope))


@pytest.mark.parametrize('damage', ['cache_symlink', 'source_symlink', 'cache_hardlink',
    'source_hardlink', 'directory_symlink', 'release_symlink', 'writable_directory'])
def test_unsafe_entries_are_never_deleted(root, tmp_path, damage):
    selected, cache = release(root)
    source = cache.parent.parent / 'module.py'
    outside = private(tmp_path / 'outside.py', b'outside must survive')
    if damage in {'cache_symlink', 'cache_hardlink'}:
        cache.unlink()
        if damage.endswith('symlink'):
            cache.symlink_to(outside)
        else:
            os.link(outside, cache)
    elif damage in {'source_symlink', 'source_hardlink'}:
        source.unlink()
        if damage.endswith('symlink'):
            source.symlink_to(outside)
        else:
            os.link(outside, source)
    elif damage == 'directory_symlink':
        moved = tmp_path / 'moved-cache'
        cache.parent.rename(moved)
        cache.parent.symlink_to(moved, target_is_directory=True)
    elif damage == 'release_symlink':
        moved = tmp_path / 'moved-release'
        selected.rename(moved)
        selected.symlink_to(moved, target_is_directory=True)
    else:
        cache.parent.chmod(0o777)
    report = cleaner().clean(root, apply=True)
    assert report['deleted_files'] == 0
    assert outside.read_bytes() == b'outside must survive'
    assert cache.exists()


@contextmanager
def held_lock(path):
    # Pass the private path on stdin so the test exercises flock, not the
    # independent legacy-process guard based on process arguments.
    process = subprocess.Popen([sys.executable, '-c',
        'import fcntl,sys; f=open(sys.stdin.readline().strip(), "r+"); '
        'fcntl.flock(f, fcntl.LOCK_EX); print("locked", flush=True); sys.stdin.read()'],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        process.stdin.write(str(path) + '\n')
        process.stdin.flush()
        assert process.stdout.readline().strip() == 'locked'
        yield
    finally:
        process.communicate(timeout=10)
        assert process.returncode == 0


@pytest.mark.parametrize('lock', ['tooling.lock', 'anchor/operation.lock',
    'anchor/dispatch/operation.lock', 'state/nested/operation.lock'])
def test_any_busy_scope_skips_entire_pass_without_deleting(root, lock):
    _, cache = release(root)
    other = root / 'pool-cutover/18718d96-d389-40b3-a79b-11489924d0d4'
    _, other_cache = release(other)
    path = private(other / lock)
    with held_lock(path):
        report = cleaner().clean(root, apply=True)
    assert report['status'] == 'skipped_busy' and report['deleted_files'] == 0
    assert cache.exists() and other_cache.exists()
    assert cleaner().clean(root, apply=True)['deleted_files'] == 2


def test_lockless_legacy_gateway_process_skips_cleanup(root):
    _, cache = release(root)
    process = subprocess.Popen([sys.executable, '-c', 'import sys; print("ready", flush=True); sys.stdin.read()',
        str(root / 'authority/retained/entrypoint.py')], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert process.stdout.readline().strip() == 'ready'
        report = cleaner().clean(root, apply=True)
        assert report['status'] == 'skipped_busy'
        assert cache.exists()
    finally:
        process.communicate(timeout=10)


def test_missing_and_invalid_roots_fail_closed_without_creating_state(tmp_path):
    for path in (tmp_path / 'nebius-management', tmp_path):
        report = cleaner().clean(path, apply=True)
        assert report['status'] == 'blocked' and report['deleted_files'] == 0
    assert not (tmp_path / 'nebius-management').exists()


@pytest.mark.parametrize('age', [0, -1, True, 0.5, 3651])
def test_unsafe_age_is_rejected(root, age):
    _, cache = release(root)
    assert cleaner().clean(root, apply=True, min_age_days=age)['status'] == 'blocked'
    assert cache.exists()


def test_cli_is_report_only_and_contains_no_paths_or_payloads(root):
    _, cache = release(root)
    cleaner()
    result = subprocess.run([sys.executable, '-I', '-B', str(SCRIPT), '--root', str(root)],
        capture_output=True, text=True, check=True)
    report = json.loads(result.stdout)
    assert report['status'] == 'reported' and report['candidate_files'] == 1
    assert cache.exists()
    assert str(root) not in result.stdout and 'private operation' not in result.stdout
    assert result.stderr == ''


@pytest.mark.parametrize(('used', 'free', 'pressure'), [(79, 21, 'normal'), (80, 20, 'warning'),
    (89, 11, 'warning'), (90, 10, 'critical')])
def test_pressure_thresholds(root, monkeypatch, used, free, pressure):
    from types import SimpleNamespace

    module = cleaner()
    monkeypatch.setattr(module.os, 'statvfs', lambda _: SimpleNamespace(
        f_blocks=100, f_bfree=free, f_bavail=free, f_frsize=4096))
    report = module.clean(root)
    assert report['disk_before']['pressure'] == pressure
    assert report['disk_before']['used_percent'] == used


def test_age_boundary_and_optimized_bytecode(root, monkeypatch):
    _, cache = release(root)
    module = cleaner()
    now = time.time()
    monkeypatch.setattr(module.time, 'time', lambda: now)
    os.utime(cache, (now - 7 * DAY + 1,) * 2)
    assert module.clean(root, apply=True)['deleted_files'] == 0
    optimized = cache.with_name('module.cpython-312.opt-1.pyc')
    cache.rename(optimized)
    os.utime(optimized, (now - 7 * DAY,) * 2)
    assert module.clean(root, apply=True)['deleted_files'] == 1


def test_changed_candidate_is_preserved_before_unlink(root, monkeypatch):
    _, cache = release(root)
    module = cleaner()
    unlink = module._unlink

    def replaced(*args):
        cache.unlink()
        cache.symlink_to(cache.parent.parent / 'module.py')
        return unlink(*args)

    monkeypatch.setattr(module, '_unlink', replaced)
    report = module.clean(root, apply=True)
    assert report['status'] == 'blocked' and report['deleted_files'] == 0
    assert cache.is_symlink() and cache.read_bytes() == b'value = 42\n'


def test_scan_bound_refuses_partial_inventory(root, monkeypatch):
    _, cache = release(root)
    module = cleaner()
    monkeypatch.setattr(module, 'MAX_ENTRIES', 1)
    report = module.clean(root, apply=True)
    assert report['status'] == 'blocked' and report['deleted_files'] == 0
    assert cache.exists()


def test_cache_can_be_regenerated_by_an_ordinary_import(root):
    selected, original = release(root)
    original.unlink()
    source = original.parent.parent / 'module.py'
    compiled = Path(py_compile.compile(str(source), doraise=True))
    os.utime(compiled, (time.time() - 10 * DAY,) * 2)
    report = cleaner().clean(root, apply=True)
    assert report['deleted_files'] == 1 and not compiled.exists()
    result = subprocess.run([sys.executable, '-I', '-c',
        'import sys; sys.path.insert(0, sys.argv[1]); import module; print(module.value)', str(source.parent)],
        capture_output=True, text=True, check=True)
    assert result.stdout.strip() == '42'
    assert compiled.exists() and (selected / 'complete').read_bytes() == b'complete'


def test_unreadable_source_is_not_a_regenerable_cache(root):
    _, cache = release(root)
    source = cache.parent.parent / 'module.py'
    source.chmod(0o000)
    assert cleaner().clean(root, apply=True)['deleted_files'] == 0
    assert cache.exists()


def test_symlink_loop_is_a_payload_free_blocked_report(root):
    _, cache = release(root)
    (root / 'upgrade').symlink_to(root / 'upgrade', target_is_directory=True)
    cleaner()
    result = subprocess.run([sys.executable, '-I', '-B', str(SCRIPT), '--root', str(root), '--apply'],
        capture_output=True, text=True)
    assert result.returncode == 1
    assert result.stderr == ''
    assert json.loads(result.stdout)['status'] == 'blocked'
    assert str(root) not in result.stdout and cache.exists()


def test_daily_user_service_defaults_to_report_only():
    directory = SCRIPT.parents[2] / 'deploy/systemd'
    service = configparser.ConfigParser(interpolation=None)
    assert service.read(directory / 'loom-nebius-gateway-cleanup.service')
    command = service['Service']['ExecStart']
    assert command == '/usr/bin/python3 -I -B %h/.local/libexec/loom/nebius_gateway_cleanup.py'
    assert service['Service']['UMask'] == '0077'
    timer = configparser.ConfigParser(interpolation=None)
    assert timer.read(directory / 'loom-nebius-gateway-cleanup.timer')
    assert timer['Timer']['OnCalendar'] == 'daily'
    assert timer['Timer']['Persistent'] == 'true'
    assert timer['Timer']['Unit'] == 'loom-nebius-gateway-cleanup.service'
