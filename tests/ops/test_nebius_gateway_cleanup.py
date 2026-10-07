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


def test_preview_then_default_cleanup_then_repeat_preserves_evidence(root):
    selected, cache = release(root)
    records = [private(root / name, b'keep exactly') for name in (
        'inputs.json', 'state/journal.json', 'anchor/recovery.json', 'backups/backup.dump',
        'authority/original/receipt.json')]
    records.extend([selected / 'operation.json', cache.parent.parent / 'module.py', selected / 'complete'])
    before = {path: path.read_bytes() for path in records}
    report = cleaner().clean(root, apply=False)
    assert report['status'] == 'reported'
    assert report['candidate_files'] == 1
    assert report['candidate_bytes'] == cache.stat().st_size
    assert report['deleted_bytes'] == 0 and cache.exists()
    report = cleaner().clean(root)
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


def test_cli_defaults_to_clean_and_contains_no_paths_or_payloads(root, tmp_path):
    _, cache = release(root)
    cleaner()
    config = private(tmp_path / 'cleanup.toml', b'')
    result = subprocess.run([sys.executable, '-I', '-B', str(SCRIPT), '--root', str(root), '--config', str(config)],
        capture_output=True, text=True, check=True)
    report = json.loads(result.stdout)
    assert report['status'] == 'cleaned' and report['deleted_files'] == 1
    assert not cache.exists()
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


def test_symlink_loop_is_a_payload_free_blocked_report(root, tmp_path):
    _, cache = release(root)
    (root / 'upgrade').symlink_to(root / 'upgrade', target_is_directory=True)
    cleaner()
    config = private(tmp_path / 'cleanup.toml', b'')
    result = subprocess.run([sys.executable, '-I', '-B', str(SCRIPT), '--root', str(root),
        '--config', str(config), '--apply'],
        capture_output=True, text=True)
    assert result.returncode == 1
    assert result.stderr == ''
    assert json.loads(result.stdout)['status'] == 'blocked'
    assert str(root) not in result.stdout and cache.exists()


def test_typical_uv_interpreter_and_lib64_links_are_retained(root):
    selected, cache = release(root)
    venv = selected / 'venv'
    (venv / 'bin').mkdir()
    (venv / 'bin/python').symlink_to(sys.executable)
    (venv / 'lib64').symlink_to('lib', target_is_directory=True)
    report = cleaner().clean(root, apply=True)
    assert report['deleted_files'] == 1 and not cache.exists()
    assert (venv / 'bin/python').is_symlink() and (venv / 'lib64').is_symlink()
    assert (venv / 'lib64/python3.12/site-packages/example/module.py').is_file()


def test_second_cleaner_cannot_enter_while_first_deletes(root, tmp_path, monkeypatch):
    _, cache = release(root)
    module = cleaner()
    unlink = module._unlink
    config = private(tmp_path / 'cleanup.toml', b'')

    def while_locked(*args):
        result = subprocess.run([sys.executable, '-I', '-B', str(SCRIPT), '--root', str(root),
            '--config', str(config), '--apply'],
            capture_output=True, text=True, check=True)
        report = json.loads(result.stdout)
        assert report['status'] == 'skipped_busy' and report['stage'] == 'locks'
        assert report['deleted_files'] == 0 and cache.exists()
        return unlink(*args)

    monkeypatch.setattr(module, '_unlink', while_locked)
    assert module.clean(root, apply=True)['deleted_files'] == 1
    assert not cache.exists()


def test_daily_user_service_reads_the_operator_configuration():
    directory = SCRIPT.parents[2] / 'deploy/systemd'
    service = configparser.ConfigParser(interpolation=None)
    assert service.read(directory / 'loom-nebius-gateway-cleanup.service')
    command = service['Service']['ExecStart']
    assert command == ('/usr/bin/python3 -I -B %h/.local/libexec/loom/nebius_gateway_cleanup.py '
        '--config %h/.config/loom/gateway-cleanup.toml')
    assert service['Service']['UMask'] == '0077'
    timer = configparser.ConfigParser(interpolation=None)
    assert timer.read(directory / 'loom-nebius-gateway-cleanup.timer')
    assert timer['Timer']['OnCalendar'] == 'daily'
    assert timer['Timer']['Persistent'] == 'true'
    assert timer['Timer']['Unit'] == 'loom-nebius-gateway-cleanup.service'


def run_cli(root, config, *options):
    return subprocess.run([sys.executable, '-I', '-B', str(SCRIPT), '--root', str(root),
        '--config', str(config), *options], capture_output=True, text=True)


@pytest.mark.parametrize('option', ['--dry-run', '--report-only'])
def test_preview_overrides_clean_configuration(root, tmp_path, option):
    _, cache = release(root)
    config = private(tmp_path / 'cleanup.toml', b'mode = "clean"\n')
    result = run_cli(root, config, option)
    assert result.returncode == 0 and result.stderr == ''
    assert json.loads(result.stdout)['status'] == 'reported' and cache.exists()


def test_configuration_is_reread_without_service_edits(root, tmp_path):
    _, cache = release(root)
    config = private(tmp_path / 'cleanup.toml', b'mode = "report"\n')
    assert json.loads(run_cli(root, config).stdout)['status'] == 'reported'
    assert cache.exists()
    config.write_text('mode = "clean"\nmin_age_days = 14\n')
    assert json.loads(run_cli(root, config).stdout)['deleted_files'] == 0
    assert cache.exists()
    config.write_text('mode = "clean"\nmin_age_days = 7\n')
    assert json.loads(run_cli(root, config).stdout)['deleted_files'] == 1
    assert not cache.exists()


def test_disabled_configuration_touches_no_storage_even_with_apply(root, tmp_path):
    config = private(tmp_path / 'cleanup.toml', b'enabled = false\n')
    result = run_cli(root, config, '--apply')
    assert result.returncode == 0 and result.stderr == ''
    report = json.loads(result.stdout)
    assert report['status'] == 'disabled' and report['deleted_files'] == 0
    assert list(root.iterdir()) == []


def test_cli_age_override_is_explicit(root, tmp_path):
    _, cache = release(root)
    config = private(tmp_path / 'cleanup.toml', b'min_age_days = 14\nmode = "report"\n')
    result = run_cli(root, config, '--min-age-days', '7', '--apply')
    assert result.returncode == 0
    assert json.loads(result.stdout)['deleted_files'] == 1 and not cache.exists()


@pytest.mark.parametrize('contents', [b'enabled = "false"', b'mode = "delete-all"',
    b'min_age_days = true', b'min_age_days = 0', b'min_age_days = 3651', b'min_age_days = 1.5',
    b'root = 42', b'root = "relative/path"', b'root = "/"', b'enabeld = false',
    b'[unknown]\nkey = 1', b'mode = "private incomplete', b'#' * 16385, b'\xff'],
    ids=['enabled-type', 'unknown-mode', 'boolean-age', 'zero-age', 'large-age', 'fractional-age',
        'root-type', 'relative-root', 'wrong-root', 'unknown-key', 'unknown-section', 'syntax', 'size', 'encoding'])
def test_invalid_config_blocks_without_falling_back_to_deletion(root, tmp_path, contents):
    _, cache = release(root)
    config = private(tmp_path / 'cleanup.toml', contents)
    result = run_cli(root, config, '--apply')
    assert result.returncode == 1 and result.stderr == ''
    report = json.loads(result.stdout)
    assert report['status'] == 'blocked' and report['stage'] == 'config'
    assert report['deleted_files'] == 0 and cache.exists()
    assert str(root) not in result.stdout and 'private incomplete' not in result.stdout


@pytest.mark.parametrize('damage', ['missing', 'symlink', 'writable', 'hardlink', 'unreadable'])
def test_unavailable_explicit_configuration_never_enables_cleanup(root, tmp_path, damage):
    _, cache = release(root)
    config = private(tmp_path / 'cleanup.toml', b'mode = "clean"\n')
    if damage == 'missing':
        config.unlink()
    elif damage == 'symlink':
        other = private(tmp_path / 'other.toml', config.read_bytes())
        config.unlink()
        config.symlink_to(other)
    elif damage == 'writable':
        config.chmod(0o666)
    elif damage == 'hardlink':
        os.link(config, tmp_path / 'other.toml')
    else:
        config.chmod(0o000)
    result = run_cli(root, config)
    assert result.returncode == 1 and result.stderr == ''
    assert json.loads(result.stdout)['status'] == 'blocked' and cache.exists()


def test_example_configuration_has_clean_defaults_and_home_relative_storage(tmp_path, monkeypatch):
    module = cleaner()
    monkeypatch.setattr(module.Path, 'home', lambda: tmp_path)
    example = SCRIPT.parents[2] / 'config/nebius-gateway-cleanup.example.toml'
    config = private(tmp_path / 'cleanup.toml', example.read_bytes())
    settings = module.load_settings(config, required=True)
    assert settings.enabled is True and settings.mode == 'clean'
    assert settings.min_age_days == 7 and settings.root == tmp_path / '.loom/nebius-management'


def test_default_configuration_location_and_absent_file_defaults(tmp_path, monkeypatch):
    module = cleaner()
    monkeypatch.setattr(module.Path, 'home', lambda: tmp_path)
    default = tmp_path / '.config/loom/gateway-cleanup.toml'
    assert module.default_config() == default
    assert module.load_settings(default, required=False).mode == 'clean'
    private(default, b'mode = "report"\n')
    assert module.load_settings(default, required=False).mode == 'report'


def test_configured_root_is_used_without_a_cli_override(root, tmp_path):
    _, cache = release(root)
    config = private(tmp_path / 'cleanup.toml', ('root = ' + json.dumps(str(root)) + '\n').encode())
    result = subprocess.run([sys.executable, '-I', '-B', str(SCRIPT), '--config', str(config)],
        capture_output=True, text=True, check=True)
    assert json.loads(result.stdout)['deleted_files'] == 1 and not cache.exists()


def test_cli_root_override_leaves_configured_root_untouched(root, tmp_path):
    _, cache = release(root)
    other = tmp_path / 'other/nebius-management'
    _, other_cache = release(other)
    config = private(tmp_path / 'cleanup.toml', ('root = ' + json.dumps(str(other)) + '\n').encode())
    result = run_cli(root, config)
    assert json.loads(result.stdout)['deleted_files'] == 1
    assert not cache.exists() and other_cache.exists()


def test_implicit_invalid_configuration_cannot_fall_back_to_cleanup(root, tmp_path, monkeypatch, capsys):
    _, cache = release(root)
    module = cleaner()
    monkeypatch.setattr(module.Path, 'home', lambda: tmp_path)
    private(tmp_path / '.config/loom/gateway-cleanup.toml', b'mode = "typo"')
    monkeypatch.setattr(sys, 'argv', [str(SCRIPT), '--root', str(root)])
    assert module.main() == 1
    report = json.loads(capsys.readouterr().out)
    assert report['status'] == 'blocked' and report['stage'] == 'config'
    assert cache.exists()


@pytest.mark.parametrize('root_value', ['~//.loom/nebius-management', '~///.loom/nebius-management'])
def test_home_root_with_repeated_slashes_remains_below_home(tmp_path, monkeypatch, root_value):
    module = cleaner()
    monkeypatch.setattr(module.Path, 'home', lambda: tmp_path)
    config = private(tmp_path / 'cleanup.toml', ('root = ' + json.dumps(root_value)).encode())
    assert module.load_settings(config, required=True).root == tmp_path / '.loom/nebius-management'
