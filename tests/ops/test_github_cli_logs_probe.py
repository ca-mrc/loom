"""The standalone native CLI probe isolates credentials and fails closed."""

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PROBE = ROOT / "scripts/check_github_cli_logs.py"
SECRET = "private-auth-must-never-escape"


@pytest.fixture
def cli(tmp_path):
    script = tmp_path / "gh.py"
    script.write_text('''import json, os, re, sys, urllib.request
from pathlib import Path
assert os.environ['GH_TOKEN'] == 'loopback-only-test-token'
assert os.environ['GH_ENTERPRISE_TOKEN'] == 'loopback-only-test-token'
assert 'GITHUB_TOKEN' not in os.environ and 'GH_HOST' not in os.environ
assert 'HTTP_PROXY' not in os.environ and 'https_proxy' not in os.environ
assert not list(Path(os.environ['GH_CONFIG_DIR']).iterdir())
with open(os.environ['RECORD'], 'a') as output:
 output.write(json.dumps(sys.argv[1:]) + '\\n')
mode = os.environ['MODE']
if sys.argv[1:] == ['--version']:
 print('gh version 2.102.0 (2026-10-01)'); sys.exit(0)
assert sys.argv[1] == 'api'
if mode == 'old' and '--allow-escape-sequences' in sys.argv:
 print('unknown flag: --allow-escape-sequences', file=sys.stderr); sys.exit(1)
if mode != 'old' and '--allow-escape-sequences' not in sys.argv:
 print(os.environ['PRIVATE_SENTINEL'], file=sys.stderr); sys.exit(1)
url = sys.argv[-1]
assert url.startswith('http://127.0.0.1:')
if mode == 'http': url = url.replace('/repos/', '/unexpected/')
token = os.environ['PRIVATE_SENTINEL'] if mode == 'token' else os.environ['GH_TOKEN']
request = urllib.request.Request(url, headers={'Authorization': 'token ' + token})
try:
 with urllib.request.urlopen(request, timeout=2) as response: body = response.read().decode()
except Exception:
 print(os.environ['PRIVATE_SENTINEL'], file=sys.stderr); sys.exit(1)
if mode == 'body': body = re.sub(r'\\x1b\\[[0-9;]*m', '', body)
if mode == 'empty': body = ''
sys.stdout.write(body)
''')
    gh = tmp_path / "gh"
    gh.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} {shlex.quote(str(script))} "$@"\n')
    gh.chmod(0o700)
    record = tmp_path / "calls.jsonl"
    config = tmp_path / "real-gh-config"
    config.mkdir()
    (config / "hosts.yml").write_text(SECRET)
    env = {**os.environ, "PATH": str(tmp_path), "MODE": "modern", "RECORD": str(record),
           "PRIVATE_SENTINEL": SECRET, "GH_TOKEN": SECRET, "GITHUB_TOKEN": SECRET,
           "GH_ENTERPRISE_TOKEN": SECRET, "GH_CONFIG_DIR": str(config), "GH_HOST": "github.com",
           "HTTP_PROXY": "http://private-proxy.invalid", "https_proxy": "http://private-proxy.invalid"}

    def run(mode):
        result = subprocess.run([sys.executable, str(PROBE)], capture_output=True, text=True,
                                timeout=30, env={**env, "MODE": mode})
        assert SECRET not in result.stdout + result.stderr
        return result, [json.loads(line) for line in record.read_text().splitlines()]

    return run


@pytest.mark.parametrize("mode,requests", [("modern", 1), ("old", 2)])
def test_actual_probe_cli_preserves_raw_color_with_modern_and_older_cli(cli, mode, requests):
    result, calls = cli(mode)
    assert result.returncode == 0, result.stderr
    assert result.stdout == "gh version 2.102.0 (2026-10-01)\nPASS: colored raw job log preserved\n"
    assert result.stderr == ""
    assert calls[0] == ["--version"] and len(calls) == 1 + requests
    assert "--allow-escape-sequences" in calls[1]
    if mode == "old":
        assert "--allow-escape-sequences" not in calls[2]


@pytest.mark.parametrize("mode", ["http", "token", "body", "empty"])
def test_actual_probe_cli_rejects_http_credentials_and_body_errors(cli, mode):
    result, calls = cli(mode)
    assert result.returncode == 1 and result.stdout == ""
    assert result.stderr.startswith("FAIL: ")
    assert len(calls) == 2  # HTTP/credential/body errors never get the old-client retry.


def test_missing_github_cli_always_fails_even_outside_actions(tmp_path):
    result = subprocess.run([sys.executable, str(PROBE)], capture_output=True, text=True,
                            env={**os.environ, "PATH": str(tmp_path)}, timeout=10)
    assert result.returncode == 1 and result.stdout == ""
    assert result.stderr == "FAIL: installed GitHub CLI (gh) is required\n"
