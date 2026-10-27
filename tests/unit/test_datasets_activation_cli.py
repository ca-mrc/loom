"""Canonical activation parser, adapter boundaries and compatibility syntax."""

import json
import subprocess
import sys
from argparse import Namespace

import pytest

from loom.models.benchmark_profile import BenchmarkProfileSpec
from loom_cli import datasets_cmd


def _descriptor(tmp_path, *, contract="canonical.v1"):
    profile = "arbitrary-example@one"
    path = tmp_path / "profile.json"
    path.write_text(json.dumps({
        "id": profile,
        "origin": {"kind": "json", "locator": "fixture:example", "revision": "r1",
                   "release": "one", "importer": "loom.canonical.v1"},
        "expected_task_count": 1, "selected_task_ids": ["first"],
        "validation_contract": contract,
    }))
    return path


def test_canonical_help_does_not_import_optional_dataset_adapters():
    result = subprocess.run([sys.executable, "-c", """
import importlib.abc
import sys
class NoAdapters(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'loom_benchmarks', 'datasets', 'loom_benchmark_terminal_bench_2'}:
            raise AssertionError('optional adapter import: ' + fullname)
sys.meta_path.insert(0, NoAdapters())
from loom_cli.datasets_cmd import dispatch
dispatch(['activate', '--help'])
"""], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    assert "--descriptor" in result.stdout
    assert "--expect-absent" in result.stdout


@pytest.mark.parametrize("flags,diagnostic", [
    (["--alias", "public"], "requires --expected-current or --expect-absent"),
    (["--expect-absent"], "expected-current options require --alias"),
    (["--profile", "other"], "does not match the descriptor"),
    (["--audit-json", "untrusted.json"], "belongs to a reviewed legacy validation contract"),
])
def test_canonical_input_rejects_unsafe_promotion_or_policy(tmp_path, capsys, flags, diagnostic):
    descriptor = _descriptor(tmp_path)
    code = datasets_cmd.dispatch([
        "activate", "--descriptor", str(descriptor),
        "--profile", "arbitrary-example@one", "--db-url", "postgresql://fixture", *flags,
    ])
    assert code == 2
    assert diagnostic in capsys.readouterr().err


def test_native_wrapper_normalizes_to_canonical_handler(tmp_path, monkeypatch):
    path = tmp_path / "harbor.json"
    path.write_text(json.dumps({
        "id": "example-native@one", "display_name": "Example", "series": "test",
        "license_spdx": "MIT", "license_url": "https://example.test/license",
        "expected_task_count": 1,
        "origin": {"kind": "git", "locator": "https://example.test/repo",
                   "revision": "a" * 40, "release": "one"},
    }))
    captured = []
    monkeypatch.setattr(datasets_cmd, "_cmd_activate", lambda args: captured.append(args) or 0)
    assert datasets_cmd._cmd_activate_native(Namespace(harbor_source=path, db_url="fixture")) == 0
    assert captured[0].profile == "example-native@one"
    assert captured[0].harbor_source == path
    assert captured[0].alias is None


def test_neutral_descriptor_cannot_supply_execution_or_verified_policy(tmp_path):
    raw = json.loads(_descriptor(tmp_path).read_text())
    raw["verified"] = True
    raw["validator"] = "user_supplied.run"
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        BenchmarkProfileSpec.model_validate(raw)
