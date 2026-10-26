"""Current-checkout coverage must not absorb disposable source trees."""
from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "pyproject.toml"


@pytest.fixture(params=["tests-root", "tests-packages", "integration"])
def ci_coverage_args(request):
    """Use each real CI entrypoint's arguments, including config propagation."""
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    commands = "\n".join(step.get("run", "") for step in workflow["jobs"][request.param]["steps"])
    matches = re.findall(r"coverage_args=\((--cov=src[^)\n]*)\)", commands)
    assert len(matches) == 1
    return shlex.split(matches[0])


def _run(argv: list[str], *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, cwd=cwd, env=env, capture_output=True, text=True, timeout=40, check=False)


def _coverage_env(tmp_path: Path) -> dict[str, str]:
    # An outer pytest-cov session must not collect or combine this sample's data.
    env = {key: value for key, value in os.environ.items()
        if not key.startswith(("COV_CORE_", "COVERAGE_"))}
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(ROOT / "src"),
        COVERAGE_FILE=str(tmp_path / "shards" / ".coverage.sample"))
    (tmp_path / "shards").mkdir()
    return env


def _combine(tmp_path: Path, env: dict[str, str]) -> dict[str, str]:
    combined_env = {**env, "COVERAGE_FILE": str(tmp_path / ".coverage")}
    result = _run([sys.executable, "-B", "-m", "coverage", "combine", "--rcfile", str(CONFIG),
        str(tmp_path / "shards")], cwd=tmp_path, env=combined_env)
    assert result.returncode == 0, result.stdout + result.stderr
    return combined_env


def test_historical_candidate_execution_does_not_enter_current_source_coverage(tmp_path, ci_coverage_args):
    """Exercise real git archive, candidate Python, cleanup, combine, XML and text."""
    source = tmp_path / "src" / "loom" / "__init__.py"
    source.parent.mkdir(parents=True)
    source.write_text("FLAVOR = 'historical'\nHISTORICAL_ONLY = True\n")
    renderer = tmp_path / "scripts" / "ops" / "render_nebius_platform.py"
    renderer.parent.mkdir(parents=True)
    renderer.write_text(
        "import argparse, sys\nfrom pathlib import Path\n"
        "sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))\n"
        "import loom\n"
        "parser = argparse.ArgumentParser()\n"
        "parser.add_argument('--output', type=Path)\n"
        "args, _ = parser.parse_known_args()\n"
        "args.output.mkdir(parents=True, exist_ok=True)\n"
        "(args.output / 'executed.json').write_text(__import__('json').dumps({"
        "'flavor': loom.FLAVOR, 'source': loom.__file__}))\n"
    )
    manifests = tmp_path / "deploy" / "k8s"
    manifests.mkdir(parents=True)
    (manifests / "candidate.yaml").write_text("kind: ConfigMap\n")
    (tmp_path / "packages").mkdir()
    env = _coverage_env(tmp_path)
    for args in (["init", "--quiet"], ["add", "src", "scripts", "deploy"],
            ["-c", "user.name=Coverage Test", "-c", "user.email=coverage@example.invalid",
             "commit", "--quiet", "-m", "Historical candidate"]):
        result = _run(["git", *args], cwd=tmp_path, env=env)
        assert result.returncode == 0, result.stdout + result.stderr
    candidate = _run(["git", "rev-parse", "HEAD"], cwd=tmp_path, env=env).stdout.strip()
    source.write_text("FLAVOR = 'current'\ndef current_only():\n    return FLAVOR\n")
    # CI starts with the current config in its checkout. The archived renderer
    # lacks that checkout's pyproject, so subprocess collection must inherit it.
    (tmp_path / "pyproject.toml").write_bytes(CONFIG.read_bytes())
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    driver = tmp_path / "test_sample.py"
    driver.write_text(
        "import json, runpy, sys\nfrom pathlib import Path\nfrom types import SimpleNamespace\n"
        f"sys.path.insert(0, {str(ROOT)!r})\n"
        "from scripts.ops import nebius_idle_rollout as rollout\n"
        "def test_sample():\n"
        f"    root = Path({str(tmp_path)!r})\n"
        "    current = runpy.run_path(str(root / 'src/loom/__init__.py'))\n"
        "    assert current['current_only']() == 'current'\n"
        "    rollout.ROOT = root\n"
        f"    args = SimpleNamespace(candidate={candidate!r}, publication_dir=root, render_dir=root / 'rendered')\n"
        "    rollout.render_candidate(args, {}, '{}')\n"
        "    execution = json.loads((args.render_dir / 'executed.json').read_text())\n"
        "    assert execution['flavor'] == 'historical'\n"
        "    assert not Path(execution['source']).exists()\n"
    )
    result = _run([sys.executable, "-B", "-m", "pytest", "-p", "no:cacheprovider", "-q",
        "-c", str(tmp_path / "pytest.ini"), "--rootdir", str(tmp_path),
        "--basetemp", str(tmp_path / "child-pytest"), *ci_coverage_args, str(driver)], cwd=tmp_path, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    combined_env = _combine(tmp_path, env)
    for command in ("xml", "report"):
        result = _run([sys.executable, "-B", "-m", "coverage", command, "--rcfile", str(CONFIG)],
            cwd=tmp_path, env=combined_env)
        assert result.returncode == 0, result.stdout + result.stderr
    result = _run([sys.executable, "-B", "-m", "coverage", "json", "--rcfile", str(CONFIG),
        "-o", str(tmp_path / "coverage.json")], cwd=tmp_path, env=combined_env)
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((tmp_path / "coverage.json").read_text())
    assert not any("loom-candidate-manifests-" in path for path in report["files"])
    assert report["files"]["src/loom/__init__.py"]["executed_lines"] == [1, 2, 3]


def test_copied_image_imports_do_not_enter_current_source_coverage(tmp_path, ci_coverage_args):
    """Run the actual slim-image copy/import check, remove it, then report."""
    source = tmp_path / "src" / "owned.py"
    source.parent.mkdir()
    source.write_text("FLAVOR = 'current'\ndef current_only():\n    return FLAVOR\n")
    (tmp_path / "packages").mkdir()
    (tmp_path / "pyproject.toml").write_bytes(CONFIG.read_bytes())
    (tmp_path / "pytest.ini").write_text("[pytest]\n")
    env = _coverage_env(tmp_path)
    driver = tmp_path / "test_sample.py"
    driver.write_text(
        "import runpy, shutil\nfrom pathlib import Path\n"
        "def test_sample():\n"
        f"    root = Path({str(tmp_path)!r})\n"
        "    current = runpy.run_path(str(root / 'src/owned.py'))\n"
        "    assert current['current_only']() == 'current'\n"
        f"    checks = runpy.run_path({str(ROOT / 'tests/unit/test_production_dockerfiles.py')!r})\n"
        "    image = root / 'image'\n"
        "    image.mkdir()\n"
        "    checks['test_actuator_image_copied_source_imports_both_processes_without_checkout_fallback'](image)\n"
        "    assert (image / 'src/loom/__init__.py').is_file()\n"
        "    shutil.rmtree(image)\n"
    )
    result = _run([sys.executable, "-B", "-m", "pytest", "-p", "no:cacheprovider", "-q",
        "-c", str(tmp_path / "pytest.ini"), "--rootdir", str(tmp_path),
        "--basetemp", str(tmp_path / "child-pytest"), *ci_coverage_args, str(driver)], cwd=tmp_path, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    combined_env = _combine(tmp_path, env)
    for command in ("xml", "report"):
        result = _run([sys.executable, "-B", "-m", "coverage", command, "--rcfile", str(CONFIG)],
            cwd=tmp_path, env=combined_env)
        assert result.returncode == 0, result.stdout + result.stderr
    result = _run([sys.executable, "-B", "-m", "coverage", "json", "--rcfile", str(CONFIG),
        "-o", str(tmp_path / "coverage.json")], cwd=tmp_path, env=combined_env)
    assert result.returncode == 0, result.stdout + result.stderr
    report = json.loads((tmp_path / "coverage.json").read_text())
    assert not any("image/src/" in path for path in report["files"])
    assert report["files"]["src/owned.py"]["executed_lines"] == [1, 2, 3]


@pytest.mark.parametrize("report", ["xml", "report"])
def test_other_missing_source_remains_an_aggregation_error(tmp_path, report):
    source = tmp_path / "src" / "owned.py"
    source.parent.mkdir()
    source.write_text("VALUE = 'ordinary current source'\n")
    driver = tmp_path / "sample.py"
    driver.write_text("import runpy\nrunpy.run_path('src/owned.py')\n")
    env = _coverage_env(tmp_path)
    result = _run([sys.executable, "-B", "-m", "coverage", "run", "--rcfile", str(CONFIG),
        "--source=src", str(driver)], cwd=tmp_path, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    source.unlink()
    combined_env = _combine(tmp_path, env)
    result = _run([sys.executable, "-B", "-m", "coverage", report, "--rcfile", str(CONFIG)],
        cwd=tmp_path, env=combined_env)
    assert result.returncode != 0
    assert "No source for code" in result.stdout + result.stderr
    assert "owned.py" in result.stdout + result.stderr
