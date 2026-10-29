"""CI placement and unique cross-language test ownership after Nebius cutover."""

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_candidate_matrix_runs_only_amd64(
    tmp_path: Path,
) -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())
    step = next(
        item for item in workflow["jobs"]["plan"]["steps"] if item.get("id") == "build-matrices"
    )
    rows = [
        {"image": image, "architecture": arch, "platform": f"linux/{arch}"}
        for image in ("service", "control-plane")
        for arch in ("amd64", "arm64")
    ]
    output = tmp_path / "output"
    result = subprocess.run(
        ["bash", "-c", step["run"]],
        text=True,
        capture_output=True,
        env={**os.environ, "NATIVE_BUILDS": json.dumps(rows), "GITHUB_OUTPUT": str(output)},
    )
    assert result.returncode == 0, result.stderr
    matrices = dict(line.split("=", 1) for line in output.read_text().splitlines())
    selected = json.loads(matrices["ordinary_builds"])
    assert selected == [row for row in rows if row["architecture"] == "amd64"]


@pytest.mark.parametrize("tree", ["src", "packages"])
def test_coverage_combines_real_data_from_all_checkout_roots(
    tmp_path, monkeypatch, tree
):
    import shutil

    import coverage

    checkout = tmp_path / "checkout"
    (checkout / tree).mkdir(parents=True)
    (checkout / tree / "sample.py").write_text("value = 1\n")
    shutil.copyfile(ROOT / "pyproject.toml", checkout / "pyproject.toml")
    monkeypatch.chdir(checkout)
    fragments = tmp_path / "coverage"
    fragments.mkdir()
    for index, base in enumerate(
        ["/home/runner/work/loom/loom", "/opt/actions-runner/_work/loom/loom",
         "/tmp/loom-hosted-worker-example/_work/loom/loom"]
    ):
        data = coverage.CoverageData(basename=str(fragments / f".coverage.{index}"))
        data.add_lines({f"{base}/{tree}/sample.py": {1}})
        data.write()
    collector = coverage.Coverage(
        data_file=str(checkout / ".coverage"), config_file=str(checkout / "pyproject.toml")
    )
    collector.combine(data_paths=[str(fragments)], strict=True)
    assert list(collector.get_data().measured_files()) == [str(checkout / tree / "sample.py")]
    assert collector.xml_report(outfile=str(checkout / "coverage.xml")) == 100
