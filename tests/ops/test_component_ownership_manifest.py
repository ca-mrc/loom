from __future__ import annotations

import ast
import json
import os
import re
import shlex
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import replace
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath

import pytest
import scripts.component_ownership as component_ownership

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_component_ownership_authority_files_exist() -> None:
    assert (REPO_ROOT / "config/component-ownership.toml").is_file()
    assert (REPO_ROOT / "scripts/component_ownership.py").is_file()


def test_path_matcher_is_segment_safe_and_supports_recursive_globs() -> None:
    matches_path = component_ownership.matches_path

    assert matches_path("tests/unit/test_example.py", "tests/unit/**/*.py")
    assert matches_path("tests/unit/nested/test_example.py", "tests/unit/**/*.py")
    assert not matches_path("tests/unit/nested/test_example.py", "tests/unit/*.py")
    assert not matches_path("tests/unit/test_example.pyc", "tests/unit/**/*.py")


def test_load_manifest_parses_typed_component_and_test_suite(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["tests-root"]
smoke_owners = ["cluster-smoke"]
scan_owners = ["image-scan"]
attestation_owners = ["release-attestation"]

[[components]]
id = "example"
kind = "release-image"
dockerfile = "deploy/Dockerfile.example"
build_context = "."
source_paths = ["deploy/Dockerfile.example", "src/example/**"]
smoke_owner = "cluster-smoke"
scan_owner = "image-scan"
attestation_owner = "release-attestation"
release_digest = "loom-example"
runtime_policy = "start"

[[test_suites]]
id = "python-fast"
language = "python"
lane = "tests-root"
include_paths = ["tests/unit/**/*.py"]
""".strip(),
        encoding="utf-8",
    )

    manifest = component_ownership.load_manifest(manifest_path)

    assert manifest.schema_version == 2
    assert manifest.smoke_owners == ("cluster-smoke",)
    assert manifest.scan_owners == ("image-scan",)
    assert manifest.attestation_owners == ("release-attestation",)
    assert manifest.components[0].release_digest == "loom-example"
    assert manifest.components[0].rollout_role == "none"
    assert manifest.test_suites[0].lane == "tests-root"
    assert manifest.execution_policies == ()
    assert manifest.execution_cases == ()
    assert manifest.test_suites[0].execution_policy is None
    assert manifest.test_sharding == ()


def test_load_manifest_rejects_unknown_keys(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "example"
kind = "release-image"
dockerfile = "deploy/Dockerfile.example"
build_context = "."
source_paths = ["deploy/Dockerfile.example"]
smoke_owner = "cluster-smoke"
scan_owner = "image-scan"
attestation_owner = "release-attestation"
release_digest = "loom-example"
runtime_policy = "start"
typo_owner = "ignored"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match=r"unknown keys.*typo_owner"):
        component_ownership.load_manifest(manifest_path)


@pytest.mark.parametrize("schema_version", ["true", "1.0"])
def test_load_manifest_rejects_non_integer_schema_version(
    tmp_path: Path,
    schema_version: str,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        f"schema_version = {schema_version}\n",
        encoding="utf-8",
    )

    with pytest.raises(
        component_ownership.ManifestError,
        match="schema_version must be the integer 2",
    ):
        component_ownership.load_manifest(manifest_path)


@pytest.mark.parametrize("unsafe_path", ["/absolute", "../escape", "dir\\file"])
def test_load_manifest_rejects_unsafe_repository_paths(
    tmp_path: Path,
    unsafe_path: str,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        f"""
schema_version = 2

[[components]]
id = "example"
kind = "release-image"
dockerfile = "{unsafe_path}"
build_context = "."
source_paths = ["deploy/Dockerfile.example"]
smoke_owner = "cluster-smoke"
scan_owner = "image-scan"
attestation_owner = "release-attestation"
release_digest = "loom-example"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match="safe repository-relative path"):
        component_ownership.load_manifest(manifest_path)


def test_legacy_runtime_payload_boolean_is_rejected(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["tests-root"]

[[test_suites]]
id = "fixture-tests"
language = "python"
lane = "tests-root"
runtime_payload = true
include_paths = ["tests/fixtures/**/*.py"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match="unknown keys: runtime_payload"):
        component_ownership.load_manifest(manifest_path)


def test_execution_policy_must_be_declared_and_assigned_to_a_lane(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["runtime-payload"]

[[execution_policies]]
id = "virtual-workspace-v1"
language = "python"
runner = "python-zero-arg-v1"
container_image = "python@sha256:baf89808ec37adeaab83cec287adb4a2afa4a11c1d51e961c7ec737877e61af6"
virtual_root = "/workspace"

[[test_suites]]
id = "fixture-tests"
language = "python"
lane = "runtime-payload"
execution_policy = "undeclared-policy"
include_paths = ["tests/fixtures/**/*.py"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        component_ownership.ManifestError,
        match="uses undeclared execution policy: undeclared-policy",
    ):
        component_ownership.load_manifest(manifest_path)


@pytest.mark.parametrize(
    "container_image",
    [
        "python:3.11-slim",
        "python@sha256:deadbeef",
        "Python@sha256:" + "a" * 64,
    ],
)
def test_execution_policy_requires_full_lowercase_image_digest(
    tmp_path: Path,
    container_image: str,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        f"""
schema_version = 2

[[execution_policies]]
id = "virtual-workspace-v1"
language = "python"
runner = "python-zero-arg-v1"
container_image = "{container_image}"
virtual_root = "/workspace"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(
        component_ownership.ManifestError,
        match="must be pinned by a full sha256 digest",
    ):
        component_ownership.load_manifest(manifest_path)


def test_execution_policy_id_requires_explicit_version(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[execution_policies]]
id = "virtual-workspace"
language = "python"
runner = "python-zero-arg-v1"
container_image = "python@sha256:baf89808ec37adeaab83cec287adb4a2afa4a11c1d51e961c7ec737877e61af6"
virtual_root = "/workspace"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match="explicit -vN version"):
        component_ownership.load_manifest(manifest_path)


def test_test_owner_matcher_applies_exclusions_and_fails_on_unknown_paths(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["integration", "integration-docker"]

[[test_suites]]
id = "integration-fast"
language = "python"
lane = "integration"
include_paths = ["tests/integration/**/*.py"]
exclude_paths = ["tests/integration/test_docker.py"]

[[test_suites]]
id = "integration-docker"
language = "python"
lane = "integration-docker"
include_paths = ["tests/integration/test_docker.py"]
""".strip(),
        encoding="utf-8",
    )
    manifest = component_ownership.load_manifest(manifest_path)

    assert manifest.test_owner_for_path("tests/integration/test_fast.py").lane == "integration"
    assert (
        manifest.test_owner_for_path("tests/integration/test_docker.py").lane
        == "integration-docker"
    )
    with pytest.raises(component_ownership.ManifestError, match="unowned test path"):
        manifest.test_owner_for_path("tests/new-suite/test_unknown.py")


def test_component_matcher_returns_all_affected_components(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "service"
kind = "release-image"
dockerfile = "deploy/Dockerfile.service"
build_context = "."
source_paths = ["deploy/Dockerfile.service", "src/shared/**"]
smoke_owner = "staging-smoke"
scan_owner = "image-scan"
attestation_owner = "release-attestation"
release_digest = "loom-service"
runtime_policy = "start"

[[components]]
id = "worker"
kind = "release-image"
dockerfile = "deploy/Dockerfile.worker"
build_context = "."
source_paths = ["deploy/Dockerfile.worker", "src/shared/**"]
smoke_owner = "staging-smoke"
scan_owner = "image-scan"
attestation_owner = "release-attestation"
release_digest = "loom-worker"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    manifest = component_ownership.load_manifest(manifest_path)

    assert [owner.id for owner in manifest.component_owners_for_path("src/shared/model.py")] == [
        "service",
        "worker",
    ]
    assert [
        owner.id for owner in manifest.component_owners_for_path("deploy/Dockerfile.worker")
    ] == ["worker"]


def test_neutral_bundle_checksum_rebuilds_every_consuming_image() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    owners = {
        owner.id
        for owner in manifest.component_owners_for_path(
            "packages/loom-bundle-checksum/loom_bundle_checksum/__init__.py"
        )
    }

    assert owners == {
        "control-plane",
        "family-orchestrator",
        "llm-gateway",
        "pipeline-orchestrator",
        "service",
        "worker",
    }


def test_validator_fails_closed_for_unowned_dockerfile(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text("schema_version = 2\n", encoding="utf-8")
    dockerfile = tmp_path / "deploy/Dockerfile.new-runtime"
    dockerfile.parent.mkdir()
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.new-runtime",),
    )

    assert "Dockerfile has no component owner: deploy/Dockerfile.new-runtime" in errors


@pytest.mark.parametrize(
    ("test_path", "language"),
    [
        ("tests/new_suite/test_unknown.py", "python"),
        ("deploy/catalog/new/tasks/example/tests/test_result.py", "python"),
        ("new-runtime/tests/helper.py", "python"),
        ("cmd/new-runtime/runtime_test.go", "go"),
        ("web/src/new-feature.test.tsx", "web"),
        ("new-ui/src/new-feature.spec.ts", "web"),
    ],
)
def test_validator_fails_closed_for_unowned_test_files(
    tmp_path: Path,
    test_path: str,
    language: str,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text("schema_version = 2\n", encoding="utf-8")
    target = tmp_path / test_path
    target.parent.mkdir(parents=True)
    target.write_text("# test\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=(test_path,),
    )

    assert f"{language} test has no CI owner: {test_path}" in errors


def test_vendor_projection_tests_are_component_inputs_not_loom_test_entries(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text("schema_version = 2\n", encoding="utf-8")
    test_path = "third_party/vendor/runtime/tests/test_upstream.py"
    target = tmp_path / test_path
    target.parent.mkdir(parents=True)
    target.write_text("def test_upstream(): pass\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=(test_path,),
    )

    assert errors == []


def test_validator_rejects_duplicate_release_digest_ownership(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "one"
kind = "release-image"
dockerfile = "deploy/Dockerfile.one"
build_context = "."
source_paths = ["deploy/Dockerfile.one"]
smoke_owner = "staging-smoke"
scan_owner = "image-scan"
attestation_owner = "release-attestation"
release_digest = "loom-duplicate"
runtime_policy = "start"

[[components]]
id = "two"
kind = "release-image"
dockerfile = "deploy/Dockerfile.two"
build_context = "."
source_paths = ["deploy/Dockerfile.two"]
smoke_owner = "staging-smoke"
scan_owner = "image-scan"
attestation_owner = "release-attestation"
release_digest = "loom-duplicate"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    for name in ("one", "two"):
        target = tmp_path / f"deploy/Dockerfile.{name}"
        target.parent.mkdir(exist_ok=True)
        target.write_text("FROM scratch\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.one", "deploy/Dockerfile.two"),
    )

    assert "release digest has multiple component owners: loom-duplicate: one, two" in errors


@pytest.mark.parametrize(
    ("kind", "runtime_policy", "release_digest_line"),
    [
        ("unknown", "start", 'release_digest = "loom-example"'),
        ("release-image", "runtime-payload", 'release_digest = "loom-example"'),
        ("release-image", "start", ""),
        ("runtime-payload-image", "runtime-payload", 'release_digest = "loom-example"'),
        ("runtime-payload-image", "start", ""),
    ],
)
def test_component_kind_enforces_release_and_runtime_policy_contract(
    tmp_path: Path,
    kind: str,
    runtime_policy: str,
    release_digest_line: str,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        f"""
schema_version = 2

[[components]]
id = "example"
kind = "{kind}"
dockerfile = "deploy/Dockerfile.example"
build_context = "."
source_paths = ["deploy/Dockerfile.example"]
smoke_owner = "staging-smoke"
scan_owner = "image-scan"
attestation_owner = "release-attestation"
{release_digest_line}
runtime_policy = "{runtime_policy}"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match="component kind contract"):
        component_ownership.load_manifest(manifest_path)


def test_repository_manifest_owns_every_dockerfile_and_test() -> None:
    tracked_paths = tuple(
        subprocess.check_output(
            ["git", "ls-files"],
            cwd=REPO_ROOT,
            text=True,
        ).splitlines()
    )
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=REPO_ROOT,
        tracked_paths=tracked_paths,
    )

    assert errors == []
    tracked_dockerfiles = {
        path
        for path in tracked_paths
        if PurePosixPath(path).name == "Dockerfile"
        or PurePosixPath(path).name.startswith("Dockerfile.")
    }
    release_dockerfiles = {
        path for path in tracked_dockerfiles if path.startswith("deploy/Dockerfile.")
    }
    payload_dockerfiles = tracked_dockerfiles - release_dockerfiles
    assert {
        item.dockerfile for item in manifest.components if item.kind == "release-image"
    } == release_dockerfiles
    assert {
        item.dockerfile for item in manifest.components if item.kind == "runtime-payload-image"
    } == payload_dockerfiles


def test_validator_requires_any_docker_marked_pytest_module_in_docker_lane(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["tests-root"]

[[test_suites]]
id = "root-fast"
language = "python"
lane = "tests-root"
include_paths = ["tests/unit/**/*.py"]
""".strip(),
        encoding="utf-8",
    )
    test_path = "tests/unit/test_docker_runtime.py"
    target = tmp_path / test_path
    target.parent.mkdir(parents=True)
    target.write_text("pytestmark = pytest.mark.docker\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=(test_path,),
    )

    assert (
        "docker-marked pytest module must use integration-docker lane: "
        "tests/unit/test_docker_runtime.py: tests-root" in errors
    )


def test_marker_free_ascii_source_is_parsed_without_ast_walk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parse = ast.parse
    walk = ast.walk
    parsed: list[str] = []
    walked: list[ast.AST] = []

    def record_parse(source: str) -> ast.AST:
        parsed.append(source)
        return parse(source)

    def record_walk(tree: ast.AST) -> Iterator[ast.AST]:
        walked.append(tree)
        return walk(tree)

    source = "def test_example():\n    assert 1 + 2 == 3\n"
    with monkeypatch.context() as patch:
        patch.setattr(ast, "parse", record_parse)
        patch.setattr(ast, "walk", record_walk)
        assert not component_ownership._uses_pytest_docker_marker(source)

    assert parsed == [source]
    assert walked == []


@pytest.mark.parametrize("source", ["def broken(:\n", "value = (\n"])
def test_marker_free_ascii_syntax_errors_are_still_rejected(source: str) -> None:
    with pytest.raises(SyntaxError):
        component_ownership._uses_pytest_docker_marker(source)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("pytestmark = pytest.mark.ｄｏｃｋｅｒ\n", True),
        ("π = 3\n", False),
        ("pytestmark = pytest.mark.docker\n", True),
        ("if False:\n    pytest.mark.docker\n", True),
        ('value = f"{pytest.mark.docker}"\n', True),
        ('value = "pytest.mark.docker"\n', False),
        ("# pytest.mark.docker\nvalue = 1\n", False),
        ("pytestmark = other.mark.docker\n", False),
        ("pytestmark = pytest.other.docker\n", False),
    ],
)
def test_possible_docker_marker_sources_retain_exact_ast_inspection(
    monkeypatch: pytest.MonkeyPatch, source: str, expected: bool
) -> None:
    walk = ast.walk
    walked: list[ast.AST] = []

    def record_walk(tree: ast.AST) -> Iterator[ast.AST]:
        walked.append(tree)
        return walk(tree)

    with monkeypatch.context() as patch:
        patch.setattr(ast, "walk", record_walk)
        assert component_ownership._uses_pytest_docker_marker(source) is expected

    assert len(walked) == 1


def test_validator_ignores_docker_marker_text_inside_strings(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["tests-root"]

[[test_suites]]
id = "root-fast"
language = "python"
lane = "tests-root"
include_paths = ["tests/unit/**/*.py"]
""".strip(),
        encoding="utf-8",
    )
    test_path = "tests/unit/test_marker_documentation.py"
    target = tmp_path / test_path
    target.parent.mkdir(parents=True)
    target.write_text(
        'DOCUMENTED_MARKER = "pytest.mark.docker"\n\n'
        "def test_example() -> None:\n"
        "    assert DOCUMENTED_MARKER\n",
        encoding="utf-8",
    )
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=(test_path,),
    )

    assert errors == []


def test_cli_validate_and_query_are_fail_closed() -> None:
    script = REPO_ROOT / "scripts/component_ownership.py"
    validate = subprocess.run(
        [sys.executable, str(script), "validate"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    known = subprocess.run(
        [sys.executable, str(script), "query", "deploy/Dockerfile.worker"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    unknown = subprocess.run(
        [sys.executable, str(script), "query", "new-runtime/unknown.bin"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert validate.returncode == 0, validate.stderr
    assert re.fullmatch(
        r"validated \d+ components and \d+ tracked test files\n",
        validate.stdout,
    )
    assert known.returncode == 0, known.stderr
    assert [item["id"] for item in json.loads(known.stdout)["components"]] == ["worker"]
    assert unknown.returncode != 0
    assert "unowned path: new-runtime/unknown.bin" in unknown.stderr


def test_validator_rejects_duplicate_component_ids(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "duplicate"
kind = "release-image"
dockerfile = "deploy/Dockerfile.one"
build_context = "."
source_paths = ["deploy/Dockerfile.one"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-one"
runtime_policy = "start"

[[components]]
id = "duplicate"
kind = "release-image"
dockerfile = "deploy/Dockerfile.two"
build_context = "."
source_paths = ["deploy/Dockerfile.two"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-two"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    for name in ("one", "two"):
        target = tmp_path / f"deploy/Dockerfile.{name}"
        target.parent.mkdir(exist_ok=True)
        target.write_text("FROM scratch\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.one", "deploy/Dockerfile.two"),
    )

    assert "component id has multiple definitions: duplicate" in errors


def test_validator_rejects_component_dockerfile_that_is_not_tracked(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "missing"
kind = "release-image"
dockerfile = "deploy/Dockerfile.missing"
build_context = "."
source_paths = ["deploy/Dockerfile.missing"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-missing"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=(),
    )

    assert "component Dockerfile is not tracked: missing: deploy/Dockerfile.missing" in errors


def test_validator_rejects_missing_build_context(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "example"
kind = "release-image"
dockerfile = "deploy/Dockerfile.example"
build_context = "missing-context"
source_paths = ["deploy/Dockerfile.example"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-example"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    dockerfile = tmp_path / "deploy/Dockerfile.example"
    dockerfile.parent.mkdir()
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.example",),
    )

    assert "component build context is not a directory: example: missing-context" in errors


def test_validator_requires_source_paths_to_own_component_dockerfile(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "example"
kind = "release-image"
dockerfile = "deploy/Dockerfile.example"
build_context = "."
source_paths = ["src/example/**"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-example"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    dockerfile = tmp_path / "deploy/Dockerfile.example"
    dockerfile.parent.mkdir()
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.example",),
    )

    assert (
        "component source paths do not own Dockerfile: example: deploy/Dockerfile.example" in errors
    )


@pytest.mark.parametrize(
    ("dockerfile_path", "kind", "policy", "digest_line", "expected_kind"),
    [
        (
            "deploy/Dockerfile.example",
            "runtime-payload-image",
            "runtime-payload",
            "",
            "release-image",
        ),
        (
            "tests/fixtures/task/Dockerfile",
            "release-image",
            "conformance",
            'release_digest = "loom-fixture"',
            "runtime-payload-image",
        ),
    ],
)
def test_validator_derives_release_or_payload_kind_from_dockerfile_location(
    tmp_path: Path,
    dockerfile_path: str,
    kind: str,
    policy: str,
    digest_line: str,
    expected_kind: str,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        f"""
schema_version = 2

[[components]]
id = "example"
kind = "{kind}"
dockerfile = "{dockerfile_path}"
build_context = "."
source_paths = ["{dockerfile_path}"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
{digest_line}
runtime_policy = "{policy}"
""".strip(),
        encoding="utf-8",
    )
    dockerfile = tmp_path / dockerfile_path
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=(dockerfile_path,),
    )

    assert (
        f"component kind does not match Dockerfile location: example: {kind}; "
        f"expected {expected_kind}" in errors
    )


def test_release_image_validator_rejects_digest_name_mismatch(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "service"
kind = "release-image"
dockerfile = "deploy/Dockerfile.service"
build_context = "."
source_paths = ["deploy/Dockerfile.service"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-wrong"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_release_image_ownership(
        manifest,
        (("loom-service", "deploy/Dockerfile.service"),),
    )

    assert (
        "release image digest mismatch: deploy/Dockerfile.service: "
        "manifest=loom-wrong consumer=loom-service" in errors
    )


def test_rollout_release_images_have_exact_manifest_owner() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    rollout_images = component_ownership.release_images_for_runtime_policy(
        manifest,
        runtime_policy="start",
    )

    assert (
        component_ownership.validate_release_image_ownership(
            manifest,
            tuple((entry["image_name"], entry["dockerfile"]) for entry in rollout_images),
        )
        == []
    )
    assert len(rollout_images) == 10


def test_rollout_roles_define_exact_primary_and_auxiliary_sets() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    primary = component_ownership.release_images_for_rollout_role(
        manifest,
        rollout_role="primary",
    )
    auxiliary = component_ownership.release_images_for_rollout_role(
        manifest,
        rollout_role="auxiliary",
    )
    primary_names = {entry["image_name"] for entry in primary}
    auxiliary_names = {entry["image_name"] for entry in auxiliary}

    assert len(primary) == 10
    assert len(auxiliary) == 1
    assert not primary_names & auxiliary_names
    assert auxiliary_names == {
        "loom-rehearsal-postgres",
    }
    assert {
        component.release_digest
        for component in manifest.release_components()
        if "sandbox" in component.id
    }.isdisjoint(primary_names | auxiliary_names)


def test_release_image_matrix_is_derived_from_all_release_components() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    matrix = component_ownership.release_image_matrix(manifest)

    assert len(matrix) == 14
    assert {entry["image_name"] for entry in matrix} == {
        component.release_digest for component in manifest.release_components()
    }
    assert all(entry["context"] == "." for entry in matrix)


def test_execution_actuator_image_owns_capacity_collector_source() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    owners = manifest.component_owners_for_path("src/loom_execution_capacity_collector/nebius.py")
    assert "execution-actuator" in {component.id for component in owners}
    selected = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=("src/loom_execution_capacity_collector/nebius.py",),
        force_all=False,
    )
    assert any(item["image"] == "execution-actuator" for item in selected)


@pytest.mark.parametrize("module", ["execution_requirements", "mutable_paths", "sandbox_identity"])
def test_task_declaration_changes_select_actuator_image(module: str) -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    selected = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=(f"src/loom/{module}.py",),
        force_all=False,
    )
    assert any(item["image"] == "execution-actuator" for item in selected)


@pytest.mark.parametrize("module", ["execution_diagnosis_store", "execution_failure_diagnosis"])
def test_native_diagnosis_changes_select_actuator_image(module: str) -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    selected = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=(f"src/loom/{module}.py",),
        force_all=False,
        image_set="nebius",
        fallback_all=True,
    )
    assert any(item["image"] == "execution-actuator" for item in selected)


def test_pipeline_core_fixture_is_conformance_only_and_never_a_rollout_image() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    component = next(
        item for item in manifest.release_components() if item.id == "pipeline-core-fixture"
    )

    assert component.dockerfile == "deploy/Dockerfile.pipeline-core-fixture"
    assert component.release_digest == "loom-pipeline-core-fixture"
    assert component.runtime_policy == "conformance"
    assert component.rollout_role == "none"
    assert component.smoke_owner == "worker-runtime-conformance"
    assert component.scan_owner == "images"
    assert component.attestation_owner == "release-provenance"
    assert component.release_digest not in {
        entry["image_name"]
        for entry in component_ownership.release_images_for_rollout_role(
            manifest,
            rollout_role="primary",
        )
    }
    assert component.release_digest not in {
        entry["image_name"]
        for entry in component_ownership.release_images_for_rollout_role(
            manifest,
            rollout_role="auxiliary",
        )
    }


def test_native_release_image_matrix_crosses_every_image_with_both_architectures() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    images = component_ownership.release_image_matrix(manifest)
    matrix = component_ownership.native_release_image_matrix(images)

    assert len(matrix) == len(images) * 2
    assert {(entry["architecture"], entry["platform"]) for entry in matrix} == {
        ("amd64", "linux/amd64"),
        ("arm64", "linux/arm64"),
    }
    for image in images:
        matching = [entry for entry in matrix if entry["image"] == image["image"]]
        assert [entry["architecture"] for entry in matching] == ["amd64", "arm64"]
        assert all({key: entry[key] for key in image} == image for entry in matching)


def test_behavior_stage1_image_is_dormant_and_excluded_from_ci_planning() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    component = next(item for item in manifest.components if item.id == "behavior-stage1-sim")

    assert component.ci_enabled is False
    assert component.platforms == ("linux/amd64",)
    assert component not in manifest.release_components()
    assert (
        component_ownership.select_release_image_matrix(
            manifest,
            changed_paths=("deploy/Dockerfile.behavior-stage1-sim",),
            force_all=False,
        )
        == ()
    )
    assert all(
        item["image"] != "behavior-stage1-sim"
        for item in component_ownership.select_release_image_matrix(
            manifest,
            changed_paths=(),
            force_all=True,
        )
    )


def test_release_image_selection_uses_manifest_source_ownership() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    matrix = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=("deploy/nginx-spa-security-headers.conf",),
        force_all=False,
    )

    assert matrix == (
        {
            "image": "web",
            "image_name": "loom-web",
            "dockerfile": "deploy/Dockerfile.web",
            "context": ".",
        },
    )


def test_rehearsal_postgres_dockerfile_selects_its_conformance_image() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    matrix = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=("deploy/Dockerfile.rehearsal-postgres",),
        force_all=False,
    )

    assert matrix == (
        {
            "image": "rehearsal-postgres",
            "image_name": "loom-rehearsal-postgres",
            "dockerfile": "deploy/Dockerfile.rehearsal-postgres",
            "context": ".",
        },
    )


def test_release_image_selection_fails_safe_for_authority_changes() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    matrix = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=("config/component-ownership.toml",),
        force_all=False,
    )

    assert matrix == component_ownership.release_image_matrix(manifest)


def test_each_release_dockerfile_selects_its_owned_build_and_required_companions() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    for component in manifest.release_components():
        matrix = component_ownership.select_release_image_matrix(
            manifest,
            changed_paths=(component.dockerfile,),
            force_all=False,
        )

        expected = {component.id}
        if component.id == "service":
            expected.add("execution-runtime")
        assert {entry["image"] for entry in matrix} == expected


def test_service_release_selects_exact_nebius_runtime_companion() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    matrix = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=("database/migrations/versions/0129_service_execution_materialization.py",),
        force_all=False,
    )

    selected = {entry["image"] for entry in matrix}
    assert "service" in selected
    assert "execution-runtime" in selected


def test_release_image_pair_rejects_matrix_tampering() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    errors = component_ownership.validate_release_image_pair(
        manifest,
        image="worker",
        image_name="loom-service",
        dockerfile="deploy/Dockerfile.worker",
        build_context=".",
    )

    assert errors == [
        "release image matrix row differs from manifest: worker: "
        "observed=('loom-service', 'deploy/Dockerfile.worker', '.') "
        "expected=('loom-worker', 'deploy/Dockerfile.worker', '.')"
    ]


@pytest.mark.parametrize(
    "lane",
    [
        "tests-root",
        "tests-packages",
        "integration",
        "integration-docker",
        "runtime-payload",
    ],
)
def test_required_ci_lane_paths_are_derived_from_manifest(lane: str) -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    tracked_paths = component_ownership._tracked_paths(REPO_ROOT)

    paths = component_ownership.test_paths_for_lane(
        manifest,
        tracked_paths=tracked_paths,
        lane=lane,
    )

    assert paths
    assert all(manifest.test_owner_for_path(path).lane == lane for path in paths)


def test_runtime_payload_lane_paths_are_exactly_policy_owned() -> None:
    manifest = component_ownership.load_manifest(
        REPO_ROOT / "config/component-ownership.toml",
    )
    tracked_paths = component_ownership._tracked_paths(REPO_ROOT)
    lane_paths = component_ownership.test_paths_for_lane(
        manifest,
        tracked_paths=tracked_paths,
        lane="runtime-payload",
    )

    assert len(lane_paths) == 10
    policy_paths = {
        path
        for policy in manifest.execution_policies
        for path in component_ownership.test_paths_for_policy(
            manifest,
            tracked_paths=tracked_paths,
            policy=policy.id,
        )
    }
    assert len(policy_paths) == 10
    assert policy_paths == set(lane_paths)
    assert all(
        manifest.test_owner_for_path(path).execution_policy is not None for path in policy_paths
    )


def test_root_lane_includes_previously_unexecuted_owned_directories() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    paths = component_ownership.test_paths_for_lane(
        manifest,
        tracked_paths=component_ownership._tracked_paths(REPO_ROOT),
        lane="tests-root",
    )

    assert any(path.startswith("tests/loom_config/") for path in paths)
    assert any(path.startswith("tests/loom_egress_xds/") for path in paths)


def test_behavior_tests_are_owned_but_excluded_from_ci_lanes() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    tracked_paths = component_ownership._tracked_paths(REPO_ROOT)
    behavior_tests = {
        path
        for path in tracked_paths
        if component_ownership._is_runnable_test_path(path)
        and (
            path.startswith("tests/integrations/behavior/")
            or "behavior" in Path(path).name
            or "stage1" in Path(path).name
        )
    }

    assert behavior_tests
    assert all(not manifest.test_owner_for_path(path).ci_enabled for path in behavior_tests)
    selected = {
        path
        for lane in manifest.ci_lanes
        for path in component_ownership.test_paths_for_lane(
            manifest,
            tracked_paths=tracked_paths,
            lane=lane,
        )
    }
    assert behavior_tests.isdisjoint(selected)


def test_behavior_frontend_sources_are_excluded_from_coverage_gate() -> None:
    vite_config = (REPO_ROOT / "web/vite.config.ts").read_text(encoding="utf-8")

    assert '"src/components/artifacts/BehaviorRollout*.tsx"' in vite_config
    assert '"src/components/artifacts/useBoundedJson.ts"' in vite_config


@pytest.mark.parametrize("lane", ["tests-root", "integration", "cluster-smoke"])
def test_manifest_lane_shards_are_disjoint_and_complete(lane: str) -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    policy = manifest.test_shard_policy(lane)
    assert policy is not None
    paths = component_ownership.test_paths_for_lane(
        manifest,
        tracked_paths=component_ownership._tracked_paths(REPO_ROOT),
        lane=lane,
    )

    shards = [
        set(
            component_ownership.shard_paths(
                paths,
                shard_index=index,
                shard_count=policy.shard_count,
                strategy=policy.strategy,
                salt=policy.salt,
                pins=policy.pins,
            )
        )
        for index in range(policy.shard_count)
    ]

    assert all(shard.isdisjoint(other) for index, shard in enumerate(shards) for other in shards[index + 1:])
    assert set().union(*shards) == set(paths)


def test_stable_hash_sharding_is_order_independent_and_has_zero_insertion_churn() -> None:
    paths = tuple(f"tests/unit/test_{index:03d}.py" for index in range(20))

    def assignments(candidates: tuple[str, ...]) -> dict[str, int]:
        return {
            path: shard_index
            for shard_index in range(2)
            for path in component_ownership.shard_paths(
                candidates,
                shard_index=shard_index,
                shard_count=2,
                strategy="stable-hash",
                salt="tests-root-v2-8275",
            )
        }

    original = assignments(paths)
    reversed_order = assignments(tuple(reversed(paths)))
    expanded = assignments(("tests/unit/test_000_early.py", *paths))

    assert reversed_order == original
    assert {path: expanded[path] for path in paths} == original


def test_manifest_root_shard_salt_is_bound_to_measured_collection_balance() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    policy = manifest.test_shard_policy("tests-root")

    assert policy is not None
    assert policy.strategy == "stable-hash"
    assert policy.salt == "tests-root-v2-8275"
    assert {(pin.path, pin.shard_index) for pin in policy.pins} == {
        ("tests/ops/test_ci_secret_isolation.py", 0),
        ("tests/ops/test_component_ownership_manifest.py", 1),
        ("tests/ops/test_nebius_pool_role_restoration_live.py", 4),
        ("tests/ops/test_nebius_pool_template_restoration_live.py", 5),
        ("tests/ops/test_nebius_pool_predecessor_live.py", 7),
        ("tests/ops/test_nebius_pool_refresh_connected.py", 3),
    }


def test_manifest_integration_shards_preserve_auth_schema_order() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    policy = manifest.test_shard_policy("integration")

    assert policy is not None
    assert policy.shard_count == 4
    assert policy.strategy == "stable-hash"
    assert policy.salt == "integration-v3-4"
    assert {(pin.path, pin.shard_index) for pin in policy.pins} == {
        ("tests/integration/test_username_password_auth.py", 3),
        ("tests/integration/test_username_password_schema.py", 3),
    }


def test_manifest_rejects_duplicate_test_shard_pins(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["integration"]

[[test_sharding]]
lane = "integration"
shard_count = 2
strategy = "contiguous"
pins = [
  { path = "tests/integration/test_duplicate.py", shard_index = 0 },
  { path = "tests/integration/test_duplicate.py", shard_index = 1 },
]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match="duplicate paths"):
        component_ownership.load_manifest(manifest_path)


@pytest.mark.parametrize(
    ("path", "message"),
    [
        ("tests/integration/test_missing.py", "pin path is not tracked"),
        ("tests/unit/test_worker_cmd.py", "pin crosses CI lane"),
    ],
)
def test_manifest_validation_rejects_missing_or_cross_lane_shard_pins(
    path: str,
    message: str,
) -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    integration = manifest.test_shard_policy("integration")
    assert integration is not None
    invalid_policy = replace(
        integration,
        pins=(*integration.pins, component_ownership.TestShardPin(path=path, shard_index=1)),
    )
    invalid_manifest = replace(
        manifest,
        test_sharding=tuple(
            invalid_policy if policy.lane == "integration" else policy
            for policy in manifest.test_sharding
        ),
    )

    errors = component_ownership.validate_manifest(
        invalid_manifest,
        repo_root=REPO_ROOT,
        tracked_paths=component_ownership._tracked_paths(REPO_ROOT),
    )

    assert any(message in error and path in error for error in errors)


def test_contiguous_sharding_limits_assignment_churn_from_a_new_early_path() -> None:
    paths = tuple(f"tests/integration/test_{index:03d}.py" for index in range(20))
    original = {
        path: shard_index
        for shard_index in range(2)
        for path in component_ownership.shard_paths(
            paths,
            shard_index=shard_index,
            shard_count=2,
            strategy="contiguous",
        )
    }
    expanded = ("tests/integration/test_000_new.py", *paths)
    updated = {
        path: shard_index
        for shard_index in range(2)
        for path in component_ownership.shard_paths(
            expanded,
            shard_index=shard_index,
            shard_count=2,
            strategy="contiguous",
        )
    }

    assert sum(original[path] != updated[path] for path in paths) <= 1


def test_lane_execution_excludes_conftest_and_helper_modules() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    paths = component_ownership.test_paths_for_lane(
        manifest,
        tracked_paths=component_ownership._tracked_paths(REPO_ROOT),
        lane="tests-packages",
    )

    assert not any(Path(path).name == "conftest.py" for path in paths)
    assert all(
        Path(path).name.startswith("test_") or Path(path).name.endswith("_test.py")
        for path in paths
    )


def test_test_path_query_rejects_undeclared_lane() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")

    with pytest.raises(component_ownership.ManifestError, match="undeclared CI lane"):
        component_ownership.test_paths_for_lane(
            manifest,
            tracked_paths=(),
            lane="typo-lane",
        )

    with pytest.raises(
        component_ownership.ManifestError,
        match="undeclared execution policy",
    ):
        component_ownership.test_paths_for_policy(
            manifest,
            tracked_paths=(),
            policy="typo-policy",
        )


def test_load_manifest_rejects_unknown_test_language(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["unknown"]

[[test_suites]]
id = "unknown"
language = "rust"
lane = "unknown"
include_paths = ["tests/**/*.rs"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match="language must be one of"):
        component_ownership.load_manifest(manifest_path)


def test_validator_requires_dockerfile_inside_build_context(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "example"
kind = "release-image"
dockerfile = "deploy/Dockerfile.example"
build_context = "other-context"
source_paths = ["deploy/Dockerfile.example"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-example"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    dockerfile = tmp_path / "deploy/Dockerfile.example"
    dockerfile.parent.mkdir()
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    (tmp_path / "other-context").mkdir()
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.example",),
    )

    assert (
        "component Dockerfile is outside build context: example: "
        "deploy/Dockerfile.example not under other-context" in errors
    )


def test_cli_query_rejects_globally_invalid_manifest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        (REPO_ROOT / "config/component-ownership.toml").read_text(encoding="utf-8")
        + """

[[components]]
id = "worker"
kind = "release-image"
dockerfile = "deploy/Dockerfile.worker"
build_context = "."
source_paths = ["deploy/Dockerfile.worker"]
smoke_owner = "worker-runtime-conformance"
scan_owner = "images"
attestation_owner = "release-provenance"
release_digest = "loom-worker-copy"
runtime_policy = "start"
""",
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts/component_ownership.py"),
            "--manifest",
            str(manifest_path),
            "query",
            "deploy/Dockerfile.worker",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "component id has multiple definitions: worker" in result.stderr


def test_release_image_validator_rejects_duplicate_consumer_pairs(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "service"
kind = "release-image"
dockerfile = "deploy/Dockerfile.service"
build_context = "."
source_paths = ["deploy/Dockerfile.service"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-service"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_release_image_ownership(
        manifest,
        (
            ("loom-service", "deploy/Dockerfile.service"),
            ("loom-service", "deploy/Dockerfile.service"),
        ),
    )

    assert "consumer release image name is duplicated: loom-service" in errors
    assert "consumer release Dockerfile is duplicated: deploy/Dockerfile.service" in errors


def test_load_manifest_rejects_lane_not_declared_in_registry(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["integration"]

[[test_suites]]
id = "integration-fast"
language = "python"
lane = "integrtaion"
include_paths = ["tests/integration/**/*.py"]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match="undeclared CI lane"):
        component_ownership.load_manifest(manifest_path)


def test_validator_rejects_stale_component_source_pattern(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2

[[components]]
id = "service"
kind = "release-image"
dockerfile = "deploy/Dockerfile.service"
build_context = "."
source_paths = ["deploy/Dockerfile.service", "src/missing.py"]
smoke_owner = "smoke"
scan_owner = "scan"
attestation_owner = "attest"
release_digest = "loom-service"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    dockerfile = tmp_path / "deploy/Dockerfile.service"
    dockerfile.parent.mkdir()
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.service",),
    )

    assert "component source pattern matches no tracked path: service: src/missing.py" in errors


def test_validator_rejects_stale_test_include_and_exclude_patterns(tmp_path: Path) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["tests-root"]

[[test_suites]]
id = "root"
language = "python"
lane = "tests-root"
include_paths = ["tests/**/*.py", "tests/missing/**/*.py"]
exclude_paths = ["tests/removed.py"]
""".strip(),
        encoding="utf-8",
    )
    test_path = "tests/test_present.py"
    target = tmp_path / test_path
    target.parent.mkdir()
    target.write_text("def test_present(): pass\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=(test_path,),
    )

    assert "test include pattern matches no tracked test: root: tests/missing/**/*.py" in errors
    assert "test exclude pattern matches no tracked test: root: tests/removed.py" in errors


@pytest.mark.parametrize(
    "field",
    ["id", "smoke_owner", "scan_owner", "attestation_owner"],
)
def test_load_manifest_rejects_non_slug_component_identity_fields(
    tmp_path: Path,
    field: str,
) -> None:
    values = {
        "id": "example",
        "smoke_owner": "smoke",
        "scan_owner": "scan",
        "attestation_owner": "attest",
    }
    values[field] = "Not Allowed"
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        f"""
schema_version = 2

[[components]]
id = "{values["id"]}"
kind = "release-image"
dockerfile = "deploy/Dockerfile.example"
build_context = "."
source_paths = ["deploy/Dockerfile.example"]
smoke_owner = "{values["smoke_owner"]}"
scan_owner = "{values["scan_owner"]}"
attestation_owner = "{values["attestation_owner"]}"
release_digest = "loom-example"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(component_ownership.ManifestError, match="lowercase slug"):
        component_ownership.load_manifest(manifest_path)


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("smoke_owner", "stagin-smoke", "component smoke owner is undeclared"),
        ("scan_owner", "imagse", "component scan owner is undeclared"),
        (
            "attestation_owner",
            "release-provennce",
            "component attestation owner is undeclared",
        ),
    ],
)
def test_validator_rejects_undeclared_component_owner(
    tmp_path: Path,
    field: str,
    value: str,
    expected: str,
) -> None:
    owners = {
        "smoke_owner": "staging-smoke",
        "scan_owner": "images",
        "attestation_owner": "release-provenance",
    }
    owners[field] = value
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        f"""
schema_version = 2
smoke_owners = ["staging-smoke"]
scan_owners = ["images"]
attestation_owners = ["release-provenance"]

[[components]]
id = "service"
kind = "release-image"
dockerfile = "deploy/Dockerfile.service"
build_context = "."
source_paths = ["deploy/Dockerfile.service"]
smoke_owner = "{owners["smoke_owner"]}"
scan_owner = "{owners["scan_owner"]}"
attestation_owner = "{owners["attestation_owner"]}"
release_digest = "loom-service"
runtime_policy = "start"
""".strip(),
        encoding="utf-8",
    )
    dockerfile = tmp_path / "deploy/Dockerfile.service"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.service",),
    )

    assert any(expected in error and value in error for error in errors)


def test_validator_rejects_unused_lane_and_component_owner_registry_entries(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "component-ownership.toml"
    manifest_path.write_text(
        """
schema_version = 2
ci_lanes = ["tests-root", "unused-lane"]
smoke_owners = ["staging-smoke", "unused-smoke"]
scan_owners = ["images", "unused-scan"]
attestation_owners = ["release-provenance", "unused-attestation"]

[[components]]
id = "service"
kind = "release-image"
dockerfile = "deploy/Dockerfile.service"
build_context = "."
source_paths = ["deploy/Dockerfile.service"]
smoke_owner = "staging-smoke"
scan_owner = "images"
attestation_owner = "release-provenance"
release_digest = "loom-service"
runtime_policy = "start"

[[test_suites]]
id = "root-fast"
language = "python"
lane = "tests-root"
include_paths = ["tests/unit/**/*.py"]
""".strip(),
        encoding="utf-8",
    )
    dockerfile = tmp_path / "deploy/Dockerfile.service"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    test_path = tmp_path / "tests/unit/test_present.py"
    test_path.parent.mkdir(parents=True)
    test_path.write_text("def test_present() -> None: pass\n", encoding="utf-8")
    manifest = component_ownership.load_manifest(manifest_path)

    errors = component_ownership.validate_manifest(
        manifest,
        repo_root=tmp_path,
        tracked_paths=("deploy/Dockerfile.service", "tests/unit/test_present.py"),
    )

    assert "CI lane has no test suite owner: unused-lane" in errors
    assert "smoke owner has no component: unused-smoke" in errors
    assert "scan owner has no component: unused-scan" in errors
    assert "attestation owner has no component: unused-attestation" in errors


@pytest.mark.parametrize("script", ["component_ownership.py", "plan_ci_validations.py"])
def test_ci_planners_start_before_package_installation(script):
    environment = {key: value for key, value in os.environ.items() if key != "PYTHONPATH"}
    result = subprocess.run([sys.executable, "-S", "scripts/" + script, "--help"],
                            env=environment, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_guard_migration_changes_rebuild_both_consuming_images() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    matrix = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=("database/capacity_guard_migrations/env.py",),
        force_all=False,
        image_set="nebius",
    )
    assert {"control-plane", "service"} <= {entry["image"] for entry in matrix}


def _local_copy_sources(dockerfile: str) -> Iterator[str]:
    # All retained Dockerfiles use shell COPY, including flags and continuations.
    for line in dockerfile.replace("\\\n", " ").splitlines():
        instruction = line.strip().split(maxsplit=1)
        if not instruction or instruction[0].upper() != "COPY":
            continue
        arguments = shlex.split(instruction[1], comments=True)
        if any(word.startswith("--from=") for word in arguments):
            continue
        assert not any(word.startswith("[") for word in arguments), "JSON COPY needs parsing"
        yield from (word for word in arguments[:-1] if not word.startswith("--"))


def _copy_includes(path: str, source: str) -> bool:
    # Docker globs match individual path segments; a matched directory copies
    # all descendants. Unlike fnmatch on a full path, '*' cannot cross '/'.
    pattern = PurePosixPath(source).parts
    parts = PurePosixPath(path).parts
    return len(parts) >= len(pattern) and all(
        fnmatchcase(part, glob) for part, glob in zip(parts, pattern, strict=False)
    )


def test_copy_input_matching_handles_globs_and_stages() -> None:
    dockerfile = "COPY --chmod=0444 ./src/*.py \\\n    ./dest/\nCOPY assets/* /assets/\nCOPY --from=build /dest /app\n"
    assert list(_local_copy_sources(dockerfile)) == ["./src/*.py", "assets/*"]
    assert _copy_includes("src/main.py", "./src/*.py")
    assert not _copy_includes("src/nested/main.py", "./src/*.py")
    assert _copy_includes("assets/images/logo.png", "assets/*")
    assert _copy_includes("src/main.py", ".")
    assert not _copy_includes("src-other/main.py", "src")


def test_all_image_ownership_covers_every_local_copy_input() -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    tracked = (
        subprocess.check_output(
            ["git", "ls-files", "-z"],
            cwd=REPO_ROOT,
            text=True,
        )
        .rstrip("\0")
        .split("\0")
    )
    missing = []
    for component in manifest.components:
        context = PurePosixPath(component.build_context)
        context_paths = {
            path: PurePosixPath(path).relative_to(context).as_posix()
            for path in tracked
            if PurePosixPath(path).is_relative_to(context)
        }
        dockerfile = (REPO_ROOT / component.dockerfile).read_text()
        for source in _local_copy_sources(dockerfile):
            inputs = [
                path for path, relative in context_paths.items() if _copy_includes(relative, source)
            ]
            assert inputs, (component.id, component.build_context, source)
            for path in inputs:
                if not any(
                    component_ownership.matches_path(path, pattern)
                    for pattern in component.source_paths
                ):
                    missing.append((component.id, path))
    assert not missing, missing


@pytest.mark.parametrize(
    "path",
    [
        "src/loom/application_image_build.py",
        "src/loom/application_session.py",
        "src/loom/application_source.py",
        "src/loom/application_source_archive.py",
        "src/loom/application_source_upload.py",
        "src/loom/native_image_build.py",
        "src/loom/nebius_application_contract.py",
        "src/loom/nebius_environment_contract.py",
        "src/loom/nebius_management_authority.py",
        "src/loom/nebius_pool_application_image.py",
    ],
)
def test_application_build_dependency_changes_select_actuator_image(path: str) -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    selected = component_ownership.select_release_image_matrix(
        manifest, changed_paths=(path,), force_all=False, image_set="nebius",
    )
    assert "execution-actuator" in {entry["image"] for entry in selected}


@pytest.mark.parametrize(
    ("path", "consumer"),
    [
        ("src/loom_listen/metrics.py", "egress-xds"),
        ("src/loom_service/app.py", "pipeline-orchestrator"),
        ("config/resource-profiles.toml", "pipeline-orchestrator"),
    ],
)
def test_retained_image_copy_changes_select_consumers(path: str, consumer: str) -> None:
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    assert (REPO_ROOT / path).is_file()
    # The compatibility matrix still supports selective queries; this does not
    # change the native image allowlist or enable dormant image publication.
    legacy = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=(path,),
        force_all=False,
    )
    assert consumer in {entry["image"] for entry in legacy}
    assert "behavior-stage1-sim" not in {entry["image"] for entry in legacy}
    native = component_ownership.select_release_image_matrix(
        manifest,
        changed_paths=(path,),
        force_all=False,
        image_set="nebius",
    )
    assert consumer not in {entry["image"] for entry in native}
    assert "behavior-stage1-sim" not in {entry["image"] for entry in native}
