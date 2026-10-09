"""TB2.1 rev-6 provenance and private-workspace policy contracts."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from uuid import uuid4

import pytest
import tomli_w
from loom_benchmark_terminal_bench_2.upstream import load_tb21_lock
from loom_benchmarks.util import sha256_of_dir

from loom.trajectory.storage import bundle_file_metadata_sha256
from loom.trial.workspace import WorkspaceStagingPolicy, materialize_workspace
from loom_benchmark_tool.audit_cmd import (
    AuditResult,
    ProfileActivationError,
    activate_tb21_alias,
    audit_tb21_profile,
)
from loom_benchmark_tool.manifest import (
    MANIFEST_SCHEMA_VERSION,
    TB21_AGENT_WORKSPACE_POLICY,
)
from loom_worker import main_loop as worker_main_loop
from loom_worker.main_loop import (
    _tb21_workspace_staging_policy_from_provenance,
    _verify_materialized_tb21_bundle_checksum,
)
from loom_worker.runner_pool import RunnerPool
from loom_worker.vllm_registry import WorkerVLLMRegistry


class _RecordingDriver:
    def __init__(self) -> None:
        self.uploaded: list[str] = []

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        self.uploaded.append(dst.as_posix())


class _TB21AuditSession:
    def __init__(self, *, benchmark: object, task_rows: list[object]) -> None:
        self._benchmark = benchmark
        self._task_rows = task_rows

    async def get(self, _model: object, _key: object) -> object:
        return self._benchmark

    async def execute(self, _statement: object) -> object:
        return SimpleNamespace(scalar_one_or_none=lambda: self._benchmark)

    async def scalars(self, _statement: object) -> object:
        return SimpleNamespace(all=lambda: self._task_rows)

    def begin(self) -> object:
        class _Transaction:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *_args: object) -> None:
                return None

        return _Transaction()


class _TB21ObjectStore:
    def __init__(self, bundles: dict[str, Path]) -> None:
        self._bundles = bundles

    async def download_prefix(
        self,
        *,
        bucket: str,
        prefix: str,
        out_dir: Path,
    ) -> int:
        assert bucket == "loom-benchmarks"
        bundle = self._bundles[prefix]
        count = 0
        for source in bundle.rglob("*"):
            if source.is_dir():
                continue
            target = out_dir / source.relative_to(bundle)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(source.read_bytes())
            target.chmod(source.stat().st_mode & 0o777)
            count += 1
        return count


def _tb21_audit_fixture(
    tmp_path: Path,
) -> tuple[_TB21AuditSession, _TB21ObjectStore, list[object], dict[str, Path]]:
    lock = load_tb21_lock()
    profile = "terminal-bench-2@tb2.1-r6"
    bundles: dict[str, Path] = {}
    task_rows: list[object] = []
    for task in lock.tasks:
        short_name = task.name.removeprefix("terminal-bench/")
        task_id = f"{profile}/{short_name}"
        config = {
            "schema_version": "1",
            "task": {"id": task_id, "name": short_name},
            "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
            "agent": {"name": "oracle"},
            "verifier": {
                "name": "script",
                "args": {"script_path": "/workspace/verifier/run.sh"},
            },
            "steps": [{"name": "main"}],
        }
        bundle = tmp_path / short_name
        bundle.mkdir()
        (bundle / "task.toml").write_text(tomli_w.dumps(config))
        verifier = bundle / "verifier" / "run.sh"
        verifier.parent.mkdir()
        verifier.write_text(
            '#!/bin/sh\nprintf \'{"rewards": {"tb21": 1}}\\n\' > "$LOOM_VERIFIER_OUTPUT"\n'
        )
        verifier.chmod(0o755)
        bundles[f"{short_name}/"] = bundle
        task_rows.append(
            SimpleNamespace(
                id=task_id,
                checksum=sha256_of_dir(bundle),
                config=config,
                source=f"s3://loom-benchmarks/{short_name}/",
                source_provenance={
                    "harbor_package_digest": task.digest,
                    "harbor_metadata_version": lock.hub_metadata_version,
                    "source_reference": lock.source_reference_for(task.name),
                    "verifier_identity": "tb21-native-reward-file-v1",
                    "verifier_asset": {
                        "script_path": "/workspace/verifier/run.sh",
                        "sha256": f"sha256:{sha256(verifier.read_bytes()).hexdigest()}",
                        "mode": "0755",
                    },
                    "bundle_file_metadata_sha256": bundle_file_metadata_sha256(bundle),
                    "image_provenance": {
                        "docker_image": "python:3.12-slim",
                        "dockerfile": None,
                        "docker_build_context": None,
                        "cpu_arch": "x86_64",
                    },
                    "resource_limits": {
                        "cpus": None,
                        "memory_mb": None,
                        "storage_mb": None,
                        "gpus": 0,
                    },
                    "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
                },
            )
        )
    benchmark = SimpleNamespace(
        id=profile,
        execution_state="pending",
        upstream_kind="harbor-package",
        upstream_locator=lock.dataset,
        upstream_revision=lock.revision,
        profile_provenance={
            "physical_profile": profile,
            "hub_metadata_version": lock.hub_metadata_version,
            "source_reference_snapshot": lock.source_revision,
            "source_reference_divergences": lock.source_manifest_divergences,
            "verifier_identity": "tb21-native-reward-file-v1",
            "workspace_staging_policy": TB21_AGENT_WORKSPACE_POLICY,
        },
    )
    return (
        _TB21AuditSession(benchmark=benchmark, task_rows=task_rows),
        _TB21ObjectStore(
            bundles,
        ),
        task_rows,
        bundles,
    )


@pytest.mark.asyncio
async def test_activate_then_fresh_audit_keeps_the_same_snapshot_identity(
    tmp_path: Path,
) -> None:
    session, object_store, _rows, _bundles = _tb21_audit_fixture(tmp_path)

    before_activation = await audit_tb21_profile(session, object_store=object_store)
    assert before_activation.issues == ()

    activated = await activate_tb21_alias(
        session,
        object_store=object_store,
        audit_evidence=before_activation,
    )
    after_activation = await audit_tb21_profile(session, object_store=object_store)

    assert session._benchmark.execution_state == "runnable"
    assert activated.snapshot_id == before_activation.snapshot_id
    assert after_activation.snapshot_id == activated.snapshot_id


@pytest.mark.asyncio
async def test_worker_rejects_tb21_bundle_mutated_after_its_clean_audit(
    tmp_path: Path,
) -> None:
    session, object_store, task_rows, bundles = _tb21_audit_fixture(tmp_path)
    audited = await audit_tb21_profile(session, object_store=object_store)
    assert audited.issues == ()

    short_name = "chess-best-move"
    (bundles[f"{short_name}/"] / "unaudited.txt").write_text("tampered\n")
    task_row = next(row for row in task_rows if row.id.endswith(f"/{short_name}"))
    trial_id = uuid4()
    worker_id = uuid4()
    patches: list[dict[str, object]] = []

    class _ControlPlane:
        async def get_task_bundle(self, _task_id: str) -> dict[str, object]:
            return {
                "id": task_row.id,
                "checksum": task_row.checksum,
                "config": task_row.config,
                "source": task_row.source,
                "source_provenance": task_row.source_provenance,
            }

        async def pre_start_heartbeat(self, **_kwargs: object) -> bool:
            return True

        async def patch_state(self, **kwargs: object) -> bool:
            patches.append(kwargs)
            return True

    class _Settings:
        trajectory_cache_dir = tmp_path / "trajectories"
        gateway_url = "http://gateway.test"
        fixtures_root = None
        benchmark_cache = None
        task_materialize_timeout_sec = 5.0
        pre_start_heartbeat_interval_sec = 0.01

    pool = RunnerPool(max_concurrent=1)
    await worker_main_loop._spawn_trial(
        pool=pool,
        settings=_Settings(),  # type: ignore[arg-type]
        cp_client=_ControlPlane(),  # type: ignore[arg-type]
        gateway_client=None,  # type: ignore[arg-type]
        object_store=object_store,  # type: ignore[arg-type]
        worker_id=worker_id,
        payload={
            "trial_id": str(trial_id),
            "team_id": str(uuid4()),
            "task_id": task_row.id,
            "attempt_count": 1,
            "config": {"agent_name": "oracle", "agent_model": None},
        },
        vllm_registry=WorkerVLLMRegistry(enabled=False),
    )
    await pool.wait_all(timeout=2.0)

    assert len(patches) == 1
    assert patches[0]["trial_id"] == trial_id
    assert patches[0]["worker_id"] == worker_id
    assert patches[0]["state"] == "failed"
    assert patches[0]["failure_reason"] == "internal_error"
    assert "materialized TB2.1 bundle checksum mismatch" in str(
        patches[0]["failure_message"],
    )
    assert task_row.checksum in str(patches[0]["failure_message"])


def test_worker_rejects_tb21_bundle_when_mode_sidecar_is_lost_or_tampered(
    tmp_path: Path,
) -> None:
    (tmp_path / "task.toml").write_text("[task]\nid='x'\n")
    verifier = tmp_path / "verifier" / "run.sh"
    verifier.parent.mkdir()
    verifier.write_text("#!/bin/sh\n")
    verifier.chmod(0o755)
    checksum = sha256_of_dir(tmp_path)
    provenance = {"bundle_file_metadata_sha256": bundle_file_metadata_sha256(tmp_path)}

    # S3 without the trusted sidecar materializes ordinary 0644 files. Bytes
    # still match the task checksum, so only the bound mode digest catches it.
    verifier.chmod(0o644)

    with pytest.raises(ValueError, match="file mode metadata mismatch"):
        _verify_materialized_tb21_bundle_checksum(
            task_dir=tmp_path,
            expected_checksum=checksum,
            source_provenance=provenance,
        )


def test_tb21_manifest_schema_requires_a_private_agent_workspace_policy() -> None:
    assert MANIFEST_SCHEMA_VERSION == 4
    assert TB21_AGENT_WORKSPACE_POLICY == {
        "schema_version": 1,
        "agent_excluded_paths": [
            "solution/**",
            "tests/**",
            "verifier/**",
            "upstream-task.toml",
        ],
        "verifier_only_paths": [
            "solution/**",
            "tests/**",
            "verifier/**",
            "upstream-task.toml",
        ],
        "trusted_oracle_paths": ["solution/**"],
    }


@pytest.mark.asyncio
async def test_tb21_agent_staging_excludes_private_bundle_content(
    tmp_path: Path,
) -> None:
    (tmp_path / "instruction.md").write_text("Solve the task.\n")
    (tmp_path / "solution").mkdir()
    (tmp_path / "solution" / "solve.sh").write_text("reference answer\n")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test.sh").write_text("private verifier\n")
    (tmp_path / "verifier").mkdir()
    (tmp_path / "verifier" / "run.sh").write_text("private bridge\n")
    (tmp_path / "upstream-task.toml").write_text("native private fields\n")

    policy = WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)
    driver = _RecordingDriver()

    await materialize_workspace(
        driver=driver,
        task_dir=tmp_path,
        dst=PurePosixPath("/workspace"),
        policy=policy,
        phase="agent",
    )

    assert driver.uploaded == ["/workspace/instruction.md"]

    oracle_driver = _RecordingDriver()
    await materialize_workspace(
        driver=oracle_driver,
        task_dir=tmp_path,
        dst=PurePosixPath("/oracle-workspace"),
        policy=policy,
        phase="agent",
        trusted_private_paths=policy.trusted_oracle_paths,
    )

    assert "/oracle-workspace/solution/solve.sh" in oracle_driver.uploaded
    assert "/oracle-workspace/tests/test.sh" not in oracle_driver.uploaded
    assert "/oracle-workspace/verifier/run.sh" not in oracle_driver.uploaded
    assert "/oracle-workspace/upstream-task.toml" not in oracle_driver.uploaded

    await materialize_workspace(
        driver=driver,
        task_dir=tmp_path,
        dst=PurePosixPath("/workspace"),
        policy=policy,
        phase="verifier",
    )

    assert set(driver.uploaded) == {
        "/workspace/instruction.md",
        "/workspace/solution/solve.sh",
        "/workspace/tests/test.sh",
        "/workspace/verifier/run.sh",
        "/workspace/upstream-task.toml",
    }


def test_activation_rejects_partial_or_unisolated_audit() -> None:
    with pytest.raises(ProfileActivationError, match="89 verified bundles"):
        AuditResult(
            profile="terminal-bench-2@tb2.1-r6",
            verified_bundles=88,
            private_workspace_isolation=True,
        ).require_exact_profile("terminal-bench-2@tb2.1-r6", task_count=89)


def test_worker_tb21_gate_rejects_a_well_formed_noncanonical_policy() -> None:
    noncanonical = {
        "schema_version": 1,
        "agent_excluded_paths": [
            "solution/**",
            "tests/**",
            "verifier/**",
            "private-upstream.toml",
        ],
        "verifier_only_paths": [
            "solution/**",
            "tests/**",
            "verifier/**",
            "private-upstream.toml",
        ],
        "trusted_oracle_paths": ["solution/**"],
    }

    with pytest.raises(ValueError, match="canonical"):
        _tb21_workspace_staging_policy_from_provenance(noncanonical)


@pytest.mark.asyncio
async def test_audit_verifies_locked_provenance_config_and_bundle_bytes(
    tmp_path: Path,
) -> None:
    session, object_store, _task_rows, _bundles = _tb21_audit_fixture(tmp_path)

    result = await audit_tb21_profile(session, object_store=object_store, lock_rows=True)

    assert result.verified_bundles == 89
    assert result.private_workspace_isolation is True
    assert result.issues == ()
    assert result.snapshot_id is not None

    with pytest.raises(ProfileActivationError, match="private workspace isolation"):
        AuditResult(
            profile="terminal-bench-2@tb2.1-r6",
            verified_bundles=89,
            private_workspace_isolation=False,
        ).require_exact_profile("terminal-bench-2@tb2.1-r6", task_count=89)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mutation", "expected_issue"),
    [
        ("missing", "configured verifier script is missing"),
        ("nonexecutable", "configured verifier script is not executable"),
        ("mismatched", "verifier asset checksum mismatch"),
    ],
)
async def test_audit_rejects_invalid_native_verifier_asset(
    tmp_path: Path,
    mutation: str,
    expected_issue: str,
) -> None:
    session, object_store, task_rows, bundles = _tb21_audit_fixture(tmp_path)
    row = next(row for row in task_rows if row.id.endswith("/chess-best-move"))
    bundle = bundles["chess-best-move/"]
    verifier = bundle / "verifier" / "run.sh"
    if mutation == "missing":
        verifier.unlink()
        row.checksum = sha256_of_dir(bundle)
        row.source_provenance["bundle_file_metadata_sha256"] = bundle_file_metadata_sha256(bundle)
    elif mutation == "nonexecutable":
        verifier.chmod(0o644)
        row.source_provenance["bundle_file_metadata_sha256"] = bundle_file_metadata_sha256(bundle)
    else:
        row.source_provenance["verifier_asset"]["sha256"] = "sha256:" + "0" * 64

    result = await audit_tb21_profile(session, object_store=object_store)

    assert result.verified_bundles == 88
    assert expected_issue in "\n".join(result.issues)


@pytest.mark.asyncio
async def test_audit_uses_the_worker_compatibility_gate(tmp_path: Path) -> None:
    session, object_store, task_rows, bundles = _tb21_audit_fixture(tmp_path)
    row = next(row for row in task_rows if row.id.endswith("/chess-best-move"))
    bundle = bundles["chess-best-move/"]
    # This prebuilt task does not consume a default workspace Dockerfile.
    # Verifier recipes still pass through the worker's blocking compatibility gate.
    (bundle / "tests").mkdir()
    (bundle / "tests" / "Dockerfile").write_text(
        "FROM python:3.12-slim\nRUN sed -i 's/x/y/' /etc/resolv.conf\n",
    )
    row.checksum = sha256_of_dir(bundle)
    row.source_provenance["bundle_file_metadata_sha256"] = bundle_file_metadata_sha256(bundle)

    result = await audit_tb21_profile(session, object_store=object_store)

    assert result.verified_bundles == 88
    assert "TASK_COMPAT_DNS_MUTATION" in "\n".join(result.issues)


@pytest.mark.asyncio
async def test_activation_reaudits_current_snapshot_and_rejects_stale_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Transaction:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *_args: object) -> None:
            return None

    class _Session:
        executed = False

        def begin(self) -> _Transaction:
            return _Transaction()

        async def execute(self, _statement: object) -> None:
            self.executed = True

    async def current_audit(
        _session: object, *, object_store: object, **_kwargs: object
    ) -> AuditResult:
        assert object_store is current_store
        return AuditResult(
            profile="terminal-bench-2@tb2.1-r6",
            verified_bundles=89,
            private_workspace_isolation=True,
            snapshot_id="current-snapshot",
        )

    current_store = object()
    session = _Session()
    monkeypatch.setattr("loom_benchmark_tool.audit_cmd.audit_tb21_profile", current_audit)

    with pytest.raises(ProfileActivationError, match="snapshot"):
        await activate_tb21_alias(
            session,  # type: ignore[arg-type]
            object_store=current_store,  # type: ignore[arg-type]
            audit_evidence=AuditResult(
                profile="terminal-bench-2@tb2.1-r6",
                verified_bundles=89,
                private_workspace_isolation=True,
                snapshot_id="forged-or-stale-snapshot",
            ),
        )

    assert session.executed is False


@pytest.mark.asyncio
async def test_activation_ignores_a_forged_clean_assertion_when_fresh_audit_is_dirty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Transaction:
        async def __aenter__(self) -> None:
            return None

        async def __aexit__(self, *_args: object) -> None:
            return None

    class _Session:
        executed = False

        def begin(self) -> _Transaction:
            return _Transaction()

        async def execute(self, _statement: object) -> None:
            self.executed = True

    async def dirty_current_audit(
        _session: object,
        *,
        object_store: object,
        **_kwargs: object,
    ) -> AuditResult:
        assert object_store is current_store
        return AuditResult(
            profile="terminal-bench-2@tb2.1-r6",
            verified_bundles=88,
            private_workspace_isolation=False,
            issues=("current bundle verifier asset checksum mismatch",),
            snapshot_id="sha256:" + "d" * 64,
        )

    current_store = object()
    session = _Session()
    monkeypatch.setattr("loom_benchmark_tool.audit_cmd.audit_tb21_profile", dirty_current_audit)

    with pytest.raises(ProfileActivationError, match="89 verified bundles"):
        await activate_tb21_alias(
            session,  # type: ignore[arg-type]
            object_store=current_store,  # type: ignore[arg-type]
            # These forged clean fields intentionally match the snapshot.  The
            # fresh audit must still control the decision.
            audit_evidence=AuditResult(
                profile="terminal-bench-2@tb2.1-r6",
                verified_bundles=89,
                private_workspace_isolation=True,
                snapshot_id="sha256:" + "d" * 64,
            ),
        )

    assert session.executed is False
