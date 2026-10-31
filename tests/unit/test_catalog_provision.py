from __future__ import annotations

from dataclasses import replace

import pytest

import loom_cli.catalog_provision as catalog_provision
from loom_cli.catalog_provision import (
    POSTGRES_CATALOG_UPSERT_BATCH_SIZE,
    AgentRow,
    BenchmarkRow,
    CatalogRows,
    ObjectInfo,
    PostgresCatalogStore,
    TaskRow,
    provision_ready_benchmark_catalog,
)


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def _workspace_task_config(task_id: str) -> dict[str, object]:
    return {
        **_valid_task_config(task_id),
        "required_agent_capabilities": ["workspace_exec"],
    }


class FakeCatalog:
    def __init__(self, rows: CatalogRows | None = None) -> None:
        self.rows = rows or CatalogRows(benchmarks=[], tasks=[])
        self.upserts: list[CatalogRows] = []

    async def load_rows(self) -> CatalogRows:
        return self.rows

    async def upsert_rows(self, rows: CatalogRows) -> None:
        self.upserts.append(rows)
        agents = {(row.name, row.version): row for row in self.rows.agents}
        benchmarks = {row.id: row for row in self.rows.benchmarks}
        tasks = {row.id: row for row in self.rows.tasks}
        agents.update({(row.name, row.version): row for row in rows.agents})
        benchmarks.update({row.id: row for row in rows.benchmarks})
        tasks.update({row.id: row for row in rows.tasks})
        self.rows = CatalogRows(
            agents=list(agents.values()),
            benchmarks=list(benchmarks.values()),
            tasks=list(tasks.values()),
        )


class FakeObjects:
    def __init__(self, objects: dict[tuple[str, str], bytes] | None = None) -> None:
        self.objects = objects or {}
        self.buckets: set[str] = set()
        self.put_calls: list[tuple[str, str]] = []

    async def ensure_bucket(self, bucket: str) -> None:
        self.buckets.add(bucket)

    async def list_objects(self, *, bucket: str, prefix: str) -> list[ObjectInfo]:
        return [
            ObjectInfo(bucket=bucket, key=key, size=len(body), etag=str(hash(body)))
            for (obj_bucket, key), body in sorted(self.objects.items())
            if obj_bucket == bucket and key.startswith(prefix)
        ]

    async def head_object(self, *, bucket: str, key: str) -> ObjectInfo | None:
        body = self.objects.get((bucket, key))
        if body is None:
            return None
        return ObjectInfo(bucket=bucket, key=key, size=len(body), etag=str(hash(body)))

    async def get_object(self, *, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)]

    async def put_object(self, *, bucket: str, key: str, body: bytes) -> None:
        self.put_calls.append((bucket, key))
        self.objects[(bucket, key)] = body


@pytest.mark.asyncio
async def test_postgres_catalog_store_batches_large_task_upserts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeEngine:
        def __init__(self) -> None:
            self.disposed = False

        async def dispose(self) -> None:
            self.disposed = True

    class FakeResult:
        def __init__(self, values: list[TaskRow]) -> None:
            self.values = values

        def scalars(self) -> FakeResult:
            return self

        def all(self) -> list[TaskRow]:
            return self.values

    class FakeSession:
        def __init__(self) -> None:
            self.execute_count = 0
            self.commits = 0

        async def __aenter__(self) -> FakeSession:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def execute(self, _statement: object) -> FakeResult:
            start = self.execute_count * POSTGRES_CATALOG_UPSERT_BATCH_SIZE
            self.execute_count += 1
            return FakeResult(
                sorted(rows.tasks, key=lambda row: row.id)[
                    start : start + POSTGRES_CATALOG_UPSERT_BATCH_SIZE
                ]
            )

        async def commit(self) -> None:
            self.commits += 1

    engine = FakeEngine()
    session = FakeSession()

    monkeypatch.setattr(
        catalog_provision,
        "create_async_engine",
        lambda _db_url: engine,
    )
    monkeypatch.setattr(
        catalog_provision,
        "async_sessionmaker",
        lambda *_args, **_kwargs: lambda: session,
    )

    task_count = POSTGRES_CATALOG_UPSERT_BATCH_SIZE * 2 + 1
    rows = CatalogRows(
        benchmarks=[],
        tasks=[
            TaskRow(
                id=f"bench/task-{index}",
                checksum="a" * 64,
                config=_valid_task_config(f"bench/task-{index}"),
                source=f"s3://loom-benchmarks/bench/task-{index}/",
                license="MIT",
                benchmark_id="bench",
                tags={"split": "test"},
            )
            for index in range(task_count)
        ],
    )

    await PostgresCatalogStore("postgresql://loom:loom@example/loom").upsert_rows(rows)

    assert session.execute_count == 3
    assert session.commits == 1
    assert engine.disposed is True


def test_agent_rows_from_service_catalog_include_contract_and_provenance() -> None:
    rows = catalog_provision.agent_rows_from_service_catalog(
        imported_by="release:staging",
    )

    by_name = {row.name: row for row in rows}
    assert {"oracle", "direct-completion"}.issubset(by_name)
    assert "litellm" not in by_name

    oracle = by_name["oracle"]
    assert oracle.version == "service-catalog-v2"
    assert oracle.mode == "builtin"
    assert oracle.spec["name"] == "oracle"
    assert oracle.spec["needs_model"] is False
    assert oracle.spec["runtime_contract"]["execution"] == "builtin-oracle"
    assert oracle.spec["catalog_provenance"] == {
        "source": "loom_service.agent_catalog",
        "schema_version": 2,
        "provisioned_by": "release:staging",
    }


def test_ready_catalog_requires_workspace_benchmarks_to_be_republished() -> None:
    benchmark_id = "skillflow"
    task_id = f"{benchmark_id}/task"
    source = CatalogRows(
        benchmarks=[
            BenchmarkRow(
                id=benchmark_id,
                display_name="SkillFlow",
                upstream_kind="huggingface",
                upstream_locator="example/skillflow",
                upstream_revision="rev",
                license_spdx="MIT",
                license_url="https://example.test/license",
                splits=["test"],
                series="workspace",
                imported_by="source",
            ),
        ],
        tasks=[
            TaskRow(
                id=task_id,
                checksum="a" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://source-bucket/{benchmark_id}/task/",
                license="MIT",
                benchmark_id=benchmark_id,
                tags={},
            ),
        ],
    )

    with pytest.raises(ValueError, match=r"republish.*workspace_exec"):
        catalog_provision._ready_catalog_rows(
            source,
            target_bucket="target-bucket",
        )


@pytest.mark.parametrize(
    "benchmark_id",
    ["swe-bench", "swe-bench-verified", "swe-bench-multimodal"],
)
def test_ready_catalog_requires_swe_bench_tasks_to_be_republished(
    benchmark_id: str,
) -> None:
    task_id = f"{benchmark_id}/task"
    source = CatalogRows(
        benchmarks=[
            BenchmarkRow(
                id=benchmark_id,
                display_name="SWE-Bench",
                upstream_kind="huggingface",
                upstream_locator="princeton-nlp/SWE-bench",
                upstream_revision="rev",
                license_spdx="MIT",
                license_url="https://example.test/license",
                splits=["test"],
                series="code",
                imported_by="source",
            ),
        ],
        tasks=[
            TaskRow(
                id=task_id,
                checksum="a" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://source-bucket/{benchmark_id}/task/",
                license="MIT",
                benchmark_id=benchmark_id,
                tags={},
            ),
        ],
    )

    with pytest.raises(ValueError, match=r"republish.*workspace_exec"):
        catalog_provision._ready_catalog_rows(
            source,
            target_bucket="target-bucket",
        )


def test_tb21_catalog_provision_requires_target_local_activation() -> None:
    profile_id = "terminal-bench-2@tb2.1-r6"
    task_id = f"{profile_id}/task"
    source = CatalogRows(
        benchmarks=[
            BenchmarkRow(
                id=profile_id,
                display_name="Terminal-Bench 2.1",
                upstream_kind="harbor-package",
                upstream_locator="terminal-bench/terminal-bench-2-1",
                upstream_revision="6",
                license_spdx="Apache-2.0",
                license_url="https://example.test/license",
                splits=["test"],
                series=None,
                imported_by="source",
                execution_state="runnable",
                profile_provenance={
                    "physical_profile": profile_id,
                    "activation_audit": {"snapshot_id": "sha256:source-only"},
                },
            )
        ],
        tasks=[
            TaskRow(
                id=task_id,
                checksum="a" * 64,
                config=_valid_task_config(task_id),
                source=f"s3://source-bucket/{profile_id}/task/",
                license="Apache-2.0",
                benchmark_id=profile_id,
                tags={},
            )
        ],
    )

    ready = catalog_provision._ready_catalog_rows(
        source,
        target_bucket="target-bucket",
    )

    assert len(ready.benchmarks) == 1
    assert ready.benchmarks[0].execution_state == "pending"
    assert ready.benchmarks[0].profile_provenance == {
        "physical_profile": profile_id,
    }
    assert ready.tasks[0].source == f"s3://target-bucket/{profile_id}/task/"


@pytest.mark.asyncio
async def test_tb21_catalog_drift_fails_before_target_object_copy() -> None:
    profile_id = "terminal-bench-2@tb2.1-r6"
    task_id = f"{profile_id}/task"
    benchmark = BenchmarkRow(
        id=profile_id,
        display_name="Terminal-Bench 2.1",
        upstream_kind="harbor-package",
        upstream_locator="terminal-bench/terminal-bench-2-1",
        upstream_revision="6",
        license_spdx="Apache-2.0",
        license_url="https://example.test/license",
        splits=["test"],
        series=None,
        imported_by="source",
        execution_state="runnable",
        profile_provenance={"physical_profile": profile_id},
    )
    source_task = TaskRow(
        id=task_id,
        checksum="a" * 64,
        config=_workspace_task_config(task_id),
        source=f"s3://source-bucket/{profile_id}/task/",
        license="Apache-2.0",
        benchmark_id=profile_id,
        tags={},
    )
    target_task = replace(
        source_task,
        checksum="b" * 64,
        source=f"s3://target-bucket/{profile_id}/task/",
    )
    target = FakeCatalog(CatalogRows(benchmarks=[benchmark], tasks=[target_task]))
    target_objects = FakeObjects(
        {("target-bucket", f"{profile_id}/task/task.toml"): b"activated"},
    )

    with pytest.raises(ValueError, match=r"immutable catalog profile drift"):
        await provision_ready_benchmark_catalog(
            source_catalog=FakeCatalog(
                CatalogRows(benchmarks=[benchmark], tasks=[source_task]),
            ),
            target_catalog=target,
            source_objects=FakeObjects(
                {("source-bucket", f"{profile_id}/task/task.toml"): b"replacement"},
            ),
            target_objects=target_objects,
            target_bucket="target-bucket",
        )

    assert target_objects.put_calls == []
    assert target_objects.objects == {
        ("target-bucket", f"{profile_id}/task/task.toml"): b"activated",
    }
    assert target.upserts == []


@pytest.mark.asyncio
async def test_exact_runnable_tb21_target_is_reset_for_local_reactivation() -> None:
    profile_id = "terminal-bench-2@tb2.1-r6"
    task_id = f"{profile_id}/task"
    source_benchmark = BenchmarkRow(
        id=profile_id,
        display_name="Terminal-Bench 2.1",
        upstream_kind="harbor-package",
        upstream_locator="terminal-bench/terminal-bench-2-1",
        upstream_revision="6",
        license_spdx="Apache-2.0",
        license_url="https://example.test/license",
        splits=["test"],
        series=None,
        imported_by="source",
        execution_state="runnable",
        profile_provenance={
            "physical_profile": profile_id,
            "activation_audit": {"snapshot_id": "sha256:source"},
        },
    )
    source_task = TaskRow(
        id=task_id,
        checksum="a" * 64,
        config=_workspace_task_config(task_id),
        source=f"s3://source-bucket/{profile_id}/task/",
        license="Apache-2.0",
        benchmark_id=profile_id,
        tags={},
    )
    target = FakeCatalog(
        CatalogRows(
            benchmarks=[
                replace(
                    source_benchmark,
                    imported_by="target",
                    profile_provenance={
                        "physical_profile": profile_id,
                        "activation_audit": {"snapshot_id": "sha256:target"},
                    },
                ),
            ],
            tasks=[
                replace(
                    source_task,
                    source=f"s3://target-bucket/{profile_id}/task/",
                ),
            ],
        ),
    )
    source_objects = FakeObjects(
        {("source-bucket", f"{profile_id}/task/task.toml"): b"task"},
    )

    class FailClosedObjects(FakeObjects):
        async def put_object(self, *, bucket: str, key: str, body: bytes) -> None:
            current = next(row for row in target.rows.benchmarks if row.id == profile_id)
            assert current.execution_state == "pending"
            assert "activation_audit" not in current.profile_provenance
            await super().put_object(bucket=bucket, key=key, body=body)

    target_objects = FailClosedObjects(
        {("target-bucket", f"{profile_id}/task/task.toml"): b"stale"},
    )

    await provision_ready_benchmark_catalog(
        source_catalog=FakeCatalog(
            CatalogRows(benchmarks=[source_benchmark], tasks=[source_task]),
        ),
        target_catalog=target,
        source_objects=source_objects,
        target_objects=target_objects,
        target_bucket="target-bucket",
        imported_by="restore",
    )

    restored = next(row for row in target.rows.benchmarks if row.id == profile_id)
    assert restored.execution_state == "pending"
    assert restored.profile_provenance == {"physical_profile": profile_id}
    assert target_objects.put_calls == [
        ("target-bucket", f"{profile_id}/task/task.toml"),
    ]
    assert target.upserts[0].benchmarks[0].execution_state == "pending"
    assert "activation_audit" not in target.upserts[0].benchmarks[0].profile_provenance


@pytest.mark.asyncio
async def test_provision_ready_catalog_filters_blocked_rows_and_copies_missing_objects() -> None:
    source = FakeCatalog(
        CatalogRows(
            benchmarks=[
                BenchmarkRow(
                    id="humaneval",
                    display_name="HumanEval",
                    upstream_kind="huggingface",
                    upstream_locator="openai/openai_humaneval",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/humaneval",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
                BenchmarkRow(
                    id="legacy",
                    display_name="Legacy",
                    upstream_kind="huggingface",
                    upstream_locator="example/legacy",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/legacy",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
                BenchmarkRow(
                    id="metadata-only",
                    display_name="Metadata only",
                    upstream_kind="huggingface",
                    upstream_locator="example/metadata-only",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/metadata-only",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
            ],
            tasks=[
                TaskRow(
                    id="humaneval/HumanEval/0",
                    checksum="a" * 64,
                    config=_valid_task_config("humaneval/HumanEval/0"),
                    source="s3://loom-benchmarks/humaneval/HumanEval/0/",
                    license="MIT",
                    benchmark_id="humaneval",
                    tags={"split": "test"},
                ),
                TaskRow(
                    id="legacy/0",
                    checksum="b" * 64,
                    config={},
                    source="s3://loom-benchmarks/legacy/0/",
                    license="MIT",
                    benchmark_id="legacy",
                    tags={},
                ),
            ],
        ),
    )
    target = FakeCatalog()
    source_objects = FakeObjects(
        {
            ("loom-benchmarks", "humaneval/HumanEval/0/task.toml"): b"task",
            ("loom-benchmarks", "humaneval/HumanEval/0/solution/solve.sh"): b"solve",
            ("loom-benchmarks", "legacy/0/task.toml"): b"legacy",
        }
    )
    target_objects = FakeObjects(
        {
            ("loom-benchmarks", "humaneval/HumanEval/0/task.toml"): b"task",
        }
    )

    stats = await provision_ready_benchmark_catalog(
        source_catalog=source,
        target_catalog=target,
        source_objects=source_objects,
        target_objects=target_objects,
        target_bucket="loom-benchmarks",
        imported_by="staging-provision",
    )

    assert stats.ready_agents >= 2
    assert stats.ready_benchmarks == 1
    assert stats.ready_tasks == 1
    assert stats.source_objects == 2
    assert stats.target_objects_uploaded == 1
    assert stats.target_objects_skipped == 1
    assert stats.target_objects_missing == 0
    assert stats.bytes_uploaded == 5
    assert stats.bytes_skipped == 4
    assert target_objects.buckets == {"loom-benchmarks"}
    assert {"oracle", "direct-completion"}.issubset(
        {row.name for row in target.rows.agents},
    )
    assert all(row.version == "service-catalog-v2" for row in target.rows.agents)
    assert target.rows.benchmarks == [
        replace(source.rows.benchmarks[0], imported_by="staging-provision"),
    ]
    assert [row.id for row in target.rows.tasks] == ["humaneval/HumanEval/0"]
    assert target.rows.tasks[0].source == "s3://loom-benchmarks/humaneval/HumanEval/0/"
    assert ("loom-benchmarks", "legacy/0/task.toml") not in target_objects.objects

    second = await provision_ready_benchmark_catalog(
        source_catalog=source,
        target_catalog=target,
        source_objects=source_objects,
        target_objects=target_objects,
        target_bucket="loom-benchmarks",
        imported_by="staging-provision",
    )

    assert second.target_objects_uploaded == 0
    assert second.target_objects_skipped == 2
    assert second.bytes_uploaded == 0
    assert second.bytes_skipped == 9


@pytest.mark.asyncio
async def test_provision_ready_catalog_reports_missing_source_bundles() -> None:
    source = FakeCatalog(
        CatalogRows(
            benchmarks=[
                BenchmarkRow(
                    id="humaneval",
                    display_name="HumanEval",
                    upstream_kind="huggingface",
                    upstream_locator="openai/openai_humaneval",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/humaneval",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
            ],
            tasks=[
                TaskRow(
                    id="humaneval/HumanEval/0",
                    checksum="a" * 64,
                    config=_valid_task_config("humaneval/HumanEval/0"),
                    source="s3://loom-benchmarks/humaneval/HumanEval/0/",
                    license="MIT",
                    benchmark_id="humaneval",
                    tags={},
                ),
            ],
        ),
    )

    target = FakeCatalog()

    stats = await provision_ready_benchmark_catalog(
        source_catalog=source,
        target_catalog=target,
        source_objects=FakeObjects(),
        target_objects=FakeObjects(),
        target_bucket="loom-benchmarks",
    )

    assert stats.target_objects_missing == 1
    assert stats.source_objects == 0
    assert target.upserts == []


@pytest.mark.asyncio
async def test_provision_ready_catalog_preserves_existing_agent_rows_on_second_run() -> None:
    source = FakeCatalog(
        CatalogRows(
            benchmarks=[
                BenchmarkRow(
                    id="humaneval",
                    display_name="HumanEval",
                    upstream_kind="huggingface",
                    upstream_locator="openai/openai_humaneval",
                    upstream_revision="main",
                    license_spdx="MIT",
                    license_url="https://example/humaneval",
                    splits=["test"],
                    series=None,
                    imported_by="dev",
                ),
            ],
            tasks=[
                TaskRow(
                    id="humaneval/HumanEval/0",
                    checksum="a" * 64,
                    config=_valid_task_config("humaneval/HumanEval/0"),
                    source="s3://loom-benchmarks/humaneval/HumanEval/0/",
                    license="MIT",
                    benchmark_id="humaneval",
                    tags={"split": "test"},
                ),
            ],
        ),
    )
    target = FakeCatalog(
        CatalogRows(
            agents=[
                AgentRow(
                    name="oracle",
                    version="service-catalog-v1",
                    mode="builtin",
                    spec={"stale": True},
                ),
            ],
            benchmarks=[],
            tasks=[],
        )
    )
    source_objects = FakeObjects(
        {
            ("loom-benchmarks", "humaneval/HumanEval/0/task.toml"): b"task",
        }
    )

    first = await provision_ready_benchmark_catalog(
        source_catalog=source,
        target_catalog=target,
        source_objects=source_objects,
        target_objects=FakeObjects(),
        target_bucket="loom-benchmarks",
        imported_by="staging-provision",
    )
    second = await provision_ready_benchmark_catalog(
        source_catalog=source,
        target_catalog=target,
        source_objects=source_objects,
        target_objects=FakeObjects(
            {
                ("loom-benchmarks", "humaneval/HumanEval/0/task.toml"): b"task",
            }
        ),
        target_bucket="loom-benchmarks",
        imported_by="staging-provision",
    )

    assert first.ready_agents >= 2
    assert second.ready_agents == first.ready_agents
    by_name = {row.name: row for row in target.rows.agents}
    assert "stale" not in by_name["oracle"].spec
    assert by_name["oracle"].spec["catalog_provenance"]["provisioned_by"] == ("staging-provision")


def test_arbitrary_catalog_copy_does_not_inherit_admission_or_qualification() -> None:
    profile = BenchmarkRow(
        id="arbitrary-new-json@7", display_name="New format", upstream_kind="json",
        upstream_locator="https://example.test/tasks.json", upstream_revision="release-7",
        license_spdx="MIT", license_url="", splits=["test"], series=None, imported_by=None,
        execution_state="runnable", profile_provenance={
            "runtime_verified": True, "qualification": {"state": "verified"},
            "source_task_count": 1,
        },
    )
    task = TaskRow(
        id=profile.id + "/one", checksum="a" * 64, config=_valid_task_config(profile.id + "/one"),
        source="s3://source-bucket/profile/one/", license="MIT", benchmark_id=profile.id, tags={},
    )
    copied = catalog_provision._ready_catalog_rows(CatalogRows([profile], [task]), target_bucket="target-bucket")
    assert copied.benchmarks[0].execution_state == "pending"
    assert copied.benchmarks[0].profile_provenance == {"source_task_count": 1}
    assert copied.tasks[0].source == "s3://target-bucket/profile/one/"
