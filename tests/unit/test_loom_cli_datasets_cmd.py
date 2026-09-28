"""Full subcommand surface: list (filters + --json), show, install, refresh."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.datasets_cmd import dispatch
from loom_cli.discovery import DatasetEntry


@pytest.mark.parametrize("adapter", [False, True])
def test_publish_selects_input_and_forwards_options(monkeypatch, capsys, tmp_path, adapter):
    captured = {}
    for key, value in {
        "LOOM_DB_URL": "postgresql://fixture/db",
        "LOOM_MINIO_ACCESS_KEY": "fixture-access",
        "LOOM_MINIO_SECRET_KEY": "fixture-secret",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", lambda **kw: object())

    async def publish(path, **kwargs):
        captured.update(path=path, **kwargs)
        return SimpleNamespace(
            benchmark_id="fixture", task_count=1, inserted=1, updated=0,
            unchanged=0, uploaded_objects=3, compat_flattened_files=0,
            source_prefix="s3://fixture/", execution_profile=None,
            warnings=("fixture: optional attachment missing",),
        )
    monkeypatch.setattr("loom_cli.benchmark_publish.publish_benchmark", publish)
    selection = [
        "--benchmark", "humaneval", "--cache-dir", str(tmp_path / "cache"),
        "--refresh", "--limit", "2", "--instance-id", "HumanEval/0",
    ] if adapter else [str(tmp_path)]
    assert dispatch(["publish", *selection, "--minio-endpoint", "https://example.test"]) == 0
    assert captured["path"] == (None if adapter else Path(tmp_path))
    assert captured["benchmark"] == ("humaneval" if adapter else None)
    if adapter:
        assert captured["cache_dir"] == tmp_path / "cache"
        assert captured["refresh"] is True
        assert captured["limit"] == 2
        assert captured["instance_ids"] == {"HumanEval/0"}
    output = capsys.readouterr()
    assert "publish fixture:" in output.out
    assert "optional attachment missing" in output.err


def test_publish_error_redacts_resolved_credentials(monkeypatch, capsys):
    secrets = {
        "LOOM_DB_URL": "postgresql://fixture:db-password@db.example/loom",
        "LOOM_MINIO_ACCESS_KEY": "resolved-access-secret",
        "LOOM_MINIO_SECRET_KEY": "resolved-minio-secret",
    }
    for key, value in secrets.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", lambda **kw: object())

    async def fail(*args, **kwargs):
        raise ValueError("failed: " + " ".join(secrets.values()))
    monkeypatch.setattr("loom_cli.benchmark_publish.publish_benchmark", fail)
    assert dispatch(["publish", "--benchmark", "humaneval", "--minio-endpoint", "https://example.test"]) == 1
    err = capsys.readouterr().err
    for secret in (*secrets.values(), "db-password"):
        assert secret not in err


def test_publish_rejects_retired_writer_flags(capsys):
    with pytest.raises(SystemExit) as error:
        dispatch(["publish", "--benchmark", "humaneval", "--target=hf"])
    assert error.value.code == 2
    assert "unrecognized arguments" in capsys.readouterr().err


@pytest.fixture()
def patched_discovery(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_builtin() -> list[DatasetEntry]:
        return [DatasetEntry(
            slug="humaneval", source="builtin", display_name="HumanEval",
            license_spdx="MIT", license_url="", task_count=164,
            status="installed", available_pip_spec=None,
            entry_point="loom_benchmarks.adapters.humaneval:HumanEvalAdapter",
        )]

    def fake_catalog(*, url: str | None) -> list[DatasetEntry]:
        return [DatasetEntry(
            slug="terminal-bench-2", source="catalog",
            display_name="Terminal-Bench 2.0", license_spdx="Apache-2.0",
            license_url="", task_count=None, status="available",
            available_pip_spec="loom-benchmark-terminal-bench-2",
            entry_point=None,
        )]

    def fake_remote(
        *, server_url: str | None, token: str | None,
    ) -> list[DatasetEntry]:
        return [DatasetEntry(
            slug="custom-rl-bench", source="remote",
            display_name="Custom RL Bench", license_spdx="proprietary",
            license_url="", task_count=None, status="remote-only",
            available_pip_spec=None, entry_point=None,
        )]

    monkeypatch.setattr("loom_cli.builtin.load_builtin_entries", fake_builtin)
    monkeypatch.setattr("loom_cli.catalog.load_catalog_entries", fake_catalog)
    monkeypatch.setattr("loom_cli.remote.load_remote_entries", fake_remote)


def test_list_default_unions_all_three(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = dispatch(["list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "humaneval" in out
    assert "terminal-bench-2" in out
    assert "custom-rl-bench" in out


def test_list_installed_filter(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    dispatch(["list", "--installed"])
    out = capsys.readouterr().out
    assert "humaneval" in out
    assert "terminal-bench-2" not in out
    assert "custom-rl-bench" not in out


def test_list_available_filter(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    dispatch(["list", "--available"])
    out = capsys.readouterr().out
    assert "terminal-bench-2" in out
    assert "humaneval" not in out


def test_list_remote_filter(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    dispatch(["list", "--remote"])
    out = capsys.readouterr().out
    assert "custom-rl-bench" in out
    assert "humaneval" not in out


def test_list_json_emits_machine_readable(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    dispatch(["list", "--json"])
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["count"] == 3
    assert {i["slug"] for i in parsed["items"]} == {
        "humaneval", "terminal-bench-2", "custom-rl-bench",
    }


def test_show_prints_full_details(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = dispatch(["show", "humaneval"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "humaneval" in out
    assert "MIT" in out
    assert "164" in out
    assert "loom_benchmarks.adapters.humaneval:HumanEvalAdapter" in out


def test_show_unknown_slug_exits_nonzero(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = dispatch(["show", "no-such-thing"])
    assert rc == 2
    assert "not found" in capsys.readouterr().err.lower()


def test_install_invokes_install_dataset(
    patched_discovery: None,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: dict[str, str] = {}

    def fake_install(*, pip_spec: str) -> str:
        seen["spec"] = pip_spec
        return "Successfully installed ...\n"

    monkeypatch.setattr("loom_cli.install.install_dataset", fake_install)
    rc = dispatch(["install", "terminal-bench-2"])
    assert rc == 0
    assert seen["spec"] == "loom-benchmark-terminal-bench-2"


def test_install_unknown_slug(
    patched_discovery: None, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = dispatch(["install", "no-such-thing"])
    assert rc == 2
    assert "not found" in capsys.readouterr().err.lower()


def test_refresh_catalog_purges_cache(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    called = {"n": 0}

    def fake_purge() -> None:
        called["n"] += 1

    monkeypatch.setattr("loom_cli.catalog.purge_catalog_cache", fake_purge)
    rc = dispatch(["refresh-catalog"])
    assert rc == 0
    assert called["n"] == 1
    assert "purged" in capsys.readouterr().out.lower()


def test_provision_catalog_provision_invokes_provisioner(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    class FakeCatalog:
        def __init__(self, db_url: str) -> None:
            captured.setdefault("catalogs", []).append(db_url)

    class FakeObjects:
        def __init__(self, **kwargs: object) -> None:
            captured.setdefault("stores", []).append(kwargs)

    async def fake_provision(**kwargs: object):
        from loom_cli.catalog_provision import ProvisionStats

        captured.update(kwargs)
        return ProvisionStats(
            ready_agents=2,
            ready_benchmarks=2,
            ready_tasks=3,
            source_objects=5,
            target_objects_uploaded=1,
            target_objects_skipped=4,
            target_objects_missing=0,
            bytes_uploaded=11,
            bytes_skipped=44,
        )

    monkeypatch.setattr(
        "loom_cli.catalog_provision.PostgresCatalogStore",
        FakeCatalog,
    )
    monkeypatch.setattr(
        "loom_cli.catalog_provision.Boto3CatalogObjectStore",
        FakeObjects,
    )
    monkeypatch.setattr(
        "loom_cli.catalog_provision.provision_ready_benchmark_catalog",
        fake_provision,
    )

    rc = dispatch([
        "provision-catalog",
        "--source-db-url",
        "postgresql://source/db",
        "--target-db-url",
        "postgresql://target/db",
        "--source-minio-endpoint",
        "http://source-minio:9000",
        "--source-minio-access-key",
        "source-access",
        "--source-minio-secret-key",
        "source-secret",
        "--target-minio-endpoint",
        "http://target-minio:9000",
        "--target-minio-access-key",
        "target-access",
        "--target-minio-secret-key",
        "target-secret",
        "--target-bucket",
        "loom-benchmarks",
        "--imported-by",
        "release:staging",
    ])

    assert rc == 0
    assert captured["catalogs"] == [
        "postgresql://source/db",
        "postgresql://target/db",
    ]
    assert captured["target_bucket"] == "loom-benchmarks"
    assert captured["imported_by"] == "release:staging"
    out = capsys.readouterr().out
    assert "ready_agents=2" in out
    assert "ready_benchmarks=2" in out
    assert "ready_tasks=3" in out
    assert "source_objects=5" in out
    assert "uploaded=1" in out
    assert "skipped=4" in out
    assert "missing=0" in out


def test_provision_catalog_provision_uses_service_target_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.setenv("LOOM_CATALOG_SOURCE_DB_URL", "postgresql://source/db")
    monkeypatch.setenv("LOOM_CATALOG_SOURCE_MINIO_ENDPOINT", "http://source-minio:9000")
    monkeypatch.setenv("LOOM_CATALOG_SOURCE_MINIO_ACCESS_KEY", "source-access")
    monkeypatch.setenv("LOOM_CATALOG_SOURCE_MINIO_SECRET_KEY", "source-secret")
    monkeypatch.delenv("LOOM_DB_URL", raising=False)
    monkeypatch.delenv("LOOM_MINIO_ENDPOINT", raising=False)
    monkeypatch.delenv("LOOM_MINIO_ACCESS_KEY", raising=False)
    monkeypatch.delenv("LOOM_MINIO_SECRET_KEY", raising=False)
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql://target/db")
    monkeypatch.setenv("LOOM_SVC_MINIO_ENDPOINT", "http://target-minio:9000")
    monkeypatch.setenv("LOOM_SVC_MINIO_ACCESS_KEY", "target-access")
    monkeypatch.setenv("LOOM_SVC_MINIO_SECRET_KEY", "target-secret")

    class FakeCatalog:
        def __init__(self, db_url: str) -> None:
            captured.setdefault("catalogs", []).append(db_url)

    class FakeObjects:
        def __init__(self, **kwargs: object) -> None:
            captured.setdefault("stores", []).append(kwargs)

    async def fake_provision(**kwargs: object):
        from loom_cli.catalog_provision import ProvisionStats

        captured.update(kwargs)
        return ProvisionStats(
            ready_agents=2,
            ready_benchmarks=1,
            ready_tasks=100,
            source_objects=200,
            target_objects_uploaded=200,
            target_objects_skipped=0,
            target_objects_missing=0,
            bytes_uploaded=123,
            bytes_skipped=0,
        )

    monkeypatch.setattr(
        "loom_cli.catalog_provision.PostgresCatalogStore",
        FakeCatalog,
    )
    monkeypatch.setattr(
        "loom_cli.catalog_provision.Boto3CatalogObjectStore",
        FakeObjects,
    )
    monkeypatch.setattr(
        "loom_cli.catalog_provision.provision_ready_benchmark_catalog",
        fake_provision,
    )

    rc = dispatch(["provision-catalog"])

    assert rc == 0
    assert captured["catalogs"] == [
        "postgresql://source/db",
        "postgresql://target/db",
    ]
    assert captured["stores"] == [
        {
            "endpoint_url": "http://source-minio:9000",
            "access_key": "source-access",
            "secret_key": "source-secret",
        },
        {
            "endpoint_url": "http://target-minio:9000",
            "access_key": "target-access",
            "secret_key": "target-secret",
        },
    ]
    assert "ready_tasks=100" in capsys.readouterr().out


def test_register_uses_service_db_url_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    monkeypatch.delenv("LOOM_DB_URL", raising=False)
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql://target/db")

    async def fake_register(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "registered": 100,
            "legacy_placeholders": 0,
            "skipped": 0,
            "source": "hf",
            "repo_id": "PRHW/loom-benchmark-skilllearnbench",
            "revision": "main",
        }

    monkeypatch.setattr("loom_benchmark_tool.register_cmd.run_register", fake_register)

    rc = dispatch(["register", "skilllearnbench"])

    assert rc == 0
    assert captured["db_url"] == "postgresql://target/db"
    assert "registered=100" in capsys.readouterr().out


def test_register_db_url_precedence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: list[str] = []

    monkeypatch.setenv("LOOM_DB_URL", "postgresql://legacy/db")
    monkeypatch.setenv("LOOM_SVC_DB_URL", "postgresql://service/db")

    async def fake_register(**kwargs: object) -> dict[str, object]:
        seen.append(str(kwargs["db_url"]))
        return {
            "registered": 1,
            "legacy_placeholders": 0,
            "skipped": 0,
            "source": "hf",
            "repo_id": "PRHW/loom-benchmark-skilllearnbench",
            "revision": "main",
        }

    monkeypatch.setattr("loom_benchmark_tool.register_cmd.run_register", fake_register)

    assert dispatch(["register", "skilllearnbench"]) == 0
    assert dispatch([
        "register",
        "skilllearnbench",
        "--db-url",
        "postgresql://flag/db",
    ]) == 0

    assert seen == ["postgresql://legacy/db", "postgresql://flag/db"]
    assert capsys.readouterr().out.count("registered=1") == 2


def test_register_mirror_to_object_store_passes_minio_target(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}
    stores: list[dict[str, object]] = []

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            stores.append(kwargs)

    async def fake_register(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "registered": 100,
            "legacy_placeholders": 0,
            "skipped": 0,
            "mirrored": 100,
            "mirror_uploaded": 200,
            "mirror_skipped": 0,
            "source": "hf",
            "repo_id": "PRHW/loom-benchmark-skilllearnbench",
            "revision": "7908700",
        }

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.register_cmd.run_register", fake_register)

    rc = dispatch([
        "register",
        "skilllearnbench",
        "--db-url",
        "postgresql://target/db",
        "--revision",
        "7908700",
        "--mirror-to-object-store",
        "--minio-endpoint",
        "http://target-minio:9000",
        "--minio-access-key",
        "target-access",
        "--minio-secret-key",
        "target-secret",
        "--bucket",
        "loom-benchmarks",
    ])

    assert rc == 0
    assert stores == [
        {
            "endpoint_url": "http://target-minio:9000",
            "access_key": "target-access",
            "secret_key": "target-secret",
        }
    ]
    assert captured["mirror_to_object_store"] is True
    assert captured["object_store"] is not None
    assert captured["bucket"] == "loom-benchmarks"
    out = capsys.readouterr().out
    assert "mirrored=100" in out
    assert "mirror_uploaded=200" in out


def test_register_source_object_store_requires_minio_creds(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--source=object-store bails cleanly when MinIO creds are missing."""
    for var in (
        "LOOM_MINIO_ENDPOINT",
        "LOOM_MINIO_ACCESS_KEY",
        "LOOM_MINIO_SECRET_KEY",
        "LOOM_SVC_MINIO_ENDPOINT",
        "LOOM_SVC_MINIO_ACCESS_KEY",
        "LOOM_SVC_MINIO_SECRET_KEY",
    ):
        monkeypatch.delenv(var, raising=False)

    rc = dispatch([
        "register",
        "aime-25",
        "--source",
        "object-store",
        "--db-url",
        "postgresql://target/db",
        "--revision",
        "abcd1234",
    ])

    assert rc == 2
    assert "--source=object-store requires" in capsys.readouterr().err


def test_register_source_object_store_requires_explicit_revision(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """No --revision (falls back to 'main') is nonsensical for
    content-addressed layouts; fail loud before touching the bucket."""
    rc = dispatch([
        "register",
        "aime-25",
        "--source",
        "object-store",
        "--db-url",
        "postgresql://target/db",
        "--minio-endpoint",
        "http://minio.local:9000",
        "--minio-access-key",
        "minioadmin",
        "--minio-secret-key",
        "minioadmin123",
    ])

    assert rc == 2
    assert "--revision" in capsys.readouterr().err


def test_register_source_object_store_threads_flags_through(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--source=object-store instantiates MinIO client + forwards
    every relevant flag to run_register without touching HF."""
    captured: dict[str, object] = {}
    stores: list[dict[str, object]] = []

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            stores.append(kwargs)

    async def fake_register(**kwargs: object) -> dict[str, object]:
        captured.update(kwargs)
        return {
            "registered": 30,
            "legacy_placeholders": 0,
            "skipped": 0,
            "mirrored": 0,
            "mirror_uploaded": 0,
            "mirror_skipped": 0,
            "source": "object-store",
            "repo_id": "s3://loom-benchmarks/aime-25",
            "revision": "abcd1234",
        }

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.register_cmd.run_register", fake_register)

    rc = dispatch([
        "register",
        "aime-25",
        "--source",
        "object-store",
        "--db-url",
        "postgresql://target/db",
        "--revision",
        "abcd1234",
        "--minio-endpoint",
        "http://minio.local:9000",
        "--minio-access-key",
        "minioadmin",
        "--minio-secret-key",
        "minioadmin123",
        "--bucket",
        "loom-benchmarks",
    ])

    assert rc == 0
    assert captured["source"] == "object-store"
    assert captured["revision"] == "abcd1234"
    assert captured["bucket"] == "loom-benchmarks"
    assert captured["object_store"] is not None
    assert captured["mirror_to_object_store"] is False
    assert stores == [
        {
            "endpoint_url": "http://minio.local:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin123",
        }
    ]
    out = capsys.readouterr().out
    assert "source=object-store" in out
    assert "rev=abcd1234" in out


def test_verify_minio_env_precedence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    stores: list[dict[str, object]] = []

    monkeypatch.setenv("LOOM_MINIO_ENDPOINT", "http://legacy-minio:9000")
    monkeypatch.setenv("LOOM_MINIO_ACCESS_KEY", "legacy-access")
    monkeypatch.setenv("LOOM_MINIO_SECRET_KEY", "legacy-secret")
    monkeypatch.setenv("LOOM_SVC_MINIO_ENDPOINT", "http://service-minio:9000")
    monkeypatch.setenv("LOOM_SVC_MINIO_ACCESS_KEY", "service-access")
    monkeypatch.setenv("LOOM_SVC_MINIO_SECRET_KEY", "service-secret")

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            stores.append(kwargs)

    async def fake_verify(**kwargs: object) -> dict[str, object]:
        return {
            "total": 1,
            "passed": 1,
            "failed": 0,
            "results": [{"passed": True}],
        }

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.verify_cmd.run_verify", fake_verify)

    assert dispatch(["verify", "skilllearnbench", "--limit", "1"]) == 0
    assert dispatch([
        "verify",
        "skilllearnbench",
        "--limit",
        "1",
        "--minio-endpoint",
        "http://flag-minio:9000",
        "--minio-access-key",
        "flag-access",
        "--minio-secret-key",
        "flag-secret",
    ]) == 0

    assert stores == [
        {
            "endpoint_url": "http://legacy-minio:9000",
            "access_key": "legacy-access",
            "secret_key": "legacy-secret",
        },
        {
            "endpoint_url": "http://flag-minio:9000",
            "access_key": "flag-access",
            "secret_key": "flag-secret",
        },
    ]
    assert capsys.readouterr().out.count("passed=1") == 2


def test_verify_reports_immutable_revision_discovery_as_blocked(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("LOOM_MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("LOOM_MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("LOOM_MINIO_SECRET_KEY", "secret")

    class FakeObjectStore:
        def __init__(self, **_kwargs: object) -> None:
            pass

    async def fake_verify(**_kwargs: object) -> dict[str, object]:
        return {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "results": [],
            "blocked_reason": "use registered task sources",
        }

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.verify_cmd.run_verify", fake_verify)

    assert dispatch(["verify", "team-evals"]) == 2
    out = capsys.readouterr().out
    assert "BLOCKED: use registered task sources" in out
    assert "WARNING: no tasks found" not in out


def test_import_passes_instance_ids_to_benchmark_tool(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, object] = {}

    class FakeObjectStore:
        def __init__(self, **kwargs: object) -> None:
            captured["store_kwargs"] = kwargs

    async def fake_run_import(**kwargs: object) -> dict[str, int]:
        captured.update(kwargs)
        return {"converted": 2, "warnings": 0}

    monkeypatch.setattr("loom.trajectory.storage.MinioObjectStore", FakeObjectStore)
    monkeypatch.setattr("loom_benchmark_tool.import_cmd.run_import", fake_run_import)

    rc = dispatch([
        "import",
        "humaneval",
        "--db-url",
        "postgresql://loom:loom@db/loom",
        "--minio-endpoint",
        "http://minio:9000",
        "--minio-access-key",
        "access",
        "--minio-secret-key",
        "secret",
        "--instance-id",
        "HumanEval/1",
        "--instance-id",
        "HumanEval/0",
    ])

    assert rc == 0
    assert captured["instance_ids"] == {"HumanEval/0", "HumanEval/1"}
    assert "converted=2" in capsys.readouterr().out


def test_top_level_main_routes_to_datasets(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_cli.__main__ import main

    def fake_dispatch(argv: list[str]) -> int:
        print("ROUTED:" + " ".join(argv))
        return 0

    monkeypatch.setattr("loom_cli.datasets_cmd.dispatch", fake_dispatch)
    rc = main(["datasets", "list", "--json"])
    assert rc == 0
    assert "ROUTED:list --json" in capsys.readouterr().out


def test_sync_mirror_requires_source_and_dest_creds(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """sync-mirror bails cleanly when either side's creds are missing."""
    for var in (
        "LOOM_MINIO_ENDPOINT", "LOOM_MINIO_ACCESS_KEY", "LOOM_MINIO_SECRET_KEY",
        "LOOM_SVC_MINIO_ENDPOINT", "LOOM_SVC_MINIO_ACCESS_KEY", "LOOM_SVC_MINIO_SECRET_KEY",
        "LOOM_R2_ENDPOINT", "LOOM_R2_ACCESS_KEY", "LOOM_R2_SECRET_KEY",
    ):
        monkeypatch.delenv(var, raising=False)

    rc = dispatch(["sync-mirror"])
    err = capsys.readouterr().err
    assert rc == 2
    assert "sync-mirror requires" in err
    assert "--source-endpoint" in err
    assert "--dest-endpoint" in err


def test_sync_mirror_threads_flags_and_prints_stats(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """sync-mirror forwards every flag to run_sync_mirror and formats
    the returned SyncStats deterministically."""
    from loom_benchmark_tool.sync_cmd import SyncStats

    captured: dict[str, object] = {}

    async def fake_run_sync_mirror(**kwargs: object) -> SyncStats:
        captured.update(kwargs)
        return SyncStats(
            listed=10,
            uploaded=7,
            skipped_size_match=3,
            bytes_uploaded=1024,
            bytes_skipped=512,
        )

    monkeypatch.setattr(
        "loom_benchmark_tool.sync_cmd.run_sync_mirror", fake_run_sync_mirror,
    )

    rc = dispatch([
        "sync-mirror",
        "--source-endpoint", "http://minio.local:9000",
        "--source-access-key", "src-a",
        "--source-secret-key", "src-s",
        "--source-bucket", "loom-benchmarks",
        "--dest-endpoint", "https://acct.r2.cloudflarestorage.com",
        "--dest-access-key", "dst-a",
        "--dest-secret-key", "dst-s",
        "--dest-bucket", "loom-benchmarks-public",
        "--prefix", "aime-25/",
    ])

    assert rc == 0
    assert captured["source_endpoint"] == "http://minio.local:9000"
    assert captured["dest_endpoint"] == "https://acct.r2.cloudflarestorage.com"
    assert captured["source_bucket"] == "loom-benchmarks"
    assert captured["dest_bucket"] == "loom-benchmarks-public"
    assert captured["prefix"] == "aime-25/"
    assert captured["dry_run"] is False
    out = capsys.readouterr().out
    assert "sync-mirror:" in out
    assert "listed=10" in out
    assert "uploaded=7" in out
    assert "skipped=3" in out
    assert "[dry-run]" not in out


def test_sync_mirror_dry_run_marker_in_output(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--dry-run appears in the header so operators can spot no-op runs."""
    from loom_benchmark_tool.sync_cmd import SyncStats

    async def fake_run_sync_mirror(**_kwargs: object) -> SyncStats:
        return SyncStats(
            listed=1, uploaded=1, skipped_size_match=0,
            bytes_uploaded=100, bytes_skipped=0,
        )

    monkeypatch.setattr(
        "loom_benchmark_tool.sync_cmd.run_sync_mirror", fake_run_sync_mirror,
    )

    dispatch([
        "sync-mirror",
        "--source-endpoint", "http://minio.local:9000",
        "--source-access-key", "src-a",
        "--source-secret-key", "src-s",
        "--dest-endpoint", "https://acct.r2.cloudflarestorage.com",
        "--dest-access-key", "dst-a",
        "--dest-secret-key", "dst-s",
        "--dry-run",
    ])
    assert "[dry-run]" in capsys.readouterr().out
