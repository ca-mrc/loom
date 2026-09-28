from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import tarfile
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom.models.batch import Combination
from loom_cli import nebius_acceptance
from loom_cli.nebius_acceptance import (
    _ACCEPTANCE_REQUIRED_OUTPUTS,
    NebiusAcceptanceError,
    _batch_shape,
    _capacity_sample,
    _pool_is_idle,
    acceptance_stages,
    configure_parser,
    load_capacity_policy,
    resume_cleanup,
    run_acceptance,
    run_cli,
    validate_trial_bundle,
)

_CANDIDATE = "a" * 40
_CONNECTION_ID = "00000000-0000-0000-0000-0000000000aa"
_BATCH_ID = "00000000-0000-0000-0000-0000000000bb"
_TRIAL_ID = "00000000-0000-0000-0000-0000000000cc"


def _canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _bundle(
    *,
    trial_id: str = _TRIAL_ID,
    answer: bytes = b"complete\n",
    omit: str | None = None,
) -> bytes:
    outputs = {
        "files/artifacts/answer.txt": answer,
        "files/artifacts/reasoning.md": b"reasoning\n",
        "files/trajectory/events.jsonl": b'{"seq":1}\n',
        "files/accounting/usage.json": b'{"call_count":1}\n',
        "files/verifier/output.json": b'{"rewards":{"artifact_complete":1.0}}\n',
    }
    if omit is not None:
        del outputs[omit]
    manifest = {
        "schema_version": "loom.canonical-trial-bundle-export.v1",
        "artifact_id": "00000000-0000-0000-0000-0000000000dd",
        "trial_id": trial_id,
        "task_id": "ts/test/task-1",
        "attempt": 1,
        "manifest_sha256": "sha256:" + "b" * 64,
        "content_sha256": "sha256:" + "c" * 64,
        "files": [
            {
                "relative_path": name,
                "size_bytes": len(payload),
                "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
                "media_type": "application/octet-stream",
            }
            for name, payload in outputs.items()
        ],
    }
    files = {"bundle.json": _canonical(manifest), **outputs}
    ledger = "".join(
        f"{hashlib.sha256(payload).hexdigest()}  {name}\n" for name, payload in files.items()
    ).encode()
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0, filename="") as gz:
        with tarfile.open(fileobj=gz, mode="w") as archive:
            for name, payload in {**files, "checksums/SHA256SUMS": ledger}.items():
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                info.mtime = 0
                archive.addfile(info, io.BytesIO(payload))
    return raw.getvalue()


def _policy(tmp_path: Path, *, accepted: int = 56, target_id: str | None = None) -> dict[str, Any]:
    path = tmp_path / "policy.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "loom.nebius-development-capacity.v1",
                "target_id": target_id or "nebius-eu-north1-development",
                "accepted_concurrency": accepted,
                "target_concurrency": 200,
                "admission_policies": [
                    {
                        "scope_kind": "global",
                        "scope_key": "*",
                        "max_concurrent": accepted,
                        "enabled": True,
                    },
                    {
                        "scope_kind": "pool",
                        "scope_key": "nebius-cpu",
                        "max_concurrent": accepted,
                        "enabled": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return load_capacity_policy(path)


def _monitor(
    *,
    nodes: int,
    slots: int,
    occupied: int = 0,
    target_id: str = "nebius-eu-north1-development",
) -> dict[str, Any]:
    return {
        "resources": {"pools": []},
        "service_execution": {
            "activity": {"execution_states": {"running": occupied}},
            "targets": [
                {
                    "provider": "nebius",
                    "target_id": target_id,
                    "pool_id": "nebius-cpu",
                    "environment": "development",
                    "health_status": "healthy",
                    "command_backlog": 0,
                    "blockers": [],
                    "resource_profile": {"observed_fit_slots": slots},
                    "observation": {
                        "is_fresh": True,
                        "active_nodes": nodes,
                        "requested_cpu_millis": occupied * 2000,
                        "pending_jobs": 0,
                        "node_states": {
                            "desired": nodes,
                            "creating": 0,
                            "ready": nodes,
                            "failed": 0,
                            "deleting": 0,
                        },
                        "provider_capacity_state": "available",
                        "autoscaler_state": "ready",
                    },
                }
            ],
        },
    }


def test_acceptance_stages_follow_current_and_target_envelopes() -> None:
    assert acceptance_stages(56, []) == [1, 20, 40, 56]
    assert acceptance_stages(200, []) == [1, 20, 50, 100, 150, 200]
    assert acceptance_stages(250, [])[-1] == 250
    with pytest.raises(NebiusAcceptanceError, match="exceeds persisted"):
        acceptance_stages(56, [57])


def test_batch_shape_supports_200_trials_without_exceeding_api_sample_limit() -> None:
    trial_config, n_per_task, combinations = _batch_shape(
        trials_per_task=200,
        agent_name="litellm",
        agent_model={"provider": "openai", "name": "model", "source": "api"},
        provider_connection_id=_CONNECTION_ID,
        provider_model_id="model",
    )
    assert trial_config == {}
    assert n_per_task == 1
    assert [row["n_per_task"] for row in combinations] == [100, 100]
    assert [row["label"] for row in combinations] == [
        "nebius-acceptance-1",
        "nebius-acceptance-2",
    ]
    assert [Combination.model_validate(row).n_per_task for row in combinations] == [100, 100]


def test_required_outputs_match_real_canonical_export_layout() -> None:
    validate_trial_bundle(
        _bundle(), trial_id=_TRIAL_ID, required_outputs=_ACCEPTANCE_REQUIRED_OUTPUTS
    )
    with pytest.raises(NebiusAcceptanceError, match="missing required outputs"):
        validate_trial_bundle(
            _bundle(omit="files/trajectory/events.jsonl"),
            trial_id=_TRIAL_ID,
            required_outputs=_ACCEPTANCE_REQUIRED_OUTPUTS,
        )


def test_capacity_uses_fresh_service_target_without_inventing_pool_slots() -> None:
    monitor = _monitor(nodes=1, slots=7, occupied=2)
    sample = _capacity_sample(monitor, pool_id="nebius-cpu", environment="development")
    assert sample["running_tasks"] == 2
    assert "current_active_slots" not in sample
    monitor["service_execution"]["targets"][0]["observation"]["is_fresh"] = False
    with pytest.raises(NebiusAcceptanceError, match="stale"):
        _capacity_sample(monitor, pool_id="nebius-cpu", environment="development")


@pytest.mark.parametrize("missing", ["node_states", "requested_cpu_millis", "pending_jobs"])
def test_absent_capacity_fields_cannot_prove_zero(missing: str) -> None:
    monitor = _monitor(nodes=0, slots=0)
    del monitor["service_execution"]["targets"][0]["observation"][missing]
    try:
        sample = _capacity_sample(monitor, pool_id="nebius-cpu", environment="development")
    except NebiusAcceptanceError:
        return
    assert not _pool_is_idle(sample)


def test_capacity_policy_requires_matching_admission_limits(tmp_path: Path) -> None:
    policy = _policy(tmp_path)
    assert policy["accepted_concurrency"] == 56
    assert len(policy["_sha256"]) == 64
    policy_path = tmp_path / "policy.json"
    body = json.loads(policy_path.read_text())
    body["admission_policies"][1]["max_concurrent"] = 55
    policy_path.write_text(json.dumps(body), encoding="utf-8")
    with pytest.raises(NebiusAcceptanceError, match="admission limits"):
        load_capacity_policy(policy_path)


def test_validate_trial_bundle_checks_manifest_and_every_member() -> None:
    payload = _bundle()
    manifest, manifest_sha256 = validate_trial_bundle(payload, trial_id=_TRIAL_ID)
    assert manifest["files"][0]["relative_path"] == "files/artifacts/answer.txt"
    with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
        manifest_stream = archive.extractfile("bundle.json")
        assert manifest_stream is not None
        assert manifest_sha256 == hashlib.sha256(manifest_stream.read()).hexdigest()
    with pytest.raises(NebiusAcceptanceError, match="identity"):
        validate_trial_bundle(payload, trial_id="different")
    with pytest.raises(NebiusAcceptanceError, match="missing required outputs"):
        validate_trial_bundle(
            _bundle(omit="files/trajectory/events.jsonl"),
            trial_id=_TRIAL_ID,
            required_outputs={"files/trajectory/events.jsonl"},
        )


@pytest.mark.parametrize(
    "target_id", [None, "nebius-eu-north1-integration", "nebius-eu-west1-integration"]
)
@pytest.mark.parametrize("cleanup_state", ["complete", "retained", "running"])
@pytest.mark.parametrize("connection", [{"id": _CONNECTION_ID}])
@pytest.mark.parametrize("observed_running", [0, 1, 2])
def test_run_acceptance_uses_public_api_and_persists_complete_evidence(
    tmp_path: Path,
    cleanup_state: str,
    target_id: str | None,
    connection: dict[str, Any] | None,
    observed_running: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _bundle()
    calls: list[tuple[str, str]] = []
    batch_reads = 0

    def _target_monitor(**kwargs: int) -> dict[str, Any]:
        monitor = _monitor(**kwargs, target_id=target_id or "nebius-eu-north1-development")
        if target_id == "nebius-eu-west1-integration":
            monitor["service_execution"]["targets"].extend(
                _monitor(nodes=0, slots=0, target_id="nebius-eu-north1-integration")[
                    "service_execution"
                ]["targets"]
            )
        return monitor

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal batch_reads
        calls.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path == "/api/v1/tasksets/ts/test":
            return httpx.Response(200, json={"evaluation_ready": True, "task_count": 1})
        if request.method == "GET" and request.url.path == "/api/v1/monitor/summary":
            if request.url.params.get("batch_id"):
                return httpx.Response(
                    200, json=_target_monitor(nodes=1, slots=1, occupied=observed_running)
                )
            if any(path.endswith("/bundle/download") for _, path in calls):
                return httpx.Response(200, json=_target_monitor(nodes=0, slots=0))
            return httpx.Response(200, json=_target_monitor(nodes=0, slots=0))
        if request.method == "POST" and request.url.path == "/api/v1/batches":
            submitted = json.loads(request.content)
            assert submitted["backend"] == "nebius"
            assert submitted["n_per_task"] == 1
            # #2054: model-backed acceptance always names its connection.
            assert submitted["provider_connection_id"] == connection["id"]
            return httpx.Response(
                201,
                json={
                    "batch_id": _BATCH_ID,
                    "name": submitted["name"],
                    "backend": "nebius",
                    "expected_trial_count": 1,
                },
            )
        if request.method == "GET" and request.url.path == f"/api/v1/batches/{_BATCH_ID}":
            batch_reads += 1
            terminal = batch_reads > 1
            return httpx.Response(
                200,
                json={
                    "id": _BATCH_ID,
                    "state": "finished" if terminal else "running",
                    "result_status": "succeeded" if terminal else None,
                    "trial_summary": {
                        "queued": 0,
                        "claimed": 0 if terminal else 1,
                        "running": 0,
                        "materializing": 0,
                        "succeeded": 1 if terminal else 0,
                        "failed": 0,
                        "cancelled": 0,
                    },
                    "service_execution_runtime_profile": {
                        "candidate_sha": _CANDIDATE,
                        "sha256": "d" * 64,
                    },
                    "service_execution_summary": {"canonical_ready_count": 1},
                },
            )
        if request.method == "GET" and request.url.path == "/api/v1/trials":
            assert request.url.params["batch_id"] == _BATCH_ID
            return httpx.Response(
                200,
                json={"items": [{"id": _TRIAL_ID, "state": "succeeded"}], "next_cursor": None},
            )
        if request.method == "GET" and request.url.path == f"/api/v1/trials/{_TRIAL_ID}":
            return httpx.Response(
                200,
                json={
                    "id": _TRIAL_ID,
                    "task_id": "ts/test/task-1",
                    "state": "succeeded",
                    "materialization": {
                        "canonical_ready": True,
                        "source_cleanup_state": cleanup_state,
                        "source_retain_until": "2026-09-06T00:08:29Z",
                        "bundle": {"file_count": 1},
                    },
                },
            )
        if (
            request.method == "GET"
            and request.url.path == f"/api/v1/trials/{_TRIAL_ID}/bundle/download"
        ):
            return httpx.Response(
                200,
                content=payload,
                headers={"X-Content-SHA256": "sha256:" + hashlib.sha256(payload).hexdigest()},
            )
        return httpx.Response(404, json={"detail": "unhandled test route"})

    with (
        httpx.Client(
            base_url="https://loom.test", transport=httpx.MockTransport(handler)
        ) as client,
        (
            nullcontext()
            if observed_running == 1
            else pytest.raises(
                NebiusAcceptanceError, match=r"simultaneously running|exceed the stage"
            )
        ),
    ):
        evidence = run_acceptance(
            client=client,
            output_dir=tmp_path / "evidence",
            capacity_policy=_policy(tmp_path, target_id=target_id),
            task_set_id="ts/test",
            task_count=1,
            provider_connection=connection,
            provider_model_id="model",
            agent_name="litellm",
            agent_provider="openai",
            candidate_sha=_CANDIDATE,
            environment="development",
            target_id=target_id,
            pool_id="nebius-cpu",
            stages=[1],
            poll_seconds=0,
            stage_timeout_seconds=1,
            scale_down_timeout_seconds=1,
            sleeper=lambda _: None,
        )

    if observed_running != 1:
        assert (tmp_path / "evidence" / "progress.json").is_file()
        assert not (tmp_path / "evidence" / "acceptance.json").exists()
        return

    output = tmp_path / "evidence"
    assert evidence["target_id"] == target_id
    assert evidence["accepted"] is True
    assert evidence["source_gc_complete"] is (cleanup_state == "complete")
    assert evidence["maximum_proven_concurrency"] == 1
    assert evidence["policy_ceiling_reached"] is False
    assert evidence["quota_maximum_acceptance"] == "not_evaluated"
    assert evidence["stages"][0]["max_overlapping_execution_units"] == 1
    assert evidence["stages"][0]["max_node_backed_overlapping_execution_units"] == 1
    assert (output / "stage-1" / "trials" / f"{_TRIAL_ID}.tar.gz").read_bytes() == payload
    evidence_bytes = (output / "acceptance.json").read_bytes()
    sidecar = (output / "acceptance.json.sha256").read_text()
    assert sidecar == f"{hashlib.sha256(evidence_bytes).hexdigest()}  acceptance.json\n"
    assert not any(path.startswith("/admin/") for _, path in calls)

    calls.clear()
    with httpx.Client(
        base_url="https://loom.test", transport=httpx.MockTransport(handler)
    ) as client:
        cleanup = resume_cleanup(
            client=client, evidence_dir=output, output_dir=tmp_path / "cleanup"
        )
    assert cleanup["source_gc_complete"] is (cleanup_state == "complete")
    assert cleanup["accepted"] is (cleanup_state == "complete")
    assert all(method == "GET" for method, _ in calls)
    assert (output / "acceptance.json").read_bytes() == evidence_bytes

    # The original immutable record supports repeated read-only continuation after retention expires.
    cleanup_state = "complete"
    with httpx.Client(
        base_url="https://loom.test", transport=httpx.MockTransport(handler)
    ) as client:
        cleanup = resume_cleanup(
            client=client, evidence_dir=output, output_dir=tmp_path / "cleanup-complete"
        )
    assert cleanup["accepted"] is True

    corruptions = ["stale", "candidate", "canonical", "not_ready", "failed", "archive"]
    if target_id:
        corruptions.append("target")
    for corruption in corruptions:

        def invalid_handler(request: httpx.Request, corruption: str = corruption) -> httpx.Response:
            response = handler(request)
            if corruption == "archive" and request.url.path.endswith("/bundle/download"):
                return httpx.Response(200, content=b"bad", headers=response.headers)
            if request.url.path.endswith("/bundle/download"):
                return response
            body = response.json()
            if corruption == "target" and request.url.path.endswith("/monitor/summary"):
                body["service_execution"]["targets"][0]["target_id"] = "wrong-target"
            if corruption == "stale" and request.url.path.endswith("/monitor/summary"):
                body["service_execution"]["targets"][0]["observation"]["is_fresh"] = False
            if corruption == "candidate" and request.url.path == f"/api/v1/batches/{_BATCH_ID}":
                body["service_execution_runtime_profile"]["candidate_sha"] = "f" * 40
            if request.url.path == f"/api/v1/trials/{_TRIAL_ID}":
                if corruption == "canonical":
                    body["materialization"]["canonical_ready"] = False
                elif corruption in {"not_ready", "failed"}:
                    body["materialization"]["source_cleanup_state"] = corruption
            return httpx.Response(response.status_code, json=body)

        with httpx.Client(
            base_url="https://loom.test", transport=httpx.MockTransport(invalid_handler)
        ) as client:
            with pytest.raises(NebiusAcceptanceError):
                resume_cleanup(
                    client=client,
                    evidence_dir=output,
                    output_dir=tmp_path / f"rejected-{corruption}",
                )
    assert all(method == "GET" for method, _ in calls)

    if connection is None:
        # Exercise the real parser + command handler with the normal-user gateway path.
        parser = argparse.ArgumentParser()
        configure_parser(parser.add_subparsers())
        args = parser.parse_args(
            [
                "nebius-acceptance",
                *(["--target-id", target_id] if target_id else []),
                "--task-set",
                "ts/test",
                "--model",
                "local/model",
                "--candidate-sha",
                _CANDIDATE,
                "--capacity-policy",
                str(tmp_path / "policy.json"),
                "--stage",
                "1",
                "--poll-seconds",
                "0",
                "--output",
                str(tmp_path / "cli-evidence"),
            ]
        )
        monkeypatch.setattr(nebius_acceptance, "require_logged_in", lambda: object())
        monkeypatch.setattr(
            nebius_acceptance,
            "authed_client",
            lambda *args, **kwargs: httpx.Client(
                base_url="https://loom.test", transport=httpx.MockTransport(handler)
            ),
        )
        batch_reads = 0
        calls.clear()
        assert run_cli(args) == 0
        assert not any("provider" in path for _, path in calls)
        assert (tmp_path / "cli-evidence" / "acceptance.json").is_file()

    (output / "acceptance.json").write_bytes(evidence_bytes + b" ")
    with httpx.Client(
        base_url="https://loom.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(NebiusAcceptanceError, match="checksum mismatch"):
            resume_cleanup(client=client, evidence_dir=output, output_dir=tmp_path / "tampered")


def test_run_acceptance_refuses_non_idle_shared_baseline(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_monitor(nodes=1, slots=8))

    with httpx.Client(
        base_url="https://loom.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(NebiusAcceptanceError, match="idle 0-node"):
            run_acceptance(
                client=client,
                output_dir=tmp_path / "evidence",
                capacity_policy=_policy(tmp_path),
                task_set_id="ts/test",
                task_count=1,
                provider_connection={"id": "connection"},
                provider_model_id="model",
                agent_name="litellm",
                agent_provider="openai",
                candidate_sha=_CANDIDATE,
                environment="development",
                pool_id="nebius-cpu",
                stages=[1],
                poll_seconds=0,
                stage_timeout_seconds=1,
                scale_down_timeout_seconds=1,
                sleeper=lambda _: None,
            )


def test_run_acceptance_refuses_output_file_before_api_use(tmp_path: Path) -> None:
    output = tmp_path / "evidence"
    output.write_text("owned", encoding="utf-8")

    with httpx.Client(base_url="https://loom.test") as client:
        with pytest.raises(NebiusAcceptanceError, match="not a directory"):
            run_acceptance(
                client=client,
                output_dir=output,
                capacity_policy=_policy(tmp_path),
                task_set_id="ts/test",
                task_count=1,
                provider_connection={"id": "connection"},
                provider_model_id="model",
                agent_name="litellm",
                agent_provider="openai",
                candidate_sha=_CANDIDATE,
                environment="development",
                pool_id="nebius-cpu",
                stages=[1],
                poll_seconds=0,
                stage_timeout_seconds=1,
                scale_down_timeout_seconds=1,
                sleeper=lambda _: None,
            )
    assert output.read_text(encoding="utf-8") == "owned"


def test_run_acceptance_cancels_nonterminal_batch_on_timeout(tmp_path: Path) -> None:
    cancelled = False

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal cancelled
        if request.method == "GET" and request.url.path == "/api/v1/monitor/summary":
            return httpx.Response(
                200,
                json=(
                    _monitor(nodes=1, slots=1, occupied=1)
                    if request.url.params.get("batch_id")
                    else _monitor(nodes=0, slots=0)
                ),
            )
        if request.method == "POST" and request.url.path == "/api/v1/batches":
            return httpx.Response(
                201,
                json={
                    "batch_id": _BATCH_ID,
                    "backend": "nebius",
                    "expected_trial_count": 1,
                },
            )
        if request.method == "GET" and request.url.path == f"/api/v1/batches/{_BATCH_ID}":
            return httpx.Response(
                200,
                json={
                    "state": "running",
                    "trial_summary": {"running": 1},
                },
            )
        if request.method == "POST" and request.url.path == f"/api/v1/batches/{_BATCH_ID}/cancel":
            cancelled = True
            return httpx.Response(202, json={"state": "cancelling"})
        return httpx.Response(404)

    with httpx.Client(
        base_url="https://loom.test", transport=httpx.MockTransport(handler)
    ) as client:
        with pytest.raises(NebiusAcceptanceError, match="timed out"):
            run_acceptance(
                client=client,
                output_dir=tmp_path / "evidence",
                capacity_policy=_policy(tmp_path),
                task_set_id="ts/test",
                task_count=1,
                provider_connection={"id": "connection"},
                provider_model_id="model",
                agent_name="litellm",
                agent_provider="openai",
                candidate_sha=_CANDIDATE,
                environment="development",
                pool_id="nebius-cpu",
                stages=[1],
                poll_seconds=0,
                stage_timeout_seconds=0,
                scale_down_timeout_seconds=1,
                sleeper=lambda _: None,
            )
    assert cancelled is True
    progress = json.loads((tmp_path / "evidence" / "progress.json").read_bytes())
    assert progress["accepted"] is False
    assert progress["candidate_sha"] == _CANDIDATE
    assert progress["current_stage"]["batch_id"] == _BATCH_ID
    assert len(progress["current_stage"]["capacity_samples"]) == 1


def test_parser_defaults_to_testing_connection_and_read_only_cleanup_resume() -> None:
    parser = argparse.ArgumentParser()
    configure_parser(parser.add_subparsers())
    fresh = parser.parse_args(
        [
            "nebius-acceptance",
            "--task-set",
            "ts/test",
            "--model",
            "local/model",
            "--candidate-sha",
            _CANDIDATE,
            "--output",
            "new-evidence",
        ]
    )
    assert fresh.provider == "az-gateway-loom-testing"
    resume = parser.parse_args(
        ["nebius-acceptance", "--resume-cleanup", "old-evidence", "--output", "cleanup-evidence"]
    )
    assert resume.model is None and resume.candidate_sha is None


def test_cli_reports_local_taskset_error_before_login(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    policy_path = tmp_path / "policy.json"
    policy_path.write_text(
        json.dumps(
            {
                "schema_version": "loom.nebius-development-capacity.v1",
                "target_id": "nebius-eu-north1-development",
                "accepted_concurrency": 1,
                "target_concurrency": 200,
                "admission_policies": [
                    {
                        "scope_kind": "global",
                        "scope_key": "*",
                        "max_concurrent": 1,
                        "enabled": True,
                    },
                    {
                        "scope_kind": "pool",
                        "scope_key": "nebius-cpu",
                        "max_concurrent": 1,
                        "enabled": True,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        capacity_policy=str(policy_path),
        stage=[],
        taskset_dir=str(tmp_path / "missing"),
        task_set=None,
        http_timeout=1,
    )

    assert run_cli(args) == 1
    assert "manifest.yaml not found" in capsys.readouterr().err


@pytest.mark.parametrize("actual_target", ["nebius-eu-north1-development", None])
def test_explicit_integration_target_rejects_wrong_or_absent_monitor_identity(
    actual_target: str | None,
) -> None:
    monitor = _monitor(nodes=0, slots=0)
    monitor["service_execution"]["targets"][0]["target_id"] = actual_target
    with pytest.raises(NebiusAcceptanceError, match="target identity"):
        _capacity_sample(
            monitor,
            pool_id="nebius-cpu",
            environment="development",
            target_id="nebius-eu-north1-integration",
        )


@pytest.mark.parametrize("target_id", [None, "nebius-eu-west1-integration"])
def test_monitor_rejects_ambiguous_target_identity(target_id: str | None) -> None:
    monitor = _monitor(nodes=0, slots=0, target_id="nebius-eu-west1-integration")
    monitor["service_execution"]["targets"] *= 2
    with pytest.raises(NebiusAcceptanceError, match="exactly one"):
        _capacity_sample(
            monitor,
            pool_id="nebius-cpu",
            environment="development",
            target_id=target_id,
        )


@pytest.mark.parametrize(
    "field,value", [("pool_id", "other"), ("environment", "production"), ("provider", "other")]
)
def test_explicit_target_still_requires_matching_scope(field: str, value: str) -> None:
    monitor = _monitor(nodes=0, slots=0, target_id="nebius-eu-west1-integration")
    monitor["service_execution"]["targets"][0][field] = value
    with pytest.raises(NebiusAcceptanceError):
        _capacity_sample(
            monitor,
            pool_id="nebius-cpu",
            environment="development",
            target_id="nebius-eu-west1-integration",
        )


@pytest.mark.parametrize(
    "policies",
    [
        [],
        [{"scope_kind": "global", "scope_key": "*", "enabled": False, "max_concurrent": 4}],
        [{"scope_kind": "pool", "scope_key": "nebius-cpu", "enabled": True, "max_concurrent": 100}],
    ],
)
def test_capacity_policy_separates_run_budget_from_live_admission(
    tmp_path: Path,
    policies: list[dict[str, Any]],
) -> None:
    _policy(tmp_path, accepted=1)
    path = tmp_path / "policy.json"
    body = json.loads(path.read_text())
    body["admission_policies"] = policies
    path.write_text(json.dumps(body))
    loaded = load_capacity_policy(path)
    assert acceptance_stages(loaded["accepted_concurrency"], [1]) == [1]
    assert loaded["admission_policies"] == policies
    with pytest.raises(NebiusAcceptanceError, match="exceeds persisted"):
        acceptance_stages(loaded["accepted_concurrency"], [2])
