from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from scripts.ops import prepare_nebius_runtime_profile as prepare
from tests.unit.test_nebius_platform_render import ROOT, platform_inputs  # noqa: F401

from loom.execution_image_admission import (
    ImageAdmissionKeyring,
    verify_execution_image_admission,
)
from loom.nebius_platform_render import build_platform
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

SHA = "7" * 40
REGISTRY = "cr.eu-north1.nebius.cloud/e00example"
DIGESTS = {"service": "1" * 64, "execution_runtime": "2" * 64}


@pytest.fixture(autouse=True)
def _fixed_candidate_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        prepare,
        "_candidate_issued_at",
        lambda _: datetime(2026, 9, 2, 12, 0, tzinfo=UTC),
    )
    monkeypatch.setattr(prepare, "_release_is_ancestor", lambda ancestor, candidate: True)


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _inputs(tmp_path: Path) -> argparse.Namespace:
    source_refs = {
        "service": f"ghcr.io/qianyi-sun/loom-service@sha256:{DIGESTS['service']}",
        "execution_runtime": (
            "ghcr.io/qianyi-sun/loom-execution-runtime@sha256:" + DIGESTS["execution_runtime"]
        ),
    }
    target_refs = {
        "service": f"{REGISTRY}/loom-service@sha256:{DIGESTS['service']}",
        "execution_runtime": (
            f"{REGISTRY}/loom-execution-runtime@sha256:" + DIGESTS["execution_runtime"]
        ),
    }
    mirror = tmp_path / "mirror.json"
    _write_json(
        mirror,
        {
            "schema_version": "loom.nebius-release-mirror.v1",
            "candidate_sha": SHA,
            "images": {
                key: {"source_ref": source_refs[key], "target_ref": target_refs[key]}
                for key in source_refs
            },
        },
    )
    evidence = tmp_path / "evidence.json"
    _write_json(
        evidence,
        {
            "schema_version": "loom.nebius-runtime-evidence.v1",
            "images": {
                "service": {
                    "image_ref": target_refs["service"],
                    "sbom_sha256": "sha256:" + "3" * 64,
                    "vulnerability_report_sha256": "sha256:" + "4" * 64,
                    "highest_vulnerability_severity": "unknown",
                },
                "execution_runtime": {
                    "image_ref": target_refs["execution_runtime"],
                    "sbom_sha256": "sha256:" + "5" * 64,
                    "vulnerability_report_sha256": "sha256:" + "6" * 64,
                    "highest_vulnerability_severity": "high",
                },
            },
            "runtime_binary_sha256": "sha256:" + "8" * 64,
        },
    )
    releases: dict[str, Path] = {}
    for key, component, image_name in prepare._COMPONENTS:
        path = tmp_path / f"{component}.json"
        _write_json(
            path,
            {
                "schema_version": 1,
                "image": {"component": component, "platform": "linux/amd64"},
                "release": {"commit": SHA, "ref": "refs/heads/dev"},
                "subject": {
                    "name": f"ghcr.io/qianyi-sun/{image_name}",
                    "digest": "sha256:" + DIGESTS[key],
                },
                "scan": {
                    "config_sha256": "a" * 64,
                    "ignore_sha256": "b" * 64,
                    "scanner": {"name": "Trivy", "version": "v0.74.0"},
                },
            },
        )
        releases[key] = path
    return argparse.Namespace(
        candidate_sha=SHA,
        mirror_record=mirror,
        evidence_summary=evidence,
        service_release_record=releases["service"],
        execution_runtime_release_record=releases["execution_runtime"],
        signing_key=tmp_path / "admission-signing-key.pem",
        signing_key_id="nebius-development-2026-09",
        create_signing_key=True,
        output_profile=tmp_path / "profile.json",
        output_keyring=tmp_path / "keyring.json",
        output_policy=tmp_path / "policy.json",
    )


def test_prepare_creates_reusable_key_and_verified_profile(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    result = prepare.prepare(args)

    assert result["candidate_sha"] == SHA
    assert os.stat(args.signing_key).st_mode & 0o777 == 0o600
    for path in (args.output_profile, args.output_keyring, args.output_policy):
        assert os.stat(path).st_mode & 0o777 == 0o600
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(
        args.output_profile.read_text(encoding="utf-8")
    )
    keyring = ImageAdmissionKeyring.from_json(args.output_keyring.read_text(encoding="utf-8"))
    verify_execution_image_admission(
        profile.image_admission,
        required_image_refs=(profile.task_image_ref, profile.runtime_image_ref),
        keyring=keyring,
    )
    assert {
        row.statement.highest_vulnerability_severity for row in profile.image_admission.admissions
    } == {"high", "unknown"}

    args.create_signing_key = False
    repeated = prepare.prepare(args)
    assert repeated["public_key_sha256"] == result["public_key_sha256"]
    assert repeated["profile_sha256"] == result["profile_sha256"]


def test_prepare_refuses_to_replace_signing_key(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    prepare.prepare(args)
    with pytest.raises(ValueError, match="refusing to replace"):
        prepare.prepare(args)


def test_prepare_rejects_mirror_drift(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    mirror = json.loads(args.mirror_record.read_text(encoding="utf-8"))
    mirror["images"]["service"]["target_ref"] = f"{REGISTRY}/loom-service@sha256:" + "9" * 64
    _write_json(args.mirror_record, mirror)
    with pytest.raises(ValueError, match="source, mirror, and scan differ"):
        prepare.prepare(args)


def test_prepare_accepts_ancestor_service_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _inputs(tmp_path)
    service = json.loads(args.service_release_record.read_text(encoding="utf-8"))
    service["release"]["commit"] = "6" * 40
    _write_json(args.service_release_record, service)
    result = prepare.prepare(args)

    assert result["component_candidate_shas"] == {
        "service": "6" * 40,
        "execution_runtime": SHA,
    }


def test_prepare_rejects_non_ancestor_service_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = _inputs(tmp_path)
    service = json.loads(args.service_release_record.read_text(encoding="utf-8"))
    service["release"]["commit"] = "6" * 40
    _write_json(args.service_release_record, service)
    monkeypatch.setattr(prepare, "_release_is_ancestor", lambda ancestor, candidate: False)

    with pytest.raises(ValueError, match="not an ancestor"):
        prepare.prepare(args)


def test_prepare_requires_exact_execution_runtime_release(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    runtime = json.loads(args.execution_runtime_release_record.read_text(encoding="utf-8"))
    runtime["release"]["commit"] = "6" * 40
    _write_json(args.execution_runtime_release_record, runtime)

    with pytest.raises(ValueError, match="must match the profile candidate"):
        prepare.prepare(args)


def test_prepare_optional_worker_preserves_admitted_agent_image(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    source = "ghcr.io/qianyi-sun/loom-worker@sha256:" + "9" * 64
    target = REGISTRY + "/loom-worker@sha256:" + "9" * 64
    mirror = json.loads(args.mirror_record.read_text())
    mirror["images"]["worker"] = {"source_ref": source, "target_ref": target}
    _write_json(args.mirror_record, mirror)
    evidence = json.loads(args.evidence_summary.read_text())
    evidence["images"]["worker"] = {
        **evidence["images"]["service"],
        "image_ref": target,
    }
    _write_json(args.evidence_summary, evidence)
    worker = json.loads(args.service_release_record.read_text())
    worker["image"]["component"] = "worker"
    worker["subject"] = {
        "name": "ghcr.io/qianyi-sun/loom-worker",
        "digest": "sha256:" + "9" * 64,
    }
    args.worker_release_record = tmp_path / "worker-release.json"
    _write_json(args.worker_release_record, worker)
    prepare.prepare(args)
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(args.output_profile.read_text())
    assert profile.agent_image_ref == target
    verify_execution_image_admission(
        profile.image_admission,
        required_image_refs=(profile.task_image_ref, profile.runtime_image_ref, target),
        keyring=ImageAdmissionKeyring.from_json(args.output_keyring.read_text()),
    )


@pytest.mark.parametrize('enabled', [False, True])
def test_prepare_readiness_round_trips_through_renderer(
    tmp_path: Path, enabled: bool, request: pytest.FixtureRequest,
) -> None:
    args = _inputs(tmp_path)
    capabilities = ('supports_task_web_egress', 'service_lifecycle_ready', 'supports_task_identity')
    for name in capabilities:
        setattr(args, name, enabled)
    prepare.prepare(args)
    profile = json.loads(args.output_profile.read_text())
    for name in capabilities:
        if enabled:
            assert profile[name] is True
        else:
            assert name not in profile
    config, candidate, _ = request.getfixturevalue("platform_inputs")
    candidate["candidate_sha"] = SHA
    candidate["images"]["service"]["image_ref"] = profile["task_image_ref"]
    candidate["images"]["execution_runtime"]["image_ref"] = profile["runtime_image_ref"]
    if enabled:
        config["task_egress"] = {"protected_cidrs": ["198.51.100.0/24"]}
        config["task_identity_policy"] = {
            "mode": "private-root-v1", "target_id": config["target_id"],
            "execution_namespace": config["execution_namespace"],
        }
    files = build_platform(config, candidate, profile, json.loads(args.output_keyring.read_text()), repo_root=ROOT)
    data = files["10-config-network.yaml"][0]["data"]
    published = ServiceExecutionRuntimeProfileV1.model_validate_json(data["profile.json"])
    expected = "linux-amd64-cpu-web-pod-v1" if enabled else "linux-amd64-cpu-pod-v1"
    assert published.execution_class_id == expected
    catalog = json.loads(data["catalog.json"])
    assert catalog["execution_class"]["class_id"] == expected
    assert catalog["execution_class"].get("supports_task_web_egress", False) is enabled
    assert catalog["topology"]["execution_class_id"] == expected
    assert all(row["execution_class_id"] == expected for row in catalog["topology"]["targets"])


def test_prepare_guest_settings_preserve_ordinary_profile_budgets(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    args.guest_runtime = "qemu-tcg-v1"
    args.guest_runtime_volume_mib = 1024
    args.guest_max_artifact_bytes = 6 * 1024**3
    args.supports_task_identity = True
    prepare.prepare(args)
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(args.output_profile.read_text())
    assert profile.guest_runtime == "qemu-tcg-v1"
    assert profile.guest_runtime_volume_mib == 1024
    assert profile.guest_max_artifact_bytes == 6 * 1024**3
    assert profile.runtime_volume_mib == 32
    assert profile.max_artifact_bytes == 1024**3
