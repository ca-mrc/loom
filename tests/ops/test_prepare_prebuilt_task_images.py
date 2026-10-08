from __future__ import annotations

import argparse
import base64
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from scripts.install_trivy import TRIVY_RELEASE
from scripts.ops import prepare_prebuilt_task_images as prepare
from tests.support.execution_image_admission import _PRIVATE_KEY
from tests.unit.test_service_execution_materialization import _profile

from loom.execution_image_admission import ImageAdmissionKeyring, verify_execution_image_admission
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

TAG = "ghcr.io/terminal-bench/task:rev6"
IMAGE = "ghcr.io/terminal-bench/task@sha256:" + "7" * 64


@pytest.fixture
def inputs(tmp_path, monkeypatch):
    key = tmp_path / "signer.pem"
    key.write_bytes(
        _PRIVATE_KEY.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key.chmod(0o600)
    trust = tmp_path / "trust.json"
    trust.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "keys": [
                    {
                        "signing_key_id": "test-builder",
                        "public_key_base64": base64.b64encode(
                            _PRIVATE_KEY.public_key().public_bytes(
                                serialization.Encoding.Raw, serialization.PublicFormat.Raw
                            )
                        ).decode(),
                    }
                ],
            }
        )
    )
    profile = tmp_path / "original-profile.json"
    profile.write_text(_profile().model_dump_json())
    images = tmp_path / "images.json"
    images.write_text(json.dumps({"images": [TAG]}))
    monkeypatch.setattr(prepare, "install_trivy", lambda root: root / "trivy")
    return argparse.Namespace(
        profile=profile,
        images_json=images,
        output=tmp_path / "prepared",
        crane=Path("crane"),
        signing_key=key,
        signing_key_id="test-builder",
        trusted_keyring=trust,
    )


def scanner(monkeypatch, *, severity="HIGH", architecture="amd64", report_image=IMAGE):
    commands = []

    def run(argv, **kwargs):
        commands.append(argv)
        if argv[0] == "crane":
            return (
                "sha256:" + "7" * 64
                if argv[1] == "digest"
                else json.dumps({"os": "linux", "architecture": architecture})
            )
        path = Path(argv[argv.index("--output") + 1])
        if argv[argv.index("--format") + 1] == "json":
            path.write_text(
                json.dumps(
                    {
                        "ArtifactName": report_image if report_image is not None else argv[-1],
                        "Trivy": {"Version": TRIVY_RELEASE.version.removeprefix("v")},
                        "Results": [{"Vulnerabilities": [{"Severity": severity}]}],
                    }
                )
            )
        else:
            path.write_text(
                json.dumps({"bomFormat": "CycloneDX", "specVersion": "1.6", "components": []})
            )
        return ""

    monkeypatch.setattr(prepare, "_run", run)
    return commands


def cli(monkeypatch, args):
    monkeypatch.setattr(
        "sys.argv",
        [
            "prepare_prebuilt_task_images.py",
            "--profile",
            str(args.profile),
            "--images-json",
            str(args.images_json),
            "--output",
            str(args.output),
            "--crane",
            str(args.crane),
            "--signing-key",
            str(args.signing_key),
            "--signing-key-id",
            args.signing_key_id,
            "--trusted-keyring",
            str(args.trusted_keyring),
        ],
    )
    return prepare.main()


def test_operator_preparation_signs_exact_image_and_emits_native_harbor_overlay(
    inputs, monkeypatch, capsys
):
    commands = scanner(monkeypatch)
    original = inputs.profile.read_bytes()
    assert cli(monkeypatch, inputs) == 0
    assert json.loads(capsys.readouterr().out)["prepared_images"] == 1
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(
        (inputs.output / "runtime-profile.json").read_bytes()
    )
    assert profile.prebuilt_image_pins == {TAG: IMAGE}
    verify_execution_image_admission(
        profile.image_admission,
        required_image_refs=profile.published_image_refs(),
        keyring=ImageAdmissionKeyring.from_json(inputs.trusted_keyring.read_text()),
    )
    assert inputs.profile.read_bytes() == original
    overlay = inputs.output / json.loads((inputs.output / "harbor-overlays.json").read_text())[TAG]
    assert (
        overlay.read_text()
        == f"services:\n  main:\n    image: {IMAGE}\n    platform: linux/amd64\n"
    )
    assert next(row for row in commands if row[0] == "crane") == [
        "crane",
        "digest",
        "--platform",
        "linux/amd64",
        TAG,
    ]
    assert "--ignore-unfixed=false" in commands[-1]
    with pytest.raises(ValueError, match="must not exist"):
        prepare.prepare(inputs)


@pytest.mark.parametrize("failure", ["critical", "different-image", "arm64"])
def test_material_image_failures_never_emit_a_runtime_profile(inputs, monkeypatch, failure):
    scanner(
        monkeypatch,
        severity="CRITICAL" if failure == "critical" else "HIGH",
        architecture="arm64" if failure == "arm64" else "amd64",
        report_image="registry.invalid/different" if failure == "different-image" else IMAGE,
    )
    assert cli(monkeypatch, inputs) == 1
    assert not (inputs.output / "runtime-profile.json").exists()
    if failure != "arm64":
        qualified = json.loads((inputs.output / "image-qualification.json").read_text())
        assert qualified[0]["status"] == "rejected"
        if failure == "critical":
            assert qualified[0]["reason"] == "critical_vulnerability"


@pytest.mark.parametrize("change", ["source-tag-moved", "merged-map-too-large"])
def test_existing_source_pins_cannot_move_or_exceed_bound_when_merged(inputs, monkeypatch, change):
    from tests.support.execution_image_admission import signed_image_admission_bundle

    original = _profile()
    image = IMAGE.replace("7" * 64, "8" * 64)
    pins = (
        {TAG: image}
        if change == "source-tag-moved"
        else {f"registry.example/task-{index}:rev6": image for index in range(128)}
    )
    payload = original.model_dump(mode="json")
    payload.update(
        prebuilt_image_pins=pins,
        image_admission=signed_image_admission_bundle(
            (*original.published_image_refs(), image)
        ).model_dump(mode="json"),
    )
    inputs.profile.write_text(
        ServiceExecutionRuntimeProfileV1.model_validate(payload).model_dump_json()
    )
    scanner(monkeypatch)
    assert cli(monkeypatch, inputs) == 1
    assert not (inputs.output / "runtime-profile.json").exists()


def test_all_resolved_images_are_classified_after_a_critical_failure(inputs, monkeypatch):
    inputs.images_json.write_text(
        json.dumps({"images": [TAG, "ghcr.io/terminal-bench/other:rev6"]})
    )
    scanner(monkeypatch, report_image=None)
    run = prepare._run

    def mixed(argv, **kwargs):
        result = run(argv, **kwargs)
        if "--format" in argv and argv[argv.index("--format") + 1] == "json" and argv[-1] == IMAGE:
            path = Path(argv[argv.index("--output") + 1])
            payload = json.loads(path.read_text())
            payload["Results"][0]["Vulnerabilities"][0]["Severity"] = "CRITICAL"
            path.write_text(json.dumps(payload))
        return result

    monkeypatch.setattr(prepare, "_run", mixed)
    assert cli(monkeypatch, inputs) == 1
    rows = json.loads((inputs.output / "image-qualification.json").read_text())
    assert len(rows) == 2
    assert {row["status"] for row in rows} == {"qualified", "rejected"}
    assert not (inputs.output / "runtime-profile.json").exists()


@pytest.mark.parametrize("severity", ["HIGH", "CRITICAL"])
def test_large_realistic_reports_keep_cli_success_and_critical_classification(
    inputs, monkeypatch, severity
):
    scanner(monkeypatch, severity=severity)
    run = prepare._run

    def large_report(argv, **kwargs):
        result = run(argv, **kwargs)
        if "--format" in argv and argv[argv.index("--format") + 1] == "json":
            # Actual canonical reports are 29-41 MB. JSON whitespace retains the
            # same image identity and vulnerability content at this realistic size.
            with Path(argv[argv.index("--output") + 1]).open("ab") as handle:
                handle.write(b" " * (33 * 1024**2))
        return result

    monkeypatch.setattr(prepare, "_run", large_report)
    assert cli(monkeypatch, inputs) == (1 if severity == "CRITICAL" else 0)
    rows = json.loads((inputs.output / "image-qualification.json").read_text())
    assert rows[0]["status"] == ("rejected" if severity == "CRITICAL" else "qualified")
    if severity == "CRITICAL":
        assert rows[0]["reason"] == "critical_vulnerability"
        assert not (inputs.output / "runtime-profile.json").exists()


@pytest.mark.parametrize("label", ["vulnerability", "SBOM"])
def test_scanner_report_budget_accepts_exact_limit_and_rejects_next_byte(tmp_path, label):
    assert prepare.MAX_IMAGE_REPORT_BYTES == 64 * 1024**2
    report = tmp_path / "report.json"
    with report.open("wb") as handle:
        handle.truncate(prepare.MAX_IMAGE_REPORT_BYTES)
    assert len(prepare._read_image_report(report, label=label)) == prepare.MAX_IMAGE_REPORT_BYTES
    with report.open("ab") as handle:
        handle.write(b" ")
    with pytest.raises(ValueError, match="64 MiB report budget"):
        prepare._read_image_report(report, label=label)


def test_checked_in_tb21_source_list_is_complete_and_deduplicated():
    source = Path(__file__).resolve().parents[2] / "deploy/catalog/tb21-r6-prebuilt-images.json"
    document = json.loads(source.read_text())
    assert document["revision"] == "6"
    assert len(document["tasks"]) == len({task["task"] for task in document["tasks"]}) == 89
    assert document["images"] == sorted({task["image"] for task in document["tasks"]})


@pytest.mark.parametrize(
    "mode,event,reviewed",
    [
        ("platform", "workflow_dispatch", True),
        ("platform", "push", True),
        ("harness-only", "workflow_dispatch", True),
        ("platform", "workflow_dispatch", False),
    ],
)
def test_candidate_opt_in_uses_only_reviewed_explicit_platform_publication(
    inputs, monkeypatch, mode, event, reviewed
):
    from scripts.ops import nebius_candidate as candidate

    args = argparse.Namespace(
        **vars(inputs),
        mode=mode,
        prebuilt_crane=inputs.crane,
        prebuilt_images_json=candidate.ROOT / "deploy/catalog/tb21-r6-prebuilt-images.json"
        if reviewed
        else inputs.images_json,
    )
    monkeypatch.setenv("GITHUB_EVENT_NAME", event)
    original = _profile().model_dump(mode="json")
    called = []

    def stage(args):
        called.append(args)
        args.output.mkdir()
        (args.output / "runtime-profile.json").write_text(json.dumps(original))

    monkeypatch.setattr(prepare, "prepare", stage)
    if mode == "platform" and event == "workflow_dispatch" and reviewed:
        assert candidate.prepare_prebuilt_profile(original, args) == original
        assert len(called) == 1
        assert called[0].images_json == args.prebuilt_images_json
    else:
        with pytest.raises(ValueError, match="explicit protected"):
            candidate.prepare_prebuilt_profile(original, args)
        assert called == []
