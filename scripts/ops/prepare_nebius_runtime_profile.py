#!/usr/bin/env python3
"""Prepare and sign one immutable Nebius service-execution runtime profile."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

if __package__ in {None, ""}:  # pragma: no cover - direct script execution
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from loom.execution_image_admission import (
    ExecutionImageAdmissionBundleV1,
    ImageAdmissionKeyring,
    ImageAdmissionStatementV1,
    SignedImageAdmissionV1,
    verify_execution_image_admission,
)
from loom.pipeline.keys import canonical_document
from loom.service_execution_materialization import build_nebius_runtime_profile

_SHA = re.compile(r"^[0-9a-f]{40}$")
_KEY_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_MAX_JSON_BYTES = 24 * 1024 * 1024
_COMPONENTS = (
    ("service", "service", "loom-service"),
    ("execution_runtime", "execution-runtime", "loom-execution-runtime"),
)
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _object(value: object, *, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{label} must be a JSON object")
    return cast(dict[str, object], value)


def _read_json(path: Path, *, label: str) -> tuple[dict[str, object], bytes]:
    resolved = path.resolve()
    if not resolved.is_file() or resolved.is_symlink():
        raise ValueError(f"{label} must be a regular file")
    payload = resolved.read_bytes()
    if not payload or len(payload) > _MAX_JSON_BYTES:
        raise ValueError(f"{label} has an invalid size")
    return _object(json.loads(payload), label=label), payload


def _write(path: Path, payload: bytes, *, replace: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not replace and path.exists():
        raise ValueError(f"refusing to replace existing {path.name}")
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _canonical_json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _private_key(path: Path, *, create: bool) -> Ed25519PrivateKey:
    resolved = path.resolve()
    if create:
        key = Ed25519PrivateKey.generate()
        encoded = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        _write(resolved, encoded, replace=False)
        return key
    if not resolved.is_file() or resolved.is_symlink():
        raise ValueError("signing key must be a regular file")
    if stat.S_IMODE(resolved.stat().st_mode) & 0o077:
        raise ValueError("signing key must be owner-only")
    loaded = serialization.load_pem_private_key(resolved.read_bytes(), password=None)
    if not isinstance(loaded, Ed25519PrivateKey):
        raise ValueError("signing key must be Ed25519")
    return loaded


def _candidate_issued_at(candidate_sha: str) -> datetime:
    completed = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "show", "-s", "--format=%cI", candidate_sha],
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        issued_at = datetime.fromisoformat(completed.stdout.strip()).astimezone(UTC)
    except ValueError as exc:
        raise ValueError("candidate commit timestamp is invalid") from exc
    if issued_at > datetime.now(UTC):
        raise ValueError("candidate commit timestamp is in the future")
    return issued_at


def _release_is_ancestor(release_sha: str, candidate_sha: str) -> bool:
    completed = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "merge-base", "--is-ancestor", release_sha, candidate_sha],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0:
        return True
    if completed.returncode == 1:
        return False
    raise subprocess.CalledProcessError(
        completed.returncode,
        completed.args,
        output=completed.stdout,
        stderr=completed.stderr,
    )


def _release_binding(
    record: dict[str, object],
    *,
    component: str,
    image_name: str,
) -> tuple[str, dict[str, object], str]:
    if record.get("schema_version") != 1:
        raise ValueError(f"{component} release record schema is invalid")
    image = _object(record.get("image"), label=f"{component} release image")
    release = _object(record.get("release"), label=f"{component} release")
    subject = _object(record.get("subject"), label=f"{component} release subject")
    scan = _object(record.get("scan"), label=f"{component} release scan")
    release_sha = release.get("commit")
    if (
        image.get("component") != component
        or image.get("platform") != "linux/amd64"
        or not isinstance(release_sha, str)
        or _SHA.fullmatch(release_sha) is None
        or release.get("ref") != "refs/heads/dev"
        or subject.get("name") != f"ghcr.io/qianyi-sun/{image_name}"
    ):
        raise ValueError(f"{component} release identity is inconsistent")
    digest = subject.get("digest")
    if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
        raise ValueError(f"{component} release digest is invalid")
    return f"{subject['name']}@{digest}", scan, release_sha


def prepare(args: argparse.Namespace) -> dict[str, object]:
    if _SHA.fullmatch(args.candidate_sha) is None:
        raise ValueError("candidate SHA must contain 40 lowercase hex characters")
    if _KEY_ID.fullmatch(args.signing_key_id) is None:
        raise ValueError("signing key id is invalid")
    mirror, _ = _read_json(args.mirror_record, label="mirror record")
    evidence, _ = _read_json(args.evidence_summary, label="runtime evidence")
    if (
        mirror.get("schema_version") != "loom.nebius-release-mirror.v1"
        or mirror.get("candidate_sha") != args.candidate_sha
        or evidence.get("schema_version") != "loom.nebius-runtime-evidence.v1"
    ):
        raise ValueError("Nebius release evidence identity is inconsistent")
    mirror_images = _object(mirror.get("images"), label="mirrored images")
    evidence_images = _object(evidence.get("images"), label="runtime evidence images")

    release_inputs = {
        "service": args.service_release_record,
        "execution_runtime": args.execution_runtime_release_record,
    }
    components = list(_COMPONENTS)
    worker_record = getattr(args, "worker_release_record", None)
    if worker_record is not None:
        components.append(("worker", "worker", "loom-worker"))
        release_inputs["worker"] = worker_record
    bindings: dict[str, dict[str, str]] = {}
    component_candidate_shas: dict[str, str] = {}
    scan_policies: list[dict[str, object]] = []
    for key, component, image_name in components:
        release_record, release_bytes = _read_json(
            release_inputs[key], label=f"{component} release record"
        )
        source_ref, scan, release_sha = _release_binding(
            release_record,
            component=component,
            image_name=image_name,
        )
        if key == "execution_runtime" and release_sha != args.candidate_sha:
            raise ValueError("execution-runtime release must match the profile candidate")
        if key != "execution_runtime" and not _release_is_ancestor(release_sha, args.candidate_sha):
            raise ValueError(f"{component} release is not an ancestor of the profile candidate")
        component_candidate_shas[key] = release_sha
        mirrored = _object(mirror_images.get(key), label=f"mirrored {component}")
        observed = _object(evidence_images.get(key), label=f"observed {component}")
        target_ref = mirrored.get("target_ref")
        if (
            mirrored.get("source_ref") != source_ref
            or not isinstance(target_ref, str)
            or observed.get("image_ref") != target_ref
        ):
            raise ValueError(f"{component} source, mirror, and scan differ")
        sbom_sha256 = observed.get("sbom_sha256")
        vulnerability_report_sha256 = observed.get("vulnerability_report_sha256")
        if (
            not isinstance(sbom_sha256, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", sbom_sha256) is None
        ):
            raise ValueError(f"{component} sbom_sha256 is invalid")
        if (
            not isinstance(vulnerability_report_sha256, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", vulnerability_report_sha256) is None
        ):
            raise ValueError(f"{component} vulnerability_report_sha256 is invalid")
        severity = observed.get("highest_vulnerability_severity")
        if not isinstance(severity, str) or severity not in {
            "none",
            "negligible",
            "low",
            "medium",
            "high",
            "unknown",
        }:
            raise ValueError(f"{component} vulnerability severity is invalid")
        bindings[key] = {
            "image_ref": target_ref,
            "sbom_sha256": sbom_sha256,
            "vulnerability_report_sha256": vulnerability_report_sha256,
            "highest_vulnerability_severity": severity,
            "provenance_sha256": "sha256:" + hashlib.sha256(release_bytes).hexdigest(),
        }
        scan_policies.append(
            {
                "component": component,
                "config_sha256": scan.get("config_sha256"),
                "ignore_sha256": scan.get("ignore_sha256"),
                "scanner": scan.get("scanner"),
            }
        )

    policy = {
        "schema_version": "loom.nebius-image-admission-policy.v1",
        "blocking_severities": ["critical"],
        "recorded_nonblocking_severities": [
            "unknown",
            "high",
            "medium",
            "low",
            "negligible",
        ],
        "release_scan_policies": scan_policies,
    }
    policy_bytes = _canonical_json(policy)
    policy_sha256 = "sha256:" + hashlib.sha256(policy_bytes).hexdigest()
    private_key = _private_key(args.signing_key, create=args.create_signing_key)
    issued_at = _candidate_issued_at(args.candidate_sha)
    expires_at = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
    admissions: list[SignedImageAdmissionV1] = []
    for key_name, _, _ in components:
        binding = bindings[key_name]
        statement = ImageAdmissionStatementV1(
            schema_version="loom.image-admission-statement.v1",
            image_ref=binding["image_ref"],
            platform="linux/x86_64",
            sbom_sha256=binding["sbom_sha256"],
            provenance_sha256=binding["provenance_sha256"],
            vulnerability_report_sha256=binding["vulnerability_report_sha256"],
            policy_sha256=policy_sha256,
            highest_vulnerability_severity=binding["highest_vulnerability_severity"],
            issued_at=issued_at,
            expires_at=expires_at,
        )
        admissions.append(
            SignedImageAdmissionV1(
                statement=statement,
                signing_key_id=args.signing_key_id,
                signature_base64=base64.b64encode(
                    private_key.sign(canonical_document(statement.model_dump(mode="json")))
                ).decode("ascii"),
            )
        )
    bundle = ExecutionImageAdmissionBundleV1(
        schema_version="loom.execution-image-admission.v1",
        admissions=tuple(admissions),
    )
    runtime_binary_sha256 = evidence.get("runtime_binary_sha256")
    if not isinstance(runtime_binary_sha256, str):
        raise ValueError("runtime binary digest is missing")
    profile = build_nebius_runtime_profile(
        candidate_sha=args.candidate_sha,
        task_image_ref=bindings["service"]["image_ref"],
        runtime_image_ref=bindings["execution_runtime"]["image_ref"],
        agent_image_ref=bindings["worker"]["image_ref"] if "worker" in bindings else None,
        runtime_binary_sha256=runtime_binary_sha256,
        supports_task_web_egress=getattr(args, "supports_task_web_egress", False),
        service_lifecycle_ready=getattr(args, "service_lifecycle_ready", False),
        supports_task_identity=getattr(args, "supports_task_identity", False),
        guest_runtime=getattr(args, "guest_runtime", None),
        supports_emulated_pkcs11=getattr(args, "supports_emulated_pkcs11", False),
        guest_runtime_volume_mib=getattr(args, "guest_runtime_volume_mib", None),
        guest_max_artifact_bytes=getattr(args, "guest_max_artifact_bytes", None),
        resource_allocation_policy="node-share-v1" if getattr(args, "node_share_resources", False) else None,
        image_admission=bundle,
    )
    public_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    keyring = {
        "schema_version": 1,
        "keys": [
            {
                "signing_key_id": args.signing_key_id,
                "public_key_base64": base64.b64encode(public_bytes).decode("ascii"),
            }
        ],
    }
    parsed_keyring = ImageAdmissionKeyring.from_json(_canonical_json(keyring).decode())
    verify_execution_image_admission(
        bundle,
        required_image_refs=tuple(binding["image_ref"] for binding in bindings.values()),
        keyring=parsed_keyring,
        now=issued_at,
    )
    profile_payload = profile.model_dump(mode="json")
    _write(args.output_policy.resolve(), policy_bytes)
    _write(args.output_keyring.resolve(), _canonical_json(keyring))
    _write(args.output_profile.resolve(), _canonical_json(profile_payload))
    return {
        "schema_version": "loom.nebius-runtime-profile-preparation.v1",
        "candidate_sha": args.candidate_sha,
        "profile_sha256": "sha256:" + hashlib.sha256(_canonical_json(profile_payload)).hexdigest(),
        "policy_sha256": policy_sha256,
        "signing_key_id": args.signing_key_id,
        "public_key_sha256": "sha256:" + hashlib.sha256(public_bytes).hexdigest(),
        "component_candidate_shas": component_candidate_shas,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--mirror-record", required=True, type=Path)
    parser.add_argument("--evidence-summary", required=True, type=Path)
    parser.add_argument("--service-release-record", required=True, type=Path)
    parser.add_argument("--execution-runtime-release-record", required=True, type=Path)
    parser.add_argument("--worker-release-record", type=Path)
    parser.add_argument("--supports-task-web-egress", action="store_true")
    parser.add_argument("--service-lifecycle-ready", action="store_true")
    parser.add_argument("--supports-task-identity", action="store_true")
    parser.add_argument("--guest-runtime", choices=("qemu-tcg-v1",))
    parser.add_argument("--supports-emulated-pkcs11", action="store_true")
    parser.add_argument("--guest-runtime-volume-mib", type=int)
    parser.add_argument("--guest-max-artifact-bytes", type=int)
    parser.add_argument("--node-share-resources", action="store_true")
    parser.add_argument("--signing-key", required=True, type=Path)
    parser.add_argument("--signing-key-id", required=True)
    parser.add_argument("--create-signing-key", action="store_true")
    parser.add_argument("--output-profile", required=True, type=Path)
    parser.add_argument("--output-keyring", required=True, type=Path)
    parser.add_argument("--output-policy", required=True, type=Path)
    return parser


def main() -> int:
    try:
        result = prepare(_parser().parse_args())
    except (OSError, ValueError, json.JSONDecodeError, subprocess.CalledProcessError) as exc:
        raise SystemExit(f"error: {exc}") from exc
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
