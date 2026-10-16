#!/usr/bin/env python3
"""Build and authenticate one independent Nebius integration candidate."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from scripts.component_ownership import NEBIUS_PLATFORM_IMAGES as COMPONENTS
from scripts.component_ownership import load_manifest, release_image_matrix
from scripts.install_trivy import install_trivy
from scripts.ops.collect_nebius_runtime_evidence_via_gateway import _severity
from scripts.ops.nebius_registry_auth import refresh_registry_auth
from scripts.validate_trivy_release_report import validate_trivy_release_report
from scripts.write_trivy_release_policy import write_release_policy

from loom.execution_image_admission import (
    ExecutionImageAdmissionBundleV1,
    ImageAdmissionKeyring,
    ImageAdmissionStatementV1,
    SignedImageAdmissionV1,
)
from loom.pipeline.keys import canonical_document
from loom.repository_identity import REPOSITORY_ID, REPOSITORY_NAMES, is_repository_name
from loom.service_execution_materialization import (
    ControllerComputeResourcesV1,
    build_nebius_runtime_profile,
)

ROOT = Path(__file__).resolve().parents[2]
SOURCE_REF = "refs/heads/dev"
REPOSITORY = "qianyi-sun/loom"
WORKFLOW = ".github/workflows/nebius-candidate.yml"
EXECUTION_COMPONENTS = ("service", "execution_runtime", "harbor_runtime")
LEGACY_COMPONENTS = frozenset(COMPONENTS) - {"harbor_runtime"}
HISTORICAL_COMPONENTS = LEGACY_COMPONENTS | {"worker", "tb90_task"}
HISTORICAL_HARBOR_COMPONENTS = frozenset(COMPONENTS) | {"tb90_task"}
# Read old releases without requiring their workload images in new publication.
HISTORICAL_IMAGE_NAMES = {"worker": "loom-worker", "tb90_task": "loom-nebius-terminal-bench"}
AGENT_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
REGISTRY = re.compile(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+\Z")
_diagnostic_dir: Path | None = None
_build_diagnostic = threading.local()


def encoded(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode()


def sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def source_archive_digest(revision: str) -> str:
    """Hash actual committed source bytes; never substitute a digest of Git SHA."""
    try:
        def git(*arguments: str) -> bytes:
            result = subprocess.run(['git', *arguments], cwd=ROOT, check=False, capture_output=True, timeout=30)
            if result.returncode:
                raise ValueError()
            return result.stdout.strip()

        if (SHA.fullmatch(revision) is None or git('rev-parse', 'HEAD') != revision.encode()
                or git('status', '--porcelain', '--untracked-files=normal')):
            raise ValueError()
        with tempfile.TemporaryFile() as archive:
            result = subprocess.run(['git', 'archive', '--format=tar', revision], cwd=ROOT,
                stdout=archive, stderr=subprocess.PIPE, check=False, timeout=60)
            if result.returncode or not 0 < archive.tell() <= 512 * 1024**2:
                raise ValueError()
            archive.seek(0)
            return 'sha256:' + hashlib.file_digest(archive, 'sha256').hexdigest()
    except Exception:
        raise ValueError('candidate source archive unavailable or checkout changed') from None


def source_metadata(revision: str) -> dict[str, str]:
    """Bind the schema graph to the same clean source as the published images."""
    from loom.db.schema_startup import service_schema_head
    from loom.nebius_candidate_contract import candidate_service_schema_head

    before = source_archive_digest(revision)
    head = candidate_service_schema_head({'service_schema_head': service_schema_head(ROOT / 'database/migrations/alembic.ini')})
    if source_archive_digest(revision) != before:
        raise ValueError('candidate schema source changed')
    return {'source_archive_sha256': before, 'service_schema_head': head}


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("expected a bounded regular JSON file")
    result = json.loads(path.read_bytes(), object_pairs_hook=_unique)
    if not isinstance(result, dict):
        raise ValueError("expected a JSON object")
    return result


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as handle:
        handle.write(encoded(value))


def _private_key(path: Path) -> Ed25519PrivateKey:
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError("established signing key must be an owner-only regular file")
    key = serialization.load_pem_private_key(path.read_bytes(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("signing key must be Ed25519")
    return key


def _trusted_signer(path: Path, key_id: str, keyring_json: str) -> Ed25519PrivateKey:
    ImageAdmissionKeyring.from_json(keyring_json)
    key = _private_key(path)
    matching = [row for row in json.loads(keyring_json)["keys"] if row["signing_key_id"] == key_id]
    public = key.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    )
    if len(matching) != 1 or base64.b64decode(matching[0]["public_key_base64"]) != public:
        raise ValueError("publication signing key does not match independent runtime trust")
    return key


def validate_source_identity(document: dict[str, Any]) -> None:
    if (
        document.get("schema_version") != "loom.nebius-candidate.v1"
        or not is_repository_name(document.get("repository"))
        or document.get("source_ref") not in {SOURCE_REF, "refs/heads/codex/nebius-main"}
        or document.get("workflow_path") != WORKFLOW
        or SHA.fullmatch(str(document.get("candidate_sha"))) is None
        or type(document.get("run_id")) is not int
        or document["run_id"] <= 0
        or REGISTRY.fullmatch(str(document.get("registry_prefix"))) is None
    ):
        raise ValueError("candidate source identity is invalid")


def validate_identity(document: dict[str, Any], *, require_current_images: bool = False) -> None:
    validate_source_identity(document)
    images = document.get("images")
    allowed = {frozenset(COMPONENTS)}
    if not require_current_images:
        allowed.update((LEGACY_COMPONENTS, HISTORICAL_COMPONENTS, HISTORICAL_HARBOR_COMPONENTS))
    if not isinstance(images, dict) or frozenset(images) not in allowed:
        raise ValueError("candidate must contain the configured platform and execution images")
    for component in images:
        name = COMPONENTS.get(component) or HISTORICAL_IMAGE_NAMES[component]
        row = images[component]
        prefix = f"{document['registry_prefix']}/{name}@"
        if (
            not isinstance(row, dict)
            or not str(row.get("image_ref", "")).startswith(prefix)
            or DIGEST.fullmatch(str(row.get("image_ref", ""))[len(prefix) :]) is None
        ):
            raise ValueError(f"candidate image identity is invalid: {component}")


def _sign_admission(
    row: dict[str, Any], policy_sha256: str, provenance: str, *,
    key: Ed25519PrivateKey, signing_key_id: str,
) -> SignedImageAdmissionV1:
    statement = ImageAdmissionStatementV1(
        schema_version="loom.image-admission-statement.v1",
        image_ref=row["image_ref"], platform="linux/x86_64",
        sbom_sha256=row["sbom_sha256"],
        vulnerability_report_sha256=row["vulnerability_report_sha256"],
        provenance_sha256=provenance, policy_sha256=policy_sha256,
        highest_vulnerability_severity=row["highest_vulnerability_severity"],
        issued_at=datetime.now(UTC),
        expires_at=datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
    )
    return SignedImageAdmissionV1(
        statement=statement, signing_key_id=signing_key_id,
        signature_base64=base64.b64encode(
            key.sign(canonical_document(statement.model_dump(mode="json")))
        ).decode(),
    )


def validate_runtime_metadata(metadata: dict[str, str]) -> None:
    if (
        metadata.get("agent_name") != "terminus-2"
        or metadata.get("runtime_contract") != "loom.terminus-controller.v1"
        or AGENT_VERSION.fullmatch(metadata.get("agent_version", "")) is None
        or SHA.fullmatch(metadata.get("harbor_source_revision", "")) is None
        or SHA.fullmatch(metadata.get("publisher_source_revision", "")) is None
        or not metadata.get("harbor_version")
        or not metadata.get("loom_bridge_revision")
    ):
        raise ValueError("runtime image metadata is invalid")


def _runtime_release_payload(document: dict[str, Any]) -> dict[str, Any]:
    """Prepare runtime metadata without creating a partial platform candidate."""
    validate_source_identity(document)
    if set(document.get("images", {})) != {"harbor_runtime"}:
        raise ValueError("runtime release requires exactly one Harbor image")
    metadata = document["runtime_metadata"]
    validate_runtime_metadata(metadata)
    if metadata["publisher_source_revision"] != document["candidate_sha"]:
        raise ValueError("runtime metadata does not match its publisher source")
    row = document["images"]["harbor_runtime"]
    prefix = f"{document['registry_prefix']}/{COMPONENTS['harbor_runtime']}@"
    ref = row["image_ref"]
    if not ref.startswith(prefix) or DIGEST.fullmatch(ref[len(prefix):]) is None:
        raise ValueError("runtime release image must be in the native Harbor repository")
    result = {
        "schema_version": "loom.agent-runtime-release.v1",
        **{name: metadata[name] for name in (
            "agent_name", "agent_version", "runtime_contract", "harbor_version",
            "harbor_source_revision", "loom_bridge_revision", "publisher_source_revision",
        )},
        "agent_image_ref": ref,
    }
    return result


def create_runtime_release(
    document: dict[str, Any], *, signing_key: Path, signing_key_id: str, keyring_json: str,
) -> dict[str, Any]:
    """Sign a standalone release once; registration reuses this original record."""
    result = _runtime_release_payload(document)
    row = document["images"]["harbor_runtime"]
    key = _trusted_signer(signing_key, signing_key_id, keyring_json)
    result["image_admission"] = _sign_admission(
        row, document["policy_sha256"], sha256(encoded(result)),
        key=key, signing_key_id=signing_key_id,
    ).model_dump(mode="json")
    return result


def create_candidate(
    document: dict[str, Any],
    *,
    signing_key: Path,
    signing_key_id: str,
    keyring_json: str,
    supports_task_web_egress: bool = False,
    service_lifecycle_ready: bool = False,
    supports_task_identity: bool = False,
    guest_runtime: Literal["qemu-tcg-v1"] | None = None,
    supports_emulated_pkcs11: bool = False,
    supports_shared_guest: bool = False,
    guest_runtime_volume_mib: int | None = None,
    guest_max_artifact_bytes: int | None = None,
    node_share_resources: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    validate_identity(document, require_current_images=True)
    key = _trusted_signer(signing_key, signing_key_id, keyring_json)
    result = {
        name: document[name]
        for name in (
            "schema_version",
            "repository",
            "source_ref",
            "candidate_sha",
            "workflow_path",
            "run_id",
            "registry_prefix",
        )
    }
    result["images"] = {
        component: {"image_ref": row["image_ref"]} for component, row in document["images"].items()
    }
    if "source_archive_sha256" in document:
        source_digest = document["source_archive_sha256"]
        if not isinstance(source_digest, str) or DIGEST.fullmatch(source_digest) is None:
            raise ValueError("invalid candidate source archive digest")
        result["source_archive_sha256"] = source_digest
    if 'service_schema_head' in document:
        from loom.nebius_candidate_contract import candidate_service_schema_head

        result['service_schema_head'] = candidate_service_schema_head(document)
    provenance = sha256(encoded(result))
    admissions = [
        _sign_admission(document["images"][component], document["policy_sha256"], provenance,
                        key=key, signing_key_id=signing_key_id)
        for component in EXECUTION_COMPONENTS
    ]
    profile = build_nebius_runtime_profile(
        candidate_sha=document["candidate_sha"],
        controller_resources=ControllerComputeResourcesV1(cpu_millis=1000, memory_mib=2048),
        task_image_ref=document["images"]["service"]["image_ref"],
        runtime_image_ref=document["images"]["execution_runtime"]["image_ref"],
        agent_image_ref=document["images"]["harbor_runtime"]["image_ref"],
        runtime_binary_sha256=document["runtime_binary_sha256"],
        supports_task_web_egress=supports_task_web_egress,
        service_lifecycle_ready=service_lifecycle_ready,
        supports_task_artifact_inputs=True,
        supports_task_identity=supports_task_identity,
        guest_runtime=guest_runtime,
        supports_emulated_pkcs11=supports_emulated_pkcs11,
        supports_shared_guest=supports_shared_guest,
        guest_runtime_volume_mib=guest_runtime_volume_mib,
        guest_max_artifact_bytes=guest_max_artifact_bytes,
        resource_allocation_policy="node-share-v1" if node_share_resources else None,
        image_admission=ExecutionImageAdmissionBundleV1(
            schema_version="loom.execution-image-admission.v1",
            admissions=tuple(admissions),
        ),
    ).model_dump(mode="json")
    return result, profile


def prepare_prebuilt_profile(profile: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    """Optional reviewed upstream images enter the original protected artifact."""
    images = getattr(args, "prebuilt_images_json", None)
    if images is None:
        return profile
    if (images.resolve() != ROOT / "deploy/catalog/tb21-r6-prebuilt-images.json"
            or os.environ.get("GITHUB_EVENT_NAME") != "workflow_dispatch"
            or getattr(args, "mode", "platform") != "platform"):
        raise ValueError("prebuilt task qualification requires explicit protected TB2.1 publication")
    from scripts.ops.prepare_prebuilt_task_images import prepare

    with tempfile.TemporaryDirectory(prefix="loom-prebuilt-profile-") as temporary:
        original = Path(temporary) / "runtime-profile.json"
        write_json(original, profile)
        output = args.output.parent / "nebius-prebuilt-task-images"
        prepare(argparse.Namespace(profile=original, images_json=images,
            output=output, crane=args.prebuilt_crane, signing_key=args.signing_key,
            signing_key_id=args.signing_key_id, trusted_keyring=args.trusted_keyring))
        return read_json(output / "runtime-profile.json")


def sanitize_diagnostic(text: str) -> str:
    """Bound and redact subprocess evidence before publishing it."""
    text = text[-16_384:]
    for name, value in os.environ.items():
        if len(value) >= 8 and re.search(r"TOKEN|PASSWORD|SECRET|PRIVATE_KEY|API_KEY", name):
            text = text.replace(value, "[redacted]")
    text = re.sub(r"https?://[^\s]+", "[url]", text)
    text = re.sub(r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b", "[ip]", text)
    text = "\n".join(
        "[redacted credential-related diagnostic]"
        if re.search(
            r"password|authorization|credential|private.key|api.key|bearer|access.token",
            line,
            re.IGNORECASE,
        )
        else line
        for line in text.splitlines()
    )
    return text[-16_384:]


def _run(*command: str) -> str:
    operation = Path(command[0]).name + (" " + command[1] if len(command) > 1 else "")
    print(f"Nebius publication: {operation}", flush=True)
    result = subprocess.run(command, cwd=ROOT, check=False, capture_output=True, text=True)
    if result.returncode:
        text = sanitize_diagnostic(result.stderr or result.stdout)
        if _diagnostic_dir is not None:
            write_json(
                _diagnostic_dir / getattr(_build_diagnostic, "filename", "failed-command.json"),
                {
                    "operation": operation,
                    "returncode": result.returncode,
                    "diagnostic": text,
                },
            )
        filename = getattr(_build_diagnostic, "filename", "failed-command.json")
        raise ValueError(
            f"{operation} failed with exit code {result.returncode}; inspect {filename}"
        )
    return result.stdout.strip()


def _copy_image(archive: Path, tag: str, *, timeout_seconds: float = 900) -> None:
    """Share one wall-clock budget across an upload and at most one network retry."""
    if timeout_seconds <= 0:
        raise ValueError("upload timeout must be positive")
    operation = "skopeo copy"
    print(f"Nebius publication: {operation} (total timeout {timeout_seconds:g}s)", flush=True)
    started = time.monotonic()
    timed_out = threading.Event()
    diagnostic = ""
    returncode = None
    attempts = 0
    for attempt in range(1, 3):
        remaining = timeout_seconds - (time.monotonic() - started)
        if remaining <= 0:
            timed_out.set()
            break
        attempts = attempt
        attempt_diagnostic = ""
        diagnostic = (diagnostic + f"Attempt {attempt}\n")[-16_384:]
        with subprocess.Popen(
            ["skopeo", "copy", "--preserve-digests", f"oci-archive:{archive}", f"docker://{tag}"],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace",
        ) as process:
            def expire() -> None:
                if process.poll() is None:
                    timed_out.set()
                    process.kill()

            timer = threading.Timer(remaining, expire)
            timer.daemon = True
            timer.start()
            try:
                assert process.stdout is not None
                for line in process.stdout:
                    safe_line = sanitize_diagnostic(line)
                    attempt_diagnostic = (attempt_diagnostic + safe_line + "\n")[-16_384:]
                    diagnostic = (diagnostic + safe_line + "\n")[-16_384:]
                    # Prefix every line so tool output cannot become a workflow command.
                    for safe_part in safe_line.splitlines():
                        print(f"{operation} [{attempt}/2]: {safe_part}", flush=True)
                returncode = process.wait()
            finally:
                timer.cancel()
                timer.join()
                if process.poll() is None:
                    process.kill()
                    process.wait()
        if returncode == 0 and not timed_out.is_set():
            elapsed = round(time.monotonic() - started, 2)
            print(f"Nebius publication: {operation} completed in {elapsed:g}s ({attempt} attempt(s))", flush=True)
            return
        # Only explicit network errors qualify. Authentication, permission and
        # unknown errors fail closed; a total-budget timeout never starts over.
        transient = re.search(
            r"connection reset by peer|connection refused|i/o timeout|"
            r"TLS handshake timeout|net/http: timeout awaiting response headers",
            attempt_diagnostic, re.IGNORECASE,
        ) and not re.search(
            r"unauthorized|denied|authentication|forbidden|certificate|credential",
            attempt_diagnostic, re.IGNORECASE,
        )
        if (timed_out.is_set() or attempt == 2 or not transient
                or timeout_seconds - (time.monotonic() - started) <= 1):
            break
        print(f"Nebius publication: {operation} transient network failure; retrying once in 1s", flush=True)
        time.sleep(1)
    elapsed = round(time.monotonic() - started, 2)
    if _diagnostic_dir is not None:
        write_json(_diagnostic_dir / "failed-command.json", {
            "operation": operation,
            "returncode": returncode,
            "attempts": attempts,
            "timed_out": timed_out.is_set(),
            "timeout_seconds": timeout_seconds,
            "elapsed_seconds": elapsed,
            "diagnostic": diagnostic,
        })
    reason = f"timed out after {timeout_seconds:g}s" if timed_out.is_set() else f"failed with exit code {returncode}"
    raise ValueError(f"{operation} {reason}; inspect failed-command.json")


def inspect_oci_archive(
    archive: Path, *, candidate: str, runtime: bool = False,
    runtime_metadata: dict[str, str] | None = None,
    component: str | None = None, source_ref: str | None = None,
    verify_layers: bool = False,
) -> tuple[str, str | None]:
    """Bind the exact scanned OCI bytes; never execute an image to inspect it."""
    with tarfile.open(archive) as bundle:
        if verify_layers:
            # Optional cache archives must be safe for the same native data
            # filter used by the scan-layout extractor. Preflight without
            # writing files so unsafe cache members rebuild this component.
            layout = archive.parent / f".{archive.name}.scan-layout"
            for member in bundle.getmembers():
                tarfile.data_filter(member, str(layout))

        def blob(descriptor: dict[str, Any]) -> bytes:
            digest = descriptor["digest"]
            if DIGEST.fullmatch(digest) is None:
                raise ValueError("invalid OCI blob digest")
            member = bundle.getmember("blobs/sha256/" + digest.split(":")[1])
            if not member.isfile():
                raise ValueError("OCI blob is not a regular file")
            stream = bundle.extractfile(member)
            assert stream is not None
            payload = stream.read()
            if sha256(payload) != digest or len(payload) != descriptor["size"]:
                raise ValueError("OCI blob checksum mismatch")
            return payload

        index_stream = bundle.extractfile("index.json")
        if index_stream is None:
            raise ValueError("missing OCI index")
        index = json.load(index_stream)
        if len(index["manifests"]) != 1:
            raise ValueError("candidate archive must contain exactly one platform image")
        descriptor = index["manifests"][0]
        manifest = json.loads(blob(descriptor))
        config = json.loads(blob(manifest["config"]))
        if (
            config.get("architecture") != "amd64"
            or config.get("os") != "linux"
            or config.get("config", {}).get("Labels", {}).get("org.opencontainers.image.revision")
            != candidate
        ):
            raise ValueError("built OCI image source/platform mismatch")
        labels = config.get("config", {}).get("Labels", {})
        if component is not None and labels.get("io.loom.component") != component:
            raise ValueError("OCI image component mismatch")
        if source_ref is not None and labels.get("io.loom.source-ref") != source_ref:
            raise ValueError("OCI image source ref mismatch")
        if verify_layers and not runtime:
            for layer in manifest["layers"]:
                blob(layer)
        if runtime_metadata is not None:
            for field in ("agent_name", "agent_version", "runtime_contract", "harbor_version",
                          "harbor_source_revision", "loom_bridge_revision"):
                runtime_metadata[field] = labels.get("io.loom." + field, "")
            runtime_metadata["publisher_source_revision"] = candidate
        binary_digest = None
        if runtime:
            import io

            # The scratch runtime image has one regular binary. Refuse symlink
            # replacement or whiteouts rather than infer a misleading digest.
            for layer in manifest["layers"]:
                with tarfile.open(fileobj=io.BytesIO(blob(layer)), mode="r:*") as files:
                    for member in files:
                        name = member.name.removeprefix("./").lstrip("/")
                        if name in {".wh.loom-execution-runtime", ".wh..wh..opq"}:
                            binary_digest = None
                        elif name == "loom-execution-runtime":
                            if not member.isfile() or not member.mode & 0o111:
                                raise ValueError("runtime binary must be a regular executable")
                            stream = files.extractfile(member)
                            assert stream is not None
                            binary_digest = sha256(stream.read())
            if binary_digest is None:
                raise ValueError("runtime binary is missing from candidate OCI image")
        return str(descriptor["digest"]), binary_digest


@contextmanager
def oci_scan_layout(archive: Path, layout: Path) -> Iterator[Path]:
    """Expose the built OCI archive as Trivy's supported image-layout input."""
    layout.mkdir()
    try:
        with tarfile.open(archive) as bundle:
            bundle.extractall(layout, filter="data")
        yield layout
    finally:
        shutil.rmtree(layout)


def default_build_workers() -> int:
    """Bound shared-builder pressure using the process CPU/memory allowance."""
    cpus = len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else (os.cpu_count() or 1)
    try:
        quota, period = Path("/sys/fs/cgroup/cpu.max").read_text().split()
        if quota != "max":
            cpus = min(cpus, max(1, int(quota) // int(period)))
    except (OSError, ValueError, ZeroDivisionError):
        pass
    memory = None
    for limit_path, usage_path in (
        (Path("/sys/fs/cgroup/memory.max"), Path("/sys/fs/cgroup/memory.current")),
        (Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
         Path("/sys/fs/cgroup/memory/memory.usage_in_bytes")),
    ):
        try:
            value = int(limit_path.read_text().strip())
        except (OSError, ValueError):
            continue
        if 0 <= value < 1 << 60:
            try:
                used = int(usage_path.read_text().strip())
                if used >= 0:
                    value = max(0, value - used)
            except (OSError, ValueError):
                pass
            memory = value if memory is None else min(memory, value)
    try:
        available = next(
            int(line.split()[1]) * 1024 for line in Path("/proc/meminfo").read_text().splitlines()
            if line.startswith("MemAvailable:")
        )
        memory = available if memory is None else min(memory, available)
    except (OSError, StopIteration, ValueError):
        pass
    # Reserve 2 GiB for the runner/scanner and allow roughly 2 GiB per build.
    memory_workers = max(1, (memory - 2 * 1024**3) // (2 * 1024**3)) if memory is not None else 4
    return min(4, max(1, cpus), memory_workers)


def build_image_command(
    root: Path, candidate: str, version: str, owner: dict[str, Any],
    component: str, tag: str, archive: Path,
) -> list[str]:
    """One producer/consumer image contract; identity is the final source SHA."""
    return [
        os.environ.get("BUILDKIT_CLIENT", os.environ.get("BUILDKIT_BIN", "/opt/buildkit/buildctl")),
        "build", "--frontend", "dockerfile.v0",
        "--local", f"context={root / owner['context']}",
        "--local", f"dockerfile={(root / owner['dockerfile']).parent}",
        "--opt", f"filename={Path(owner['dockerfile']).name}",
        "--opt", "platform=linux/amd64",
        "--opt", f"label:org.opencontainers.image.revision={candidate}",
        "--opt", f"label:io.loom.component={component}",
        "--opt", f"label:io.loom.source-ref={SOURCE_REF}",
        "--opt", f"build-arg:LOOM_BUILD_SHA={candidate}",
        "--opt", f"build-arg:LOOM_SOURCE_REF={SOURCE_REF}",
        *(["--opt", f"build-arg:LOOM_AGENT_VERSION={version}"] if component == "harbor_runtime" else []),
        "--output", f"type=oci,oci-mediatypes=true,name={tag},dest={archive}",
    ]


def prepare_images(
    root: Path, candidate: str, version: str, rows: list[dict[str, Any]], outputdir: Path,
    *, artifacts_dir: Path | None = None, workers: int | None = None,
    components: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Prepare independent final-SHA OCI images with bounded concurrency.

    Optional artifacts are untrusted cache inputs, never admission evidence.
    Copy into this invocation's owned work directory before inspecting/scanning;
    missing or invalid inputs rebuild, while any new-build failure is fatal.
    All returned images still pass the existing scans and publication checks.
    """
    if SHA.fullmatch(candidate) is None or AGENT_VERSION.fullmatch(version) is None:
        raise ValueError("invalid image source SHA or agent version")
    workers = default_build_workers() if workers is None else workers
    if isinstance(workers, bool) or not isinstance(workers, int) or not 1 <= workers <= 7:
        raise ValueError("build workers must be between 1 and 7")
    components = COMPONENTS if components is None else components
    if not components or any(COMPONENTS.get(component) != name for component, name in components.items()):
        raise ValueError("unknown candidate image component")
    ownership = {row["image_name"]: row for row in rows}
    outputdir.mkdir(parents=True, exist_ok=False)

    def inspect_image(archive: Path, component: str, *, reused: bool) -> dict[str, Any]:
        metadata: dict[str, str] = {}
        digest, runtime_digest = inspect_oci_archive(
            archive, candidate=candidate, runtime=component == "execution_runtime",
            runtime_metadata=metadata if component == "harbor_runtime" else None,
            component=component, source_ref=SOURCE_REF, verify_layers=reused,
        )
        if component == "harbor_runtime":
            validate_runtime_metadata(metadata)
            if metadata["agent_version"] != version:
                raise ValueError("runtime agent version does not match publication")
        return {"scanned_digest": digest, "runtime_digest": runtime_digest, "runtime_metadata": metadata}

    def prepare(component: str, name: str) -> tuple[str, dict[str, Any]]:
        started = time.monotonic()
        _build_diagnostic.filename = f"failed-command-{component}.json"
        archive = outputdir / f"{component}.tar"
        reason = "no artifact directory"
        if artifacts_dir is not None:
            source = artifacts_dir / f"{component}.tar"
            try:
                if source.is_symlink() or not source.is_file():
                    raise ValueError("artifact missing or not a regular non-symlink file")
                shutil.copyfile(source, archive)
                image = inspect_image(archive, component, reused=True)
            except (OSError, ValueError, KeyError, TypeError, AttributeError, IndexError, EOFError, tarfile.TarError) as exc:
                reason = f"{type(exc).__name__}: {sanitize_diagnostic(str(exc))}"
                archive.unlink(missing_ok=True)
                print(f"Rebuilding {component}: {reason}", flush=True)
            else:
                return component, {
                    **image, "archive": archive, "origin": "reused", "reuse_reason": "exact final SHA/component/ref",
                    "elapsed_seconds": time.monotonic() - started,
                }
        owner = ownership[name]
        _run(*build_image_command(root, candidate, version, owner, component, f"{name}:candidate-{candidate}", archive))
        image = inspect_image(archive, component, reused=False)
        return component, {
            **image, "archive": archive, "origin": "rebuilt", "reuse_reason": reason,
            "elapsed_seconds": time.monotonic() - started,
        }

    with ThreadPoolExecutor(max_workers=min(workers, len(components))) as pool:
        futures = [pool.submit(prepare, component, name) for component, name in components.items()]
        # Await all work on failure before this invocation's temporary root is removed.
        return dict(future.result() for future in futures)


def build(args: argparse.Namespace) -> None:
    """Only the protected integration workflow may build/publish this candidate."""
    global _diagnostic_dir
    if args.upload_timeout_seconds <= 0:
        raise ValueError("upload timeout must be positive")
    candidate = os.environ.get("GITHUB_SHA", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    if (
        repository not in REPOSITORY_NAMES
        or os.environ.get("GITHUB_REPOSITORY_ID") != str(REPOSITORY_ID)
        or os.environ.get("GITHUB_REF") != SOURCE_REF
        or os.environ.get("GITHUB_EVENT_NAME") not in {"workflow_dispatch", "push"}
        or os.environ.get("GITHUB_WORKFLOW_REF") != f"{repository}/{WORKFLOW}@{SOURCE_REF}"
        or SHA.fullmatch(candidate) is None
        or _run("git", "rev-parse", "HEAD") != candidate
        or REGISTRY.fullmatch(args.registry_prefix) is None
    ):
        raise ValueError("build must run from the fixed protected Nebius workflow checkout")
    if _run("uname", "-m") != "x86_64":
        raise ValueError("Nebius release requires a native AMD64 runner")
    _trusted_signer(args.signing_key, args.signing_key_id, args.trusted_keyring.read_text())
    mode = getattr(args, "mode", "platform")
    version = getattr(args, "agent_version", None)
    if mode == "harness-only" and not version:
        raise ValueError("harness-only publication requires an explicit agent version")
    version = version or "nebius-" + candidate
    if AGENT_VERSION.fullmatch(version) is None:
        raise ValueError("invalid agent version label")
    source = source_metadata(candidate) if mode != "harness-only" else {}
    args.output.mkdir(parents=True, exist_ok=False)
    _diagnostic_dir = args.output
    component_manifest = load_manifest(ROOT / "config/component-ownership.toml")
    rows = release_image_matrix(component_manifest, image_set="nebius")
    ownership = {row["image_name"]: row for row in rows}
    document: dict[str, Any] = {
        "schema_version": "loom.nebius-candidate.v1",
        "repository": repository,
        "source_ref": SOURCE_REF,
        "candidate_sha": candidate,
        "workflow_path": WORKFLOW,
        "run_id": int(os.environ["GITHUB_RUN_ID"]),
        "registry_prefix": args.registry_prefix,
        "images": {},
    }
    document.update(source)
    with tempfile.TemporaryDirectory(prefix="loom-nebius-build-") as temporary:
        work = Path(temporary)
        scanner = install_trivy(work, architecture="amd64")
        policy, exceptions = work / "trivy.yaml", work / "ignore.yaml"
        write_release_policy(policy, exceptions, use_exceptions=False)
        document["policy_sha256"] = sha256(policy.read_bytes() + exceptions.read_bytes())
        components = {"harbor_runtime": COMPONENTS["harbor_runtime"]} if mode == "harness-only" else COMPONENTS
        prepared = prepare_images(
            ROOT, candidate, version, rows, work / "images",
            artifacts_dir=getattr(args, "image_artifacts_dir", None),
            workers=getattr(args, "build_workers", None), components=components,
        )
        write_json(args.output / "image-preparation.json", {
            component: {key: value for key, value in image.items() if key != "archive"}
            for component, image in prepared.items()
        })
        for component, name in components.items():
            owner = ownership[name]
            tag_label = f"runtime-{candidate}-{document['run_id']}" if mode == "harness-only" else f"candidate-{candidate}"
            tag = f"{args.registry_prefix}/{name}:{tag_label}"
            prepared_image = prepared[component]
            archive = prepared_image["archive"]
            scanned_digest = prepared_image["scanned_digest"]
            runtime_digest = prepared_image["runtime_digest"]
            if component == "harbor_runtime":
                document["runtime_metadata"] = prepared_image["runtime_metadata"]
            report = args.output / f"{component}.vulnerability.json"
            policy_report = args.output / f"{component}.policy.json"
            sbom = args.output / f"{component}.sbom.cdx.json"
            print(f"Scanning and publishing {component} ({prepared_image['origin']})", flush=True)
            with oci_scan_layout(
                archive, work / f"{component}.oci"
            ) as layout:
                _run(
                    str(scanner),
                    "--config",
                    str(policy),
                    "image",
                    "--input",
                    str(layout),
                    "--format",
                    "json",
                    "--output",
                    str(policy_report),
                    "--ignorefile",
                    str(exceptions),
                    "--show-suppressed",
                )
                validate_trivy_release_report(
                    owner["image"], "amd64", policy_report, exceptions, use_exceptions=False,
                    expected_artifact=layout,
                )
                _run(
                    str(scanner),
                    "image",
                    "--input",
                    str(layout),
                    "--scanners",
                    "vuln",
                    "--format",
                    "json",
                    "--output",
                    str(report),
                    "--ignorefile",
                    str(exceptions),
                )
                _run(
                    str(scanner),
                    "image",
                    "--input",
                    str(layout),
                    "--format",
                    "cyclonedx",
                    "--output",
                    str(sbom),
                )
            if component == "execution_runtime":
                document["runtime_binary_sha256"] = runtime_digest
            refresh_registry_auth(
                Path(os.environ["NEBIUS_REGISTRY_CREDENTIALS_FILE"]),
                args.registry_prefix,
                Path(os.environ["REGISTRY_AUTH_FILE"]),
            )
            _copy_image(archive, tag, timeout_seconds=args.upload_timeout_seconds)
            published_digest = _run(
                "skopeo", "inspect", "--format", "{{.Digest}}", f"docker://{tag}"
            )
            if published_digest != scanned_digest:
                raise ValueError("published manifest differs from the scanned OCI bytes")
            document["images"][component] = {
                "image_ref": f"{args.registry_prefix}/{name}@{scanned_digest}",
            }
            if component in EXECUTION_COMPONENTS:
                document["images"][component].update(
                    {
                        "sbom_sha256": sha256(sbom.read_bytes()),
                        "vulnerability_report_sha256": sha256(report.read_bytes()),
                        "highest_vulnerability_severity": _severity(report.read_bytes()),
                    }
                )
            archive.unlink()
        if mode == "harness-only":
            release = create_runtime_release(
                document, signing_key=args.signing_key,
                signing_key_id=args.signing_key_id, keyring_json=args.trusted_keyring.read_text(),
            )
            write_json(args.output / "agent-runtime-release.json", release)
        else:
            manifest, profile = create_candidate(
                document,
                signing_key=args.signing_key,
                signing_key_id=args.signing_key_id,
                keyring_json=args.trusted_keyring.read_text(),
                supports_task_web_egress=getattr(args, "supports_task_web_egress", False),
                service_lifecycle_ready=getattr(args, "service_lifecycle_ready", False),
                supports_task_identity=getattr(args, "supports_task_identity", False),
                guest_runtime=getattr(args, "guest_runtime", None),
                supports_emulated_pkcs11=getattr(args, "supports_emulated_pkcs11", False),
                supports_shared_guest=getattr(args, "supports_shared_guest", False),
                guest_runtime_volume_mib=getattr(args, "guest_runtime_volume_mib", None),
                guest_max_artifact_bytes=getattr(args, "guest_max_artifact_bytes", None),
                node_share_resources=True,
            )
            profile = prepare_prebuilt_profile(profile, args)
            write_json(args.output / "candidate.json", manifest)
            write_json(args.output / "runtime-profile.json", profile)
            release = _runtime_release_payload({
                **document, "images": {"harbor_runtime": document["images"]["harbor_runtime"]},
            })
            release["image_admission"] = next(
                item for item in profile["image_admission"]["admissions"]
                if item["statement"]["image_ref"] == release["agent_image_ref"]
            )
            write_json(args.output / "agent-runtime-release.json", release)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("check-shape")
    inspect.add_argument("--candidate", type=Path, required=True)
    create = commands.add_parser("create")
    create.add_argument("--build-record", type=Path, required=True)
    release = commands.add_parser("create-runtime-release")
    release.add_argument("--build-record", type=Path, required=True)
    builder = commands.add_parser("build")
    builder.add_argument("--mode", choices=("platform", "harness-only"), default="platform")
    builder.add_argument("--agent-version")
    builder.add_argument("--prebuilt-images-json", type=Path)
    builder.add_argument("--prebuilt-crane", type=Path, default=Path("crane"))
    builder.add_argument("--registry-prefix", required=True)
    builder.add_argument("--image-artifacts-dir", type=Path,
                         help="Optional per-component OCI archives; invalid/missing inputs rebuild at final SHA")
    builder.add_argument("--build-workers", type=int, choices=range(1, 8),
                         help="Concurrent image preparations (default: resource-aware, at most 4)")
    builder.add_argument("--upload-timeout-seconds", type=int, default=900,
                         help="Total per-image upload budget, including one transient-network retry (default: 900)")
    for command in (create, builder, release):
        command.add_argument("--signing-key", type=Path, required=True)
        command.add_argument("--signing-key-id", required=True)
        command.add_argument("--output", type=Path, required=True)
    for command in (create, builder, release):
        command.add_argument("--trusted-keyring", type=Path, required=True)
    for command in (create, builder):
        command.add_argument("--supports-task-web-egress", action="store_true")
        command.add_argument("--service-lifecycle-ready", action="store_true")
        command.add_argument("--supports-task-identity", action="store_true")
        command.add_argument("--guest-runtime", choices=("qemu-tcg-v1",))
        command.add_argument("--supports-emulated-pkcs11", action="store_true")
        command.add_argument("--supports-shared-guest", action="store_true")
        command.add_argument("--guest-runtime-volume-mib", type=int)
        command.add_argument("--guest-max-artifact-bytes", type=int)
    create.add_argument("--node-share-resources", action="store_true",
                        help="Only for a build record whose runtime supports node-share-v1")
    args = parser.parse_args()
    try:
        if args.command == "check-shape":
            validate_identity(read_json(args.candidate))
        elif args.command == "create-runtime-release":
            result = create_runtime_release(
                read_json(args.build_record), signing_key=args.signing_key,
                signing_key_id=args.signing_key_id, keyring_json=args.trusted_keyring.read_text(),
            )
            args.output.mkdir(parents=True, exist_ok=False)
            write_json(args.output / "agent-runtime-release.json", result)
        elif args.command == "create":
            manifest, profile = create_candidate(
                read_json(args.build_record),
                signing_key=args.signing_key,
                signing_key_id=args.signing_key_id,
                keyring_json=args.trusted_keyring.read_text(),
                supports_task_web_egress=args.supports_task_web_egress,
                service_lifecycle_ready=args.service_lifecycle_ready,
                supports_task_identity=args.supports_task_identity,
                guest_runtime=args.guest_runtime,
                supports_emulated_pkcs11=args.supports_emulated_pkcs11,
                supports_shared_guest=args.supports_shared_guest,
                guest_runtime_volume_mib=args.guest_runtime_volume_mib,
                guest_max_artifact_bytes=args.guest_max_artifact_bytes,
                node_share_resources=args.node_share_resources,
            )
            args.output.mkdir(parents=True, exist_ok=False)
            write_json(args.output / "candidate.json", manifest)
            write_json(args.output / "runtime-profile.json", profile)
        else:
            build(args)
    except Exception as exc:
        # Avoid serializing subprocess output or credential-bearing inputs.
        print(f"Nebius candidate {args.command} failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(f"Nebius candidate {args.command} succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
