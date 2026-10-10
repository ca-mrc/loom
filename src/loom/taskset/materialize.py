"""TaskSet materialization orchestration (#242 sub-plan 3)."""

from __future__ import annotations

import io
import json
import re
import tarfile
import tempfile
import tomllib
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

import tomli_w
from pydantic import ValidationError

from loom.execution_architecture import execution_cpu_arch
from loom.models.task import TaskConfig, normalize_steps
from loom.models.task_checksum import task_checksum
from loom.models.taskset import (
    TaskSetVerifier,
    UserTaskSetManifest,
    bundle_object_key,
    validate_bundle_relative_path,
)
from loom.service_execution_materialization import (
    prepare_service_execution_input_manifest,
)
from loom.task_bundle_compat import (
    CompatibilitySeverity,
    TaskBundleCompatibilityIssue,
    collect_task_dir_compatibility_issues,
)
from loom.taskset.instance_mapping import MappingError, resolve_mapping
from loom.taskset.status import cap_error_summary, compute_task_set_status
from loom.taskset.storage_bytes import generated_tasks_prefix, taskset_root
from loom.taskset.template_render import render_task_template
from loom.taskset.transform_sandbox import (
    TransformSandboxConfig,
    TransformSandboxError,
)
from loom.taskset.upstream_rows import UpstreamFetchError, iter_upstream_rows
from loom.terminal_bench_normalize import normalize_terminal_bench_task_toml
from loom.trajectory.storage import discard_staged_bundle_file_metadata

_NOOP_VERIFIER = b"#!/bin/sh\nexit 0\n"
_UNSAFE_TASK_ID = re.compile(r"[^a-zA-Z0-9._-]+")


@dataclass(frozen=True)
class TaskRowDraft:
    id: str
    checksum: str
    config: dict[str, Any]
    source: str
    source_provenance: dict[str, Any] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class MaterializeOutput:
    task_rows: list[TaskRowDraft] = field(default_factory=list)
    task_count: int = 0
    status: str = "failed"
    status_reason: str | None = None
    evaluation_ready: bool = False
    error_summary: list[dict[str, str]] = field(default_factory=list)
    job_failure_reason: str | None = None
    job_failure_message: str | None = None
    retry_source: bool = False


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"expected s3 uri, got {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"invalid s3 uri: {uri!r}")
    return bucket, key


def _safe_task_segment(task_id: str) -> str:
    cleaned = _UNSAFE_TASK_ID.sub("_", task_id.strip())
    if not cleaned:
        raise ValueError("task id rendered empty after sanitization")
    return cleaned


def _db_task_id(*, task_set_id: str, rendered_task_id: str) -> str:
    return f"{task_set_id}/tasks/{_safe_task_segment(rendered_task_id)}"


def _apply_verifier_defaults(
    config: dict[str, Any],
    *,
    manifest_verifier: TaskSetVerifier | None,
    has_evaluation_intent: bool,
) -> dict[str, Any]:
    if "verifier" in config:
        return config
    if manifest_verifier is not None and has_evaluation_intent:
        verifier_block: dict[str, Any] = {"name": manifest_verifier.type}
        if manifest_verifier.type == "script":
            verifier_block["args"] = {
                "script_path": manifest_verifier.file.replace("\\", "/"),
            }
        return {**config, "verifier": verifier_block}
    return {
        **config,
        "verifier": {
            "name": "script",
            "args": {"script_path": "verifier/noop.sh"},
        },
    }


def _put_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str = "application/octet-stream",
) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def _fetch_blob_bytes(client: Any, blob_uri: str | None) -> bytes | None:
    if blob_uri is None:
        return None
    bucket, key = _parse_s3_uri(blob_uri)
    resp = client.get_object(Bucket=bucket, Key=key)
    body: bytes = resp["Body"].read()
    return body


def _fetch_verifier_bytes(client: Any, verifier_blob_uri: str | None) -> bytes | None:
    return _fetch_blob_bytes(client, verifier_blob_uri)


def _write_local_bundle(
    bundle_dir: Path,
    *,
    task_config: TaskConfig,
    instruction: str | None,
    verifier_bytes: bytes | None,
    manifest_verifier: TaskSetVerifier | None,
    has_evaluation_intent: bool,
) -> None:
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "task.toml").write_bytes(
        tomli_w.dumps(task_config.model_dump(mode="json", exclude_none=True)).encode(),
    )
    if instruction is not None:
        (bundle_dir / "instruction.md").write_text(instruction, encoding="utf-8")
    if manifest_verifier is not None and has_evaluation_intent and verifier_bytes is not None:
        rel = manifest_verifier.file.replace("\\", "/").lstrip("/")
        target = bundle_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(verifier_bytes)
        if manifest_verifier.type == "pytest" and rel.endswith(".py"):
            tests_target = bundle_dir / "tests" / PurePosixPath(rel).name
            tests_target.parent.mkdir(parents=True, exist_ok=True)
            tests_target.write_bytes(verifier_bytes)
    else:
        noop_path = bundle_dir / "verifier" / "noop.sh"
        noop_path.parent.mkdir(parents=True, exist_ok=True)
        noop_path.write_bytes(_NOOP_VERIFIER)


class _BundleSizeExceededError(Exception):
    """Raised when cumulative bundle bytes exceed the configured limit."""

    def __init__(self, cumulative: int, limit: int) -> None:
        self.cumulative = cumulative
        self.limit = limit
        super().__init__(f"bundle size {cumulative} exceeds limit {limit}")


class _UnsafeBundleArchiveError(Exception):
    """Raised when an uploaded TaskSet archive cannot be safely extracted."""


def _upload_bundle_dir(
    client: Any,
    *,
    bucket: str,
    bundle_prefix: str,
    bundle_dir: Path,
    cumulative_bytes: int = 0,
    max_bundle_bytes: int | None = None,
    team_storage_baseline: int = 0,
    max_team_storage_bytes: int | None = None,
) -> int:
    """Upload a bundle directory; returns updated cumulative byte count."""
    for path in bundle_dir.rglob("*"):
        if not path.is_file():
            continue
        data = path.read_bytes()
        cumulative_bytes += len(data)
        if max_bundle_bytes is not None and cumulative_bytes > max_bundle_bytes:
            raise _BundleSizeExceededError(cumulative_bytes, max_bundle_bytes)
        if (
            max_team_storage_bytes is not None
            and team_storage_baseline + cumulative_bytes > max_team_storage_bytes
        ):
            raise _BundleSizeExceededError(
                team_storage_baseline + cumulative_bytes,
                max_team_storage_bytes,
            )
        rel = path.relative_to(bundle_dir).as_posix()
        _put_object(
            client,
            bucket=bucket,
            key=f"{bundle_prefix}/{rel}",
            body=data,
            content_type="application/octet-stream",
        )
    return cumulative_bytes


def _publish_service_execution_input_manifest(
    client: Any,
    *,
    bucket: str,
    manifest_key: str,
    bundle_dir: Path,
    task_checksum_value: str,
) -> tuple[dict[str, Any], int]:
    body, provenance = prepare_service_execution_input_manifest(
        bundle_dir,
        task_checksum=task_checksum_value,
        bucket=bucket,
        manifest_key=manifest_key,
    )
    _put_object(
        client,
        bucket=bucket,
        key=manifest_key,
        body=body,
        content_type="application/json",
    )
    return provenance, len(body)


def _assert_safe_tar_member(member: tarfile.TarInfo) -> PurePosixPath:
    name = member.name
    if not name or "\\" in name:
        raise _UnsafeBundleArchiveError("bundle archive contains an unsafe path")
    if member.issym() or member.islnk():
        raise _UnsafeBundleArchiveError("bundle archive must not contain links")
    if member.isdev() or member.isfifo():
        raise _UnsafeBundleArchiveError("bundle archive must not contain device entries")
    path = PurePosixPath(name)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise _UnsafeBundleArchiveError("bundle archive contains an unsafe path")
    if not (member.isdir() or member.isfile()):
        raise _UnsafeBundleArchiveError("bundle archive contains an unsupported entry")
    return path


def _safe_extract_tar_bytes(
    archive_bytes: bytes,
    *,
    destination: Path,
    max_bundle_bytes: int | None = None,
) -> None:
    extracted = 0
    destination_resolved = destination.resolve()
    try:
        with tarfile.open(fileobj=io.BytesIO(archive_bytes), mode="r:*") as tar:
            for member in tar.getmembers():
                rel = _assert_safe_tar_member(member)
                target = destination / rel.as_posix()
                try:
                    target.resolve().relative_to(destination_resolved)
                except ValueError as exc:
                    raise _UnsafeBundleArchiveError(
                        "bundle archive contains an unsafe path",
                    ) from exc
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                extracted += member.size
                if max_bundle_bytes is not None and extracted > max_bundle_bytes:
                    raise _BundleSizeExceededError(extracted, max_bundle_bytes)
                target.parent.mkdir(parents=True, exist_ok=True)
                src = tar.extractfile(member)
                if src is None:
                    raise _UnsafeBundleArchiveError(
                        "bundle archive contains an unreadable file",
                    )
                with src, target.open("wb") as dst:
                    while chunk := src.read(1024 * 1024):
                        dst.write(chunk)
    except tarfile.TarError as exc:
        raise _UnsafeBundleArchiveError("bundle archive is not a readable tar file") from exc


def _task_root_for_bundle_upload(
    extracted_root: Path,
    *,
    subset: str | None,
) -> Path:
    if subset:
        rel = validate_bundle_relative_path(subset).replace("\\", "/").strip("/")
        task_root = extracted_root / rel
        if not task_root.is_dir():
            raise FileNotFoundError(f"bundle task root not found: {rel}")
        return task_root
    default_root = extracted_root / "tasks"
    if default_root.is_dir():
        return default_root
    return extracted_root


def _iter_bundle_task_tomls(task_root: Path) -> list[Path]:
    return sorted(
        path for path in task_root.rglob("task.toml")
        if path.is_file() and not path.is_symlink()
    )


def _compatibility_issue_error(
    *,
    row_index: int,
    issue: TaskBundleCompatibilityIssue,
) -> dict[str, str]:
    return {
        "row": str(row_index),
        "code": issue.code,
        "severity": issue.severity.value,
        "path": issue.path,
        "line": str(issue.line),
        "phase": issue.phase,
        "message": issue.message,
        "hint": issue.hint,
        "evidence": json.dumps(issue.evidence, sort_keys=True, separators=(",", ":")),
    }


def _materialize_bundle_upload(
    *,
    manifest: UserTaskSetManifest,
    task_set_id: str,
    owning_team_id: str,
    materialization_job_id: UUID,
    materialization_epoch: int,
    has_evaluation: bool,
    minio_client: Any,
    artifacts_bucket: str,
    max_instances: int,
    max_bundle_bytes: int | None,
    team_storage_baseline: int = 0,
    max_team_storage_bytes: int | None = None,
) -> MaterializeOutput:
    slug = manifest.slug
    storage_prefix = taskset_root(team_id=owning_team_id, slug=slug).removesuffix("/")
    output_tasks_prefix = generated_tasks_prefix(
        team_id=owning_team_id,
        slug=slug,
        job_id=materialization_job_id,
        epoch=materialization_epoch,
    )
    bundle_key = bundle_object_key(
        prefix=storage_prefix,
        relative_path=manifest.source.locator,
    )
    bundle_uri = f"s3://{artifacts_bucket}/{bundle_key}"
    try:
        bundle_bytes = _fetch_blob_bytes(minio_client, bundle_uri)
    except Exception as exc:
        return MaterializeOutput(
            status="failed",
            status_reason="bundle_blob_missing",
            job_failure_reason="bundle_blob_missing",
            job_failure_message=str(exc),
        )
    if not bundle_bytes:
        return MaterializeOutput(
            status="failed",
            status_reason="bundle_blob_missing",
            job_failure_reason="bundle_blob_missing",
            job_failure_message="bundle archive blob missing or empty",
        )

    errors: list[dict[str, str]] = []
    drafts: list[TaskRowDraft] = []
    skipped = 0
    attempted = 0
    cumulative_bytes = 0
    seen_short_ids: set[str] = set()

    try:
        with tempfile.TemporaryDirectory() as tmp:
            extracted_root = Path(tmp) / "bundle"
            extracted_root.mkdir(parents=True, exist_ok=True)
            _safe_extract_tar_bytes(
                bundle_bytes,
                destination=extracted_root,
                max_bundle_bytes=max_bundle_bytes,
            )
            try:
                task_root = _task_root_for_bundle_upload(
                    extracted_root,
                    subset=manifest.source.subset,
                )
            except (FileNotFoundError, ValueError) as exc:
                return MaterializeOutput(
                    status="failed",
                    status_reason="bundle_no_tasks",
                    job_failure_reason="bundle_no_tasks",
                    job_failure_message=str(exc),
                )
            task_tomls = _iter_bundle_task_tomls(task_root)
            if not task_tomls:
                return MaterializeOutput(
                    status="failed",
                    status_reason="bundle_no_tasks",
                    job_failure_reason="bundle_no_tasks",
                    job_failure_message="bundle archive contains no task.toml files",
                )

            for task_toml in task_tomls:
                if attempted >= max_instances:
                    break
                attempted += 1
                row_index = attempted
                bundle_dir = task_toml.parent
                try:
                    with task_toml.open("rb") as f:
                        raw_cfg: dict[str, Any] = tomllib.load(f)
                    relative = bundle_dir.relative_to(task_root)
                    contextual_id = slug if relative == Path(".") else relative.as_posix()
                    raw_cfg = normalize_terminal_bench_task_toml(raw_cfg, task_id=contextual_id)
                    task_config = normalize_steps(TaskConfig.model_validate(raw_cfg))
                    execution_cpu_arch(task_config.environment.cpu_arch)
                    rendered_task_id = task_config.task.id
                    short_id = _safe_task_segment(rendered_task_id)
                    if short_id in seen_short_ids:
                        skipped += 1
                        errors.append({
                            "row": str(row_index),
                            "code": "duplicate_task_id",
                            "message": (
                                f"task id {rendered_task_id!r} collides after "
                                "TaskSet id sanitization"
                            ),
                        })
                        continue
                    seen_short_ids.add(short_id)
                    compatibility_issues = [
                        issue
                        for issue in collect_task_dir_compatibility_issues(bundle_dir)
                        if issue.severity == CompatibilitySeverity.ERROR
                    ]
                    if compatibility_issues:
                        skipped += 1
                        errors.extend(
                            _compatibility_issue_error(
                                row_index=row_index,
                                issue=issue,
                            )
                            for issue in compatibility_issues
                        )
                        continue
                    discard_staged_bundle_file_metadata(bundle_dir)
                    checksum = task_checksum(bundle_dir)
                    bundle_prefix = f"{output_tasks_prefix}{short_id}"
                    cumulative_bytes = _upload_bundle_dir(
                        minio_client,
                        bucket=artifacts_bucket,
                        bundle_prefix=bundle_prefix,
                        bundle_dir=bundle_dir,
                        cumulative_bytes=cumulative_bytes,
                        max_bundle_bytes=max_bundle_bytes,
                        team_storage_baseline=team_storage_baseline,
                        max_team_storage_bytes=max_team_storage_bytes,
                    )
                    manifest_key = (
                        f"{output_tasks_prefix.rstrip('/')}-service-inputs/{short_id}.json"
                    )
                    source_provenance, manifest_bytes = (
                        _publish_service_execution_input_manifest(
                            minio_client,
                            bucket=artifacts_bucket,
                            manifest_key=manifest_key,
                            bundle_dir=bundle_dir,
                            task_checksum_value=checksum,
                        )
                    )
                    cumulative_bytes += manifest_bytes
                    if max_bundle_bytes is not None and cumulative_bytes > max_bundle_bytes:
                        raise _BundleSizeExceededError(cumulative_bytes, max_bundle_bytes)
                    if (
                        max_team_storage_bytes is not None
                        and team_storage_baseline + cumulative_bytes > max_team_storage_bytes
                    ):
                        raise _BundleSizeExceededError(
                            team_storage_baseline + cumulative_bytes,
                            max_team_storage_bytes,
                        )
                    db_id = _db_task_id(
                        task_set_id=task_set_id,
                        rendered_task_id=rendered_task_id,
                    )
                    drafts.append(
                        TaskRowDraft(
                            id=db_id,
                            checksum=checksum,
                            config=task_config.model_dump(mode="json"),
                            source=f"s3://{artifacts_bucket}/{bundle_prefix}/",
                            source_provenance=source_provenance,
                            # Derive from the validated immutable bundle, never
                            # from a user assertion or verifier-name convention.
                            tags={"oracle_eligible": (
                                "true" if (bundle_dir / "solution/solve.sh").is_file()
                                else "false"
                            )},
                        ),
                    )
                except (tomllib.TOMLDecodeError, ValidationError) as exc:
                    skipped += 1
                    errors.append({
                        "row": str(row_index),
                        "code": "task_config_invalid",
                        "message": str(exc),
                    })
                except _BundleSizeExceededError:
                    raise
                except Exception as exc:
                    skipped += 1
                    errors.append({
                        "row": str(row_index),
                        "code": "materialize_error",
                        "message": str(exc),
                    })
    except _UnsafeBundleArchiveError as exc:
        return MaterializeOutput(
            status="failed",
            status_reason="bundle_extract_unsafe",
            job_failure_reason="bundle_extract_unsafe",
            job_failure_message=str(exc),
        )
    except _BundleSizeExceededError as exc:
        return MaterializeOutput(
            status="failed",
            status_reason="size_exceeded",
            job_failure_reason="size_exceeded",
            job_failure_message=(
                f"bundle size {exc.cumulative} bytes exceeds limit "
                f"{exc.limit} bytes"
            ),
        )

    materialized = len(drafts)
    status, status_reason = compute_task_set_status(
        materialized=materialized,
        skipped=skipped,
    )
    if materialized == 0 and any(
        error.get("code", "").startswith("TASK_COMPAT_")
        for error in errors
    ):
        status_reason = "bundle_compatibility_error"
    return MaterializeOutput(
        task_rows=drafts,
        task_count=materialized,
        status=status,
        status_reason=status_reason,
        evaluation_ready=(
            has_evaluation
            and materialized > 0
            and status in {"ready", "partial"}
        ),
        error_summary=cap_error_summary(errors),
        job_failure_reason=(
            status_reason if status == "failed" and status_reason else None
        ),
    )


def materialize_task_set(
    *,
    manifest: UserTaskSetManifest,
    task_set_id: str,
    owning_team_id: str,
    materialization_job_id: UUID,
    materialization_epoch: int,
    intents: list[str],
    verifier_blob_uri: str | None,
    transform_blob_uri: str | None = None,
    transform_config: TransformSandboxConfig | None = None,
    minio_client: Any,
    artifacts_bucket: str,
    upstream_cache_root: Path,
    max_bundle_bytes: int | None = None,
    team_storage_baseline: int = 0,
    max_team_storage_bytes: int | None = None,
) -> MaterializeOutput:
    """Materialize one TaskSet synchronously. Caller owns DB writes."""
    slug = manifest.slug
    output_tasks_prefix = generated_tasks_prefix(
        team_id=owning_team_id,
        slug=slug,
        job_id=materialization_job_id,
        epoch=materialization_epoch,
    )
    has_evaluation = "evaluation" in intents
    max_instances = (manifest.limits.max_instances if manifest.limits else 500)

    # v1 internal-trusted workloads deliberately have no transform execution
    # capability. This guard precedes every blob fetch and subprocess path so
    # direct callers cannot recover the retired legacy execution path by
    # supplying permissive TransformSandboxConfig flags.
    if manifest.transform is not None:
        return MaterializeOutput(
            status="failed",
            status_reason="transform_unavailable_in_internal_trusted",
            job_failure_reason="transform_unavailable_in_internal_trusted",
        )

    if manifest.source.type == "bundle-upload":
        return _materialize_bundle_upload(
            manifest=manifest,
            task_set_id=task_set_id,
            owning_team_id=owning_team_id,
            materialization_job_id=materialization_job_id,
            materialization_epoch=materialization_epoch,
            has_evaluation=has_evaluation,
            minio_client=minio_client,
            artifacts_bucket=artifacts_bucket,
            max_instances=max_instances,
            max_bundle_bytes=max_bundle_bytes,
            team_storage_baseline=team_storage_baseline,
            max_team_storage_bytes=max_team_storage_bytes,
        )

    verifier_bytes = _fetch_verifier_bytes(minio_client, verifier_blob_uri)

    try:
        row_iter = iter_upstream_rows(
            manifest.source,
            cache_root=upstream_cache_root,
        )
    except UpstreamFetchError as exc:
        return MaterializeOutput(
            status="failed",
            status_reason="source_unreachable",
            job_failure_reason="source_unreachable",
            job_failure_message=str(exc),
            retry_source=True,
        )

    errors: list[dict[str, str]] = []
    drafts: list[TaskRowDraft] = []
    skipped = 0
    attempted = 0
    cumulative_bytes = 0

    try:
        for row in row_iter:
            if attempted >= max_instances:
                break
            attempted += 1
            row_index = attempted
            try:
                working_row = row
                instance = resolve_mapping(working_row, manifest.instance_mapping)
                rendered = render_task_template(
                    manifest.task_template,
                    instance=instance,
                    metadata_name=slug,
                    metadata_display_name=manifest.metadata.display_name,
                )
                if "schema_version" not in rendered:
                    rendered = {"schema_version": "1", **rendered}
                rendered = _apply_verifier_defaults(
                    rendered,
                    manifest_verifier=manifest.verifier,
                    has_evaluation_intent=has_evaluation,
                )
                task_config = normalize_steps(TaskConfig.model_validate(rendered))
                execution_cpu_arch(task_config.environment.cpu_arch)
                rendered_task_id = task_config.task.id
                short_id = _safe_task_segment(rendered_task_id)
                db_id = _db_task_id(
                    task_set_id=task_set_id,
                    rendered_task_id=rendered_task_id,
                )
                prompt = instance.get("prompt")
                instruction = None
                if prompt is not None:
                    instruction = (
                        f"{prompt}\n" if not str(prompt).endswith("\n") else str(prompt)
                    )

                with tempfile.TemporaryDirectory() as tmp:
                    bundle_dir = Path(tmp)
                    _write_local_bundle(
                        bundle_dir,
                        task_config=task_config,
                        instruction=instruction,
                        verifier_bytes=verifier_bytes,
                        manifest_verifier=manifest.verifier,
                        has_evaluation_intent=has_evaluation,
                    )
                    compatibility_issues = [
                        issue
                        for issue in collect_task_dir_compatibility_issues(bundle_dir)
                        if issue.severity == CompatibilitySeverity.ERROR
                    ]
                    if compatibility_issues:
                        skipped += 1
                        errors.extend(
                            _compatibility_issue_error(
                                row_index=row_index,
                                issue=issue,
                            )
                            for issue in compatibility_issues
                        )
                        continue
                    checksum = task_checksum(bundle_dir)
                    bundle_prefix = f"{output_tasks_prefix}{short_id}"
                    cumulative_bytes = _upload_bundle_dir(
                        minio_client,
                        bucket=artifacts_bucket,
                        bundle_prefix=bundle_prefix,
                        bundle_dir=bundle_dir,
                        cumulative_bytes=cumulative_bytes,
                        max_bundle_bytes=max_bundle_bytes,
                        team_storage_baseline=team_storage_baseline,
                        max_team_storage_bytes=max_team_storage_bytes,
                    )
                    manifest_key = (
                        f"{output_tasks_prefix.rstrip('/')}-service-inputs/{short_id}.json"
                    )
                    source_provenance, manifest_bytes = (
                        _publish_service_execution_input_manifest(
                            minio_client,
                            bucket=artifacts_bucket,
                            manifest_key=manifest_key,
                            bundle_dir=bundle_dir,
                            task_checksum_value=checksum,
                        )
                    )
                    cumulative_bytes += manifest_bytes
                    if max_bundle_bytes is not None and cumulative_bytes > max_bundle_bytes:
                        raise _BundleSizeExceededError(cumulative_bytes, max_bundle_bytes)
                    if (
                        max_team_storage_bytes is not None
                        and team_storage_baseline + cumulative_bytes > max_team_storage_bytes
                    ):
                        raise _BundleSizeExceededError(
                            team_storage_baseline + cumulative_bytes,
                            max_team_storage_bytes,
                        )
                source = f"s3://{artifacts_bucket}/{bundle_prefix}/"
                drafts.append(
                    TaskRowDraft(
                        id=db_id,
                        checksum=checksum,
                        config=task_config.model_dump(mode="json"),
                        source=source,
                        source_provenance=source_provenance,
                    ),
                )
            except MappingError as exc:
                skipped += 1
                errors.append({
                    "row": str(row_index),
                    "code": "mapping_error",
                    "message": str(exc),
                })
            except TransformSandboxError as exc:
                skipped += 1
                errors.append({
                    "row": str(row_index),
                    "code": exc.code,
                    "message": exc.message,
                })
            except ValidationError as exc:
                skipped += 1
                errors.append({
                    "row": str(row_index),
                    "code": "task_config_invalid",
                    "message": str(exc.errors()),
                })
            except _BundleSizeExceededError:
                raise
            except Exception as exc:
                skipped += 1
                errors.append({
                    "row": str(row_index),
                    "code": "materialize_error",
                    "message": str(exc),
                })
    except _BundleSizeExceededError as exc:
        return MaterializeOutput(
            status="failed",
            status_reason="size_exceeded",
            job_failure_reason="size_exceeded",
            job_failure_message=(
                f"bundle size {exc.cumulative} bytes exceeds limit "
                f"{exc.limit} bytes"
            ),
        )
    except UpstreamFetchError as exc:
        return MaterializeOutput(
            status="failed",
            status_reason="source_unreachable",
            job_failure_reason="source_unreachable",
            job_failure_message=str(exc),
            retry_source=True,
        )

    materialized = len(drafts)
    status, status_reason = compute_task_set_status(
        materialized=materialized,
        skipped=skipped,
    )
    if materialized == 0 and any(
        error.get("code", "").startswith("TASK_COMPAT_")
        for error in errors
    ):
        status_reason = "bundle_compatibility_error"
    evaluation_ready = (
        has_evaluation
        and manifest.verifier is not None
        and verifier_bytes is not None
        and materialized > 0
        and status in {"ready", "partial"}
    )
    return MaterializeOutput(
        task_rows=drafts,
        task_count=materialized,
        status=status,
        status_reason=status_reason,
        evaluation_ready=evaluation_ready,
        error_summary=cap_error_summary(errors),
        job_failure_reason=(
            status_reason if status == "failed" and status_reason else None
        ),
    )
