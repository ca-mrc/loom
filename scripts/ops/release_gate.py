#!/usr/bin/env python3
"""Validate release-promotion gate evidence for production deploys.

The heavy staging gate includes live cluster, API, benchmark, provider, worker,
and rollback checks that are partly operator-driven. This script validates the
structured evidence manifest those checks produce so a production deploy can
machine-reject missing evidence or leaked secrets.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if (REPO_ROOT / 'src').is_dir() and str(REPO_ROOT / 'src') not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / 'src'))

from loom.nebius_candidate_contract import NEBIUS_PLATFORM_IMAGES  # noqa: E402

REQUIRED_IMAGE_DIGESTS = tuple(NEBIUS_PLATFORM_IMAGES.values())

REQUIRED_CHECKS: dict[str, tuple[str, ...]] = {
    "repository_ci": ("url",),
    "image_build": ("url",),
    "cluster_render_audit": ("url", "staging_config", "production_config"),
    "migration_dry_run": ("url", "db_recovery_point"),
    "public_api_spa_smoke": ("url", "batch_id", "trial_id", "artifact_url"),
    "frontend_route_evidence": (
        "url",
        "production_route",
        "staging_route",
        "production_api_base",
        "staging_api_base",
    ),
    "secret_redaction": ("url",),
    "provider_smoke": ("url", "provider_path"),
    "benchmark_reward_gate": ("url", "batch_id", "benchmarks"),
    "score_positive_canary": ("url", "batch_id"),
    "benchmark_score_alignment": ("url", "manifest", "benchmarks"),
    "hf_mirror_token_boundary": (
        "url",
        "benchmark_id",
        "runtime_source_prefix",
        "upstream_locator",
        "upstream_revision",
    ),
    "execution_capacity_smoke": ("url", "batch_id", "provider"),
    "prod_staging_isolation": (
        "url",
        "state_profile_evidence",
        "worker_identity_evidence",
        "frontend_api_base_evidence",
    ),
    "raw_delivery_export_status": ("url", "requirement_status"),
    "rollback_plan": (
        "previous_production_image_digest",
        "rendered_manifest",
        "db_recovery_point",
    ),
    "release_owner_approval": ("owner", "url"),
}
CANONICAL_FRONTEND_ROUTES = {
    "production_route": "https://yylx.world/prod",
    "staging_route": "https://yylx.world/staging",
    "production_api_base": "https://yylx.world/prod/api",
    "staging_api_base": "https://yylx.world/staging/api",
}
CANONICAL_STATE_IDENTITIES: dict[str, dict[str, Any]] = {
    "production": {
        "environment": "production",
        "github_environment": "production",
        "namespace": "loom-prod",
        "database_name": "loom_prod",
        "provider_connection_namespace": "production",
        "object_storage": {
            "task_bucket": "loom-prod-tasks",
            "trajectories_bucket": "loom-prod-trajectories",
            "artifacts_bucket": "loom-prod-artifacts",
        },
        "secret_ref_prefix": "github-environment:production/",
    },
    "staging": {
        "environment": "staging",
        "github_environment": "staging",
        "namespace": "loom-staging",
        "database_name": "loom_staging",
        "provider_connection_namespace": "staging",
        "object_storage": {
            "task_bucket": "loom-staging-tasks",
            "trajectories_bucket": "loom-staging-trajectories",
            "artifacts_bucket": "loom-staging-artifacts",
        },
        "secret_ref_prefix": "github-environment:staging/",
    },
}
PROD_STAGING_STORAGE_FIELDS = ("task_bucket", "trajectories_bucket", "artifacts_bucket")
PROD_STAGING_SECRET_REF_FIELDS = (
    "secret_store_key_ref",
    "service_api_token_ref",
    "worker_token_ref",
    "provider_secret_ref",
    "yibuapi_secret_ref",
)

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"(^|@)sha256:[0-9a-f]{64}$")
URL_RE = re.compile(r"^https://[^\s]+$")
PROD_TAG_RE = re.compile(r"^v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$")
URL_CREDENTIAL_RE = re.compile(r"://([^:/@\s]+):([^@\s]+)@")
SECRET_VALUE_PATTERNS = (
    re.compile(r"authorization:\s*bearer", re.IGNORECASE),
    re.compile(r"\bbearer\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{10,}"),
    re.compile(r"\bhf_[A-Za-z0-9_]{20,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bloom_(?:api|w)_[A-Za-z0-9._-]{8,}", re.IGNORECASE),
    re.compile(r"[?&](X-Amz-Signature|AWSAccessKeyId|Signature)=", re.IGNORECASE),
    re.compile(
        r"(?i)(token|api_key|access_key|secret|password|signature)=[^&\s]+",
    ),
    URL_CREDENTIAL_RE,
)
FORBIDDEN_PATTERNS = (
    *SECRET_VALUE_PATTERNS,
    re.compile(r"\bloom://", re.IGNORECASE),
    re.compile(r"https?://loom-(minio|postgres|llm-gateway|control-plane)([:/.]|$)", re.IGNORECASE),
    re.compile(r"\.svc\.cluster\.local\b", re.IGNORECASE),
    re.compile(r"\bhost\.docker\.internal\b", re.IGNORECASE),
)


def _load_manifest(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("manifest root must be a JSON object")
    return raw


def _iter_strings(value: Any, path: str) -> list[tuple[str, str]]:
    if isinstance(value, str):
        return [(path, value)]
    if isinstance(value, dict):
        pairs: list[tuple[str, str]] = []
        for key, child in value.items():
            pairs.extend(_iter_strings(child, f"{path}.{key}" if path else str(key)))
        return pairs
    if isinstance(value, list):
        pairs = []
        for index, child in enumerate(value):
            pairs.extend(_iter_strings(child, f"{path}[{index}]"))
        return pairs
    return []


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _as_dict(value: Any, path: str, errors: list[str]) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    errors.append(f"{path} must be an object")
    return None


def _string_value(value: Any, path: str, errors: list[str]) -> str | None:
    if _is_non_empty_string(value):
        return str(value)
    errors.append(f"{path} must be a non-empty string")
    return None


def _validate_top_level(
    manifest: dict[str, Any],
    *,
    candidate_sha: str | None,
    image_tag: str | None,
) -> list[str]:
    errors: list[str] = []
    schema_version = manifest.get("schema_version")
    if type(schema_version) is not int or schema_version != 1:
        errors.append("schema_version must be 1")

    manifest_sha = manifest.get("candidate_sha")
    if not isinstance(manifest_sha, str) or not SHA_RE.fullmatch(manifest_sha):
        errors.append("candidate_sha must be a 40-character lowercase git SHA")
    if candidate_sha and manifest_sha != candidate_sha:
        errors.append(
            f"candidate_sha mismatch: manifest={manifest_sha!r} expected={candidate_sha!r}"
        )

    manifest_image_tag = manifest.get("image_tag")
    if not _is_non_empty_string(manifest_image_tag):
        errors.append("image_tag must be a non-empty string")
    if image_tag and manifest_image_tag != image_tag:
        errors.append(f"image_tag mismatch: manifest={manifest_image_tag!r} expected={image_tag!r}")

    prod_tag = manifest.get("prod_tag")
    if not isinstance(prod_tag, str) or not PROD_TAG_RE.fullmatch(prod_tag):
        errors.append("prod_tag must be an immutable SemVer tag like v1.0.0")

    staging_url = manifest.get("staging_url")
    if not isinstance(staging_url, str) or not URL_RE.fullmatch(staging_url):
        errors.append("staging_url must be an https URL")

    digests = manifest.get("image_digests")
    if not isinstance(digests, dict):
        errors.append("image_digests must be an object")
        return errors
    extra_images = sorted(set(digests) - set(REQUIRED_IMAGE_DIGESTS))
    if extra_images:
        errors.append(
            "image_digests contains images without a manifest release owner: "
            + ", ".join(extra_images)
        )
    for image_name in REQUIRED_IMAGE_DIGESTS:
        digest = digests.get(image_name)
        if not isinstance(digest, str) or not DIGEST_RE.search(digest):
            errors.append(f"image_digests.{image_name} must end with @sha256:<64 hex>")
    return errors


def _validate_checks(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    checks = manifest.get("checks")
    if not isinstance(checks, dict):
        return ["checks must be an object"]

    for check_name, required_fields in REQUIRED_CHECKS.items():
        check = checks.get(check_name)
        if not isinstance(check, dict):
            errors.append(f"missing required check '{check_name}'")
            continue
        if check.get("status") != "pass":
            errors.append(f"{check_name}.status must be 'pass'")
        for field in required_fields:
            value = check.get(field)
            if field == "benchmarks":
                if (
                    not isinstance(value, list)
                    or not value
                    or not all(_is_non_empty_string(item) for item in value)
                ):
                    errors.append(f"{check_name}.benchmarks must be a non-empty string list")
                continue
            if not _is_non_empty_string(value):
                errors.append(f"{check_name}.{field} must be a non-empty string")

        if check_name == "execution_capacity_smoke":
            errors.extend(_validate_execution_capacity_smoke(check))
        if check_name == "score_positive_canary":
            errors.extend(_validate_score_positive_canary(check))
        if check_name == "frontend_route_evidence":
            errors.extend(_validate_frontend_route_evidence(check))
        if check_name == "hf_mirror_token_boundary":
            errors.extend(_validate_hf_mirror_token_boundary(check))
        if check_name == "prod_staging_isolation":
            errors.extend(_validate_prod_staging_isolation(check, manifest=manifest))

    return errors


def _validate_hf_mirror_token_boundary(check: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if check.get("benchmark_id") != "skilllearnbench":
        errors.append("hf_mirror_token_boundary.benchmark_id must be 'skilllearnbench'")
    if check.get("environment") not in {"staging", "production"}:
        errors.append("hf_mirror_token_boundary.environment must be staging or production")
    if check.get("runtime_source_scheme") != "s3":
        errors.append("hf_mirror_token_boundary.runtime_source_scheme must be 's3'")
    runtime_prefix = check.get("runtime_source_prefix")
    if not isinstance(runtime_prefix, str) or not runtime_prefix.startswith("s3://"):
        errors.append("hf_mirror_token_boundary.runtime_source_prefix must start with s3://")
    runnable_tasks = check.get("runnable_tasks")
    if not isinstance(runnable_tasks, int) or runnable_tasks <= 0:
        errors.append("hf_mirror_token_boundary.runnable_tasks must be an integer > 0")
    total_sources = check.get("total_task_sources")
    internal_sources = check.get("internal_s3_sources")
    if not isinstance(total_sources, int) or total_sources <= 0:
        errors.append("hf_mirror_token_boundary.total_task_sources must be an integer > 0")
    if internal_sources != total_sources:
        errors.append(
            "hf_mirror_token_boundary.internal_s3_sources must equal total_task_sources",
        )
    if check.get("hf_provenance_retained") is not True:
        errors.append("hf_mirror_token_boundary.hf_provenance_retained must be true")
    if check.get("upstream_kind") != "huggingface":
        errors.append("hf_mirror_token_boundary.upstream_kind must be 'huggingface'")
    if not _is_non_empty_string(check.get("upstream_locator")):
        errors.append("hf_mirror_token_boundary.upstream_locator must be a non-empty string")
    if not _is_non_empty_string(check.get("upstream_revision")):
        errors.append("hf_mirror_token_boundary.upstream_revision must be a non-empty string")
    if check.get("worker_hf_token_present") is not False:
        errors.append("hf_mirror_token_boundary.worker_hf_token_present must be false")
    if check.get("direct_hf_egress_required") is not False:
        errors.append("hf_mirror_token_boundary.direct_hf_egress_required must be false")
    if check.get("secret_safe") is not True:
        errors.append("hf_mirror_token_boundary.secret_safe must be true")
    return errors


def _validate_frontend_route_evidence(check: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for field, expected in CANONICAL_FRONTEND_ROUTES.items():
        if check.get(field) != expected:
            errors.append(f"frontend_route_evidence.{field} must be {expected}")
    if check.get("production_route") == check.get("staging_route"):
        errors.append("frontend_route_evidence production and staging routes must differ")
    if check.get("production_api_base") == check.get("staging_api_base"):
        errors.append("frontend_route_evidence production and staging API bases must differ")
    prod_label = check.get("production_environment_label")
    if isinstance(prod_label, str) and "beta" in prod_label.lower():
        errors.append("frontend_route_evidence.production_environment_label must not contain beta")
    return errors


def _validate_score_positive_canary(check: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    scored = check.get("scored_trial_count")
    positive = check.get("positive_reward_trial_count")
    non_full = check.get("non_full_reward_trial_count")
    if not isinstance(scored, int) or scored <= 0:
        errors.append("score_positive_canary.scored_trial_count must be an integer > 0")
    if not isinstance(positive, int) or positive <= 0:
        errors.append(
            "score_positive_canary.positive_reward_trial_count must be an integer > 0",
        )
    if not isinstance(non_full, int) or non_full <= 0:
        errors.append(
            "score_positive_canary.non_full_reward_trial_count must be an integer > 0",
        )
    if isinstance(scored, int) and isinstance(positive, int) and positive > scored:
        errors.append(
            "score_positive_canary.positive_reward_trial_count must be <= scored_trial_count",
        )
    if isinstance(scored, int) and isinstance(non_full, int) and non_full > scored:
        errors.append(
            "score_positive_canary.non_full_reward_trial_count must be <= scored_trial_count",
        )
    return errors


def _validate_execution_capacity_smoke(check: dict[str, Any]) -> list[str]:
    """Require native workload evidence, distinct from configured pool capacity."""
    errors: list[str] = []
    prefix = "execution_capacity_smoke"
    if check.get("provider") != "nebius":
        errors.append(f"{prefix}.provider must be 'nebius'")
    runtime_seconds = check.get("runtime_seconds")
    if (
        isinstance(runtime_seconds, bool)
        or not isinstance(runtime_seconds, int | float)
        or not math.isfinite(runtime_seconds)
        or runtime_seconds < 0
    ):
        errors.append(f"{prefix}.runtime_seconds must be a finite non-negative number")
    failures = check.get("failures")
    if type(failures) is not int or failures != 0:
        errors.append(f"{prefix}.failures must be zero")
    records = check.get("execution_records")
    if not isinstance(records, list) or not records:
        errors.append(f"{prefix}.execution_records must contain native execution evidence")
        return errors
    required_fields = (
        "trial_id",
        "attempt_id",
        "target_id",
        "project_id",
        "cluster_id",
        "namespace",
        "job_uid",
        "node_name",
    )
    seen: set[tuple[str, str]] = set()
    for index, record in enumerate(records):
        path = f"{prefix}.execution_records[{index}]"
        if not isinstance(record, dict):
            errors.append(f"{path} must be an object")
            continue
        for field in required_fields:
            if not _is_non_empty_string(record.get(field)):
                errors.append(f"{path}.{field} must be a non-empty string")
        identity = (str(record.get("trial_id", "")), str(record.get("attempt_id", "")))
        if identity in seen:
            errors.append(f"{path} repeats an execution attempt")
        seen.add(identity)
    return errors


def _profiles_by_environment(check: dict[str, Any], errors: list[str]) -> dict[str, dict[str, Any]]:
    raw_profiles = check.get("state_profiles")
    if raw_profiles is None and isinstance(check.get("environment_isolation"), dict):
        raw_profiles = check["environment_isolation"].get("profiles")

    profiles: dict[str, dict[str, Any]] = {}
    if isinstance(raw_profiles, list):
        for index, item in enumerate(raw_profiles):
            if not isinstance(item, dict):
                errors.append(f"prod_staging_isolation.state_profiles[{index}] must be an object")
                continue
            environment = item.get("environment")
            if not _is_non_empty_string(environment):
                errors.append(
                    f"prod_staging_isolation.state_profiles[{index}].environment "
                    "must be a non-empty string",
                )
                continue
            profiles[str(environment)] = item
    elif isinstance(raw_profiles, dict):
        for key, item in raw_profiles.items():
            if not isinstance(item, dict):
                errors.append(f"prod_staging_isolation.state_profiles.{key} must be an object")
                continue
            profiles[str(key)] = item
            environment = item.get("environment")
            if _is_non_empty_string(environment):
                profiles.setdefault(str(environment), item)
    else:
        errors.append("prod_staging_isolation.state_profiles must be an object or profile list")
    return profiles


def _environment_item(
    items: dict[str, Any],
    *,
    environment: str,
    aliases: tuple[str, ...],
    path: str,
    errors: list[str],
) -> dict[str, Any] | None:
    for key in (environment, *aliases):
        value = items.get(key)
        if isinstance(value, dict):
            return value
    for value in items.values():
        if isinstance(value, dict) and value.get("environment") in (environment, *aliases):
            return value
    errors.append(f"{path}.{environment} must be present")
    return None


def _validate_prod_staging_isolation(
    check: dict[str, Any],
    *,
    manifest: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    profiles = _profiles_by_environment(check, errors)
    prod_profile = _environment_item(
        profiles,
        environment="production",
        aliases=("prod",),
        path="prod_staging_isolation.state_profiles",
        errors=errors,
    )
    staging_profile = _environment_item(
        profiles,
        environment="staging",
        aliases=("stage", "dev", "development"),
        path="prod_staging_isolation.state_profiles",
        errors=errors,
    )
    if prod_profile is not None and staging_profile is not None:
        errors.extend(_validate_state_profiles(prod_profile, staging_profile))

    frontend = _as_dict(check.get("frontend"), "prod_staging_isolation.frontend", errors)
    if frontend is not None:
        prod_frontend = _environment_item(
            frontend,
            environment="production",
            aliases=("prod",),
            path="prod_staging_isolation.frontend",
            errors=errors,
        )
        staging_frontend = _environment_item(
            frontend,
            environment="staging",
            aliases=("stage", "dev", "development"),
            path="prod_staging_isolation.frontend",
            errors=errors,
        )
        if prod_frontend is not None and staging_frontend is not None:
            errors.extend(_validate_prod_staging_frontend(prod_frontend, staging_frontend))

    workers = _as_dict(check.get("workers"), "prod_staging_isolation.workers", errors)
    if workers is not None:
        prod_worker = _environment_item(
            workers,
            environment="production",
            aliases=("prod",),
            path="prod_staging_isolation.workers",
            errors=errors,
        )
        staging_worker = _environment_item(
            workers,
            environment="staging",
            aliases=("stage", "dev", "development"),
            path="prod_staging_isolation.workers",
            errors=errors,
        )
        if prod_worker is not None and staging_worker is not None:
            errors.extend(
                _validate_prod_staging_workers(prod_worker, staging_worker, manifest=manifest)
            )

    return errors


def _validate_state_profiles(
    prod_profile: dict[str, Any],
    staging_profile: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    for environment, profile in (("production", prod_profile), ("staging", staging_profile)):
        expected = CANONICAL_STATE_IDENTITIES[environment]
        for field in (
            "environment",
            "github_environment",
            "namespace",
            "database_name",
            "provider_connection_namespace",
        ):
            if profile.get(field) != expected[field]:
                errors.append(
                    f"prod_staging_isolation.state_profiles.{environment}.{field} "
                    f"must be {expected[field]!r}",
                )
        _as_dict(
            profile.get("object_storage"),
            f"prod_staging_isolation.state_profiles.{environment}.object_storage",
            errors,
        )
        secret_refs = _as_dict(
            profile.get("secret_refs"),
            f"prod_staging_isolation.state_profiles.{environment}.secret_refs",
            errors,
        )
        if secret_refs is not None:
            prefix = str(expected["secret_ref_prefix"])
            for field in PROD_STAGING_SECRET_REF_FIELDS:
                ref = _string_value(
                    secret_refs.get(field),
                    f"prod_staging_isolation.state_profiles.{environment}.secret_refs.{field}",
                    errors,
                )
                if ref is not None and not ref.startswith(prefix):
                    errors.append(
                        "prod_staging_isolation.state_profiles."
                        f"{environment}.secret_refs.{field} must start with {prefix!r}",
                    )

    for field in ("database_name", "provider_connection_namespace", "namespace"):
        if prod_profile.get(field) == staging_profile.get(field):
            errors.append(
                f"prod_staging_isolation.state_profiles.production.{field} "
                "must differ from staging",
            )
    prod_storage = prod_profile.get("object_storage")
    staging_storage = staging_profile.get("object_storage")
    if isinstance(prod_storage, dict) and isinstance(staging_storage, dict):
        shared_with_prefix_policy = _has_explicit_prod_prefix_policy(prod_storage)
        for environment, storage, peer_storage in (
            ("production", prod_storage, staging_storage),
            ("staging", staging_storage, prod_storage),
        ):
            expected_storage = CANONICAL_STATE_IDENTITIES[environment]["object_storage"]
            for field in PROD_STAGING_STORAGE_FIELDS:
                actual = storage.get(field)
                expected = expected_storage[field]
                shared_with_peer = actual == peer_storage.get(field)
                if actual == expected:
                    continue
                if shared_with_peer and shared_with_prefix_policy:
                    continue
                errors.append(
                    "prod_staging_isolation.state_profiles."
                    f"{environment}.object_storage.{field} must be {expected!r} "
                    "or shared with an approved prod prefix policy",
                )
                if environment == "production" and shared_with_peer:
                    errors.append(
                        "prod_staging_isolation.state_profiles.production.object_storage."
                        f"{field} must differ from staging or declare an approved "
                        "prod prefix policy",
                    )
    prod_refs = prod_profile.get("secret_refs")
    staging_refs = staging_profile.get("secret_refs")
    if isinstance(prod_refs, dict) and isinstance(staging_refs, dict):
        for field in PROD_STAGING_SECRET_REF_FIELDS:
            if prod_refs.get(field) == staging_refs.get(field):
                errors.append(
                    f"prod_staging_isolation.state_profiles.production.secret_refs.{field} "
                    "must differ from staging",
                )
    return errors


def _has_explicit_prod_prefix_policy(storage: dict[str, Any]) -> bool:
    policy = storage.get("prefix_policy")
    if not isinstance(policy, dict):
        return False
    prod_prefix = policy.get("production_prefix")
    dev_prefix = policy.get("development_prefix")
    return (
        policy.get("approved") is True
        and _is_non_empty_string(prod_prefix)
        and _is_non_empty_string(dev_prefix)
        and prod_prefix != dev_prefix
        and "prod" in str(prod_prefix).lower()
    )


def _validate_prod_staging_frontend(
    prod_frontend: dict[str, Any],
    staging_frontend: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    checks = (
        ("route", "production_route"),
        ("api_base", "production_api_base"),
    )
    for field, canonical_key in checks:
        expected = CANONICAL_FRONTEND_ROUTES[canonical_key]
        if prod_frontend.get(field) != expected:
            errors.append(
                f"prod_staging_isolation.frontend.production.{field} must be {expected}",
            )
    staging_expected = {
        "route": CANONICAL_FRONTEND_ROUTES["staging_route"],
        "api_base": CANONICAL_FRONTEND_ROUTES["staging_api_base"],
    }
    for field, expected in staging_expected.items():
        if staging_frontend.get(field) != expected:
            errors.append(
                f"prod_staging_isolation.frontend.staging.{field} must be {expected}",
            )
    if prod_frontend.get("route") == staging_frontend.get("route"):
        errors.append("prod_staging_isolation.frontend production and staging routes must differ")
    if prod_frontend.get("api_base") == staging_frontend.get("api_base"):
        errors.append(
            "prod_staging_isolation.frontend production and staging API bases must differ",
        )
    prod_label = prod_frontend.get("environment_label")
    if isinstance(prod_label, str) and "beta" in prod_label.lower():
        errors.append(
            "prod_staging_isolation.frontend.production.environment_label must not contain beta"
        )
    return errors


def _validate_prod_staging_workers(
    prod_worker: dict[str, Any],
    staging_worker: dict[str, Any],
    *,
    manifest: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    if prod_worker.get("environment") != "production":
        errors.append("prod_staging_isolation.workers.production.environment must be 'production'")
    staging_environment = staging_worker.get("environment")
    if staging_environment not in {"staging", "stage", "dev", "development"}:
        errors.append(
            "prod_staging_isolation.workers.staging.environment must identify dev/staging"
        )

    prod_api = prod_worker.get("api_url")
    staging_api = staging_worker.get("api_url")
    if prod_api != CANONICAL_FRONTEND_ROUTES["production_api_base"]:
        errors.append(
            "prod_staging_isolation.workers.production.api_url must be "
            f"{CANONICAL_FRONTEND_ROUTES['production_api_base']}",
        )
    if staging_api != CANONICAL_FRONTEND_ROUTES["staging_api_base"]:
        errors.append(
            "prod_staging_isolation.workers.staging.api_url must be "
            f"{CANONICAL_FRONTEND_ROUTES['staging_api_base']}",
        )
    if prod_api == staging_api:
        errors.append("prod_staging_isolation.workers production and staging api_url must differ")

    candidate_sha = manifest.get("candidate_sha")
    source_commit = prod_worker.get("source_commit")
    if source_commit != candidate_sha:
        errors.append(
            "prod_staging_isolation.workers.production.source_commit must match candidate_sha",
        )
    elif not isinstance(source_commit, str) or not SHA_RE.fullmatch(source_commit):
        errors.append("prod_staging_isolation.workers.production.source_commit must be a git SHA")

    # The schema retains "workers" for execution identity evidence. Nebius
    # executes through execution-runtime, not the retired shared worker image.
    image_digests = manifest.get("image_digests")
    runtime_digest = (
        image_digests.get(NEBIUS_PLATFORM_IMAGES["execution_runtime"])
        if isinstance(image_digests, dict)
        else None
    )
    if not isinstance(runtime_digest, str) or not re.fullmatch(
        r"[^@\s]+@sha256:[0-9a-f]{64}",
        runtime_digest,
    ):
        errors.append(
            "image_digests.loom-execution-runtime must be an immutable image reference",
        )
    for field in ("image", "image_digest"):
        if prod_worker.get(field) != runtime_digest or runtime_digest is None:
            errors.append(
                f"prod_staging_isolation.workers.production.{field} must match "
                "image_digests.loom-execution-runtime",
            )
    if prod_worker.get("k8s_namespace") != "loom-prod":
        errors.append("prod_staging_isolation.workers.production.k8s_namespace must be 'loom-prod'")
    if staging_worker.get("k8s_namespace") == "loom-prod":
        errors.append("prod_staging_isolation.workers.staging.k8s_namespace must not be loom-prod")
    return errors


def _validate_no_leaks(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for path, value in _iter_strings(manifest, ""):
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(value):
                errors.append(f"forbidden evidence value at {path}")
                break
    return errors


def _redact_string(value: str) -> str:
    redacted = value
    for pattern in SECRET_VALUE_PATTERNS:
        replacement = "://<redacted>@" if pattern is URL_CREDENTIAL_RE else "<redacted>"
        redacted = pattern.sub(replacement, redacted)
    return redacted


def _redact_value(value: Any) -> Any:
    if isinstance(value, str):
        return _redact_string(value)
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _redact_value(child) for key, child in value.items()}
    return value


def validate_manifest(
    manifest: dict[str, Any],
    *,
    candidate_sha: str | None = None,
    image_tag: str | None = None,
) -> list[str]:
    if type(manifest.get('schema_version')) is int and manifest['schema_version'] == 2:
        from scripts.ops.nebius_production_release_gate import validation_view
        try:
            view = validation_view(manifest, routes=CANONICAL_FRONTEND_ROUTES,
                identities=CANONICAL_STATE_IDENTITIES, secret_fields=PROD_STAGING_SECRET_REF_FIELDS)
        except (ValueError, KeyError, TypeError, AttributeError):
            return ['independent production target evidence is missing, mismatched or not isolated']
        return validate_manifest(view, candidate_sha=candidate_sha, image_tag=image_tag) + _validate_no_leaks(manifest)
    errors = _validate_top_level(manifest, candidate_sha=candidate_sha, image_tag=image_tag)
    errors.extend(_validate_checks(manifest))
    errors.extend(_validate_no_leaks(manifest))
    return errors


def _evidence_report(manifest: dict[str, Any]) -> dict[str, Any]:
    result = {
        "schema_version": manifest["schema_version"],
        "status": "pass",
        "candidate_sha": manifest["candidate_sha"],
        "image_tag": manifest["image_tag"],
        "prod_tag": manifest["prod_tag"],
        "staging_url": manifest["staging_url"],
        "image_digests": manifest["image_digests"],
        "checks": _redact_value(manifest["checks"]),
    }
    if manifest['schema_version'] == 2:
        result['targets'] = _redact_value(manifest['targets'])
        # Only the protected producer's environment can attest its attempt.
        try:
            run = {name: int(os.environ[key]) for name, key in
                (('run_id', 'GITHUB_RUN_ID'), ('run_attempt', 'GITHUB_RUN_ATTEMPT'))}
        except (KeyError, ValueError):
            raise ValueError('release gate run identity unavailable') from None
        if any(value <= 0 for value in run.values()):
            raise ValueError('release gate run identity unavailable')
        result['gate_run'] = run
    return result


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Release Gate Evidence",
        "",
        f"- Candidate SHA: `{report['candidate_sha']}`",
        f"- Image tag: `{report['image_tag']}`",
        f"- Prod tag: `{report['prod_tag']}`",
        f"- Staging URL: {report['staging_url']}",
        "",
        "## Image Digests",
        "",
    ]
    for image_name in REQUIRED_IMAGE_DIGESTS:
        lines.append(f"- `{image_name}`: `{report['image_digests'][image_name]}`")

    lines.extend(["", "## Checks", "", "| Check | Status | Evidence |", "| --- | --- | --- |"])
    for check_name in REQUIRED_CHECKS:
        check = report["checks"][check_name]
        evidence = check.get("url") or check.get("rendered_manifest") or check.get("owner") or ""
        lines.append(f"| `{check_name}` | `{check['status']}` | {evidence} |")
    lines.append("")
    return "\n".join(lines)


def _write_outputs(
    *,
    manifest: dict[str, Any],
    output_json: Path | None,
    output_markdown: Path | None,
) -> None:
    report = _evidence_report(manifest)
    if output_json is not None:
        output_json.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    if output_markdown is not None:
        output_markdown.write_text(_render_markdown(report), encoding="utf-8")


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--image-tag", required=True)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate and render gate evidence.")
    _add_common_args(validate)
    validate.add_argument("--output-json", type=Path)
    validate.add_argument("--output-markdown", type=Path)

    verify = subparsers.add_parser(
        "verify-production",
        help="Validate gate evidence before production deploy.",
    )
    _add_common_args(verify)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        manifest = _load_manifest(args.manifest)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Release gate validation: FAIL\n- failed to read manifest: {exc}", file=sys.stderr)
        return 1

    errors = validate_manifest(
        manifest,
        candidate_sha=args.candidate_sha,
        image_tag=args.image_tag,
    )
    if errors:
        print("Release gate validation: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    if args.command == "validate":
        try:
            _write_outputs(
                manifest=manifest,
                output_json=args.output_json,
                output_markdown=args.output_markdown,
            )
        except (ValueError, OSError) as exc:
            print(f'Release gate validation: FAIL\n- failed to retain evidence: {exc}', file=sys.stderr)
            return 1

    print("Release gate validation: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
