#!/usr/bin/env python3
"""Staging invite-only smoke gate.

This script verifies the public API and Run Library release boundary against a
staging deployment. Browser-only invite acceptance and SPA submission still
need operator evidence, but the post-setup API checks here are intentionally
repeatable and produce a release-comment-ready report.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

try:
    from loom_cli.secret_source import SecretSourceError, resolve_secret_source
except ModuleNotFoundError:  # pragma: no cover - direct script execution fallback
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
    from loom_cli.secret_source import SecretSourceError, resolve_secret_source

REQUEST_TIMEOUT_SEC = 30.0

REQUIRED_CHECK_IDS: tuple[str, ...] = (
    "http.health",
    "spa.logged_out",
    "auth.team_a_whoami",
    "auth.team_b_whoami",
    "providers.list",
    "providers.models",
    "agents.ready_catalog",
    "benchmarks.runnable_catalog",
    "benchmarks.ready_bundle_objects",
    "object_store.minio_write_probe",
    "service.no_oom_restarts",
    "runs.batch_detail",
    "runs.claimed_without_started",
    "runs.worker_pool_coverage",
    "runs.trial_detail",
    "artifacts.owner_atif_download",
    "artifacts.owner_trajectory_download",
    "library.my_team_contains_run",
    "library.all_teams_contains_run",
    "library.owner_team_label",
    "library.cross_team_safe_download",
    "library.direct_cross_team_download_denied",
    "library.clone_config",
    "library.reuse_artifact",
    "library.reuse_provenance",
    "library.blocked_artifact_denied",
    "library.private_artifact_denied",
    "runs.cross_team_mutation_denied",
    "security.no_secret_or_internal_url_leaks",
)

SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"loom_api_[A-Za-z0-9_-]{16,}"),
    re.compile(r"loom_invite_[A-Za-z0-9_-]{16,}"),
    re.compile(r"loom_(?:team|admin)_[A-Za-z0-9_-]{16,}"),
    re.compile(r"sk-ant-[A-Za-z0-9_-]{20,}"),
    re.compile(r"sk-(?!ant-)[A-Za-z0-9_-]{40,}"),
    re.compile(r"AIza[0-9A-Za-z_-]{35}"),
    re.compile(r"gsk_[A-Za-z0-9_-]{20,}"),
    re.compile(r"nvapi-[A-Za-z0-9_-]{20,}"),
    re.compile(r"X-Amz-Signature=[A-Za-z0-9%_-]+"),
    re.compile(r"AWSAccessKeyId=[A-Za-z0-9%_-]+"),
)

INTERNAL_URL_PATTERN = re.compile(
    r"https?://[^\s\"'<>]*(?:"
    r"\.svc(?:\.cluster\.local)?"
    r"|localhost(?::\d+)?"
    r"|127\.0\.0\.1(?::\d+)?"
    r"|loom-(?:minio|postgres|control-plane|llm-gateway|worker)"
    r")[^\s\"'<>]*",
)


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    subsystem: str
    status: str
    detail: str
    remediation: str = ""


@dataclass(frozen=True)
class SmokeReport:
    server_url: str
    results: list[CheckResult]
    response_bytes_scanned: int


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    headers: dict[str, str]
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text)


@dataclass(frozen=True)
class ServiceContainerRestartEvidence:
    pod_name: str
    container_name: str
    restart_count: int
    last_reason: str
    exit_code: Any

    @property
    def key(self) -> tuple[str, str]:
        return (self.pod_name, self.container_name)


@dataclass(frozen=True)
class ServicePodRestartSnapshot:
    status: str
    detail: str
    remediation: str = ""
    containers: tuple[ServiceContainerRestartEvidence, ...] = ()


def _mask_value(value: str) -> str:
    if value.startswith("loom_"):
        return "loom_..."
    if value.startswith("loom-"):
        return "loom-..."
    token = re.split(r"[-_\s:/]", value, maxsplit=1)[0]
    if len(token) >= 4:
        return f"{token[:8]}..."
    return f"{value[:6]}..."


def redact_text(text: str, secret_values: list[str] | tuple[str, ...]) -> str:
    redacted = text
    for value in sorted({v for v in secret_values if v}, key=len, reverse=True):
        redacted = redacted.replace(value, _mask_value(value))
    for pattern in SECRET_PATTERNS:
        redacted = pattern.sub(lambda m: _mask_value(m.group(0)), redacted)
    redacted = INTERNAL_URL_PATTERN.sub(lambda m: _mask_value(m.group(0)), redacted)
    return redacted


def scan_evidence_text(
    text: str,
    *,
    secret_needles: list[str] | tuple[str, ...] = (),
    internal_url_needles: list[str] | tuple[str, ...] = (),
) -> CheckResult:
    hits: list[str] = []
    for needle in secret_needles:
        if needle and needle in text:
            hits.append(_mask_value(needle))
    for needle in internal_url_needles:
        if needle and needle in text:
            hits.append(_mask_value(needle))
    for pattern in SECRET_PATTERNS:
        for match in pattern.finditer(text):
            hits.append(_mask_value(match.group(0)))
    for match in INTERNAL_URL_PATTERN.finditer(text):
        hits.append(_mask_value(match.group(0)))

    if hits:
        unique_hits = ", ".join(sorted(set(hits)))
        return CheckResult(
            check_id="security.no_secret_or_internal_url_leaks",
            subsystem="security",
            status="fail",
            detail=f"detected forbidden values in API evidence: {unique_hits}",
            remediation=(
                "Inspect the failing endpoint response, apply central redaction, "
                "and block raw object-store/internal URLs before release."
            ),
        )
    return CheckResult(
        check_id="security.no_secret_or_internal_url_leaks",
        subsystem="security",
        status="pass",
        detail="No seeded secrets, token patterns, signed URLs, or internal URLs found.",
    )


def _status_icon(status: str) -> str:
    return {
        "pass": "PASS",
        "fail": "FAIL",
        "skip": "SKIP",
    }.get(status, status.upper())


def render_markdown(
    report: SmokeReport,
    *,
    secret_values: list[str] | tuple[str, ...] = (),
) -> str:
    counts = {status: 0 for status in ("pass", "fail", "skip")}
    for result in report.results:
        counts[result.status] = counts.get(result.status, 0) + 1

    lines = [
        "# Loom staging smoke evidence",
        "",
        f"- Server: `{redact_text(report.server_url, secret_values)}`",
        f"- Checks: {counts.get('pass', 0)} pass, "
        f"{counts.get('fail', 0)} fail, {counts.get('skip', 0)} skip",
        f"- Response bytes scanned for leaks: {report.response_bytes_scanned}",
        "",
        "| Status | Check | Subsystem | Detail | Remediation |",
        "|---|---|---|---|---|",
    ]
    for result in report.results:
        detail = redact_text(result.detail, secret_values).replace("\n", " ")
        remediation = redact_text(result.remediation, secret_values).replace("\n", " ")
        lines.append(
            f"| {_status_icon(result.status)} | `{result.check_id}` | "
            f"{result.subsystem} | {detail} | {remediation} |",
        )
    return "\n".join(lines) + "\n"


def render_console_summary(
    report: SmokeReport,
    *,
    markdown_output: str | Path | None,
    json_output: str | Path | None,
) -> str:
    counts = {status: 0 for status in ("pass", "fail", "skip")}
    for result in report.results:
        counts[result.status] = counts.get(result.status, 0) + 1

    lines = [
        "Loom staging smoke gate:",
        f"  {counts.get('pass', 0)} pass, "
        f"{counts.get('fail', 0)} fail, {counts.get('skip', 0)} skip",
        f"  response bytes scanned: {report.response_bytes_scanned}",
    ]
    if markdown_output is not None:
        lines.append(f"  markdown evidence: {markdown_output}")
    if json_output is not None:
        lines.append(f"  json evidence: {json_output}")
    if markdown_output is None and json_output is None:
        lines.append("  rerun with --markdown-output or --json-output to save evidence")
    return "\n".join(lines) + "\n"


def _report_dict(report: SmokeReport, secret_values: list[str]) -> dict[str, Any]:
    raw = asdict(report)
    raw["server_url"] = redact_text(raw["server_url"], secret_values)
    for result in raw["results"]:
        result["detail"] = redact_text(result["detail"], secret_values)
        result["remediation"] = redact_text(result["remediation"], secret_values)
    return raw


def _resolve_optional_secret_source(value: str | None, *, flag_name: str) -> str | None:
    if value is None:
        return None
    return cast(str, resolve_secret_source(value, flag_name=flag_name))


def resolve_smoke_secret_args(args: argparse.Namespace) -> argparse.Namespace:
    """Resolve smoke-gate secret-source refs into an isolated args namespace."""

    resolved = argparse.Namespace(**vars(args))
    resolved.team_a_token = resolve_secret_source(
        args.team_a_token,
        flag_name="--team-a-token",
    )
    resolved.team_b_token = resolve_secret_source(
        args.team_b_token,
        flag_name="--team-b-token",
    )
    resolved.catalog_minio_access_key = _resolve_optional_secret_source(
        args.catalog_minio_access_key,
        flag_name="--catalog-minio-access-key",
    )
    resolved.catalog_minio_secret_key = _resolve_optional_secret_source(
        args.catalog_minio_secret_key,
        flag_name="--catalog-minio-secret-key",
    )
    resolved.secret_needle = [
        resolve_secret_source(value, flag_name="--secret-needle")
        for value in args.secret_needle
    ]
    return resolved


class SmokeClient:
    def __init__(self, server_url: str, *, max_scan_bytes: int = 1_000_000) -> None:
        self.server_url = server_url.rstrip("/")
        self.max_scan_bytes = max_scan_bytes
        self.evidence_chunks: list[str] = []
        self.response_bytes_scanned = 0

    def request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> HttpResponse:
        target = path
        url = f"{self.server_url}{path}"
        if params:
            query = urlencode(params)
            target = f"{target}?{query}"
            url = f"{url}?{query}"
        body: bytes | None = None
        headers = {"Accept": "application/json, text/plain, */*"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if json_body is not None:
            body = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(url, data=body, method=method, headers=headers)
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT_SEC) as response:
                payload = response.read(self.max_scan_bytes + 1)
                result = HttpResponse(
                    status_code=int(response.status),
                    headers={k.lower(): v for k, v in response.headers.items()},
                    body=payload[: self.max_scan_bytes],
                )
        except HTTPError as exc:
            payload = exc.read(self.max_scan_bytes + 1)
            result = HttpResponse(
                status_code=int(exc.code),
                headers={k.lower(): v for k, v in exc.headers.items()},
                body=payload[: self.max_scan_bytes],
            )
        except URLError as exc:
            reason = exc.reason
            if isinstance(reason, TimeoutError):
                result = self._request_exception_response(
                    method,
                    target,
                    reason,
                    timed_out=True,
                )
            else:
                result = self._request_exception_response(method, target, reason)
        except TimeoutError as exc:
            result = self._request_exception_response(
                method,
                target,
                exc,
                timed_out=True,
            )
        except Exception as exc:  # pragma: no cover - depends on urllib/http.client internals
            result = self._request_exception_response(method, target, exc)

        self.response_bytes_scanned += len(result.body)
        self.evidence_chunks.append(result.text)
        return result

    def _request_exception_response(
        self,
        method: str,
        target: str,
        exc: BaseException,
        *,
        timed_out: bool = False,
    ) -> HttpResponse:
        if timed_out:
            detail = (
                f"request failed: {method} {target} timed out after "
                f"{REQUEST_TIMEOUT_SEC:g}s: {type(exc).__name__}: {exc}"
            )
        else:
            detail = (
                f"request failed: {method} {target}: "
                f"{type(exc).__name__}: {exc}"
            )
        return HttpResponse(status_code=0, headers={}, body=detail.encode())


def _http_result(
    check_id: str,
    subsystem: str,
    response: HttpResponse,
    *,
    expected: int,
    pass_detail: str,
    fail_detail: str,
    remediation: str,
) -> CheckResult:
    if response.status_code == expected:
        return CheckResult(check_id, subsystem, "pass", pass_detail)
    return CheckResult(
        check_id,
        subsystem,
        "fail",
        f"{fail_detail}; got HTTP {response.status_code}: {response.text[:300]}",
        remediation,
    )


def _json_contains_id_result(
    check_id: str,
    subsystem: str,
    response: HttpResponse,
    *,
    expected_id: str,
    pass_detail: str,
    missing_detail: str,
    remediation: str,
) -> CheckResult:
    if response.status_code != 200:
        return CheckResult(
            check_id,
            subsystem,
            "fail",
            f"{missing_detail}; got HTTP {response.status_code}: {response.text[:300]}",
            remediation,
        )
    if _json_contains_id(response, expected_id):
        return CheckResult(check_id, subsystem, "pass", pass_detail)
    return CheckResult(
        check_id,
        subsystem,
        "fail",
        missing_detail,
        remediation,
    )


def _auth_whoami_result(label: str, response: HttpResponse) -> CheckResult:
    check_id = f"auth.{label}_whoami"
    if response.status_code != 200:
        return CheckResult(
            check_id,
            "auth",
            "fail",
            (
                f"{label} API token could not authenticate; got "
                f"HTTP {response.status_code}: {response.text[:300]}"
            ),
            "Create a scoped team API token and verify token expiry/scopes.",
        )
    try:
        body = response.json()
    except json.JSONDecodeError:
        return CheckResult(
            check_id,
            "auth",
            "fail",
            f"{label} whoami returned non-JSON evidence: {response.text[:300]}",
            "Fix /api/v1/auth/whoami before accepting smoke identity evidence.",
        )
    if not isinstance(body, dict):
        return CheckResult(
            check_id,
            "auth",
            "fail",
            f"{label} whoami returned a non-object JSON payload.",
            "Fix /api/v1/auth/whoami before accepting smoke identity evidence.",
        )

    credential_type = str(body.get("credential_type") or "(missing)")
    principal_type = str(body.get("principal_type") or "(missing)")
    username = str(body.get("username") or body.get("user_id") or "(none)")
    team = str(body.get("team_name") or body.get("team_id") or "(none)")
    role = str(body.get("role") or "(none)")
    is_platform_admin = bool(body.get("is_platform_admin")) or role == "platform_admin"
    has_user_identity = bool(body.get("user_id") or body.get("username"))
    principal_detail = (
        f"credential_type={credential_type}, principal_type={principal_type}, "
        f"user={username}, team={team}, role={role}, "
        f"is_platform_admin={is_platform_admin}"
    )
    if (
        credential_type == "user_owned_api_token"
        and principal_type in {"team", "user"}
        and has_user_identity
        and not is_platform_admin
    ):
        return CheckResult(
            check_id,
            "auth",
            "pass",
            f"{label} resolves as a non-admin user-owned API token: {principal_detail}.",
        )
    return CheckResult(
        check_id,
        "auth",
        "fail",
        (
            f"{label} token is not a non-admin user-owned API token: "
            f"{principal_detail}."
        ),
        (
            "Provision disposable non-admin Team A/Team B users through the "
            "registration, approval, password setup, and user API-token flow; "
            "use those non-admin user-owned API tokens for cross-team negative "
            "checks, and revoke them after smoke. Do not use legacy team tokens "
            "or platform_admin tokens."
        ),
    )


def _skip(check_id: str, subsystem: str, detail: str) -> CheckResult:
    return CheckResult(check_id, subsystem, "skip", detail, "Provide the required smoke input.")


def _json_contains_id(response: HttpResponse, expected_id: str) -> bool:
    try:
        return expected_id in json.dumps(response.json())
    except json.JSONDecodeError:
        return False


def _top_level_object_fragment(text: str, field: str) -> str | None:
    target = json.dumps(field)
    depth = 0
    in_string = False
    escaped = False
    i = 0
    while i < len(text):
        char = text[i]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            i += 1
            continue

        if char == '"':
            if depth == 1 and text.startswith(target, i):
                j = i + len(target)
                while j < len(text) and text[j].isspace():
                    j += 1
                if j >= len(text) or text[j] != ":":
                    in_string = True
                    i += 1
                    continue
                j += 1
                while j < len(text) and text[j].isspace():
                    j += 1
                if j < len(text) and text[j] == "{":
                    return _balanced_json_object(text, j)
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
        i += 1
    return None


def _balanced_json_object(text: str, start: int) -> str | None:
    depth = 0
    in_string = False
    escaped = False
    for i in range(start, len(text)):
        char = text[i]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def _json_has_owner_team(response: HttpResponse) -> bool:
    try:
        owner = response.json().get("owner_team")
    except (AttributeError, json.JSONDecodeError):
        fragment = _top_level_object_fragment(response.text, "owner_team")
        if fragment is None:
            return False
        try:
            owner = json.loads(fragment)
        except json.JSONDecodeError:
            return False
    return isinstance(owner, dict) and bool(owner.get("id") or owner.get("name"))


def _claimed_without_started_count(response: HttpResponse) -> int | None:
    try:
        body = response.json()
    except json.JSONDecodeError:
        return None
    if not isinstance(body, dict):
        return None
    debug_evidence = body.get("debug_evidence")
    if not isinstance(debug_evidence, dict):
        return None
    trials = debug_evidence.get("trials")
    if not isinstance(trials, dict):
        return None
    summary = trials.get("summary")
    if not isinstance(summary, dict):
        return None
    raw = summary.get("claimed_without_started")
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str):
        try:
            return int(raw)
        except ValueError:
            return None
    return None


def _terminal_worker_pool_counts(response: HttpResponse) -> dict[str, int] | None:
    try:
        body = response.json()
    except json.JSONDecodeError:
        return None
    if not isinstance(body, dict):
        return None
    debug_evidence = body.get("debug_evidence")
    if not isinstance(debug_evidence, dict):
        return None
    trials = debug_evidence.get("trials")
    if not isinstance(trials, dict):
        return None
    worker_pools = trials.get("worker_pools")
    if not isinstance(worker_pools, dict):
        return None
    terminal = worker_pools.get("terminal")
    if not isinstance(terminal, dict):
        return None
    counts: dict[str, int] = {}
    for raw_pool, raw_count in terminal.items():
        if not isinstance(raw_pool, str) or not raw_pool.strip():
            return None
        if isinstance(raw_count, bool):
            return None
        if isinstance(raw_count, int):
            count = raw_count
        elif isinstance(raw_count, str):
            try:
                count = int(raw_count)
            except ValueError:
                return None
        else:
            return None
        counts[raw_pool.strip()] = count
    return counts


def _worker_pool_coverage_result(
    response: HttpResponse,
    required_worker_pools: list[str],
) -> CheckResult:
    required = [pool.strip() for pool in required_worker_pools if pool.strip()]
    if not required:
        return _skip(
            "runs.worker_pool_coverage",
            "runs",
            "--required-worker-pool was not provided.",
        )
    if response.status_code != 200:
        return CheckResult(
            "runs.worker_pool_coverage",
            "runs",
            "fail",
            (
                "Could not inspect worker-pool coverage; got "
                f"HTTP {response.status_code}: {response.text[:300]}"
            ),
            "Fix batch debug evidence loading before accepting release-gate coverage.",
        )
    terminal_counts = _terminal_worker_pool_counts(response)
    if terminal_counts is None:
        return CheckResult(
            "runs.worker_pool_coverage",
            "runs",
            "fail",
            "Batch debug evidence did not include trials.worker_pools.terminal.",
            (
                "Return terminal trial counts by worker pool so release gates "
                "can assert native execution coverage without direct DB joins."
            ),
        )
    missing = [pool for pool in required if terminal_counts.get(pool, 0) <= 0]
    counts_detail = ", ".join(
        f"{pool}={terminal_counts[pool]}" for pool in sorted(terminal_counts)
    ) or "none"
    if missing:
        return CheckResult(
            "runs.worker_pool_coverage",
            "runs",
            "fail",
            (
                "Missing terminal trials for required worker pool(s): "
                f"{', '.join(missing)}. Observed terminal coverage: {counts_detail}."
            ),
            (
                "Rerun with enough compatible work or a deterministic coverage "
                "constraint until the required worker pools all have terminal trials."
            ),
        )
    return CheckResult(
        "runs.worker_pool_coverage",
        "runs",
        "pass",
        (
            "Required worker-pool coverage satisfied. "
            f"Observed terminal coverage: {counts_detail}."
        ),
    )


def _json_has_provenance(response: HttpResponse, *, batch_id: str | None = None) -> bool:
    try:
        body = response.json()
    except json.JSONDecodeError:
        return False
    provenance = body.get("source_provenance")
    if not isinstance(provenance, list) or not provenance:
        return False
    if batch_id is not None and body.get("cloned_from_batch_id") != batch_id:
        return False
    return True


def _json_model_items(response: HttpResponse) -> list[dict[str, Any]]:
    try:
        body = response.json()
    except json.JSONDecodeError:
        return []
    if isinstance(body, list):
        return [item for item in body if isinstance(item, dict)]
    if isinstance(body, dict):
        for key in ("items", "models"):
            value = body.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _json_provider_connection_names(response: HttpResponse) -> list[str]:
    names: list[str] = []
    for item in _json_items(response):
        name = item.get("name")
        if isinstance(name, str) and name:
            names.append(name)
    return sorted(names)


def _model_catalog_contains(
    models: list[dict[str, Any]],
    *,
    provider: str | None,
    name: str,
) -> bool:
    for item in models:
        if item.get("name") != name:
            continue
        if provider is not None and item.get("provider") != provider:
            continue
        return True
    return False


def _sample_model_catalog(models: list[dict[str, Any]], *, limit: int = 5) -> str:
    sample: list[str] = []
    for item in models[:limit]:
        provider = item.get("provider")
        name = item.get("name")
        if isinstance(provider, str) and isinstance(name, str):
            sample.append(f"{provider}/{name}")
        elif isinstance(name, str):
            sample.append(name)
    return ", ".join(sample) if sample else "(none)"


def _json_items(response: HttpResponse) -> list[dict[str, Any]]:
    try:
        body = response.json()
    except json.JSONDecodeError:
        return []
    if not isinstance(body, dict):
        return []
    items = body.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def _parse_s3_uri(source: str | None) -> tuple[str, str] | None:
    if source is None or not source.startswith("s3://"):
        return None
    rest = source[len("s3://"):]
    if "/" not in rest:
        return None
    bucket, prefix = rest.split("/", 1)
    if not bucket or not prefix:
        return None
    return bucket, prefix


def _s3_prefix_has_objects(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
    bucket: str,
    prefix: str,
) -> bool:
    client = _s3_client(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return bool(response.get("KeyCount", 0) or response.get("Contents"))


def _s3_client(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
    max_pool_connections: int = 10,
) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            signature_version="s3v4",
            max_pool_connections=max_pool_connections,
        ),
    )


def _s3_put_delete_probes(
    *,
    endpoint_url: str,
    access_key: str,
    secret_key: str,
    region: str,
    bucket: str,
    prefix: str,
    count: int,
    concurrency: int,
) -> list[str]:
    count = max(1, count)
    concurrency = max(1, min(concurrency, count))
    key_prefix = prefix.strip("/")
    keys = [
        f"{key_prefix}/probe-{uuid4().hex}.txt"
        if key_prefix
        else f"probe-{uuid4().hex}.txt"
        for _ in range(count)
    ]
    client = _s3_client(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        max_pool_connections=max(10, concurrency),
    )

    def _put_delete(key: str) -> str:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=b"loom staging minio write probe\n",
        )
        client.delete_object(Bucket=bucket, Key=key)
        return key

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_put_delete, key) for key in keys]
        return [future.result() for future in as_completed(futures)]


def _s3_error_summary(exc: Exception) -> str:
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        code = str(error.get("Code") or "ClientError")
        message = str(error.get("Message") or exc)
        return f"{code}: {message}"
    return f"{type(exc).__name__}: {exc}"


def run_smoke(args: argparse.Namespace) -> SmokeReport:
    client = SmokeClient(
        args.server_url,
        max_scan_bytes=args.max_response_scan_bytes,
    )
    results: list[CheckResult] = []

    if args.object_store_write_check_only:
        _append_object_store_write_probe(args, results)
        return SmokeReport(
            server_url=args.server_url,
            results=results,
            response_bytes_scanned=client.response_bytes_scanned,
        )

    initial_service_pod_restart_snapshot = _service_pod_restart_snapshot(args)

    health = client.request("GET", "/api/v1/health")
    results.append(_http_result(
        "http.health",
        "public-api",
        health,
        expected=200,
        pass_detail="Public service health endpoint returned 200.",
        fail_detail="Public service health endpoint is not reachable",
        remediation="Check ingress, TLS certificate, and loom-service readiness.",
    ))

    spa = client.request("GET", "/")
    results.append(_http_result(
        "spa.logged_out",
        "web",
        spa,
        expected=200,
        pass_detail="Logged-out SPA root returned 200.",
        fail_detail="Logged-out SPA root is not reachable",
        remediation="Check loom-web ingress backend and static asset deployment.",
    ))

    for label, token in (("team_a", args.team_a_token), ("team_b", args.team_b_token)):
        response = client.request("GET", "/api/v1/auth/whoami", token=token)
        results.append(_auth_whoami_result(label, response))

    providers = client.request("GET", "/api/v1/provider-connections", token=args.team_a_token)
    provider_names = _json_provider_connection_names(providers)
    provider_ok = providers.status_code == 200 and bool(provider_names)
    if provider_ok and args.provider_connection_name:
        provider_ok = args.provider_connection_name in provider_names
    provider_sample = ", ".join(provider_names[:5]) if provider_names else "(none)"
    results.append(CheckResult(
        "providers.list",
        "providers",
        "pass" if provider_ok else "fail",
        (
            f"Team A can list provider connections: {provider_sample}."
            if provider_ok
            else (
                f"Provider list returned HTTP {providers.status_code} but "
                f"{'no provider connections' if not provider_names else 'missing named provider ' + repr(args.provider_connection_name)}; "
                f"available: {provider_sample}."
            )
        ),
        "" if provider_ok else "Create/test the staging provider connection before the gate.",
    ))

    models = client.request("GET", "/api/v1/models", token=args.team_a_token)
    model_items = _json_model_items(models)
    models_ok = models.status_code == 200 and bool(model_items)
    if models_ok and args.provider_model_name:
        models_ok = _model_catalog_contains(
            model_items,
            provider=args.provider_model_provider,
            name=args.provider_model_name,
        )
    model_sample = _sample_model_catalog(model_items)
    expected_model_detail = ""
    if args.provider_model_name:
        expected = (
            f"{args.provider_model_provider}/{args.provider_model_name}"
            if args.provider_model_provider
            else args.provider_model_name
        )
        expected_model_detail = f" including expected {expected}"
    results.append(CheckResult(
        "providers.models",
        "providers",
        "pass" if models_ok else "fail",
        (
            (
                f"Team A model discovery surface returned {len(model_items)} models"
                f"{expected_model_detail}; "
                f"sample: {model_sample}."
            )
            if models_ok
            else (
                f"Model discovery returned HTTP {models.status_code}, "
                f"{len(model_items)} models, sample: {model_sample}; "
                f"expected provider={args.provider_model_provider!r} "
                f"model={args.provider_model_name!r}."
            )
        ),
        "" if models_ok else "Refresh provider models or resolve upstream provider entitlement.",
    ))

    _append_agent_catalog_checks(client, args, results)
    _append_benchmark_catalog_checks(client, args, results)
    _append_object_store_write_probe(args, results)

    if args.batch_id:
        batch = client.request("GET", f"/api/v1/batches/{args.batch_id}", token=args.team_a_token)
        results.append(_http_result(
            "runs.batch_detail",
            "runs",
            batch,
            expected=200,
            pass_detail="Owner team can read the completed batch detail.",
            fail_detail="Owner team cannot read the completed batch detail",
            remediation="Verify the batch id belongs to team A and has not been deleted.",
        ))
        batch_debug = client.request(
            "GET",
            f"/api/v1/batches/{args.batch_id}",
            token=args.team_a_token,
            params={"include_debug": "true"},
        )
        claimed_without_started = _claimed_without_started_count(batch_debug)
        if batch_debug.status_code != 200:
            results.append(CheckResult(
                "runs.claimed_without_started",
                "runs",
                "fail",
                (
                    "Could not inspect batch debug evidence for "
                    f"claimed_without_started; got HTTP {batch_debug.status_code}: "
                    f"{batch_debug.text[:300]}"
                ),
                "Fix batch debug evidence loading before accepting staging stress runs.",
            ))
        elif claimed_without_started is None:
            results.append(CheckResult(
                "runs.claimed_without_started",
                "runs",
                "fail",
                "Batch debug evidence did not include trials.summary.claimed_without_started.",
                (
                    "Return claimed_without_started in batch debug evidence so "
                    "release gates can distinguish active work from orphaned claims."
                ),
            ))
        elif claimed_without_started == 0:
            results.append(CheckResult(
                "runs.claimed_without_started",
                "runs",
                "pass",
                "Batch debug evidence reports claimed_without_started=0.",
            ))
        else:
            results.append(CheckResult(
                "runs.claimed_without_started",
                "runs",
                "fail",
                (
                    "Batch debug evidence reports "
                    f"claimed_without_started={claimed_without_started}."
                ),
                (
                    "Wait for crash-detector reclaim to drain stale claims, then "
                    "inspect worker logs and rerun the smoke gate before release."
                ),
            ))
        results.append(
            _worker_pool_coverage_result(batch_debug, args.required_worker_pool),
        )
    else:
        results.append(_skip("runs.batch_detail", "runs", "--batch-id was not provided."))
        results.append(_skip(
            "runs.claimed_without_started",
            "runs",
            "--batch-id was not provided.",
        ))
        results.append(_skip(
            "runs.worker_pool_coverage",
            "runs",
            "--batch-id was not provided.",
        ))

    if args.trial_id:
        trial = client.request("GET", f"/api/v1/trials/{args.trial_id}", token=args.team_a_token)
        results.append(_http_result(
            "runs.trial_detail",
            "runs",
            trial,
            expected=200,
            pass_detail="Owner team can read trial detail.",
            fail_detail="Owner team cannot read trial detail",
            remediation="Verify the trial id belongs to team A and reached a terminal state.",
        ))
        for check_id, path, label in (
            (
                "artifacts.owner_atif_download",
                f"/api/v1/trials/{args.trial_id}/atif",
                "ATIF",
            ),
            (
                "artifacts.owner_trajectory_download",
                f"/api/v1/trials/{args.trial_id}/trajectory/download",
                "trajectory JSONL",
            ),
        ):
            response = client.request("GET", path, token=args.team_a_token)
            results.append(_http_result(
                check_id,
                "artifacts",
                response,
                expected=200,
                pass_detail=f"Owner team can download {label} through the service.",
                fail_detail=f"Owner team cannot download {label}",
                remediation="Check trial artifact readiness and service-proxy download routes.",
            ))
    else:
        for check_id in (
            "runs.trial_detail",
            "artifacts.owner_atif_download",
            "artifacts.owner_trajectory_download",
        ):
            results.append(_skip(check_id, "runs", "--trial-id was not provided."))

    if args.batch_id:
        mine = client.request("GET", "/api/v1/run-library/batches", token=args.team_a_token)
        results.append(_json_contains_id_result(
            "library.my_team_contains_run",
            "run-library",
            mine,
            expected_id=args.batch_id,
            pass_detail="Completed run appears in Team A My team Run Library.",
            missing_detail="Completed run was missing from Team A My team Run Library",
            remediation="Check batch terminal state and Run Library visibility fields.",
        ))

        all_teams = client.request(
            "GET",
            "/api/v1/run-library/batches",
            token=args.team_b_token,
            params={"scope": "all"},
        )
        results.append(_json_contains_id_result(
            "library.all_teams_contains_run",
            "run-library",
            all_teams,
            expected_id=args.batch_id,
            pass_detail="Completed run appears in Team B All teams Run Library.",
            missing_detail="Completed run was missing from Team B All teams Run Library",
            remediation="Check org visibility, share_status, and terminal batch state.",
        ))

        library_detail = client.request(
            "GET",
            f"/api/v1/run-library/batches/{args.batch_id}",
            token=args.team_b_token,
        )
        owner_label_ok = library_detail.status_code == 200 and _json_has_owner_team(
            library_detail,
        )
        results.append(CheckResult(
            "library.owner_team_label",
            "run-library",
            "pass" if owner_label_ok else "fail",
            (
                "Run Library detail includes an owner-team label."
                if owner_label_ok
                else "Run Library detail did not include an owner-team label."
            ),
            "" if owner_label_ok else "Return owner_team metadata from Run Library detail.",
        ))
    else:
        for check_id in (
            "library.my_team_contains_run",
            "library.all_teams_contains_run",
            "library.owner_team_label",
        ):
            results.append(_skip(check_id, "run-library", "--batch-id was not provided."))

    _append_artifact_checks(client, args, results)
    _append_mutation_checks(client, args, results)

    secret_needles = [args.team_a_token, args.team_b_token, *args.secret_needle]
    leak_scan = scan_evidence_text(
        "\n".join(client.evidence_chunks),
        secret_needles=secret_needles,
        internal_url_needles=args.internal_url_needle,
    )
    results.append(leak_scan)
    final_service_pod_restart_snapshot = (
        initial_service_pod_restart_snapshot
        if initial_service_pod_restart_snapshot.status == "skip"
        else _service_pod_restart_snapshot(args)
    )
    results.append(_service_pod_restart_result(
        args,
        initial=initial_service_pod_restart_snapshot,
        final=final_service_pod_restart_snapshot,
    ))

    return SmokeReport(
        server_url=args.server_url,
        results=results,
        response_bytes_scanned=client.response_bytes_scanned,
    )


def _append_agent_catalog_checks(
    client: SmokeClient,
    args: argparse.Namespace,
    results: list[CheckResult],
) -> None:
    agents = client.request("GET", "/api/v1/agents", token=args.team_a_token)
    items = _json_items(agents)
    ready = [
        item for item in items
        if item.get("service_mode_ready", True) is not False
    ]
    if agents.status_code == 200 and ready:
        names = sorted(str(item.get("name", "<unnamed>")) for item in ready)
        results.append(CheckResult(
            "agents.ready_catalog",
            "agents",
            "pass",
            f"{len(ready)} ready agent(s) returned by /api/v1/agents: {', '.join(names[:10])}.",
        ))
    else:
        results.append(CheckResult(
            "agents.ready_catalog",
            "agents",
            "fail",
            (
                "No ready agents were returned by /api/v1/agents."
                if agents.status_code == 200
                else f"/api/v1/agents returned HTTP {agents.status_code}."
            ),
            (
                "Run the staging agent catalog provisioning step, then "
                "rerun the smoke gate before New Batch/manual canary testing."
            ),
        ))


def _append_benchmark_catalog_checks(
    client: SmokeClient,
    args: argparse.Namespace,
    results: list[CheckResult],
) -> None:
    benchmarks = client.request(
        "GET",
        "/api/v1/benchmarks",
        token=args.team_a_token,
        params={"limit": "200"},
    )
    items = _json_items(benchmarks)
    runnable = [
        item for item in items
        if int(item.get("task_count") or 0) > 0
        and item.get("readiness_state", "runnable") == "runnable"
    ]
    if benchmarks.status_code == 200 and runnable:
        total_tasks = sum(int(item.get("task_count") or 0) for item in runnable)
        results.append(CheckResult(
            "benchmarks.runnable_catalog",
            "benchmarks",
            "pass",
            f"{len(runnable)} runnable benchmark(s) expose {total_tasks} runnable task(s).",
        ))
    else:
        results.append(CheckResult(
            "benchmarks.runnable_catalog",
            "benchmarks",
            "fail",
            (
                "No runnable benchmarks were returned by /api/v1/benchmarks."
                if benchmarks.status_code == 200
                else f"/api/v1/benchmarks returned HTTP {benchmarks.status_code}."
            ),
            (
                "Run the staging catalog provisioning step, then rerun "
                "the smoke gate before manual testing."
            ),
        ))

    missing_inputs = [
        name for name, value in (
            ("--catalog-minio-endpoint", args.catalog_minio_endpoint),
            ("--catalog-minio-access-key", args.catalog_minio_access_key),
            ("--catalog-minio-secret-key", args.catalog_minio_secret_key),
        ) if not value
    ]
    if missing_inputs:
        results.append(CheckResult(
            "benchmarks.ready_bundle_objects",
            "benchmarks",
            "skip",
            f"{', '.join(missing_inputs)} not provided.",
            "Pass catalog object-store credentials for release smoke.",
        ))
        return
    if not runnable:
        results.append(CheckResult(
            "benchmarks.ready_bundle_objects",
            "benchmarks",
            "fail",
            "No runnable benchmark tasks were available for bundle checks.",
            "Provision the benchmark catalog and object bundles first.",
        ))
        return

    checked = 0
    missing: list[str] = []
    skipped_non_s3 = 0
    for benchmark in runnable[: args.catalog_bundle_benchmark_limit]:
        remaining = args.catalog_bundle_task_limit
        cursor: str | None = None
        while remaining > 0:
            params = {
                "benchmark_id": str(benchmark["id"]),
                "limit": str(min(200, remaining)),
            }
            if cursor:
                params["cursor"] = cursor
            tasks = client.request(
                "GET",
                "/api/v1/tasks",
                token=args.team_a_token,
                params=params,
            )
            if tasks.status_code != 200:
                missing.append(f"{benchmark['id']}: tasks API HTTP {tasks.status_code}")
                break
            try:
                body = tasks.json()
            except json.JSONDecodeError:
                missing.append(f"{benchmark['id']}: tasks API returned non-JSON")
                break
            items = _json_items(tasks)
            if not items:
                break
            for task in items:
                parsed = _parse_s3_uri(task.get("source"))
                if parsed is None:
                    skipped_non_s3 += 1
                    continue
                bucket, prefix = parsed
                checked += 1
                try:
                    exists = _s3_prefix_has_objects(
                        endpoint_url=args.catalog_minio_endpoint,
                        access_key=args.catalog_minio_access_key,
                        secret_key=args.catalog_minio_secret_key,
                        region=args.catalog_minio_region,
                        bucket=bucket,
                        prefix=prefix,
                    )
                except Exception as exc:  # pragma: no cover - exercised by live smoke
                    missing.append(f"{task.get('id', '<unknown>')}: object check failed: {exc}")
                    continue
                if not exists:
                    missing.append(f"{task.get('id', '<unknown>')}: missing {bucket}/{prefix}")
            cursor = body.get("next_cursor") if isinstance(body, dict) else None
            if not cursor:
                break
            remaining -= len(items)

    if missing:
        results.append(CheckResult(
            "benchmarks.ready_bundle_objects",
            "benchmarks",
            "fail",
            "Missing ready benchmark bundle object(s): " + "; ".join(missing[:10]),
            (
                "Rerun catalog provisioning and verify the target "
                "loom-benchmarks bucket before release."
            ),
        ))
        return
    if checked == 0:
        detail = "No s3:// task bundle sources were found in the sampled ready catalog."
        if skipped_non_s3:
            detail += f" Skipped {skipped_non_s3} non-S3 task source(s)."
        results.append(CheckResult(
            "benchmarks.ready_bundle_objects",
            "benchmarks",
            "pass",
            detail,
        ))
        return
    results.append(CheckResult(
        "benchmarks.ready_bundle_objects",
        "benchmarks",
        "pass",
        f"Verified {checked} sampled ready task bundle prefix(es) contain objects.",
    ))


def _append_object_store_write_probe(
    args: argparse.Namespace,
    results: list[CheckResult],
) -> None:
    if not (args.object_store_write_check or args.object_store_write_check_only):
        results.append(CheckResult(
            "object_store.minio_write_probe",
            "object-store",
            "skip",
            "--object-store-write-check was not provided.",
            (
                "Pass --object-store-write-check for full release smoke, or "
                "--object-store-write-check-only for a pre-canary storage gate."
            ),
        ))
        return

    missing_inputs = [
        name for name, value in (
            ("--catalog-minio-endpoint", args.catalog_minio_endpoint),
            ("--catalog-minio-access-key", args.catalog_minio_access_key),
            ("--catalog-minio-secret-key", args.catalog_minio_secret_key),
        ) if not value
    ]
    if missing_inputs:
        results.append(CheckResult(
            "object_store.minio_write_probe",
            "object-store",
            "skip",
            f"{', '.join(missing_inputs)} not provided.",
            "Pass MinIO object-store credentials before submitting canary trials.",
        ))
        return

    bucket = args.object_store_write_check_bucket
    count = args.object_store_write_check_count
    concurrency = args.object_store_write_check_concurrency
    try:
        keys = _s3_put_delete_probes(
            endpoint_url=args.catalog_minio_endpoint,
            access_key=args.catalog_minio_access_key,
            secret_key=args.catalog_minio_secret_key,
            region=args.catalog_minio_region,
            bucket=bucket,
            prefix=args.object_store_write_check_prefix,
            count=count,
            concurrency=concurrency,
        )
    except Exception as exc:  # pragma: no cover - live gate covers real MinIO failures
        results.append(CheckResult(
            "object_store.minio_write_probe",
            "object-store",
            "fail",
            f"MinIO write/delete probe failed for bucket {bucket}: {_s3_error_summary(exc)}",
            (
                "Reclaim or provision object-store free space, verify the MinIO "
                "PVC/host disk, credentials, and runtime bucket, then rerun before "
                "trial execution."
            ),
        ))
        return

    results.append(CheckResult(
        "object_store.minio_write_probe",
        "object-store",
        "pass",
        (
            f"Wrote and deleted {len(keys)} probe object(s) in bucket {bucket} "
            f"with concurrency={max(1, min(concurrency, count))}; "
            f"sample={bucket}/{keys[0]}."
        ),
    ))


def _service_pod_restart_snapshot(args: argparse.Namespace) -> ServicePodRestartSnapshot:
    if not args.k8s_namespace:
        return ServicePodRestartSnapshot(
            "skip",
            "--k8s-namespace was not provided.",
            (
                "Pass --k8s-namespace during release/full100 gates so service "
                "pod restarts and OOMKills are captured in evidence."
            ),
        )

    cmd = [
        args.kubectl_bin,
        "-n",
        args.k8s_namespace,
        "get",
        "pods",
        "-l",
        args.service_pod_selector,
        "-o",
        "json",
    ]
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=args.k8s_command_timeout_sec,
        )
    except subprocess.TimeoutExpired:
        return ServicePodRestartSnapshot(
            "fail",
            f"kubectl timed out checking service pods in namespace {args.k8s_namespace}.",
            "Verify kubeconfig access and inspect loom-service pod status manually.",
        )
    except OSError as exc:
        return ServicePodRestartSnapshot(
            "fail",
            f"kubectl failed to run: {type(exc).__name__}: {exc}",
            "Install kubectl or provide --kubectl-bin for release smoke evidence.",
        )

    if proc.returncode != 0:
        return ServicePodRestartSnapshot(
            "fail",
            (
                f"kubectl returned {proc.returncode} checking service pods: "
                f"{proc.stderr[:300] or proc.stdout[:300]}"
            ),
            "Verify kubeconfig context, namespace, and service pod selector.",
        )

    try:
        payload = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        return ServicePodRestartSnapshot(
            "fail",
            f"kubectl returned non-JSON pod status: {exc}",
            "Rerun kubectl get pods -o json and inspect release gate tooling.",
        )

    items = payload.get("items") if isinstance(payload, dict) else None
    pods = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []
    if not pods:
        return ServicePodRestartSnapshot(
            "fail",
            (
                "No service pods matched selector "
                f"{args.service_pod_selector!r} in namespace {args.k8s_namespace}."
            ),
            "Check the service Deployment label selector before accepting release evidence.",
        )

    containers: list[ServiceContainerRestartEvidence] = []
    for pod in pods:
        metadata = pod.get("metadata") if isinstance(pod.get("metadata"), dict) else {}
        pod_name = str(metadata.get("name") or "<unknown-pod>")
        status = pod.get("status") if isinstance(pod.get("status"), dict) else {}
        raw_statuses = status.get("containerStatuses")
        statuses = (
            [item for item in raw_statuses if isinstance(item, dict)]
            if isinstance(raw_statuses, list)
            else []
        )
        for container in statuses:
            container_name = str(container.get("name") or "<unknown-container>")
            if (
                args.service_container_name
                and container_name != args.service_container_name
            ):
                continue
            try:
                restart_count = int(container.get("restartCount") or 0)
            except (TypeError, ValueError):
                restart_count = 0
            last_state = (
                container.get("lastState")
                if isinstance(container.get("lastState"), dict)
                else {}
            )
            terminated = (
                last_state.get("terminated")
                if isinstance(last_state.get("terminated"), dict)
                else {}
            )
            last_reason = str(terminated.get("reason") or "")
            exit_code = terminated.get("exitCode")
            containers.append(ServiceContainerRestartEvidence(
                pod_name=pod_name,
                container_name=container_name,
                restart_count=restart_count,
                last_reason=last_reason,
                exit_code=exit_code,
            ))

    if not containers:
        return ServicePodRestartSnapshot(
            "fail",
            (
                f"No container named {args.service_container_name!r} was found "
                f"under selector {args.service_pod_selector!r}."
            ),
            "Check --service-container-name or the service pod template.",
        )

    return ServicePodRestartSnapshot(
        "ok",
        f"Collected {len(containers)} service container restart status record(s).",
        containers=tuple(containers),
    )


def _service_pod_restart_result(
    args: argparse.Namespace,
    *,
    initial: ServicePodRestartSnapshot,
    final: ServicePodRestartSnapshot,
) -> CheckResult:
    if initial.status == "skip":
        return CheckResult(
            "service.no_oom_restarts",
            "public-api",
            "skip",
            initial.detail,
            initial.remediation,
        )
    if initial.status == "fail":
        return CheckResult(
            "service.no_oom_restarts",
            "public-api",
            "fail",
            "Initial service pod restart/OOM check failed before route probes: "
            f"{initial.detail}",
            initial.remediation,
        )
    if final.status != "ok":
        return CheckResult(
            "service.no_oom_restarts",
            "public-api",
            "fail",
            "Final service pod restart/OOM check failed after route probes: "
            f"{final.detail}",
            final.remediation,
        )

    initial_by_container = {
        container.key: container
        for container in initial.containers
    }
    offenders: list[str] = []
    for container in final.containers:
        reasons: list[str] = []
        baseline = initial_by_container.get(container.key)
        if baseline is not None and container.restart_count > baseline.restart_count:
            reasons.append(
                "restartCount increased during smoke "
                f"{baseline.restart_count} -> {container.restart_count}",
            )
        if container.last_reason == "OOMKilled":
            reasons.append("lastState=OOMKilled")
        if container.restart_count > args.service_restart_max_count:
            reasons.append(
                "restartCount "
                f"{container.restart_count} exceeds allowed "
                f"{args.service_restart_max_count}",
            )
        if reasons:
            detail = (
                f"{container.pod_name}/{container.container_name}: "
                + ", ".join(reasons)
            )
            if container.exit_code is not None:
                detail += f", exitCode={container.exit_code}"
            offenders.append(detail)

    if offenders:
        return CheckResult(
            "service.no_oom_restarts",
            "public-api",
            "fail",
            (
                "Final service pod restart/OOM evidence after route probes: "
                + "; ".join(offenders[:10])
            ),
            (
                "Treat route-triggered service restart/OOM/502 as the service "
                "stability root cause. Inspect loom-service memory, the heavy "
                "route probe load, and previous pod logs before investigating "
                "secondary route/security rows."
            ),
        )

    initial_summary = _service_restart_count_summary(initial.containers)
    final_summary = _service_restart_count_summary(final.containers)
    return CheckResult(
        "service.no_oom_restarts",
        "public-api",
        "pass",
        (
            "Checked service containers before and after HTTP/API route probes; "
            "no OOMKilled lastState, no restartCount increase during smoke, "
            f"and final restartCount <= {args.service_restart_max_count}. "
            f"Initial: {initial_summary}. Final: {final_summary}."
        ),
    )


def _service_restart_count_summary(
    containers: tuple[ServiceContainerRestartEvidence, ...],
) -> str:
    return ", ".join(
        (
            f"{container.pod_name}/{container.container_name}="
            f"{container.restart_count}"
        )
        for container in containers[:10]
    )


def _append_artifact_checks(
    client: SmokeClient,
    args: argparse.Namespace,
    results: list[CheckResult],
) -> None:
    if args.trial_id and args.safe_artifact_key:
        params = {"key": args.safe_artifact_key}
        shared = client.request(
            "GET",
            f"/api/v1/run-library/trials/{args.trial_id}/artifacts/download",
            token=args.team_b_token,
            params=params,
        )
        shared_ok = shared.status_code == 200 and "location" not in shared.headers
        results.append(CheckResult(
            "library.cross_team_safe_download",
            "run-library",
            "pass" if shared_ok else "fail",
            (
                "Team B can download the safe shared artifact through Loom service."
                if shared_ok
                else f"Safe shared artifact download failed with HTTP {shared.status_code}."
            ),
            "" if shared_ok else "Check artifact share_status and service-proxy route.",
        ))

        direct = client.request(
            "GET",
            f"/api/v1/trials/{args.trial_id}/artifacts/download",
            token=args.team_b_token,
            params=params,
        )
        direct_ok = direct.status_code == 403
        results.append(CheckResult(
            "library.direct_cross_team_download_denied",
            "run-library",
            "pass" if direct_ok else "fail",
            (
                "Direct owner-team artifact route denies Team B."
                if direct_ok
                else f"Direct owner-team artifact route returned HTTP {direct.status_code}."
            ),
            "" if direct_ok else "Keep execution artifact routes current-team scoped.",
        ))
    else:
        for check_id in (
            "library.cross_team_safe_download",
            "library.direct_cross_team_download_denied",
        ):
            results.append(_skip(
                check_id,
                "run-library",
                "--trial-id and --safe-artifact-key are required.",
            ))

    if args.trial_id and args.blocked_artifact_key:
        blocked = client.request(
            "GET",
            f"/api/v1/run-library/trials/{args.trial_id}/artifacts/download",
            token=args.team_b_token,
            params={"key": args.blocked_artifact_key},
        )
        blocked_ok = blocked.status_code == 403
        results.append(CheckResult(
            "library.blocked_artifact_denied",
            "run-library",
            "pass" if blocked_ok else "fail",
            (
                "Blocked artifact is denied to Team B."
                if blocked_ok
                else f"Blocked artifact returned HTTP {blocked.status_code}."
            ),
            "" if blocked_ok else "Check artifact share_status=blocked enforcement.",
        ))
    else:
        results.append(_skip(
            "library.blocked_artifact_denied",
            "run-library",
            "--trial-id and --blocked-artifact-key are required.",
        ))

    if args.private_trial_id and args.private_artifact_key:
        private = client.request(
            "GET",
            f"/api/v1/run-library/trials/{args.private_trial_id}/artifacts/download",
            token=args.team_b_token,
            params={"key": args.private_artifact_key},
        )
        private_ok = private.status_code == 403
        results.append(CheckResult(
            "library.private_artifact_denied",
            "run-library",
            "pass" if private_ok else "fail",
            (
                "Private source artifact is denied to Team B."
                if private_ok
                else f"Private source artifact returned HTTP {private.status_code}."
            ),
            "" if private_ok else "Check parent batch visibility and Run Library read policy.",
        ))
    else:
        results.append(_skip(
            "library.private_artifact_denied",
            "run-library",
            "--private-trial-id and --private-artifact-key are required.",
        ))


def _append_mutation_checks(
    client: SmokeClient,
    args: argparse.Namespace,
    results: list[CheckResult],
) -> None:
    if not args.allow_mutating_checks:
        for check_id in (
            "library.clone_config",
            "library.reuse_artifact",
            "library.reuse_provenance",
            "runs.cross_team_mutation_denied",
        ):
            results.append(CheckResult(
                check_id,
                "run-library",
                "skip",
                "--allow-mutating-checks was not provided.",
                "Rerun against disposable staging data with --allow-mutating-checks.",
            ))
        return

    if args.batch_id:
        payload: dict[str, Any] = {"name": args.clone_name}
        if args.clone_provider_connection_id:
            payload["provider_connection_id"] = args.clone_provider_connection_id
        if args.clone_provider_model_id:
            payload["provider_model_id"] = args.clone_provider_model_id
        clone = client.request(
            "POST",
            f"/api/v1/run-library/batches/{args.batch_id}/clone-config",
            token=args.team_b_token,
            json_body=payload,
        )
        clone_ok = clone.status_code == 201 and _json_has_provenance(
            clone,
            batch_id=args.batch_id,
        )
        results.append(CheckResult(
            "library.clone_config",
            "run-library",
            "pass" if clone_ok else "fail",
            (
                "Team B can clone config into its own team with provenance."
                if clone_ok
                else f"Clone config failed or lacked provenance; HTTP {clone.status_code}."
            ),
            "" if clone_ok else "Check destination provider selection and provenance writeback.",
        ))

        mutation = client.request(
            "POST",
            f"/api/v1/batches/{args.batch_id}/cancel",
            token=args.team_b_token,
        )
        mutation_ok = mutation.status_code == 403
        results.append(CheckResult(
            "runs.cross_team_mutation_denied",
            "runs",
            "pass" if mutation_ok else "fail",
            (
                "Team B cannot mutate the original Team A run."
                if mutation_ok
                else f"Cross-team mutation returned HTTP {mutation.status_code}."
            ),
            "" if mutation_ok else "Keep batch mutation routes owner-team/admin scoped.",
        ))
    else:
        results.append(_skip("library.clone_config", "run-library", "--batch-id is required."))
        results.append(_skip(
            "runs.cross_team_mutation_denied",
            "runs",
            "--batch-id is required.",
        ))

    if args.trial_id and args.safe_artifact_key:
        payload = {"key": args.safe_artifact_key, "name": args.reuse_name}
        if args.reuse_provider_connection_id:
            payload["provider_connection_id"] = args.reuse_provider_connection_id
        if args.reuse_provider_model_id:
            payload["provider_model_id"] = args.reuse_provider_model_id
        reuse = client.request(
            "POST",
            f"/api/v1/run-library/trials/{args.trial_id}/artifacts/reuse",
            token=args.team_b_token,
            json_body=payload,
        )
        reuse_ok = reuse.status_code == 201
        provenance_ok = reuse_ok and _json_has_provenance(reuse)
        results.append(CheckResult(
            "library.reuse_artifact",
            "run-library",
            "pass" if reuse_ok else "fail",
            (
                "Team B can reuse the safe shared artifact."
                if reuse_ok
                else f"Reuse artifact failed with HTTP {reuse.status_code}."
            ),
            "" if reuse_ok else "Check artifact share_status and destination-team write scope.",
        ))
        results.append(CheckResult(
            "library.reuse_provenance",
            "run-library",
            "pass" if provenance_ok else "fail",
            (
                "Reused artifact response records source provenance."
                if provenance_ok
                else "Reuse response did not record source provenance."
            ),
            "" if provenance_ok else "Persist source_trial_id and source_artifact_key.",
        ))
    else:
        results.append(_skip(
            "library.reuse_artifact",
            "run-library",
            "--trial-id and --safe-artifact-key are required.",
        ))
        results.append(_skip(
            "library.reuse_provenance",
            "run-library",
            "--trial-id and --safe-artifact-key are required.",
        ))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-url", required=True, help="Public Loom URL, e.g. https://loom.example.com")
    parser.add_argument(
        "--team-a-token",
        required=True,
        help="Owner/source team API token source: env:VAR, file:PATH, or -.",
    )
    parser.add_argument(
        "--team-b-token",
        required=True,
        help="Second team API token source: env:VAR, file:PATH, or -.",
    )
    parser.add_argument("--provider-connection-name", default=None)
    parser.add_argument(
        "--provider-model-provider",
        default=None,
        help=(
            "Expected provider namespace in /api/v1/models, for example "
            "`yibuapi`. Requires --provider-model-name."
        ),
    )
    parser.add_argument(
        "--provider-model-name",
        default=None,
        help="Expected model id in /api/v1/models, for example `glm-5.1-thinking`.",
    )
    parser.add_argument("--batch-id", default=None, help="Completed Team A batch id.")
    parser.add_argument(
        "--required-worker-pool",
        action="append",
        default=[],
        help=(
            "Require at least one terminal trial in this worker pool for the "
            "batch debug-evidence coverage gate. May be passed more than once."
        ),
    )
    parser.add_argument("--trial-id", default=None, help="Succeeded Team A trial id.")
    parser.add_argument("--safe-artifact-key", default=None)
    parser.add_argument("--blocked-artifact-key", default=None)
    parser.add_argument("--private-trial-id", default=None)
    parser.add_argument("--private-artifact-key", default=None)
    parser.add_argument("--allow-mutating-checks", action="store_true")
    parser.add_argument("--clone-name", default="staging smoke clone")
    parser.add_argument("--reuse-name", default="staging smoke reuse")
    parser.add_argument("--clone-provider-connection-id", default=None)
    parser.add_argument("--clone-provider-model-id", default=None)
    parser.add_argument("--reuse-provider-connection-id", default=None)
    parser.add_argument("--reuse-provider-model-id", default=None)
    parser.add_argument(
        "--secret-needle",
        action="append",
        default=[],
        help="Additional secret needle source to scan for: env:VAR, file:PATH, or -.",
    )
    parser.add_argument("--internal-url-needle", action="append", default=[])
    parser.add_argument("--catalog-minio-endpoint", default=None)
    parser.add_argument(
        "--catalog-minio-access-key",
        default=None,
        help="Catalog MinIO access-key source: env:VAR, file:PATH, or -.",
    )
    parser.add_argument(
        "--catalog-minio-secret-key",
        default=None,
        help="Catalog MinIO secret-key source: env:VAR, file:PATH, or -.",
    )
    parser.add_argument("--catalog-minio-region", default="us-east-1")
    parser.add_argument("--catalog-bundle-benchmark-limit", type=int, default=20)
    parser.add_argument("--catalog-bundle-task-limit", type=int, default=200)
    parser.add_argument(
        "--object-store-write-check",
        action="store_true",
        help="Run an explicit write/delete object-store probe.",
    )
    parser.add_argument(
        "--object-store-write-check-only",
        action="store_true",
        help=(
            "Only run the write/delete object-store probe. Intended as a "
            "pre-canary storage gate."
        ),
    )
    parser.add_argument("--object-store-write-check-bucket", default="trajectories")
    parser.add_argument("--object-store-write-check-prefix", default="_ops/staging-smoke")
    parser.add_argument(
        "--object-store-write-check-count",
        type=int,
        default=1,
        help="Number of probe objects to write/delete for object-store release smoke.",
    )
    parser.add_argument(
        "--object-store-write-check-concurrency",
        type=int,
        default=1,
        help="Maximum concurrent object-store write/delete probes.",
    )
    parser.add_argument(
        "--k8s-namespace",
        default=None,
        help=(
            "Kubernetes namespace to inspect for loom-service pod restarts. "
            "When omitted, service.no_oom_restarts is skipped."
        ),
    )
    parser.add_argument("--kubectl-bin", default="kubectl")
    parser.add_argument("--service-pod-selector", default="app=loom-service")
    parser.add_argument("--service-container-name", default="loom-service")
    parser.add_argument(
        "--service-restart-max-count",
        type=int,
        default=0,
        help="Maximum allowed current service container restartCount.",
    )
    parser.add_argument(
        "--k8s-command-timeout-sec",
        type=float,
        default=10.0,
        help="Timeout for kubectl pod-status diagnostics.",
    )
    parser.add_argument("--max-response-scan-bytes", type=int, default=1_000_000)
    parser.add_argument("--fail-on-skip", action="store_true")
    parser.add_argument("--json-output", type=Path, default=None)
    parser.add_argument("--markdown-output", type=Path, default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        args = resolve_smoke_secret_args(args)
    except SecretSourceError as exc:
        parser.error(str(exc))
    report = run_smoke(args)
    secret_values = [
        args.team_a_token,
        args.team_b_token,
        args.catalog_minio_endpoint,
        args.catalog_minio_access_key,
        args.catalog_minio_secret_key,
        *args.secret_needle,
        *args.internal_url_needle,
    ]
    markdown = render_markdown(report, secret_values=secret_values)
    report_json = json.dumps(_report_dict(report, secret_values), indent=2, sort_keys=True)

    if args.markdown_output:
        args.markdown_output.write_text(markdown, encoding="utf-8")
    if args.json_output:
        args.json_output.write_text(report_json + "\n", encoding="utf-8")
    print(
        render_console_summary(
            report,
            markdown_output=args.markdown_output,
            json_output=args.json_output,
        ),
        end="",
    )

    has_fail = any(result.status == "fail" for result in report.results)
    has_skip = any(result.status == "skip" for result in report.results)
    if has_fail or (args.fail_on_skip and has_skip):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
