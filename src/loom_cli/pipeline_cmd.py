"""Team-safe CLI for the fixed official-Recipe Pipeline API.

This client deliberately exposes only the recipe-backed lifecycle.  In
particular it never accepts graph, provider, worker, image, network, or object
store controls and it downloads artifacts through Loom's authenticated route.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tarfile
import tempfile
import time
import unicodedata
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, NoReturn, cast
from urllib.parse import quote
from uuid import UUID, uuid4

import httpx
import zstandard

from loom.security.redaction import redact_mapping, redact_text
from loom_cli.server_client import NotLoggedInError, authed_client, require_logged_in

_IDEMPOTENCY_HEADER = "Idempotency-Key"
_IMPORT_KINDS = ("dataset", "policy", "mop_bank")
_BUDGET_FIELDS = frozenset(
    {
        "max_artifact_bytes",
        "max_attempts_total",
        "max_gpu_seconds",
        "max_provider_cost_usd",
        "max_stage_runs",
        "max_wall_seconds",
    }
)


class PipelineCliError(Exception):
    """A stable, redacted error suitable for both human and JSON output."""

    def __init__(self, *, status: int, reason_code: str, message: str) -> None:
        self.status = status
        self.reason_code = reason_code
        self.message = redact_text(message)
        super().__init__(self.message)

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "reason_code": self.reason_code,
            "message": self.message,
        }


def _fail(reason_code: str, message: str, *, status: int = 0) -> NoReturn:
    raise PipelineCliError(status=status, reason_code=reason_code, message=message)


def _json_object(value: str, *, option: str) -> dict[str, Any]:
    if not value.startswith("@") or len(value) == 1:
        _fail("invalid_cli_input", f"{option} must be @path/to/file.json")
    path = Path(value[1:])
    try:
        raw = path.read_text(encoding="utf-8")
        parsed = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError):
        _fail("invalid_json_file", f"{option} file could not be read as JSON")
    if not isinstance(parsed, dict):
        _fail("invalid_json_file", f"{option} must contain one JSON object")
    return cast(dict[str, Any], parsed)






def _strict_budget(value: str) -> dict[str, Any]:
    budget = _json_object(value, option="--budget")
    if set(budget) != _BUDGET_FIELDS:
        _fail("invalid_budget", "--budget has missing or extra fields")
    integer_fields = _BUDGET_FIELDS - {"max_provider_cost_usd"}
    for name in integer_fields:
        item = budget[name]
        if isinstance(item, bool) or not isinstance(item, int):
            _fail("invalid_budget", f"budget field {name} must be an integer")
        minimum = 0 if name in {"max_gpu_seconds"} else 1
        if item < minimum:
            _fail("invalid_budget", f"budget field {name} is below its minimum")
    provider_cost = budget["max_provider_cost_usd"]
    if not isinstance(provider_cost, str):
        _fail("invalid_budget", "budget field max_provider_cost_usd must be a string")
    return budget


def _uuid(value: str, *, label: str) -> str:
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError):
        _fail("invalid_uuid", f"{label} must be a UUID")
    canonical = str(parsed)
    if value != canonical:
        _fail("invalid_uuid", f"{label} must be a canonical lowercase UUID")
    return canonical


def _recipe(value: str) -> tuple[str, str]:
    if re.fullmatch(r"[a-z][a-z0-9-]{0,127}@[1-9][0-9]{0,9}", value) is None:
        _fail("invalid_recipe", "recipe must use NAME@VERSION")
    name, version = value.split("@", 1)
    if not name or not version or any(ch.isspace() for ch in value):
        _fail("invalid_recipe", "recipe must use non-empty NAME@VERSION")
    if unicodedata.normalize("NFC", value) != value:
        _fail("invalid_recipe", "recipe must be NFC-normalized")
    return name, version


def _idempotency_key(value: str) -> str:
    if not 1 <= len(value) <= 128 or value != value.strip():
        _fail(
            "invalid_idempotency_key",
            "idempotency key must be 1..128 characters with no surrounding whitespace",
        )
    if any(ord(ch) < 0x20 or ord(ch) > 0x7E for ch in value):
        _fail("invalid_idempotency_key", "idempotency key must contain printable ASCII only")
    return value


def _nfc_text(value: str, *, label: str, minimum: int, maximum: int) -> str:
    size = len(value.encode("utf-8"))
    if not minimum <= size <= maximum or unicodedata.normalize("NFC", value) != value:
        _fail("invalid_cli_input", f"{label} must be NFC UTF-8 with {minimum}..{maximum} bytes")
    return value


def _bindings(values: list[str], *, option: str) -> dict[str, str]:
    if len(values) > 128:
        _fail("invalid_binding", f"{option} accepts at most 128 bindings")
    result: dict[str, str] = {}
    for value in values:
        if value.count("=") != 1:
            _fail("invalid_binding", f"{option} must use NAME=ARTIFACT_UUID")
        name, artifact_id = value.split("=", 1)
        if re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) is None or name in result:
            _fail("invalid_binding", f"{option} names must be non-empty and unique")
        result[name] = _uuid(artifact_id, label=f"{option} {name}")
    return result


def _safe_response(response: httpx.Response, *, action: str) -> dict[str, Any]:
    if response.status_code // 100 != 2:
        reason = "http_error"
        message = f"server rejected request while trying to {action}"
        try:
            body = response.json()
        except Exception:
            body = None
        detail = body.get("detail") if isinstance(body, dict) else None
        if isinstance(detail, dict):
            raw_reason = detail.get("reason_code")
            raw_message = detail.get("message")
            if isinstance(raw_reason, str) and raw_reason:
                reason = raw_reason
            if isinstance(raw_message, str) and raw_message:
                message = raw_message
        elif isinstance(detail, str) and detail:
            message = detail
        raise PipelineCliError(status=response.status_code, reason_code=reason, message=message)
    if response.status_code == 204:
        return {}
    try:
        data = response.json()
    except Exception:
        _fail(
            "invalid_server_response",
            f"server returned invalid JSON while trying to {action}",
            status=response.status_code,
        )
    if not isinstance(data, dict):
        _fail(
            "invalid_server_response",
            f"server returned a non-object while trying to {action}",
            status=response.status_code,
        )
    return cast(dict[str, Any], data)


def _request_json(
    client: httpx.Client, method: str, path: str, *, action: str, **kwargs: Any
) -> dict[str, Any]:
    try:
        response = client.request(method, path, **kwargs)
    except httpx.HTTPError:
        _fail("request_failed", f"request failed while trying to {action}")
    return _safe_response(response, action=action)


def _json_mode(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "json_output", False))


def _emit(data: Mapping[str, Any], *, json_output: bool, heading: str | None = None) -> None:
    safe = cast(dict[str, Any], redact_mapping(dict(data)))
    if json_output:
        print(json.dumps(safe, indent=2, sort_keys=True))
        return
    if heading:
        print(heading)
    preferred = (
        "id",
        "pipeline_run_id",
        "run_id",
        "stage_run_id",
        "artifact_id",
        "import_id",
        "materialization_id",
        "display_name",
        "recipe",
        "state",
        "result",
        "reason",
        "terminal_cause",
        "retry_of_pipeline_run_id",
        "retry_from_stage_run_id",
        "next_cursor",
    )
    printed: set[str] = set()
    for key in preferred:
        if key in safe and safe[key] is not None:
            print(f"{key}: {safe[key]}")
            printed.add(key)
    if "items" in safe and isinstance(safe["items"], list):
        for item in safe["items"]:
            if isinstance(item, dict):
                identity = item.get("id") or item.get("pipeline_run_id") or item.get("name")
                recipe = item.get("recipe") or item.get("recipe_identity") or ""
                state = item.get("state") or item.get("submission_policy") or ""
                result = item.get("result") or ""
                print(
                    "\t".join(
                        str(value) for value in (identity, recipe, state, result) if value != ""
                    )
                )
    for key in sorted(set(safe) - printed - {"items"}):
        value = safe[key]
        if isinstance(value, (str, int, float, bool)) or value is None:
            print(f"{key}: {value}")




















def _run(args: argparse.Namespace) -> int:
    name, version = _recipe(args.recipe)
    del name, version
    body = {
        "budget": _strict_budget(args.budget),
        "display_name": _nfc_text(args.display_name, label="display name", minimum=1, maximum=200)
        if args.display_name is not None
        else None,
        "inputs": _bindings(args.input, option="--input"),
        "judge_profile_id": _uuid(args.judge_profile, label="judge profile ID")
        if args.judge_profile is not None
        else None,
        "parameters": _json_object(args.params, option="--params"),
        "recipe": args.recipe,
    }
    headers = {_IDEMPOTENCY_HEADER: _idempotency_key(args.idempotency_key)}
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(
            client,
            "POST",
            "/api/v1/pipeline-runs",
            action="submit pipeline run",
            json=body,
            headers=headers,
        )
    _emit(data, json_output=_json_mode(args), heading="PipelineRun submitted")
    return 0


def _list(args: argparse.Namespace) -> int:
    params: dict[str, Any] = {"limit": args.limit}
    for key in ("state", "result", "recipe", "created_after", "created_before", "cursor"):
        value = getattr(args, key)
        if value is not None:
            params[key] = value
    if args.recipe is not None:
        _recipe(args.recipe)
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(
            client, "GET", "/api/v1/pipeline-runs", action="list pipeline runs", params=params
        )
    _emit(data, json_output=_json_mode(args))
    return 0


def _show(args: argparse.Namespace) -> int:
    run_id = _uuid(args.run_id, label="run ID")
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(
            client, "GET", f"/api/v1/pipeline-runs/{run_id}", action="show pipeline run"
        )
    _emit(data, json_output=_json_mode(args))
    return 0


def _stages(args: argparse.Namespace) -> int:
    run_id = _uuid(args.run_id, label="run ID")
    params: dict[str, Any] = {"limit": args.limit}
    for key in ("node_key", "state", "domain_outcome", "cursor"):
        value = getattr(args, key)
        if value is not None:
            params[key] = value
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(
            client,
            "GET",
            f"/api/v1/pipeline-runs/{run_id}/stages",
            action="list pipeline StageRuns",
            params=params,
        )
    _emit(data, json_output=_json_mode(args))
    return 0


def _artifacts(args: argparse.Namespace) -> int:
    run_id = _uuid(args.run_id, label="run ID")
    params: dict[str, Any] = {"limit": args.limit}
    if args.cursor is not None:
        params["cursor"] = args.cursor
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(
            client,
            "GET",
            f"/api/v1/pipeline-runs/{run_id}/artifacts",
            action="list pipeline Artifacts",
            params=params,
        )
    _emit(data, json_output=_json_mode(args))
    return 0


def _event_rows(data: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = data.get("items", data.get("events", []))
    if not isinstance(raw, list):
        _fail("invalid_server_response", "pipeline events response has no event list", status=200)
    result: list[dict[str, Any]] = []
    for item in raw:
        if (
            not isinstance(item, dict)
            or isinstance(item.get("seq"), bool)
            or not isinstance(item.get("seq"), int)
        ):
            _fail(
                "invalid_server_response", "pipeline events contain an invalid sequence", status=200
            )
        result.append(cast(dict[str, Any], item))
    return result


def _watch(args: argparse.Namespace) -> int:
    run_id = _uuid(args.run_id, label="run ID")
    last_seq = args.after_seq
    cfg = require_logged_in()
    try:
        with authed_client(cfg, timeout=max(30.0, args.poll_interval + 5.0)) as client:
            while True:
                data = _request_json(
                    client,
                    "GET",
                    f"/api/v1/pipeline-runs/{run_id}/events",
                    action="watch pipeline run",
                    params={"after_seq": last_seq, "limit": args.limit},
                )
                for event in _event_rows(data):
                    seq = cast(int, event["seq"])
                    if seq <= last_seq:
                        continue
                    if seq != last_seq + 1:
                        _fail(
                            "event_sequence_gap",
                            "server returned a non-contiguous event sequence",
                            status=200,
                        )
                    if _json_mode(args):
                        print(json.dumps(redact_mapping(event), sort_keys=True))
                    else:
                        _emit(event, json_output=False)
                    last_seq = seq
                next_seq = data.get("next_after_seq", last_seq)
                if (
                    not isinstance(next_seq, int)
                    or isinstance(next_seq, bool)
                    or next_seq != last_seq
                ):
                    _fail(
                        "event_sequence_gap",
                        "server returned an inconsistent next event sequence",
                        status=200,
                    )
                if data.get("terminal") is True:
                    return 0
                retry_ms = data.get("retry_after_ms")
                delay = (
                    args.poll_interval
                    if not isinstance(retry_ms, int)
                    else max(args.poll_interval, retry_ms / 1000)
                )
                time.sleep(delay)
    except KeyboardInterrupt:
        # Deliberately do not call cancel. Watching has no lifecycle authority.
        return 130


def _cancel(args: argparse.Namespace) -> int:
    run_id = _uuid(args.run_id, label="run ID")
    body = {"reason": _nfc_text(args.reason, label="reason", minimum=1, maximum=500)}
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(
            client,
            "POST",
            f"/api/v1/pipeline-runs/{run_id}/cancel",
            action="cancel pipeline run",
            json=body,
        )
    _emit(data, json_output=_json_mode(args), heading="Pipeline cancellation requested")
    return 0


def _retry_stage(args: argparse.Namespace) -> int:
    stage_id = _uuid(args.stage_run_id, label="stage run ID")
    body = {
        "budget": _strict_budget(args.budget),
        "display_name": _nfc_text(args.display_name, label="display name", minimum=1, maximum=200)
        if args.display_name is not None
        else None,
    }
    headers = {_IDEMPOTENCY_HEADER: _idempotency_key(args.idempotency_key)}
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(
            client,
            "POST",
            f"/api/v1/pipeline-stage-runs/{stage_id}/retry",
            action="create full replay pipeline run",
            json=body,
            headers=headers,
        )
    _emit(data, json_output=_json_mode(args), heading="Full replay PipelineRun created")
    return 0


def _download(args: argparse.Namespace) -> int:
    artifact_id = _uuid(args.artifact_id, label="artifact ID")
    output = Path(args.output)
    if output.exists() and not args.force:
        _fail("output_exists", "download output already exists; use --force to replace it")
    output.parent.mkdir(parents=True, exist_ok=True)
    cfg = require_logged_in()
    digest = hashlib.sha256()
    size = 0
    temporary = output.with_name(f".{output.name}.{uuid4().hex}.part")
    try:
        with authed_client(cfg, timeout=args.timeout) as client:
            try:
                context = client.stream("GET", f"/api/v1/pipeline-artifacts/{artifact_id}/download")
                with context as response:
                    if response.is_redirect:
                        _fail(
                            "unsafe_download_redirect",
                            "artifact download returned a redirect instead of authorized content",
                            status=response.status_code,
                        )
                    if response.status_code // 100 != 2:
                        # Read the bounded error before using the shared structured parser.
                        response.read()
                        _safe_response(response, action="download pipeline artifact")
                    with temporary.open("wb") as target:
                        for chunk in response.iter_bytes():
                            if chunk:
                                digest.update(chunk)
                                size += len(chunk)
                                target.write(chunk)
            except httpx.HTTPError:
                _fail("request_failed", "request failed while trying to download pipeline artifact")
        os.replace(temporary, output)
    finally:
        if temporary.exists():
            temporary.unlink()
    result = {
        "artifact_id": artifact_id,
        "output": str(output),
        "sha256": f"sha256:{digest.hexdigest()}",
        "size_bytes": size,
    }
    _emit(result, json_output=_json_mode(args), heading="Artifact downloaded")
    return 0


def _recipes(args: argparse.Namespace) -> int:
    path = "/api/v1/pipeline-recipes"
    if args.recipe is not None:
        name, version = _recipe(args.recipe)
        path += f"/{quote(name, safe='')}/{quote(version, safe='')}"
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(client, "GET", path, action="read pipeline recipes")
    _emit(data, json_output=_json_mode(args))
    return 0


def _judge_profiles(args: argparse.Namespace) -> int:
    name, version = _recipe(args.recipe)
    path = (
        f"/api/v1/pipeline-recipes/{quote(name, safe='')}/{quote(version, safe='')}/judge-profiles"
    )
    cfg = require_logged_in()
    with authed_client(cfg) as client:
        data = _request_json(client, "GET", path, action="list judge profiles")
    _emit(data, json_output=_json_mode(args))
    return 0


def _materialize_inputs(args: argparse.Namespace) -> int:
    name, version = _recipe(args.recipe)
    inputs = _bindings(args.input, option="--input")
    if args.recipe == "behavior-recovery@1" and set(inputs) != set(_IMPORT_KINDS):
        _fail(
            "invalid_materialization_inputs",
            "behavior-recovery@1 requires exactly dataset, policy, and mop_bank inputs",
        )
    params = _json_object(args.params, option="--params")
    if args.recipe == "behavior-recovery@1":
        if not set(params) <= {"episodes_per_instance", "seed_base"}:
            _fail(
                "invalid_materialization_parameters",
                "materialization parameters contain an extra field",
            )
        episodes = params.get("episodes_per_instance", 1)
        seed = params.get("seed_base", 0)
        if isinstance(episodes, bool) or not isinstance(episodes, int) or not 1 <= episodes <= 10:
            _fail(
                "invalid_materialization_parameters",
                "episodes_per_instance must be an integer from 1 through 10",
            )
        if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= 0xFFFFFFFF:
            _fail(
                "invalid_materialization_parameters", "seed_base must be an unsigned 32-bit integer"
            )
        params = {"episodes_per_instance": episodes, "seed_base": seed}
    body = {
        "inputs": inputs,
        "parameters": params,
        "task_set_id": _uuid(args.task_set, label="task set ID"),
    }
    headers = {_IDEMPOTENCY_HEADER: _idempotency_key(args.idempotency_key)}
    path = f"/api/v1/pipeline-recipes/{quote(name, safe='')}/{quote(version, safe='')}/materialize-inputs"
    cfg = require_logged_in()
    with authed_client(cfg, timeout=args.timeout) as client:
        data = _request_json(
            client, "POST", path, action="materialize pipeline inputs", json=body, headers=headers
        )
    _emit(data, json_output=_json_mode(args), heading="Pipeline inputs materialized")
    return 0


def _manifest_files(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = manifest.get("files")
    if not isinstance(raw, list) or not raw:
        _fail("invalid_import_manifest", "import manifest files must be a non-empty array")
    result: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict) or set(item) != {
            "path",
            "sha256",
            "size_bytes",
            "media_type",
        }:
            _fail(
                "invalid_import_manifest",
                "each import file must have exactly path, sha256, size_bytes, and media_type",
            )
        path = item.get("path")
        digest = item.get("sha256")
        size = item.get("size_bytes")
        if (
            not isinstance(path, str)
            or not path
            or path.startswith(("/", "\\"))
            or "\\" in path
            or any(part in {"", ".", ".."} for part in path.split("/"))
        ):
            _fail("invalid_import_manifest", "import manifest contains an unsafe file path")
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or digest.lower() != digest
            or any(ch not in "0123456789abcdef" for ch in digest)
        ):
            _fail("invalid_import_manifest", "import manifest contains an invalid SHA-256")
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            _fail("invalid_import_manifest", "import manifest contains an invalid file size")
        result.append(cast(dict[str, Any], item))
    paths = [cast(str, item["path"]) for item in result]
    if paths != sorted(paths, key=lambda value: value.encode("utf-8")) or len(paths) != len(
        set(paths)
    ):
        _fail("invalid_import_manifest", "import manifest files must be bytewise sorted and unique")
    return result


def _validate_import_tree(root: Path, manifest: Mapping[str, Any]) -> list[tuple[str, Path, int]]:
    if not root.is_dir():
        _fail("invalid_import_root", "--root must be a directory")
    declared = _manifest_files(manifest)
    expected = {cast(str, item["path"]): item for item in declared}
    actual: dict[str, Path] = {}
    casefolded: set[str] = set()
    for directory, dir_names, file_names in os.walk(root, followlinks=False):
        base = Path(directory)
        for name in list(dir_names):
            candidate = base / name
            if candidate.is_symlink():
                _fail("unsafe_import_tree", "import root contains a symlink")
        for name in file_names:
            candidate = base / name
            relative = candidate.relative_to(root).as_posix()
            info = candidate.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                _fail("unsafe_import_tree", "import root contains a special file or hardlink")
            folded = relative.casefold()
            if folded in casefolded:
                _fail("unsafe_import_tree", "import root contains a case-colliding path")
            casefolded.add(folded)
            actual[relative] = candidate
    if set(actual) != set(expected):
        _fail("import_inventory_mismatch", "import root files do not exactly match the manifest")
    verified: list[tuple[str, Path, int]] = []
    for relative, item in expected.items():
        path = actual[relative]
        digest = hashlib.sha256()
        size = 0
        try:
            with path.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
        except OSError:
            _fail("import_file_read_failed", "a declared import file could not be read")
        if size != item["size_bytes"] or digest.hexdigest() != item["sha256"]:
            _fail(
                "import_file_mismatch", "a declared import file does not match its size or SHA-256"
            )
        verified.append((relative, path, path.stat().st_mode))
    return verified


def _build_bundle(root: Path, manifest: Mapping[str, Any], destination: Path) -> None:
    files = _validate_import_tree(root, manifest)
    with destination.open("wb") as compressed:
        compressor = zstandard.ZstdCompressor(
            level=10, write_checksum=True, write_content_size=False
        )
        with compressor.stream_writer(compressed, closefd=False) as writer:
            with tarfile.open(fileobj=writer, mode="w|", format=tarfile.PAX_FORMAT) as archive:
                for relative, path, mode in files:
                    info = tarfile.TarInfo(f"payload/{relative}")
                    file_size = path.stat().st_size
                    info.size = file_size
                    info.mtime = 0
                    info.uid = 0
                    info.gid = 0
                    info.uname = ""
                    info.gname = ""
                    info.mode = 0o755 if mode & 0o111 else 0o644
                    with path.open("rb") as source:
                        archive.addfile(info, source)


def _iter_file_part(path: Path, *, offset: int, length: int) -> Iterator[bytes]:
    with path.open("rb") as source:
        source.seek(offset)
        remaining = length
        while remaining:
            chunk = source.read(min(1024 * 1024, remaining))
            if not chunk:
                _fail("import_bundle_read_failed", "local import bundle changed while uploading")
            remaining -= len(chunk)
            yield chunk


def _part_sha256(path: Path, *, offset: int, length: int) -> str:
    digest = hashlib.sha256()
    for chunk in _iter_file_part(path, offset=offset, length=length):
        digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _upload_grant(data: Mapping[str, Any]) -> tuple[str, str | None]:
    grant = data.get("upload_grant")
    if isinstance(grant, dict):
        token = grant.get("token")
        expiry = grant.get("expires_at")
    else:
        token = data.get("upload_token")
        expiry = data.get("expires_at")
    if not isinstance(token, str) or not token:
        _fail(
            "invalid_server_response", "import response did not include an upload grant", status=200
        )
    return token, expiry if isinstance(expiry, str) else None


def _upload_token_expires_soon(value: str | None) -> bool:
    if value is None:
        return False
    try:
        expiry = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        _fail("invalid_server_response", "upload grant has an invalid expiry", status=200)
    if expiry.tzinfo is None or expiry.utcoffset() is None:
        _fail("invalid_server_response", "upload grant expiry has no timezone", status=200)
    return expiry <= datetime.now(UTC) + timedelta(seconds=60)


def _abort_import(client: httpx.Client, import_id: str, session_id: str, reason: str) -> None:
    try:
        client.post(
            f"/api/v1/pipeline-input-imports/{import_id}/abort",
            json={"reason": reason, "upload_session_id": session_id},
            headers={_IDEMPOTENCY_HEADER: f"cli-abort-{uuid4()}"},
        )
    except httpx.HTTPError:
        pass


def _derived_idempotency_key(key: str, suffix: str) -> str:
    return f"cli-{suffix}-{hashlib.sha256(key.encode()).hexdigest()}"


def _import_input(args: argparse.Namespace) -> int:
    _recipe(args.recipe)
    manifest = _json_object(args.manifest, option="--manifest")
    required_manifest_fields = {
        "schema_version",
        "kind",
        "name",
        "version",
        "upstream",
        "compatibility",
        "files",
    }
    if set(manifest) != required_manifest_fields:
        _fail("invalid_import_manifest", "import manifest has missing or extra top-level fields")
    if (
        manifest.get("schema_version") != "behavior.input-import.v1"
        or manifest.get("kind") != args.kind
    ):
        _fail(
            "invalid_import_manifest",
            "manifest schema_version and kind must match the import command",
        )
    key = _idempotency_key(args.idempotency_key)
    root = Path(args.root).resolve()
    with tempfile.TemporaryDirectory(prefix="loom-pipeline-import-") as temporary_dir:
        bundle = Path(temporary_dir) / "payload.tar.zst"
        _build_bundle(root, manifest, bundle)
        bundle_size = bundle.stat().st_size
        bundle_digest = _part_sha256(bundle, offset=0, length=bundle_size)
        cfg = require_logged_in()
        with authed_client(cfg, timeout=args.timeout) as client:
            created = _request_json(
                client,
                "POST",
                "/api/v1/pipeline-input-imports",
                action="create pipeline input import",
                json={"kind": args.kind, "manifest": manifest, "recipe": args.recipe},
                headers={_IDEMPOTENCY_HEADER: key},
            )
            import_id = _uuid(str(created.get("import_id", "")), label="server import ID")
            session_id = _uuid(str(created.get("session_id", "")), label="server upload session ID")
            try:
                part_size = created.get("part_size_bytes")
                if isinstance(part_size, bool) or not isinstance(part_size, int) or part_size <= 0:
                    _fail(
                        "invalid_server_response",
                        "import response has an invalid part size",
                        status=200,
                    )
                # Rotate immediately. This makes the token fresh for the upload and
                # gives retry-after-lost-response the route's intended semantics.
                renewed = _request_json(
                    client,
                    "POST",
                    f"/api/v1/pipeline-input-imports/{import_id}/renew-upload-token",
                    action="renew import upload token",
                    json={"upload_session_id": session_id},
                )
                token, expires_at = _upload_grant(renewed)
                receipts: list[dict[str, Any]] = []
                for index, offset in enumerate(range(0, bundle_size, part_size), start=1):
                    if index > 9990:
                        _fail(
                            "import_too_many_parts",
                            "import bundle requires more than 9990 upload parts",
                        )
                    length = min(part_size, bundle_size - offset)
                    if _upload_token_expires_soon(expires_at):
                        renewed = _request_json(
                            client,
                            "POST",
                            f"/api/v1/pipeline-input-imports/{import_id}/renew-upload-token",
                            action="renew import upload token",
                            json={"upload_session_id": session_id},
                        )
                        token, expires_at = _upload_grant(renewed)
                    content_sha = _part_sha256(bundle, offset=offset, length=length)
                    headers = {
                        "X-Loom-Upload-Session-Id": session_id,
                        "X-Loom-Upload-Token": token,
                        "Content-Length": str(length),
                        "X-Loom-Content-SHA256": content_sha,
                    }
                    receipt: dict[str, Any] | None = None
                    for attempt in range(2):
                        try:
                            response = client.put(
                                f"/api/v1/pipeline-input-imports/{import_id}/parts/{index}",
                                content=_iter_file_part(bundle, offset=offset, length=length),
                                headers=headers,
                            )
                            receipt = _safe_response(response, action=f"upload import part {index}")
                            break
                        except (httpx.HTTPError, PipelineCliError):
                            if attempt:
                                raise
                    assert receipt is not None
                    receipts.append(receipt)
                completed = _request_json(
                    client,
                    "POST",
                    f"/api/v1/pipeline-input-imports/{import_id}/complete",
                    action="complete pipeline input import",
                    json={
                        "upload_session_id": session_id,
                        "bundle_sha256": bundle_digest,
                        "bundle_size_bytes": bundle_size,
                        "parts": receipts,
                    },
                    headers={_IDEMPOTENCY_HEADER: _derived_idempotency_key(key, "complete")},
                )
            except KeyboardInterrupt:
                _abort_import(client, import_id, session_id, "client interrupted")
                raise
            except Exception:
                _abort_import(client, import_id, session_id, "client upload failed")
                raise
    _emit(completed, json_output=_json_mode(args), heading="Pipeline input imported")
    return 0


def _add_json(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json", dest="json_output", action="store_true", help="Emit redacted JSON"
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="loom pipeline",
        description="Submit and inspect fixed official-Recipe Pipelines on a deployed Loom server.",
    )
    sub = parser.add_subparsers(dest="pipeline_cmd", required=True)

    run = sub.add_parser("run", help="Submit an ordinary official-Recipe PipelineRun")
    run.add_argument("--recipe", required=True)
    run.add_argument("--input", action="append", required=True, default=[])
    run.add_argument("--params", required=True)
    run.add_argument("--budget", required=True)
    run.add_argument("--idempotency-key", required=True)
    run.add_argument("--display-name")
    run.add_argument(
        "--judge-profile", help="Team-approved judge profile UUID for the Recipe's Stage 2"
    )
    _add_json(run)
    run.set_defaults(handler=_run)

    listing = sub.add_parser("list", help="List authorized PipelineRuns")
    listing.add_argument("--state")
    listing.add_argument("--result")
    listing.add_argument("--recipe")
    listing.add_argument("--created-after")
    listing.add_argument("--created-before")
    listing.add_argument("--cursor")
    listing.add_argument("--limit", type=int, choices=range(1, 101), default=100)
    _add_json(listing)
    listing.set_defaults(handler=_list)

    show = sub.add_parser("show", help="Show one authorized PipelineRun")
    show.add_argument("run_id")
    _add_json(show)
    show.set_defaults(handler=_show)

    stages = sub.add_parser("stages", help="Page through authorized StageRuns")
    stages.add_argument("run_id")
    stages.add_argument("--node-key")
    stages.add_argument(
        "--state",
        choices=(
            "blocked",
            "ready",
            "queued",
            "claimed",
            "running",
            "retry_wait",
            "succeeded",
            "failed",
            "cancelled",
            "skipped",
        ),
    )
    stages.add_argument("--domain-outcome")
    stages.add_argument("--cursor")
    stages.add_argument("--limit", type=int, choices=range(1, 501), default=200)
    _add_json(stages)
    stages.set_defaults(handler=_stages)

    artifacts = sub.add_parser("artifacts", help="Page through authorized run Artifacts")
    artifacts.add_argument("run_id")
    artifacts.add_argument("--cursor")
    artifacts.add_argument("--limit", type=int, choices=range(1, 201), default=100)
    _add_json(artifacts)
    artifacts.set_defaults(handler=_artifacts)

    watch = sub.add_parser(
        "watch", help="Poll monotonic Pipeline events; Ctrl-C stops watching without cancelling"
    )
    watch.add_argument("run_id")
    watch.add_argument("--after-seq", type=int, default=0)
    watch.add_argument("--limit", type=int, choices=range(1, 501), default=200)
    watch.add_argument("--poll-interval", type=float, default=1.0)
    _add_json(watch)
    watch.set_defaults(handler=_watch)

    cancel = sub.add_parser("cancel", help="Request cancellation of an ordinary PipelineRun")
    cancel.add_argument("run_id")
    cancel.add_argument("--reason", required=True)
    _add_json(cancel)
    cancel.set_defaults(handler=_cancel)

    retry_help = "Create a new full replay PipelineRun; never reopen the old run or reuse its outputs/checkpoints"
    retry = sub.add_parser("retry-stage", help=retry_help, description=retry_help)
    retry.add_argument("stage_run_id")
    retry.add_argument("--budget", required=True)
    retry.add_argument("--idempotency-key", required=True)
    retry.add_argument("--display-name")
    _add_json(retry)
    retry.set_defaults(handler=_retry_stage)

    download = sub.add_parser(
        "download",
        help="Stream an authorized Artifact through Loom; internal object-store URLs are never used",
    )
    download.add_argument("artifact_id")
    download.add_argument("--output", required=True)
    download.add_argument("--force", action="store_true")
    download.add_argument("--timeout", type=float, default=300.0)
    _add_json(download)
    download.set_defaults(handler=_download)

    recipes = sub.add_parser("recipes", help="List official Recipes or show NAME@VERSION")
    recipes.add_argument("recipe", nargs="?")
    _add_json(recipes)
    recipes.set_defaults(handler=_recipes)

    profiles = sub.add_parser("judge-profiles", help="List redacted compatible judge profiles")
    profiles.add_argument("--recipe", required=True)
    _add_json(profiles)
    profiles.set_defaults(handler=_judge_profiles)

    materialize = sub.add_parser(
        "materialize-inputs", help="Atomically materialize official Recipe graph inputs"
    )
    materialize.add_argument("--recipe", required=True)
    materialize.add_argument("--task-set", required=True)
    materialize.add_argument("--input", action="append", required=True, default=[])
    materialize.add_argument("--params", required=True)
    materialize.add_argument("--idempotency-key", required=True)
    materialize.add_argument("--timeout", type=float, default=300.0)
    _add_json(materialize)
    materialize.set_defaults(handler=_materialize_inputs)

    import_input = sub.add_parser(
        "import-input", help="Admin-only deterministic streaming import of a declared input tree"
    )
    import_input.add_argument("--recipe", required=True)
    import_input.add_argument("--kind", required=True, choices=_IMPORT_KINDS)
    import_input.add_argument("--manifest", required=True)
    import_input.add_argument("--root", required=True)
    import_input.add_argument("--idempotency-key", required=True)
    import_input.add_argument("--timeout", type=float, default=300.0)
    _add_json(import_input)
    import_input.set_defaults(handler=_import_input)
    return parser


def dispatch(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return cast(int, args.handler(args))
    except PipelineCliError as error:
        if _json_mode(args):
            sys.stderr.write(json.dumps(error.as_dict(), sort_keys=True) + "\n")
        else:
            status = f" (HTTP {error.status})" if error.status else ""
            sys.stderr.write(f"error{status}: {error.reason_code}: {error.message}\n")
        return 1
    except NotLoggedInError as error:
        message = redact_text(str(error))
        if _json_mode(args):
            sys.stderr.write(
                json.dumps(
                    {"status": 0, "reason_code": "not_logged_in", "message": message},
                    sort_keys=True,
                )
                + "\n"
            )
        else:
            sys.stderr.write(f"error: {message}\n")
        return 2
    except KeyboardInterrupt:
        return 130
