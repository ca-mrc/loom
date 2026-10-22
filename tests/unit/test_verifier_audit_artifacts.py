"""Unit tests for #865 verifier audit artifact collection + delivery packing."""

from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom.db.schema import Trial
from loom.trial.step_runner import (
    _artifact_patterns,
    _verifier_artifact_patterns,
)
from loom_service.delivery_export_tb2_v2 import (
    Tb2V2ExportError,
    resolve_verifier_artifacts,
)


class _FakeS3:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self._objects = objects

    def get_object(self, *, Bucket: str, Key: str):  # noqa: N803
        from botocore.exceptions import ClientError

        data = self._objects.get((Bucket, Key))
        if data is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "GetObject",
            )
        return {"Body": _Body(data), "ContentLength": len(data)}


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data
        self._position = 0
        self.closed = False

    def read(self, amount: int = -1) -> bytes:
        if amount < 0:
            amount = len(self._data) - self._position
        start = self._position
        self._position = min(len(self._data), start + amount)
        return self._data[start : self._position]

    def close(self) -> None:
        self.closed = True


def _audit_result(*paths: str) -> SimpleNamespace:
    return SimpleNamespace(
        structured={
            "loom_verifier_audit": {
                "artifacts": [{"path": path} for path in paths],
            }
        }
    )


def _trial_with_verifier_artifacts(
    *,
    log_body: bytes,
    log_name: str = "script.log",
    meta: dict[str, object] | None = None,
    share_status: str = "shared",
    content_hash: str | None = None,
    include_meta: bool = True,
    canonical: dict[str, bytes] | None = None,
) -> tuple[Trial, _FakeS3, str]:
    trial_id = uuid4()
    team_id = uuid4()
    log_key = f"{team_id}/{trial_id}/main/.loom/verifier/{log_name}"
    meta_key = f"{log_key}.meta.json"
    digest = hashlib.sha256(log_body).hexdigest()
    artifacts = [
        {
            "step_name": "main",
            "bucket": "artifacts",
            "key": log_key,
            "size": len(log_body),
            "content_hash": content_hash or f"sha256:{digest}",
            "share_status": share_status,
            "blocked_reason": "secret" if share_status == "blocked" else None,
        }
    ]
    objects: dict[tuple[str, str], bytes] = {("artifacts", log_key): log_body}
    if include_meta:
        meta_payload: dict[str, object] = {
            "schema_version": "1",
            "truncated": False,
            "original_bytes": len(log_body),
            "kept_bytes": len(log_body),
            "return_code": 0,
            "script_path": "/app/verifier/run.sh",
            "log_path": f".loom/verifier/{log_name}",
        }
        if meta is not None:
            meta_payload.update(meta)
        meta_bytes = (json.dumps(meta_payload) + "\n").encode()
        artifacts.append(
            {
                "step_name": "main",
                "bucket": "artifacts",
                "key": meta_key,
                "size": len(meta_bytes),
                "content_hash": f"sha256:{hashlib.sha256(meta_bytes).hexdigest()}",
                "share_status": "shared",
                "blocked_reason": None,
            }
        )
        objects[("artifacts", meta_key)] = meta_bytes

    for name, body in (canonical or {}).items():
        key = f"{team_id}/{trial_id}/main/.loom/verifier/{name}"
        artifacts.append(
            {
                "step_name": "main",
                "bucket": "artifacts",
                "key": key,
                "size": len(body),
                "content_hash": f"sha256:{hashlib.sha256(body).hexdigest()}",
                "share_status": "shared",
                "blocked_reason": None,
            }
        )
        objects[("artifacts", key)] = body

    trial = Trial(
        id=trial_id,
        team_id=team_id,
        task_id="source-useful/task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": "terminus-2"},
        trajectory_index={"artifacts": artifacts},
    )
    return trial, _FakeS3(objects), log_key


def test_artifact_patterns_adds_verifier_glob_for_terminus2() -> None:
    ctx = SimpleNamespace(
        agent=SimpleNamespace(name="terminus-2"),
        verifier=SimpleNamespace(name="script"),
    )
    step = SimpleNamespace(artifacts=["output.txt"])
    patterns = _artifact_patterns(ctx, step)  # type: ignore[arg-type]
    assert "output.txt" in patterns
    assert ".loom/agent/**" in patterns
    assert ".loom/verifier/**" not in patterns
    platform_patterns = _verifier_artifact_patterns(  # type: ignore[arg-type]
        ctx,
        _audit_result(
            ".loom/verifier/script.log",
            ".loom/verifier/script.log.meta.json",
            ".loom/verifier/output.json",
        ),
    )
    assert ".loom/verifier/pytest.log" in platform_patterns
    assert ".loom/verifier/pytest.log.meta.json" in platform_patterns
    assert ".loom/verifier/script.log" in platform_patterns
    assert ".loom/verifier/script.log.meta.json" in platform_patterns
    assert ".loom/verifier/output.json" in platform_patterns


@pytest.mark.parametrize("agent_name", ["openhands-sdk", "openhands"])
def test_artifact_patterns_capture_openhands_under_either_name(agent_name: str) -> None:
    """#2054: `openhands` is the legacy alias of `openhands-sdk`."""
    ctx = SimpleNamespace(
        agent=SimpleNamespace(name=agent_name),
        verifier=SimpleNamespace(name="script"),
    )
    step = SimpleNamespace(artifacts=["output.txt"])
    assert ".loom/agent/**" in _artifact_patterns(ctx, step)  # type: ignore[arg-type]


def test_artifact_patterns_adds_verifier_glob_for_script_verifier() -> None:
    ctx = SimpleNamespace(
        agent=SimpleNamespace(name="oracle"),
        verifier=SimpleNamespace(name="script"),
    )
    step = SimpleNamespace(
        artifacts=[
            ".loom/verifier/**",
            ".loom/verifier/agent-planted.txt",
            "other/../.loom/verifier/stale.log",
        ]
    )
    patterns = _artifact_patterns(ctx, step)  # type: ignore[arg-type]
    assert patterns == []
    assert _verifier_artifact_patterns(  # type: ignore[arg-type]
        ctx,
        _audit_result(
            ".loom/verifier/script.log",
            ".loom/verifier/script.log.meta.json",
            ".loom/verifier/output.json",
            ".loom/verifier/agent-planted.txt",
        ),
    ) == [
        ".loom/verifier/script.log",
        ".loom/verifier/script.log.meta.json",
        ".loom/verifier/output.json",
    ]
    assert ".loom/agent/**" not in patterns


def test_artifact_patterns_adds_verifier_glob_for_pytest_verifier() -> None:
    ctx = SimpleNamespace(
        agent=SimpleNamespace(name="oracle"),
        verifier=SimpleNamespace(name="pytest"),
    )
    step = SimpleNamespace(artifacts=["out.txt"])
    patterns = _artifact_patterns(ctx, step)  # type: ignore[arg-type]
    assert "out.txt" in patterns
    assert ".loom/verifier/**" not in patterns
    assert ".loom/agent/**" not in patterns
    platform_patterns = _verifier_artifact_patterns(  # type: ignore[arg-type]
        ctx,
        _audit_result(
            ".loom/verifier/pytest.log",
            ".loom/verifier/pytest.log.meta.json",
            ".loom/verifier/pytest-install.log",
            ".loom/verifier/pytest-install.log.meta.json",
            ".loom/verifier/junit.xml",
        ),
    )
    assert ".loom/verifier/pytest.log" in platform_patterns
    assert ".loom/verifier/pytest.log.meta.json" in platform_patterns
    assert ".loom/verifier/pytest-install.log" in platform_patterns
    assert ".loom/verifier/pytest-install.log.meta.json" in platform_patterns
    assert ".loom/verifier/junit.xml" in platform_patterns


def test_resolve_verifier_artifacts_happy_path() -> None:
    body = b"--- stdout ---\n3 passed\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
        meta={"truncated": True, "original_bytes": 9999},
    )
    resolved = resolve_verifier_artifacts(
        trial,
        client=client,
        artifacts_bucket="artifacts",
    )
    by_path = {item.archive_path: item for item in resolved}
    assert "verifier/script.log" in by_path
    assert "verifier/script.log.meta.json" in by_path
    log_entry = by_path["verifier/script.log"]
    assert log_entry.data == body
    assert log_entry.truncated is True
    assert log_entry.share_status == "shared"
    assert log_entry.content_hash.startswith("sha256:")
    assert log_entry.step_name == "main"


def test_resolve_verifier_artifacts_reads_indexed_bucket_not_settings() -> None:
    body = b"--- stdout ---\n3 passed\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
        meta={"truncated": False, "original_bytes": len(body)},
    )
    # Objects live under the indexed bucket; settings name differs.
    resolved = resolve_verifier_artifacts(
        trial,
        client=client,
        artifacts_bucket="loom-staging-artifacts",
    )
    assert any(item.archive_path == "verifier/script.log" for item in resolved)


def test_resolve_pytest_pair_and_canonical_junit() -> None:
    junit = b'<testsuite tests="1"><testcase name="ok"/></testsuite>'
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=b"1 passed\n",
        log_name="pytest.log",
        meta={"script_path": "pytest -q"},
        canonical={"junit.xml": junit},
    )
    resolved = resolve_verifier_artifacts(
        trial,
        client=client,
        artifacts_bucket="artifacts",
    )
    by_path = {item.archive_path: item for item in resolved}
    assert by_path["verifier/pytest.log"].data == b"1 passed\n"
    assert by_path["verifier/junit.xml"].data == junit
    assert by_path["verifier/junit.xml"].truncated is None


def test_resolve_script_pair_and_canonical_output() -> None:
    output = b'{"rewards":{"passed":1.0}}'
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=b"scored\n",
        canonical={"output.json": output},
    )
    resolved = resolve_verifier_artifacts(
        trial,
        client=client,
        artifacts_bucket="artifacts",
    )
    by_path = {item.archive_path: item for item in resolved}
    assert by_path["verifier/output.json"].data == output


def test_resolve_rejects_noncanonical_verifier_leaf() -> None:
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=b"ok\n",
        canonical={"agent-planted.txt": b"not platform owned"},
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "invalid_verifier_artifact_pair"


def test_resolve_scopes_duplicate_names_by_step() -> None:
    trial, client, _key = _trial_with_verifier_artifacts(log_body=b"ok\n")
    copies: list[dict[str, object]] = []
    for artifact in list(trial.trajectory_index["artifacts"]):
        copied = dict(artifact)
        old_key = str(copied["key"])
        new_key = old_key.replace("/main/.loom/", "/verify-2/.loom/")
        copied["step_name"] = "verify-2"
        copied["key"] = new_key
        copies.append(copied)
        client._objects[("artifacts", new_key)] = client._objects[
            ("artifacts", old_key)
        ]
    trial.trajectory_index["artifacts"].extend(copies)

    resolved = resolve_verifier_artifacts(
        trial,
        client=client,
        artifacts_bucket="artifacts",
    )
    assert {item.archive_path for item in resolved} == {
        "verifier/main/script.log",
        "verifier/main/script.log.meta.json",
        "verifier/verify-2/script.log",
        "verifier/verify-2/script.log.meta.json",
    }


def test_resolve_verifier_artifacts_hash_mismatch() -> None:
    body = b"ok\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
        content_hash="sha256:" + ("0" * 64),
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "verifier_artifact_hash_mismatch"


def test_resolve_verifier_artifacts_missing_object() -> None:
    body = b"ok\n"
    trial, client, key = _trial_with_verifier_artifacts(
        log_body=body,
    )
    client._objects.clear()
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "missing_verifier_artifact"
    assert key in str(exc.value.detail)


def test_resolve_verifier_artifacts_blocked_share_status() -> None:
    body = b"secret token sk-ABCDEFGHIJKLMNOP\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
        share_status="blocked",
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "verifier_artifact_blocked"


def test_resolve_verifier_artifacts_secret_scan_fails() -> None:
    body = b"authorization: Bearer sk-ABCDEFGHIJKLMNOPQRST\n"
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=body,
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "secret_scan_failed"


def test_resolve_verifier_artifacts_fails_when_absent() -> None:
    trial = Trial(
        id=uuid4(),
        team_id=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": "terminus-2"},
        trajectory_index={"artifacts": []},
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=_FakeS3({}),
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "missing_verifier_artifact"


def test_resolve_verifier_artifacts_allows_skip_verifier_without_audit_artifacts() -> None:
    trial = Trial(
        id=uuid4(),
        team_id=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": "terminus-2", "skip_verifier": True},
        trajectory_index={"artifacts": []},
    )
    resolved = resolve_verifier_artifacts(
        trial,
        client=_FakeS3({}),
        artifacts_bucket="artifacts",
    )
    assert resolved == []


def test_resolve_verifier_artifacts_requires_exact_skip_boolean() -> None:
    trial = Trial(
        id=uuid4(),
        team_id=uuid4(),
        task_id="task-1",
        batch_id=uuid4(),
        state="succeeded",
        config={"agent_name": "terminus-2", "skip_verifier": "true"},
        trajectory_index={"artifacts": []},
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=_FakeS3({}),
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "missing_verifier_artifact"


def test_resolve_verifier_artifacts_rejects_orphan_log() -> None:
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=b"ok\n",
        include_meta=False,
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "invalid_verifier_artifact_pair"


@pytest.mark.parametrize(
    "meta",
    [
        {"schema_version": "2"},
        {"truncated": "false"},
        {"kept_bytes": 999},
        {"truncated": True, "original_bytes": 3, "kept_bytes": 3},
        {"log_path": ".loom/verifier/other.log"},
        {"return_code": True},
        {"script_path": ""},
        {"driver_truncated": "false"},
    ],
)
def test_resolve_verifier_artifacts_rejects_invalid_metadata(
    meta: dict[str, object],
) -> None:
    trial, client, _key = _trial_with_verifier_artifacts(
        log_body=b"ok\n",
        meta=meta,
    )
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "invalid_verifier_artifact_metadata"


@pytest.mark.parametrize("field", ["content_hash", "size"])
def test_resolve_verifier_artifacts_requires_complete_index(field: str) -> None:
    trial, client, _key = _trial_with_verifier_artifacts(log_body=b"ok\n")
    del trial.trajectory_index["artifacts"][0][field]
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "invalid_verifier_artifact_index"


def test_resolve_verifier_artifacts_rejects_indexed_size_mismatch() -> None:
    trial, client, _key = _trial_with_verifier_artifacts(log_body=b"ok\n")
    trial.trajectory_index["artifacts"][0]["size"] = 2
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "verifier_artifact_size_mismatch"


def test_resolve_verifier_artifacts_rejects_duplicate_archive_path() -> None:
    trial, client, _key = _trial_with_verifier_artifacts(log_body=b"ok\n")
    duplicate = dict(trial.trajectory_index["artifacts"][0])
    trial.trajectory_index["artifacts"].append(duplicate)
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "duplicate_verifier_artifact"


def test_resolve_verifier_artifacts_bounds_object_read_without_content_length() -> None:
    trial, client, key = _trial_with_verifier_artifacts(log_body=b"ok\n")
    huge = b"x" * 1_048_577
    original_get_object = client.get_object

    def _get_object(*, Bucket: str, Key: str):  # noqa: N803
        if Key == key:
            return {"Body": _Body(huge)}
        return original_get_object(Bucket=Bucket, Key=Key)

    client.get_object = _get_object  # type: ignore[method-assign]
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "verifier_artifact_too_large"


def test_resolve_verifier_artifacts_closes_body_on_content_length_mismatch() -> None:
    trial, client, key = _trial_with_verifier_artifacts(log_body=b"ok\n")
    body = _Body(b"ok\n")
    original_get_object = client.get_object

    def _get_object(*, Bucket: str, Key: str):  # noqa: N803
        if Key == key:
            return {"Body": body, "ContentLength": 999}
        return original_get_object(Bucket=Bucket, Key=Key)

    client.get_object = _get_object  # type: ignore[method-assign]
    with pytest.raises(Tb2V2ExportError) as exc:
        resolve_verifier_artifacts(
            trial,
            client=client,
            artifacts_bucket="artifacts",
        )
    assert exc.value.code == "verifier_artifact_size_mismatch"
    assert body.closed is True
