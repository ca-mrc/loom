"""GET /api/v1/version (#2009): the responding instance's own build
identity, read from local files only — never GitHub, Kubernetes, or the
database, and never blocking on a missing/malformed file.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


def _base_env() -> dict[str, str]:
    return {
        "LOOM_SVC_DB_URL": "postgresql+psycopg://u:p@h/db",
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "k",
        "LOOM_SVC_MINIO_SECRET_KEY": "s",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }


def _client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    app = create_app(LoomServiceSettings(_env_file=None))
    # No DB/MinIO/CP fixtures needed: /version only reads local files, so
    # skip the lifespan the same way test_service_root_landing.py does for
    # the equally state-free `/` handler.
    return TestClient(app)


def test_version_reports_build_metadata_from_local_files(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    sha_path = tmp_path / "build-sha"
    time_path = tmp_path / "build-time"
    sha_path.write_text("a" * 40 + "\n")
    time_path.write_text("2026-09-22T10:00:00Z\n")
    monkeypatch.setenv("LOOM_BUILD_SHA_PATH", str(sha_path))
    monkeypatch.setenv("LOOM_BUILD_TIME_PATH", str(time_path))

    resp = _client(monkeypatch).get("/api/v1/version")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "buildRevision": "a" * 40,
        "buildTime": "2026-09-22T10:00:00Z",
    }
    assert resp.headers["cache-control"] == "no-store"


def test_version_is_honest_and_non_blocking_when_metadata_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """A local dev tree, or an image built without this metadata, must
    never fail the request — it reports `null`, not a 5xx."""
    monkeypatch.setenv("LOOM_BUILD_SHA_PATH", str(tmp_path / "missing-sha"))
    monkeypatch.setenv("LOOM_BUILD_TIME_PATH", str(tmp_path / "missing-time"))

    resp = _client(monkeypatch).get("/api/v1/version")

    assert resp.status_code == 200, resp.text
    assert resp.json() == {"buildRevision": None, "buildTime": None}


def test_version_rejects_malformed_build_sha_rather_than_trusting_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    sha_path = tmp_path / "build-sha"
    sha_path.write_text("not-a-real-sha\n")
    monkeypatch.setenv("LOOM_BUILD_SHA_PATH", str(sha_path))
    monkeypatch.setenv("LOOM_BUILD_TIME_PATH", str(tmp_path / "missing-time"))

    resp = _client(monkeypatch).get("/api/v1/version")

    assert resp.status_code == 200, resp.text
    assert resp.json()["buildRevision"] is None


def test_version_not_in_openapi_schema_is_not_required_but_stays_lightweight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unlike `/`, `/version` is a real (if tiny) API surface, so it stays
    in the OpenAPI schema — this just pins that it's registered at all."""
    client = _client(monkeypatch)
    schema = client.app.openapi()
    assert "/api/v1/version" in schema.get("paths", {})


@pytest.mark.parametrize("digest,base", [
    ("sha256:" + "b" * 64, "a" * 40),
    ("sha256:" + "c" * 64, "a" * 64),
    ("malformed", "unknown"),
])
def test_personal_version_reports_source_not_an_approved_commit(monkeypatch, tmp_path, digest, base):
    # Even an older authored Dockerfile stamping its base SHA must not cause
    # the personal source to be reported as the actual Git revision.
    for name, value in {"SHA": "a" * 40, "KIND": "personal", "SOURCE_DIGEST": digest,
                        "SOURCE_BASE_COMMIT": base}.items():
        path = tmp_path / name
        path.write_text(value + "\n")
        monkeypatch.setenv("LOOM_BUILD_" + name + "_PATH", str(path))
    monkeypatch.setenv("LOOM_BUILD_TIME_PATH", str(tmp_path / "missing-time"))
    response = _client(monkeypatch).get("/api/v1/version")
    assert response.status_code == 200
    assert response.json() == {
        "buildRevision": None, "buildTime": None, "buildKind": "personal",
        "sourceDigest": digest if digest.startswith("sha256:") else None,
        "sourceBaseCommit": None if base == "unknown" else base,
    }
    assert response.headers["cache-control"] == "no-store"
