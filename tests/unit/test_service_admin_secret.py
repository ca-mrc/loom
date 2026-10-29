from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from loom.admin_secret import AdminSecretConfigError, AdminSecretVerifier
from loom_service.app import _load_admin_secret_verifier
from loom_service.config import LoomServiceSettings
from loom_service.dependencies import authed_session

RAW_ADMIN_TOKEN = "loom_admin_" + "B" * 43


def _settings(*, admin_secret_file: Path | None = None) -> LoomServiceSettings:
    return LoomServiceSettings(
        _env_file=None,
        db_url="postgresql+psycopg://loom:pw@localhost:5432/loom",
        minio_endpoint="http://localhost:9000",
        minio_access_key="loomdev",
        minio_secret_key="loomdev123",
        control_plane_url="http://control-plane:8080",
        gateway_url="http://gateway:8081",
        admin_secret_file=admin_secret_file,
    )


def _write_secret(path: Path) -> None:
    path.write_text(
        "[admin]\n"
        f"token = \"{RAW_ADMIN_TOKEN}\"\n"
        "created_at = \"2026-06-16T00:00:00Z\"\n"
        "version = 1\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def test_load_admin_secret_verifier_uses_configured_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LOOM_ENV", raising=False)
    secret_file = tmp_path / "secrets.toml"
    _write_secret(secret_file)

    verifier = _load_admin_secret_verifier(_settings(admin_secret_file=secret_file))

    assert verifier is not None
    assert verifier.verify(RAW_ADMIN_TOKEN)


def test_load_admin_secret_verifier_requires_file_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_ENV", "production")

    with pytest.raises(AdminSecretConfigError, match="admin secret"):
        _load_admin_secret_verifier(_settings(admin_secret_file=None))


class _ExplodingSession:
    async def execute(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("singleton admin auth should not query DB")

    async def commit(self) -> None:
        return None


class _SessionContext:
    async def __aenter__(self) -> _ExplodingSession:
        return _ExplodingSession()

    async def __aexit__(self, *_exc: object) -> None:
        return None


async def test_authed_session_passes_app_admin_verifier() -> None:
    verifier = AdminSecretVerifier.from_token(RAW_ADMIN_TOKEN)
    request = SimpleNamespace(
        method="GET",
        cookies={},
        app=SimpleNamespace(
            state=SimpleNamespace(
                admin_secret_verifier=verifier,
                session_factory=lambda: _SessionContext(),
            ),
        ),
    )

    session_iter = authed_session(
        request,  # type: ignore[arg-type]
        authorization=f"Bearer {RAW_ADMIN_TOKEN}",
    )
    session, ctx = await anext(session_iter)
    await session_iter.aclose()

    assert isinstance(session, _ExplodingSession)
    assert ctx.type == "admin"
    assert ctx.team_id is None
