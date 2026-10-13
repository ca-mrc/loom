from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from loom_service.config import LoomServiceSettings
from tests.support.minio_images import MINIO_TLS_IMAGE

REPO_ROOT = Path(__file__).resolve().parents[2]


def _render_dev_compose(tmp_path: Path, overrides: dict[str, str]) -> dict:
    """Render the real Compose contract without starting Docker or any service."""
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker CLI with the Compose plugin is required for config rendering")
    available = subprocess.run(
        [docker, "compose", "version"], capture_output=True, text=True, check=False,
    )
    if available.returncode:
        pytest.skip("Docker Compose plugin is required for config rendering")
    env_file = tmp_path / "compose.env"
    env_file.write_text("".join(f"{key}={value}\n" for key, value in overrides.items()))
    result = subprocess.run(
        [docker, "compose", "--env-file", str(env_file), "-f",
         str(REPO_ROOT / "deploy/docker-compose.dev.yml"), "config", "--format", "json"],
        env={key: value for key, value in os.environ.items()
             if not key.startswith(("LOOM_", "COMPOSE_"))},
        capture_output=True, text=True, check=True,
    )
    return json.loads(result.stdout)["services"]


@pytest.mark.parametrize("proxy,expected", [
    (None, "http://egress-proxy:30443"),
    ("", ""),
    ("http://alternate-proxy:30443", "http://alternate-proxy:30443"),
])
def test_dev_gateway_proxy_can_select_default_direct_or_custom_routing(
    tmp_path: Path, proxy: str | None, expected: str,
) -> None:
    overrides = {} if proxy is None else {"LOOM_GW_EGRESS_PROXY_URL": proxy}
    services = _render_dev_compose(tmp_path, overrides)
    assert services["llm-gateway"]["environment"]["LOOM_GW_EGRESS_PROXY_URL"] == expected


def test_dev_google_key_reaches_gateway_container(tmp_path: Path) -> None:
    services = _render_dev_compose(tmp_path, {"LOOM_GW_GOOGLE_API_KEY": "test-google-key"})
    assert services["llm-gateway"]["environment"].get("LOOM_GW_GOOGLE_API_KEY") == "test-google-key"


def test_dev_browser_session_overrides_reach_service_container(tmp_path: Path) -> None:
    services = _render_dev_compose(tmp_path, {
        "LOOM_SVC_AUTH_SESSION_TTL_SEC": "1200",
        "LOOM_SVC_AUTH_LOGIN_CHALLENGE_TTL_SEC": "120",
        "LOOM_SVC_AUTH_RETURN_LOGIN_TOKEN": "true",
        "LOOM_SVC_TEAM_REGISTRATION_OPEN": "true",
    })
    environment = services["loom-service"]["environment"]
    assert environment.get("LOOM_SVC_AUTH_SESSION_TTL_SEC") == "1200"
    assert environment.get("LOOM_SVC_AUTH_LOGIN_CHALLENGE_TTL_SEC") == "120"
    assert environment.get("LOOM_SVC_AUTH_RETURN_LOGIN_TOKEN") == "true"
    assert environment.get("LOOM_SVC_TEAM_REGISTRATION_OPEN") == "true"


def test_dev_browser_defaults_keep_login_tokens_private_and_registration_gated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    services = _render_dev_compose(tmp_path, {})
    environment = services["loom-service"]["environment"]
    for key in list(os.environ):
        if key.startswith("LOOM_SVC_"):
            monkeypatch.delenv(key)
    settings = LoomServiceSettings(_env_file=None, **{
        key.removeprefix("LOOM_SVC_").lower(): value
        for key, value in environment.items() if key.startswith("LOOM_SVC_")
    })
    assert settings.auth_return_login_token is False
    assert settings.team_registration_open is False


def test_dev_compose_minio_uses_quay_pin() -> None:
    """Local Compose must not depend on Docker Hub minio/minio (#1969 / #1462)."""
    compose = (REPO_ROOT / "deploy" / "docker-compose.dev.yml").read_text()
    minio_block = compose.split("\n  minio:\n", 1)[1].split("\n\n  ", 1)[0]
    image_line = next(
        line for line in minio_block.splitlines() if line.strip().startswith("image:")
    )
    # Prefer the current Quay release digest (not the 2022 testcontainers pin):
    # laptop volumes already store newer xl headers.
    assert image_line.strip() == f"image: {MINIO_TLS_IMAGE}"
    assert "image: minio/minio" not in compose


def test_dev_compose_gateway_has_minio_credentials() -> None:
    """GatewaySettings requires MinIO keys; keep local Compose aligned (#1462)."""
    compose = (REPO_ROOT / "deploy" / "docker-compose.dev.yml").read_text()
    gateway_block = compose.split("\n  llm-gateway:\n", 1)[1].split(
        "\n\n  control-plane:\n",
        1,
    )[0]
    assert "LOOM_GW_MINIO_ENDPOINT: http://minio:9000" in gateway_block
    assert "LOOM_GW_MINIO_ACCESS_KEY:" in gateway_block
    assert "LOOM_GW_MINIO_SECRET_KEY:" in gateway_block


def test_web_dev_container_uses_lockfile_stable_bootstrap() -> None:
    """The dev web container must not rewrite bind-mounted package-lock.json."""
    compose = REPO_ROOT / "deploy" / "docker-compose.dev.yml"
    text = compose.read_text()
    web_block = text.split("\n  web:\n", 1)[1].split("\n\nvolumes:", 1)[0]
    command_line = next(
        line for line in web_block.splitlines() if line.strip().startswith("command:")
    )

    assert "image: node:20-slim" not in web_block
    assert re.search(r"(?m)^\s+image: node:20\.\d+\.\d+-slim$", web_block)
    assert "npm ci --no-audit --no-fund" in command_line
    assert "npm install" not in command_line


def test_web_image_runtime_files_are_readable_by_unprivileged_nginx() -> None:
    """The nginx entrypoint invokes scripts with `sh`, which requires read permission."""
    dockerfile = (REPO_ROOT / "deploy" / "Dockerfile.web").read_text(encoding="utf-8")

    assert "chmod 755 /docker-entrypoint.d/40-loom-frontend-config.sh" in dockerfile
    assert "chmod 644 /etc/nginx/conf.d/default.conf" in dockerfile
    assert "chmod +x /docker-entrypoint.d/40-loom-frontend-config.sh" not in dockerfile


def test_loom_service_dev_container_uses_internal_minio_endpoint() -> None:
    """Downloads are service-proxied, so dev does not need public MinIO URLs."""
    compose = REPO_ROOT / "deploy" / "docker-compose.dev.yml"
    text = compose.read_text()
    service_block = text.split("\n  loom-service:\n", 1)[1].split(
        "\n\n  worker:",
        1,
    )[0]

    assert "LOOM_SVC_MINIO_ENDPOINT: http://minio:9000" in service_block
    assert "LOOM_SVC_MINIO_PUBLIC_ENDPOINT" not in service_block
