"""`loom models {list,test}` argparse + dispatch."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from loom_cli.__main__ import main


def test_list_shows_configured_local_providers(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    main(["config", "set", "local.vllm.base_url", "http://localhost:8000/v1"])
    main(["config", "set", "token.anthropic", "sk-ant-xxx"])
    capsys.readouterr()
    rc = main(["models", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "anthropic" in out
    assert "✓" in out
    assert "vllm" in out
    assert "http://localhost:8000/v1" in out


def test_list_empty_says_so(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["models", "list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "no token" in out  # remote providers all unset
    assert "(none" in out  # no local providers


def test_test_unknown_provider_errors(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["models", "test", "local/missing"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "missing" in err
    assert "loom config set local.missing.base_url" in err


def test_test_probes_models_endpoint(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    main(["config", "set", "local.vllm.base_url", "http://localhost:8000/v1"])
    capsys.readouterr()

    fake_resp = MagicMock()
    fake_resp.status_code = 200
    fake_resp.json.return_value = {
        "data": [
            {"id": "llama-3.1-8b"},
            {"id": "mistral-7b"},
        ],
    }
    with patch("loom_cli.models_cmd.httpx.get", return_value=fake_resp) as mock_get:
        rc = main(["models", "test", "local/vllm"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "✓ vllm reachable" in out
    assert "llama-3.1-8b" in out
    assert "mistral-7b" in out
    # Asserts the URL was built correctly (base_url + /models, no /v1/v1)
    mock_get.assert_called_once()
    url_arg = mock_get.call_args.args[0]
    assert url_arg == "http://localhost:8000/v1/models"


def test_test_handles_connection_error_with_hints(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    main(["config", "set", "local.vllm.base_url", "http://localhost:8000/v1"])
    capsys.readouterr()
    with patch(
        "loom_cli.models_cmd.httpx.get",
        side_effect=httpx.ConnectError("connection refused"),
    ):
        rc = main(["models", "test", "local/vllm"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "could not reach" in err
    assert "is the server running?" in err
    assert "vLLM" in err
    assert "ollama" in err


def test_test_handles_401_with_api_key_hint(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    main(["config", "set", "local.vllm.base_url", "http://localhost:8000/v1"])
    capsys.readouterr()
    fake_resp = MagicMock()
    fake_resp.status_code = 401
    fake_resp.text = '{"error":"unauthorized"}'
    with patch("loom_cli.models_cmd.httpx.get", return_value=fake_resp):
        rc = main(["models", "test", "local/vllm"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "401" in err
    assert "loom config set local.<name>.api_key" in err


def test_native_local_discovery_normalizes_prefix_and_uses_native_auth(monkeypatch):
    from loom_cli.config import LocalProvider
    from loom_cli.models_cmd import _probe

    seen = []
    def get(url, **kwargs):
        seen.append((url, kwargs["headers"]))
        return httpx.Response(200, json={"data": [{"id": "claude"}]})
    monkeypatch.setattr("loom_cli.models_cmd.httpx.get", get)
    assert _probe(LocalProvider(base_url="https://proxy.test/prefix", api_key="secret", api_dialect="anthropic")) == (0, ["claude"])
    assert seen == [("https://proxy.test/prefix/v1/models", {"x-api-key": "secret", "anthropic-version": "2023-06-01"})]
