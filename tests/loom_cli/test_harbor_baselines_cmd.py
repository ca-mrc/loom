"""CLI budget arguments, one-time bearer persistence and failure cleanup."""

from __future__ import annotations

import json
import stat

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig


@pytest.fixture
def cli(monkeypatch, tmp_path):
    from loom_cli import harbor_baselines_cmd as command

    requests = []
    response = {
        "status": 201,
        "json": {"id": "grant-id", "token": "loom_baseline_fixture", "model": "gpt-5.4"},
    }

    def handler(request):
        requests.append(request)
        return httpx.Response(response["status"], json=response["json"])

    monkeypatch.setattr(
        command,
        "require_logged_in",
        lambda: LoomConfig(server_url="https://test", auth_token="fixture"),
    )
    monkeypatch.setattr(
        command,
        "authed_client",
        lambda cfg: httpx.Client(base_url="https://test", transport=httpx.MockTransport(handler)),
    )
    path = tmp_path / "bearer"
    arguments = [
        "harbor-baselines",
        "create",
        "--provider-connection-id",
        "provider-id",
        "--model",
        "gpt-5.4",
        "--label",
        "pilot",
        "--ttl-seconds",
        "600",
        "--max-calls",
        "1000",
        "--max-output-tokens",
        "8192",
        "--max-total-tokens",
        "100000000",
        "--token-file",
        str(path),
    ]
    return arguments, path, requests, response


def test_create_writes_owner_only_bearer_without_printing_it(cli, capsys):
    arguments, path, requests, _ = cli
    assert main(arguments) == 0
    assert path.read_text() == "loom_baseline_fixture\n"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert "loom_baseline_fixture" not in capsys.readouterr().out
    sent = json.loads(requests[0].content)
    assert sent["max_calls"] == 1000 and "budget_usd" not in sent


def test_existing_secret_is_never_overwritten_and_no_grant_created(cli):
    arguments, path, requests, _ = cli
    path.write_text("existing user input")
    assert main(arguments) == 1
    assert path.read_text() == "existing user input" and not requests


def test_failed_authorization_removes_only_new_empty_file(cli):
    arguments, path, _, response = cli
    response.update(status=403, json={"detail": "missing submit scope"})
    assert main(arguments) == 1
    assert not path.exists()


def test_missing_explicit_budget_is_rejected_before_http(cli):
    arguments, path, requests, _ = cli
    index = arguments.index("--max-calls")
    del arguments[index : index + 2]
    with pytest.raises(SystemExit) as error:
        main(arguments)
    assert error.value.code == 2 and not requests and not path.exists()
