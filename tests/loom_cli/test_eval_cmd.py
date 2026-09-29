"""`loom eval {run, batch {create,list,show,cancel}, trial {list,show,cancel}}`
against an httpx MockTransport. The route layer is exercised by the
integration tests in tests/integration; this file pins the CLI surface
(argparse, payload shape, output, error paths)."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom_cli.__main__ import main

_CONN_ID = "00000000-0000-0000-0000-0000000000aa"
_BATCH_ID = "00000000-0000-0000-0000-0000000000bb"
_TRIAL_ID = "00000000-0000-0000-0000-0000000000cc"


@pytest.fixture(autouse=True)
def _isolated_logged_in_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("MY_TOK", "loom_admin_test123456")
    main(
        [
            "auth",
            "login",
            "--server",
            "https://loom.test",
            "--token",
            "env:MY_TOK",
        ]
    )


class MockServer:
    """Same shape as the providers_cmd test MockServer — records every
    request and lets tests inject canned responses by (method, path)."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.canned: dict[tuple[str, str], httpx.Response] = {}

    def __getitem__(self, idx: int) -> httpx.Request:
        return self.requests[idx]

    def __len__(self) -> int:
        return len(self.requests)


@pytest.fixture
def mock_server(monkeypatch: pytest.MonkeyPatch) -> MockServer:
    """Patches eval_cmd's authed_client to hit a MockTransport. Also
    patches providers_cmd's authed_client because eval_cmd calls
    `_resolve_by_name` which uses providers_cmd's authed_client binding."""
    server = MockServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        key = (request.method, request.url.path)
        if key in server.canned:
            return server.canned[key]
        return httpx.Response(404, json={"detail": f"no mock for {key}"})

    transport = httpx.MockTransport(_handler)

    def _patched(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    # eval_cmd binds `authed_client` at import via `from … import …`,
    # so we patch its module-level name. Same for providers_cmd's
    # _resolve_by_name lookup path.
    monkeypatch.setattr("loom_cli.eval_cmd.authed_client", _patched)
    monkeypatch.setattr("loom_cli.providers_cmd.authed_client", _patched)
    return server


def _make_connection(
    *,
    name: str = "openai-prod",
    type_: str = "openai-compatible",
) -> dict[str, Any]:
    return {
        "id": _CONN_ID,
        "team_id": "00000000-0000-0000-0000-000000000000",
        "name": name,
        "type": type_,
        "base_url": "https://api.openai.com/v1",
        "upstream_host": "api.openai.com",
        "resolved_egress_ips": ["104.18.0.1"],
        "allowed_models": None,
        "status": "valid",
        "last_validated_at": None,
        "last_validation_error": None,
        "pricing_source": "tokens-only",
        "pricing_data": None,
        "created_by": "admin:abc",
        "created_at": "2026-06-16T00:00:00Z",
        "updated_at": "2026-06-16T00:00:00Z",
    }


def _stub_connection_lookup(server: MockServer, **kwargs: Any) -> dict[str, Any]:
    conn = _make_connection(**kwargs)
    server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200,
        json={"items": [conn]},
    )
    return conn


# ──────────────────────────────────────────────────────────────────────
# eval run
# ──────────────────────────────────────────────────────────────────────


def test_run_resolves_provider_then_posts_trial(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        201,
        json={
            "id": _TRIAL_ID,
            "task_id": "humaneval/HumanEval/0",
            "state": "queued",
            "agent_name": "litellm",
            "model": {"provider": "openai", "name": "gpt-4o"},
            "submitted_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "run",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--task",
            "humaneval/HumanEval/0",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "Submitted trial" in out
    assert _TRIAL_ID in out

    # 1 GET (provider lookup) + 1 POST (submit)
    assert len(mock_server) == 2
    get_req, post_req = mock_server[0], mock_server[1]
    assert get_req.method == "GET"
    assert get_req.url.path == "/api/v1/provider-connections"
    assert post_req.method == "POST"
    assert post_req.url.path == "/api/v1/trials"
    body = json.loads(post_req.content)
    assert body == {
        "task_id": "humaneval/HumanEval/0",
        "config": {
            "agent_name": "litellm",
            "agent_model": {
                "provider": "openai",
                "name": "gpt-4o",
                "source": "api",
            },
        },
        "provider_connection_id": _CONN_ID,
        "provider_model_id": "gpt-4o",
    }
    assert post_req.headers["Authorization"] == "Bearer loom_admin_test123456"


def test_run_summary_uses_submitted_task_when_response_omits_task_id(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        201,
        json={"id": _TRIAL_ID, "state": "queued"},
    )

    rc = main(
        [
            "eval",
            "run",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--task",
            "hello-world",
        ]
    )

    assert rc == 0
    out = capsys.readouterr().out
    assert "task_id:          hello-world" in out
    assert "task_id:          (unknown)" not in out


def test_run_with_anthropic_provider_maps_type_to_provider(
    mock_server: MockServer,
) -> None:
    """type='anthropic' → agent_model.provider='anthropic'."""
    _stub_connection_lookup(
        mock_server,
        name="anthropic-prod",
        type_="anthropic",
    )
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        201,
        json={"id": _TRIAL_ID, "task_id": "t", "state": "queued"},
    )
    rc = main(
        [
            "eval",
            "run",
            "--provider",
            "anthropic-prod",
            "--model",
            "claude-opus-4-7",
            "--agent",
            "litellm",
            "--task",
            "humaneval/HumanEval/0",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["config"]["agent_model"]["provider"] == "anthropic"
    assert body["config"]["agent_model"]["name"] == "claude-opus-4-7"


def test_batch_create_team_id_filters_provider_lookup_and_payload(
    mock_server: MockServer,
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "name": "admin-on-behalf",
            "expected_trial_count": 1,
            "state": "submitted",
        },
    )

    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--team-id",
            "team-b",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o-mini",
            "--agent",
            "litellm",
            "--benchmark",
            "skilllearnbench",
            "--name",
            "admin-on-behalf",
        ]
    )

    assert rc == 0
    lookup_req = mock_server[0]
    assert lookup_req.method == "GET"
    assert lookup_req.url.path == "/api/v1/provider-connections"
    assert lookup_req.url.params.get("team_id") == "team-b"

    create_req = mock_server[1]
    assert create_req.method == "POST"
    assert create_req.url.path == "/api/v1/batches"
    body = json.loads(create_req.content)
    assert body["team_id"] == "team-b"
    assert body["provider_connection_id"] == _CONN_ID


def test_run_agent_provider_override_wins_over_type_mapping(
    mock_server: MockServer,
) -> None:
    """Issue #69: operator with a Together-hosted openai-compatible
    connection passes `--agent-provider together`; the override wins
    over the type→provider default (which would have produced 'openai'
    and silently dropped the rate-card lookup)."""
    _stub_connection_lookup(
        mock_server,
        name="together-prod",
        type_="openai-compatible",
    )
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        201,
        json={"id": _TRIAL_ID, "task_id": "t", "state": "queued"},
    )
    rc = main(
        [
            "eval",
            "run",
            "--provider",
            "together-prod",
            "--model",
            "meta-llama/Llama-3.1-70B-Instruct",
            "--agent",
            "litellm",
            "--task",
            "humaneval/HumanEval/0",
            "--agent-provider",
            "together",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["config"]["agent_model"]["provider"] == "together"
    assert body["config"]["agent_model"]["name"] == ("meta-llama/Llama-3.1-70B-Instruct")


def test_batch_create_agent_provider_override(
    mock_server: MockServer,
) -> None:
    """Same override on batch create."""
    _stub_connection_lookup(
        mock_server,
        name="fireworks-prod",
        type_="custom",
    )
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "fireworks-prod",
            "--model",
            "accounts/fireworks/models/llama-v3p1-70b-instruct",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--name",
            "fw-run",
            "--agent-provider",
            "fireworks_ai",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["trial_config"]["agent_model"]["provider"] == "fireworks_ai"


def test_run_unknown_provider_returns_1(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Provider lookup miss → exit 1, no POST issued."""
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200,
        json={"items": []},
    )
    rc = main(
        [
            "eval",
            "run",
            "--provider",
            "nope",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--task",
            "t",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "no provider connection named 'nope'" in err
    # GET happened; no POST.
    assert len(mock_server) == 1


def test_run_server_error_surfaces_detail(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        400,
        json={"detail": "unknown agent_name 'bogus'"},
    )
    rc = main(
        [
            "eval",
            "run",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "bogus",
            "--task",
            "t",
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "HTTP 400" in err
    assert "unknown agent_name" in err


# ──────────────────────────────────────────────────────────────────────
# eval batch create
# ──────────────────────────────────────────────────────────────────────


def test_batch_create_with_benchmark_shortcut(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 5,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--name",
            "smoke-run",
        ]
    )
    assert rc == 0
    assert "Created batch 'smoke-run'" in capsys.readouterr().out

    body = json.loads(mock_server[1].content)
    assert body["name"] == "smoke-run"
    assert body["purpose"] == "evaluation"
    assert body["task_filter"] == {"benchmark_id": "humaneval"}
    assert body["trial_config"] == {
        "agent_name": "litellm",
        "agent_model": {
            "provider": "openai",
            "name": "gpt-4o",
            "source": "api",
        },
    }
    assert body["provider_connection_id"] == _CONN_ID
    assert body["provider_model_id"] == "gpt-4o"


def test_batch_create_workspace_staging_policy_tb21(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """#1263: --workspace-staging-policy rides in trial_config for the batch."""
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "terminus-2",
            "--benchmark",
            "strict-pass-39",
            "--workspace-staging-policy",
            "tb21",
            "--name",
            "harbor-isolated",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["trial_config"]["workspace_staging_policy_name"] == "tb21"
    assert body["trial_config"]["agent_name"] == "terminus-2"


def test_batch_create_multi_model_default_off_omits_block(
    mock_server: MockServer,
) -> None:
    """#1380: without --multi-model, trial_config stays single-model."""
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "glm-5.1",
            "--agent",
            "terminus-2",
            "--benchmark",
            "strict-pass-39",
            "--name",
            "single-model",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert "multi_model" not in body["trial_config"]


def test_batch_create_multi_model_flag_builds_trial_config(
    mock_server: MockServer,
) -> None:
    """#1380: --multi-model nests student/teacher switch config under trial_config."""
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "glm-5.1",
            "--agent",
            "terminus-2",
            "--benchmark",
            "strict-pass-39",
            "--multi-model",
            "--multi-model-secondary",
            "qwen-test",
            "--multi-model-switch-episode",
            "3",
            "--name",
            "dual-model",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["trial_config"]["multi_model"] == {
        "enabled": True,
        "policy": "student_teacher_student",
        "secondary_model": {
            "provider": "openai",
            "name": "qwen-test",
            "source": "api",
        },
        "switch_episode": 3,
    }


def test_batch_create_multi_model_beta_builds_trial_config(
    mock_server: MockServer,
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "glm-5.1",
            "--agent",
            "terminus-2",
            "--benchmark",
            "strict-pass-39",
            "--multi-model",
            "--multi-model-secondary",
            "qwen-test",
            "--multi-model-beta",
            "0.6",
            "--multi-model-seed",
            "42",
            "--name",
            "beta-mix",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["trial_config"]["multi_model"] == {
        "enabled": True,
        "policy": "beta_mixture",
        "beta": 0.6,
        "mix_seed": "42",
        "secondary_model": {
            "provider": "openai",
            "name": "qwen-test",
            "source": "api",
        },
    }


def test_batch_create_multi_model_turn_schedule_builds_trial_config(
    mock_server: MockServer,
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "trajectory_generation",
            "--provider",
            "openai-prod",
            "--model",
            "glm-5.1",
            "--agent",
            "terminus-2",
            "--benchmark",
            "strict-pass-39",
            "--multi-model",
            "--multi-model-secondary",
            "qwen-test",
            "--multi-model-step-start",
            "2",
            "--multi-model-step-end",
            "9",
            "--multi-model-seed",
            "turn-42",
            "--name",
            "turn-mix",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["trial_config"]["multi_model"] == {
        "enabled": True,
        "policy": "student_to_teacher_turns",
        "step_start": 2,
        "step_end": 9,
        "mix_seed": "turn-42",
        "secondary_model": {
            "provider": "openai",
            "name": "qwen-test",
            "source": "api",
        },
    }


def test_batch_create_multi_model_beta_rejects_k1(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "glm-5.1",
            "--agent",
            "terminus-2",
            "--benchmark",
            "strict-pass-39",
            "--multi-model",
            "--multi-model-secondary",
            "qwen-test",
            "--multi-model-beta",
            "0.6",
            "--multi-model-switch-episode",
            "3",
        ]
    )
    assert rc == 2
    assert "--multi-model-beta cannot be combined" in capsys.readouterr().err
    assert not any(req.url.path.endswith("/batches") for req in mock_server.requests)


def test_batch_create_multi_model_rejects_non_terminus(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--multi-model",
            "--multi-model-secondary",
            "other",
        ]
    )
    assert rc == 2
    assert "--multi-model requires --agent terminus-2" in capsys.readouterr().err
    assert mock_server.requests == []


def test_batch_create_with_combinations_json_routes_provider_per_combo(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    combinations = [
        {
            "agent_name": "terminus-2",
            "agent_model": {
                "provider": "openai",
                "name": "glm-5.1-thinking",
                "source": "api",
            },
            "provider_connection_id": "11111111-1111-4111-8111-111111111111",
            "provider_model_id": "glm-5.1-thinking",
            "n_per_task": 1,
            "label": "terminus-glm",
        },
        {
            "agent_name": "opencode",
            "agent_model": {
                "provider": "openai",
                "name": "qwen3.6-35b-a3b",
                "source": "api",
            },
            "provider_connection_id": "33333333-3333-4333-8333-333333333333",
            "provider_model_id": "qwen3.6-35b-a3b",
            "n_per_task": 2,
            "label": "opencode-qwen",
        },
    ]
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 300,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": combinations,
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )

    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--combinations-json",
            json.dumps(combinations),
            "--benchmark",
            "source-useful-frontier-5003",
            "--name",
            "matrix-run",
        ]
    )

    assert rc == 0
    assert "Created batch 'matrix-run'" in capsys.readouterr().out
    assert len(mock_server.requests) == 1
    req = mock_server[0]
    assert req.method == "POST"
    assert req.url.path == "/api/v1/batches"
    body = json.loads(req.content)
    assert body["trial_config"] == {}
    assert body["purpose"] == "evaluation"
    assert body["task_filter"] == {"benchmark_id": "source-useful-frontier-5003"}
    assert body["combinations"] == combinations
    assert "provider_connection_id" not in body
    assert "provider_model_id" not in body


def test_batch_create_with_task_set_shortcut(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 2,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    task_set_id = "ts/team-uuid/sample-tasks"
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "trajectory_generation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--task-set",
            task_set_id,
            "--name",
            "taskset-run",
        ]
    )
    assert rc == 0
    assert "Created batch 'taskset-run'" in capsys.readouterr().out

    body = json.loads(mock_server[1].content)
    assert body["name"] == "taskset-run"
    assert body["purpose"] == "trajectory_generation"
    assert body["task_filter"] == {"task_set_id": task_set_id}


def test_batch_create_refuses_stopped_storage_preflight_without_override(
    tmp_path: Path,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    storage = tmp_path / "minio-storage-preflight.json"
    storage.write_text(
        json.dumps({
            "outcome": "stop",
            "filesystem": {"free_percent": 8.0, "free_bytes": 8 * 1024**3},
            "thresholds": {"stop_free_percent": 15.0},
            "checks": [
                {
                    "name": "minio-data-free-space",
                    "outcome": "stop",
                    "detail": "free space 8.0% is below stop threshold 15.0%",
                }
            ],
        }),
        encoding="utf-8",
    )

    rc = main([
        "eval",
        "batch",
        "create",
        "--purpose",
        "evaluation",
        "--agent",
        "oracle",
        "--benchmark",
        "source-useful-frontier",
        "--name",
        "big-batch",
        "--storage-preflight-evidence",
        str(storage),
    ])

    assert rc == 1
    assert "explicit storage override" in capsys.readouterr().err
    assert mock_server.requests == []


def test_batch_create_allows_explicit_storage_preflight_override(
    tmp_path: Path,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    storage = tmp_path / "minio-storage-preflight.json"
    storage.write_text(
        json.dumps({
            "outcome": "stop",
            "filesystem": {"free_percent": 8.0, "free_bytes": 8 * 1024**3},
            "thresholds": {"stop_free_percent": 15.0},
            "checks": [],
        }),
        encoding="utf-8",
    )
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )

    rc = main([
        "eval",
        "batch",
        "create",
        "--purpose",
        "evaluation",
        "--agent",
        "oracle",
        "--benchmark",
        "source-useful-frontier",
        "--name",
        "big-batch",
        "--storage-preflight-evidence",
        str(storage),
        "--override-storage-preflight-stop",
    ])

    assert rc == 0
    assert "Created batch" in capsys.readouterr().out
    assert len(mock_server.requests) == 1


def test_batch_create_oracle_does_not_require_provider_or_model(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )

    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--agent",
            "oracle",
            "--benchmark",
            "qa275-custom",
            "--name",
            "oracle-smoke",
        ]
    )

    assert rc == 0
    out = capsys.readouterr().out
    assert "Created batch" in out
    assert "oracle-smoke" in out
    assert len(mock_server.requests) == 1
    request = mock_server[0]
    assert request.method == "POST"
    assert request.url.path == "/api/v1/batches"
    body = json.loads(request.content)
    assert body == {
        "name": "oracle-smoke",
        "purpose": "evaluation",
        "task_filter": {"benchmark_id": "qa275-custom"},
        "trial_config": {"agent_name": "oracle", "agent_model": None},
    }


def test_batch_create_model_agent_requires_provider_and_model(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--name",
            "missing-model",
        ]
    )

    assert rc == 2
    assert "requires --provider and --model" in capsys.readouterr().err
    assert mock_server.requests == []


def test_batch_create_resolves_agent_from_server_when_local_launcher_missing(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Service-mode submissions should not depend on local launcher install."""
    monkeypatch.setattr("loom_cli.eval_cmd.get_agent", lambda _name: None)
    mock_server.canned[("GET", "/api/v1/agents")] = httpx.Response(
        200,
        json={
            "items": [
                {
                    "name": "codex",
                    "needs_model": True,
                    "service_mode_ready": True,
                },
            ],
        },
    )
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-07-01T00:00:00Z",
        },
    )

    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "glm-5.1-thinking",
            "--agent",
            "codex",
            "--benchmark",
            "mbpp",
            "--name",
            "codex-smoke",
        ]
    )

    assert rc == 0
    assert [request.url.path for request in mock_server.requests] == [
        "/api/v1/agents",
        "/api/v1/provider-connections",
        "/api/v1/batches",
    ]
    body = json.loads(mock_server.requests[-1].content)
    assert body["trial_config"]["agent_name"] == "codex"
    assert body["trial_config"]["agent_model"]["name"] == "glm-5.1-thinking"
    assert "Created batch" in capsys.readouterr().out


def test_batch_create_summary_uses_submitted_name_when_response_omits_name(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 5,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--name",
            "smoke-run",
        ]
    )

    assert rc == 0
    out = capsys.readouterr().out
    assert "name:                  smoke-run" in out
    assert "name:                  (unset)" not in out


def test_batch_create_allows_omitted_name_and_prints_generated_name(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "name": "humaneval | litellm/gpt-4o x1",
            "description": "Tasks: humaneval; subset: all tasks.",
            "expected_trial_count": 5,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
        ]
    )

    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert "name" not in body
    out = capsys.readouterr().out
    assert "Created batch 'humaneval | litellm/gpt-4o x1'" in out
    assert "name:                  humaneval | litellm/gpt-4o x1" in out


def test_batch_create_task_filter_json(
    mock_server: MockServer,
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 3,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    filt = '{"benchmark_id":"humaneval","subset_kind":"first_n","n":3}'
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--task-filter",
            filt,
            "--name",
            "first3",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["purpose"] == "evaluation"
    assert body["task_filter"] == {
        "benchmark_id": "humaneval",
        "subset_kind": "first_n",
        "n": 3,
    }


def test_batch_create_task_filter_at_path(
    mock_server: MockServer,
    tmp_path: Path,
) -> None:
    """`--task-filter @path/to/file.json` reads from disk."""
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    f = tmp_path / "filt.json"
    f.write_text('{"task_ids":["humaneval/HumanEval/0"]}')
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--task-filter",
            f"@{f}",
            "--name",
            "n",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["task_filter"] == {"task_ids": ["humaneval/HumanEval/0"]}


def test_batch_create_resource_requests_at_path(mock_server: MockServer, tmp_path: Path) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={"batch_id": _BATCH_ID, "expected_trial_count": 1, "n_per_task": 1,
                   "backend": "nebius", "state": "submitted", "combinations": [],
                   "created_at": "2026-09-15T00:00:00Z"},
    )
    requests = {"local/task": {"task_revision_sha256": "sha256:" + "a" * 64,
        "requests": {"controller": {"cpu_millis": 1000, "memory_mib": 2048,
                                    "ephemeral_storage_mib": 1024}}}}
    request_file = tmp_path / "requests.json"
    request_file.write_text(json.dumps(requests))
    rc = main(["eval", "batch", "create", "--purpose", "evaluation",
               "--provider", "openai-prod", "--model", "gpt-4o",
               "--agent", "terminus-2", "--backend", "nebius", "--task-filter",
               '{"task_ids":["local/task"]}', "--task-resource-requests", f"@{request_file}"])
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["task_resource_requests"] == requests
    assert "task_resource_requests" not in body["trial_config"]


def test_batch_create_invalid_resource_requests_flag(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["eval", "batch", "create", "--task-resource-requests", "[]"])
    assert exc.value.code == 2
    assert "--task-resource-requests" in capsys.readouterr().err


def test_batch_create_benchmark_and_task_filter_mutually_exclusive(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--task-filter",
            '{"benchmark_id":"humaneval"}',
            "--name",
            "n",
        ]
    )
    assert rc == 2
    assert "mutually exclusive" in capsys.readouterr().err
    # Connection lookup happened, but no POST.
    paths = [(r.method, r.url.path) for r in mock_server.requests]
    assert ("POST", "/api/v1/batches") not in paths


def test_batch_create_evaluation_rejects_task_set(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--task-set",
            "ts/team-uuid/sample-tasks",
            "--name",
            "n",
        ]
    )
    assert rc == 2
    assert "native benchmarks" in capsys.readouterr().err
    paths = [(r.method, r.url.path) for r in mock_server.requests]
    assert ("POST", "/api/v1/batches") not in paths


def test_batch_create_verifier_env_mode_shared_is_sent(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--verifier-env-mode",
            "shared",
            "--name",
            "shared-grade",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["trial_config"]["verifier_env_mode"] == "shared"
    capsys.readouterr()


def test_batch_create_rejects_verifier_env_mode_with_skip_verifier(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "trajectory_generation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--skip-verifier",
            "--verifier-env-mode",
            "shared",
            "--name",
            "n",
        ]
    )
    assert rc == 2
    assert "verifier-env-mode" in capsys.readouterr().err
    paths = [(r.method, r.url.path) for r in mock_server.requests]
    assert ("POST", "/api/v1/batches") not in paths


def test_batch_create_evaluation_rejects_skip_verifier(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--skip-verifier",
            "--name",
            "n",
        ]
    )
    assert rc == 2
    assert "skip-verifier" in capsys.readouterr().err.lower()
    paths = [(r.method, r.url.path) for r in mock_server.requests]
    assert ("POST", "/api/v1/batches") not in paths


def test_batch_create_task_set_and_benchmark_mutually_exclusive(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "trajectory_generation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--task-set",
            "ts/team-uuid/sample-tasks",
            "--name",
            "n",
        ]
    )
    assert rc == 2
    assert "mutually exclusive" in capsys.readouterr().err
    paths = [(r.method, r.url.path) for r in mock_server.requests]
    assert ("POST", "/api/v1/batches") not in paths


def test_batch_create_requires_benchmark_or_filter(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--name",
            "n",
        ]
    )
    assert rc == 2
    assert (
        "one of --benchmark, --task-set, or --task-filter"
        in capsys.readouterr().err
    )


def test_batch_create_invalid_task_filter_json_rejected_at_argparse(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "eval",
                "batch",
                "create",
                "--purpose",
                "evaluation",
                "--provider",
                "openai-prod",
                "--model",
                "gpt-4o",
                "--agent",
                "litellm",
                "--task-filter",
                "{not json",
                "--name",
                "n",
            ]
        )
    assert exc.value.code == 2
    assert "invalid JSON" in capsys.readouterr().err


def test_batch_create_forwards_optional_fields(
    mock_server: MockServer,
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201,
        json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 10,
            "n_per_task": 2,
            "backend": "fake",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main(
        [
            "eval",
            "batch",
            "create",
            "--purpose",
            "evaluation",
            "--provider",
            "openai-prod",
            "--model",
            "gpt-4o",
            "--agent",
            "litellm",
            "--benchmark",
            "humaneval",
            "--name",
            "n",
            "--description",
            "smoke",
            "--n-per-task",
            "2",
            "--backend",
            "fake",
        ]
    )
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["description"] == "smoke"
    assert body["n_per_task"] == 2
    assert body["backend"] == "fake"


_BACKEND_CREATE_ARGV = [
    "eval", "batch", "create", "--purpose", "evaluation",
    "--provider", "openai-prod", "--model", "gpt-4o",
    "--agent", "litellm", "--benchmark", "humaneval",
]


def test_batch_create_help_does_not_advertise_backend(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["eval", "batch", "create", "--help"])
    assert excinfo.value.code in (0, None)
    assert "--backend" not in capsys.readouterr().out


@pytest.mark.parametrize("backend", ["nebius", "docker"])
def test_batch_create_backend_flag_is_deprecated_but_forwarded(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
    backend: str,
) -> None:
    """The hidden legacy flag warns and forwards the value: the service is the
    single authority (hosted accepts `nebius` and rejects the rest; a disposable
    local stack defaults to a worker backend, so dropping `nebius` here would
    silently reinterpret it)."""
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={"batch_id": _BATCH_ID, "expected_trial_count": 1, "state": "submitted"},
    )
    assert main([*_BACKEND_CREATE_ARGV, "--backend", backend]) == 0
    payload = json.loads(next(r.content for r in mock_server.requests if r.method == "POST"))
    assert payload["backend"] == backend
    assert "--backend is deprecated" in capsys.readouterr().err


def test_batch_create_without_backend_flag_sends_no_backend(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={"batch_id": _BATCH_ID, "expected_trial_count": 1, "state": "submitted"},
    )
    assert main(_BACKEND_CREATE_ARGV) == 0
    payload = json.loads(next(r.content for r in mock_server.requests if r.method == "POST"))
    assert "backend" not in payload
    assert "deprecated" not in capsys.readouterr().err


def test_batch_show_prints_historical_backend_without_inventing_one(
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_cli.eval_cmd import _print_batch_summary

    _print_batch_summary({"batch_id": _BATCH_ID, "backend": "docker"})
    assert "backend:               docker" in capsys.readouterr().out
    _print_batch_summary({"batch_id": _BATCH_ID})
    assert "backend:               (unknown)" in capsys.readouterr().out


@pytest.mark.parametrize("agent_version", [None, "harbor-0.18.0-abc123"])
def test_batch_create_serializes_selected_agent_version(mock_server, agent_version):
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={"batch_id": _BATCH_ID, "expected_trial_count": 1, "state": "submitted"},
    )
    argv = ["eval", "batch", "create", "--purpose", "evaluation",
            "--provider", "openai-prod", "--model", "gpt-4o",
            "--agent", "terminus-2", "--benchmark", "terminal-bench", "--backend", "nebius"]
    if agent_version is not None:
        argv += ["--agent-version", agent_version]
    assert main(argv) == 0
    payload = json.loads(next(r.content for r in mock_server.requests if r.method == "POST"))
    assert payload["trial_config"].get("agent_version") == agent_version
    if agent_version is None:
        assert "agent_version" not in payload["trial_config"]


def test_batch_create_has_no_required_worker_pool_flag(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """#1109: pool coverage is operator-only; not on user eval create."""
    with pytest.raises(SystemExit) as excinfo:
        main(["eval", "batch", "create",
            "--purpose",
            "evaluation", "--help"])
    assert excinfo.value.code in (0, None)
    help_text = capsys.readouterr().out
    assert "--required-worker-pool" not in help_text


# ──────────────────────────────────────────────────────────────────────
# eval batch list / show / cancel
# ──────────────────────────────────────────────────────────────────────


def test_batch_list_empty(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200,
        json={"items": [], "next_cursor": None},
    )
    rc = main(["eval", "batch", "list"])
    assert rc == 0
    assert "no batches" in capsys.readouterr().out


def test_batch_list_table_and_state_filter_param(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200,
        json={
            "items": [
                {
                    "id": _BATCH_ID,
                    "team_id": "x",
                    "name": "b1",
                    "description": None,
                    "purpose": "evaluation",
                    "task_filter": {},
                    "trial_config": {},
                    "state": "running",
                    "result_status": None,
                    "created_at": "2026-06-16T00:00:00Z",
                    "finished_at": None,
                    "created_by_token_prefix": "abc",
                    "expected_trial_count": 7,
                    "n_per_task": 1,
                    "backend": "docker",
                    "combinations": [],
                }
            ],
            "next_cursor": None,
        },
    )
    rc = main(["eval", "batch", "list", "--state", "running,queued"])
    assert rc == 0
    out = capsys.readouterr().out
    assert _BATCH_ID in out
    assert "b1" in out
    # Verify state param forwarded to server.
    assert mock_server[0].url.params.get("state") == "running,queued"


def test_batch_list_forwards_structured_filter_params(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200,
        json={"items": [], "next_cursor": None},
    )
    rc = main(
        [
            "eval",
            "batch",
            "list",
            "--q",
            "needle",
            "--benchmark",
            "skilllearnbench",
            "--agent",
            "codex",
            "--model-provider",
            "openai",
            "--model",
            "qwen3.6-35b-a3b",
            "--provider-connection-id",
            "provider-1",
            "--provider-model-id",
            "qwen3.6-35b-a3b",
            "--state",
            "finished",
            "--limit",
            "25",
        ]
    )

    assert rc == 0
    params = mock_server[0].url.params
    assert params.get("q") == "needle"
    assert params.get("benchmark_id") == "skilllearnbench"
    assert params.get("agent_name") == "codex"
    assert params.get("model_provider") == "openai"
    assert params.get("model_name") == "qwen3.6-35b-a3b"
    assert params.get("provider_connection_id") == "provider-1"
    assert params.get("provider_model_id") == "qwen3.6-35b-a3b"
    assert params.get("state") == "finished"
    assert params.get("limit") == "25"
    assert "no batches" in capsys.readouterr().out


def test_batch_list_warns_on_truncation(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #67: non-null `next_cursor` triggers a stderr hint so
    operators know they're seeing a partial result. Stdout still
    contains just the table so `| awk` pipelines keep working."""
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200,
        json={
            "items": [
                {
                    "id": _BATCH_ID,
                    "team_id": "x",
                    "name": "b1",
                    "description": None,
                    "purpose": "evaluation",
                    "task_filter": {},
                    "trial_config": {},
                    "state": "running",
                    "result_status": None,
                    "created_at": "2026-06-16T00:00:00Z",
                    "finished_at": None,
                    "created_by_token_prefix": "abc",
                    "expected_trial_count": 7,
                    "n_per_task": 1,
                    "backend": "docker",
                    "combinations": [],
                }
            ],
            "next_cursor": "opaque-cursor-token",
        },
    )
    rc = main(["eval", "batch", "list"])
    assert rc == 0
    out_err = capsys.readouterr()
    # Hint on stderr only.
    assert "more" in out_err.err
    assert "--limit" in out_err.err
    assert "more" not in out_err.out  # stdout stays clean


def test_batch_list_no_warning_when_not_truncated(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`next_cursor=None` means full result; no hint."""
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200,
        json={
            "items": [
                {
                    "id": _BATCH_ID,
                    "team_id": "x",
                    "name": "b1",
                    "description": None,
                    "purpose": "evaluation",
                    "task_filter": {},
                    "trial_config": {},
                    "state": "running",
                    "result_status": None,
                    "created_at": "2026-06-16T00:00:00Z",
                    "finished_at": None,
                    "created_by_token_prefix": "abc",
                    "expected_trial_count": 7,
                    "n_per_task": 1,
                    "backend": "docker",
                    "combinations": [],
                }
            ],
            "next_cursor": None,
        },
    )
    rc = main(["eval", "batch", "list"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "more" not in err


def test_trial_list_warns_on_truncation(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #67: matching hint on trial list."""
    mock_server.canned[("GET", "/api/v1/trials")] = httpx.Response(
        200,
        json={
            "items": [
                {
                    "id": _TRIAL_ID,
                    "task_id": "t",
                    "team_id": "x",
                    "state": "succeeded",
                    "failure_reason": None,
                    "submitted_at": "2026-06-16T00:00:00Z",
                    "started_at": None,
                    "finished_at": None,
                    "attempt_count": 1,
                    "aggregate_reward": 0.5,
                    "total_prompt_tokens": 12,
                    "total_completion_tokens": 3,
                    "llm_calls_count": 1,
                    "agent_name": "litellm",
                    "model": None,
                }
            ],
            "next_cursor": "opaque-cursor-token",
        },
    )
    rc = main(["eval", "trial", "list"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "more" in err
    assert "--task-id" in err  # trial list mentions task-id filter


def test_batch_show_renders_rollup(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}")] = httpx.Response(
        200,
        json={
            "id": _BATCH_ID,
            "team_id": "x",
            "name": "b1",
            "description": None,
            "purpose": "evaluation",
            "task_filter": {},
            "trial_config": {},
            "state": "succeeded",
            "result_status": "ok",
            "created_at": "2026-06-16T00:00:00Z",
            "finished_at": None,
            "created_by_token_prefix": "abc",
            "expected_trial_count": 4,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "trial_summary": {
                "queued": 0,
                "claimed": 0,
                "running": 0,
                "succeeded": 3,
                "failed": 1,
                "cancelled": 0,
            },
            "aggregate_reward": 0.75,
            "total_prompt_tokens": 120,
            "total_completion_tokens": 45,
            "llm_calls_count": 3,
        },
    )
    rc = main(["eval", "batch", "show", _BATCH_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert "s=3" in out
    assert "f=1" in out
    assert "0.75" in out
    assert "llm_calls:             3" in out
    assert "prompt=120 completion=45" in out


def test_batch_show_formats_timestamps_in_local_timezone(
    monkeypatch: pytest.MonkeyPatch,
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    original_tz = os.environ.get("TZ")
    monkeypatch.setenv("TZ", "America/Toronto")
    if hasattr(time, "tzset"):
        time.tzset()
    try:
        mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}")] = httpx.Response(
            200,
            json={
                "id": _BATCH_ID,
                "team_id": "x",
                "name": "local-time-batch",
                "description": None,
                "purpose": "evaluation",
                "task_filter": {},
                "trial_config": {},
                "state": "finished",
                "result_status": "succeeded",
                "created_at": "2026-06-27T03:04:54Z",
                "finished_at": "2026-06-27T03:09:00Z",
                "created_by_token_prefix": "abc",
                "expected_trial_count": 1,
                "n_per_task": 1,
                "backend": "docker",
                "combinations": [],
            },
        )

        rc = main(["eval", "batch", "show", _BATCH_ID])

        assert rc == 0
        out = capsys.readouterr().out
        assert "created_at:            2026-06-26 23:04 EDT" in out
        assert "finished_at:           2026-06-26 23:09 EDT" in out
        assert "2026-06-27T03:04:54Z" not in out
    finally:
        if original_tz is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", original_tz)
        if hasattr(time, "tzset"):
            time.tzset()


def test_batch_show_warns_on_zero_call_real_provider_evidence(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}")] = httpx.Response(
        200,
        json={
            "id": _BATCH_ID,
            "team_id": "x",
            "name": "zero-call",
            "description": None,
            "purpose": "evaluation",
            "task_filter": {},
            "trial_config": {
                "agent_name": "codex",
                "agent_model": {"provider": "openai", "name": "qwen"},
            },
            "state": "finished",
            "result_status": "partial_failed",
            "created_at": "2026-06-16T00:00:00Z",
            "finished_at": None,
            "created_by_token_prefix": "abc",
            "expected_trial_count": 2,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "trial_summary": {
                "queued": 0,
                "claimed": 0,
                "running": 0,
                "succeeded": 1,
                "failed": 1,
                "cancelled": 0,
            },
            "aggregate_reward": 0.0,
            "total_prompt_tokens": 0,
            "total_completion_tokens": 0,
            "llm_calls_count": 0,
            "no_call_trial_count": 2,
            "llm_evidence_status": "no_calls_invalid",
        },
    )
    rc = main(["eval", "batch", "show", _BATCH_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert "llm_evidence_status: no_calls_invalid" in out
    assert "no_call_trials:      2" in out
    assert "invalid benchmark evidence" in out


def test_batch_show_renders_fanout_failure(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}")] = httpx.Response(
        200,
        json={
            "id": _BATCH_ID,
            "team_id": "x",
            "name": "b1",
            "description": None,
            "purpose": "evaluation",
            "task_filter": {},
            "trial_config": {},
            "state": "finished",
            "result_status": "all_failed",
            "failure_reason": "fanout_submit_failed",
            "failure_message": "task t1 submit failed: HTTP 403: blocked",
            "fanout_errors": [{"task_id": "t1", "status_code": 403}],
            "created_at": "2026-06-16T00:00:00Z",
            "finished_at": None,
            "created_by_token_prefix": "abc",
            "expected_trial_count": 0,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "trial_summary": {
                "queued": 0,
                "claimed": 0,
                "running": 0,
                "succeeded": 0,
                "failed": 0,
                "cancelled": 0,
            },
            "aggregate_reward": None,
            "total_prompt_tokens": 0,
            "total_completion_tokens": 0,
            "llm_calls_count": 0,
        },
    )
    rc = main(["eval", "batch", "show", _BATCH_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert "fanout_submit_failed" in out
    assert "HTTP 403: blocked" in out
    assert "fanout_errors:         1" in out


def test_batch_debug_fetches_machine_readable_evidence(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "schema_version": "1",
        "entity": {"type": "batch", "id": _BATCH_ID},
        "lifecycle": {
            "state": "finished",
            "terminal_status": "all_failed",
            "created_at": "2026-06-16T00:00:00Z",
            "finished_at": "2026-06-16T00:01:00Z",
        },
        "failure": {
            "reason_code": "batch.fanout_submit_failed",
            "category": "submit",
            "attribution": "platform",
            "message": "task t1 submit failed",
        },
        "next_actions": ["Inspect batch fan-out errors."],
    }
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}/debug")] = httpx.Response(
        200, json=payload
    )

    rc = main(["eval", "batch", "debug", _BATCH_ID, "--format", "json"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["failure"] == (payload["failure"])
    assert mock_server[0].url.path == f"/api/v1/batches/{_BATCH_ID}/debug"


def test_batch_debug_renders_text_summary(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}/debug")] = httpx.Response(
        200,
        json={
            "schema_version": "1",
            "entity": {"type": "batch", "id": _BATCH_ID},
            "lifecycle": {"state": "finished", "terminal_status": "all_failed"},
            "failure": {
                "reason_code": "batch.fanout_submit_failed",
                "category": "submit",
                "attribution": "platform",
                "message": "task t1 submit failed",
            },
            "provider": {"llm_calls_count": 0},
            "next_actions": ["Inspect batch fan-out errors."],
        },
    )

    rc = main(["eval", "batch", "debug", _BATCH_ID])

    assert rc == 0
    out = capsys.readouterr().out
    assert "debug_evidence:        batch" in out
    assert "batch.fanout_submit_failed" in out
    assert "Inspect batch fan-out errors." in out


def test_batch_rerun_plan_outputs_task_id_list(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}/rerun-plan")] = (
        httpx.Response(
            200,
            json={
                "schema_version": "1",
                "batch_id": _BATCH_ID,
                "supplemental_task_ids": [
                    "source-useful/task-a",
                    "source-useful/task-b",
                ],
                "summary": {
                    "auto_safe": 2,
                    "operator_approval": 1,
                    "not_rerunnable": 3,
                    "already_covered": 0,
                    "selected_final_trials": 6,
                },
                "auto_safe": [],
                "operator_approval": [],
                "not_rerunnable": [],
                "final_trial_selection": [],
            },
        )
    )

    rc = main(
        [
            "eval",
            "batch",
            "rerun-plan",
            _BATCH_ID,
            "--task-id",
            "source-useful/task-b",
            "--task-id",
            "source-useful/task-a",
            "--format",
            "task-ids",
        ]
    )

    assert rc == 0
    assert capsys.readouterr().out == (
        "source-useful/task-a\nsource-useful/task-b\n"
    )
    assert mock_server[0].url.path == f"/api/v1/batches/{_BATCH_ID}/rerun-plan"
    assert mock_server[0].url.params.get_list("task_id") == [
        "source-useful/task-b",
        "source-useful/task-a",
    ]


def test_batch_rerun_plan_renders_text_summary(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}/rerun-plan")] = (
        httpx.Response(
            200,
            json={
                "schema_version": "1",
                "batch_id": _BATCH_ID,
                "supplemental_task_ids": ["source-useful/task-a"],
                "summary": {
                    "auto_safe": 1,
                    "operator_approval": 1,
                    "not_rerunnable": 2,
                    "already_covered": 0,
                    "selected_final_trials": 4,
                },
                "auto_safe": [
                    {
                        "task_id": "source-useful/task-a",
                        "failure_class": "platform_failure",
                        "root_cause": "provider_transport",
                    }
                ],
                "operator_approval": [
                    {
                        "task_id": "source-useful/task-b",
                        "failure_class": "verifier_failure",
                        "root_cause": "verifier_harness",
                    }
                ],
                "not_rerunnable": [
                    {
                        "task_id": "source-useful/task-score-zero",
                        "failure_class": "score_failure",
                        "platform_outcome": "success",
                    }
                ],
                "final_trial_selection": [],
            },
        )
    )

    rc = main(["eval", "batch", "rerun-plan", _BATCH_ID])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Supplemental rerun plan" in out
    assert "auto_safe:           1" in out
    assert "operator_approval:   1" in out
    assert "not_rerunnable:      2" in out
    assert "source-useful/task-a" in out


def test_batch_rerun_plan_renders_supplemental_coordinates(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}/rerun-plan")] = (
        httpx.Response(
            200,
            json={
                "schema_version": "1",
                "batch_id": _BATCH_ID,
                "supplemental_task_ids": ["source-useful/task-a"],
                "supplemental_coordinates": [
                    {
                        "task_id": "source-useful/task-a",
                        "sample_idx": 0,
                        "combination_idx": 0,
                    },
                    {
                        "task_id": "source-useful/task-a",
                        "sample_idx": 2,
                        "combination_idx": 1,
                    },
                ],
                "summary": {
                    "auto_safe": 2,
                    "operator_approval": 0,
                    "not_rerunnable": 0,
                    "already_covered": 0,
                    "selected_final_trials": 2,
                },
                "auto_safe": [],
                "operator_approval": [],
                "not_rerunnable": [],
                "final_trial_selection": [],
            },
        )
    )

    rc = main(["eval", "batch", "rerun-plan", _BATCH_ID])

    assert rc == 0
    out = capsys.readouterr().out
    assert "supplemental_coordinates:" in out
    assert "source-useful/task-a sample=0 combination=0" in out
    assert "source-useful/task-a sample=2 combination=1" in out


def test_diagnose_batch_fetches_machine_readable_report(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "schema_version": "1",
        "entity": {"type": "batch", "id": _BATCH_ID},
        "summary": "The batch failed because most trials hit gateway errors.",
        "primary_cause": {
            "reason_code": "trial.gateway_error",
            "category": "gateway",
            "attribution": "provider",
            "confidence": "high",
            "affected_trials": 148,
            "affected_ratio": 0.93,
        },
        "impact": "The aggregate score is not reliable.",
        "evidence": ["148/164 trials matched trial.gateway_error"],
        "next_actions": [
            {
                "label": "Run provider preflight",
                "kind": "cli_command",
                "command": "loom providers models --preflight gpt-4o-mini",
            }
        ],
        "reason_clusters": [
            {
                "reason_code": "trial.gateway_error",
                "count": 148,
                "representative_trial_id": _TRIAL_ID,
            }
        ],
    }
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}/diagnosis")] = httpx.Response(
        200, json=payload
    )

    rc = main(["eval", "diagnose", "batch", _BATCH_ID, "--format", "json"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["primary_cause"] == (payload["primary_cause"])
    assert mock_server[0].url.path == (f"/api/v1/batches/{_BATCH_ID}/diagnosis")


def test_diagnose_batch_renders_text_report(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}/diagnosis")] = httpx.Response(
        200,
        json={
            "schema_version": "1",
            "entity": {"type": "batch", "id": _BATCH_ID},
            "summary": (
                "The batch failed because most failed child trials hit "
                "provider gateway errors before scoring."
            ),
            "primary_cause": {
                "reason_code": "trial.gateway_error",
                "category": "gateway",
                "attribution": "provider",
                "confidence": "medium",
                "affected_trials": 3,
                "affected_ratio": 0.75,
            },
            "impact": "The aggregate score is not reliable.",
            "evidence": ["3/4 affected trial(s) matched trial.gateway_error"],
            "next_actions": [
                {
                    "label": "Rerun failed trials after the provider path is healthy",
                    "kind": "web_action",
                    "action": "rerun_failed",
                }
            ],
            "reason_clusters": [
                {
                    "reason_code": "trial.gateway_error",
                    "count": 3,
                    "affected_ratio": 0.75,
                    "representative_trial_id": _TRIAL_ID,
                }
            ],
        },
    )

    rc = main(["eval", "diagnose", "batch", _BATCH_ID])

    assert rc == 0
    out = capsys.readouterr().out
    assert "Diagnosis: batch" in out
    assert "trial.gateway_error" in out
    assert "The aggregate score is not reliable." in out
    assert "Reason clusters:" in out
    assert "Rerun failed trials" in out


def test_batch_cancel(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", f"/api/v1/batches/{_BATCH_ID}/cancel")] = httpx.Response(
        200, json={"batch_id": _BATCH_ID, "state": "cancelled"}
    )
    rc = main(["eval", "batch", "cancel", _BATCH_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Cancelled batch" in out
    assert "loom eval trial cancel" in out


def test_trial_cancel_running(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}")] = httpx.Response(
        200,
        json={
            "id": _TRIAL_ID,
            "state": "running",
            "batch_id": _BATCH_ID,
            "task_id": "demo/task",
        },
    )
    mock_server.canned[("POST", f"/api/v1/trials/{_TRIAL_ID}/cancel")] = httpx.Response(
        200,
        json={"trial_id": _TRIAL_ID, "state": "cancelled"},
    )
    rc = main(["eval", "trial", "cancel", _TRIAL_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert f"Cancelled trial {_TRIAL_ID}" in out
    assert "running → cancelled" in out
    assert "Sibling trials" in out
    assert "eligible completed attempt" in out
    assert "cancelled does not select for export" in out
    assert "Native agent timeouts with completed verifier scores and complete evidence" in out
    assert "preserving their failed/timed_out metadata" in out
    assert f"loom eval batch rerun-plan {_BATCH_ID}" in out
    methods = [req.method for req in mock_server.requests]
    assert methods == ["GET", "POST"]


def test_trial_cancel_already_terminal_is_noop(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}")] = httpx.Response(
        200,
        json={
            "id": _TRIAL_ID,
            "state": "succeeded",
            "batch_id": _BATCH_ID,
        },
    )
    rc = main(["eval", "trial", "cancel", _TRIAL_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert "already succeeded" in out
    assert "no cancel needed" in out
    assert [("GET", f"/api/v1/trials/{_TRIAL_ID}")] == [
        (req.method, req.url.path) for req in mock_server.requests
    ]


def test_trial_cancel_forbidden(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}")] = httpx.Response(
        403,
        json={"detail": "not authorized for this team"},
    )
    rc = main(["eval", "trial", "cancel", _TRIAL_ID])
    assert rc != 0
    err = capsys.readouterr().err
    assert "token rejected by server while trying to show trial" in err
    assert "not authorized for this team" in err
    assert "Traceback" not in err


def test_batch_delivery_bundle_creates_downloads_and_writes_checksum(
    mock_server: MockServer,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output = tmp_path / "delivery.tar.gz"
    archive = b"delivery archive bytes"
    digest = hashlib.sha256(archive).hexdigest()
    artifact_id = "00000000-0000-0000-0000-00000000d311"
    supplemental_id = "00000000-0000-0000-0000-00000000d312"
    mock_server.canned[
        ("POST", f"/api/v1/batches/{_BATCH_ID}/delivery-export")
    ] = httpx.Response(
        201,
        json={
            "id": artifact_id,
            "status": "ready",
            "archive_filename": "source-useful-5003-delivery.tar.gz",
            "sha256": digest,
            "download_url": (
                f"/api/v1/batches/{_BATCH_ID}/delivery-export/{artifact_id}/download"
            ),
            "manifest": {
                "task_count": 5003,
                "trial_count": 5003,
                "reward_distribution": {"0.0": 1139, "1.0": 3864},
                "object_counts": {"atif": 5003, "trajectory": 5003},
            },
        },
    )
    mock_server.canned[
        (
            "GET",
            f"/api/v1/batches/{_BATCH_ID}/delivery-export/{artifact_id}/download",
        )
    ] = httpx.Response(200, content=archive)

    rc = main(
        [
            "eval",
            "batch",
            "delivery-bundle",
            _BATCH_ID,
            "--supplemental-batch-id",
            supplemental_id,
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert output.read_bytes() == archive
    assert output.with_suffix(output.suffix + ".sha256").read_text() == (
        f"{digest}  {output.name}\n"
    )
    out = capsys.readouterr().out
    assert "Delivery bundle ready" in out
    assert "5003 trials" in out
    assert digest in out

    create_req = mock_server[0]
    assert create_req.method == "POST"
    assert create_req.url.path == f"/api/v1/batches/{_BATCH_ID}/delivery-export"
    assert json.loads(create_req.content) == {
        "supplemental_batch_ids": [supplemental_id],
    }
    download_req = mock_server[1]
    assert download_req.method == "GET"
    assert download_req.url.path == (
        f"/api/v1/batches/{_BATCH_ID}/delivery-export/{artifact_id}/download"
    )


def test_batch_delivery_bundle_streams_download_and_hashes_incrementally(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "raw-harbor.tar.gz"
    chunks = [b"raw-", b"harbor-", b"archive"]
    digest = hashlib.sha256(b"".join(chunks)).hexdigest()
    artifact_id = "00000000-0000-0000-0000-00000000d313"
    calls: list[tuple[str, str]] = []

    class _StreamResponse:
        def __init__(self) -> None:
            self.status_code = 200
            self.headers: dict[str, str] = {}
            self.text = ""

        def iter_bytes(self) -> Any:
            yield from chunks

    class _StreamContext:
        def __enter__(self) -> _StreamResponse:
            return _StreamResponse()

        def __exit__(self, *_exc: object) -> None:
            return None

    class _FakeClient:
        def __enter__(self) -> _FakeClient:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def post(self, path: str, *, json: dict[str, Any]) -> httpx.Response:
            calls.append(("POST", path))
            assert json == {"mode": "raw-harbor"}
            return httpx.Response(
                201,
                json={
                    "id": artifact_id,
                    "status": "ready",
                    "archive_filename": "source-useful-raw-harbor.tar.gz",
                    "sha256": digest,
                    "download_url": (
                        f"/api/v1/batches/{_BATCH_ID}/delivery-export/"
                        f"{artifact_id}/download"
                    ),
                    "manifest": {
                        "mode": "raw-harbor",
                        "trial_count": 3,
                        "object_counts": {
                            "atif": 3,
                            "trajectory": 3,
                            "provider_logs": 3,
                            "task_bundle_files": 2,
                        },
                    },
                },
            )

        def get(self, path: str) -> httpx.Response:
            raise AssertionError(f"download must stream, not use get({path!r})")

        def stream(self, method: str, path: str) -> _StreamContext:
            calls.append((method, path))
            return _StreamContext()

    monkeypatch.setattr(
        "loom_cli.eval_cmd.authed_client",
        lambda _cfg, *, timeout=30.0: _FakeClient(),
    )

    rc = main(
        [
            "eval",
            "batch",
            "delivery-bundle",
            _BATCH_ID,
            "--mode",
            "raw-harbor",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert output.read_bytes() == b"".join(chunks)
    assert output.with_suffix(output.suffix + ".sha256").read_text() == (
        f"{digest}  {output.name}\n"
    )
    assert calls == [
        ("POST", f"/api/v1/batches/{_BATCH_ID}/delivery-export"),
        ("GET", f"/api/v1/batches/{_BATCH_ID}/delivery-export/{artifact_id}/download"),
    ]


def test_batch_delivery_bundle_forwards_raw_harbor_tb2_v2_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "raw-harbor-tb2-v2.tar.gz"
    digest = hashlib.sha256(b"v2-archive").hexdigest()
    artifact_id = "00000000-0000-0000-0000-00000000d314"
    calls: list[tuple[str, str]] = []

    class _StreamResponse:
        def __init__(self) -> None:
            self.status_code = 200
            self.headers: dict[str, str] = {}
            self.text = ""

        def iter_bytes(self) -> Any:
            yield b"v2-archive"

    class _StreamContext:
        def __enter__(self) -> _StreamResponse:
            return _StreamResponse()

        def __exit__(self, *_exc: object) -> None:
            return None

    class _FakeClient:
        def __enter__(self) -> _FakeClient:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def post(self, path: str, *, json: dict[str, Any]) -> httpx.Response:
            calls.append(("POST", path))
            assert json == {"mode": "raw-harbor-tb2-v2"}
            return httpx.Response(
                201,
                json={
                    "id": artifact_id,
                    "status": "ready",
                    "archive_filename": "source-useful-raw-harbor-tb2-v2.tar.gz",
                    "sha256": digest,
                    "download_url": (
                        f"/api/v1/batches/{_BATCH_ID}/delivery-export/"
                        f"{artifact_id}/download"
                    ),
                    "manifest": {"mode": "raw-harbor-tb2-v2", "trial_count": 1},
                },
            )

        def stream(self, method: str, path: str) -> _StreamContext:
            calls.append((method, path))
            return _StreamContext()

    monkeypatch.setattr(
        "loom_cli.eval_cmd.authed_client",
        lambda _cfg, *, timeout=30.0: _FakeClient(),
    )

    rc = main(
        [
            "eval",
            "batch",
            "delivery-bundle",
            _BATCH_ID,
            "--mode",
            "raw-harbor-tb2-v2",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert output.read_bytes() == b"v2-archive"
    assert calls[0] == ("POST", f"/api/v1/batches/{_BATCH_ID}/delivery-export")


def test_batch_delivery_bundle_forwards_explicit_trial_ids(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    output = tmp_path / "subset.tar.gz"
    digest = hashlib.sha256(b"subset-archive").hexdigest()
    artifact_id = "00000000-0000-0000-0000-00000000d315"
    trial_a = "11111111-1111-1111-1111-111111111111"
    trial_b = "22222222-2222-2222-2222-222222222222"
    posted: dict[str, Any] = {}

    class _StreamResponse:
        def __init__(self) -> None:
            self.status_code = 200
            self.headers: dict[str, str] = {}
            self.text = ""

        def iter_bytes(self) -> Any:
            yield b"subset-archive"

    class _StreamContext:
        def __enter__(self) -> _StreamResponse:
            return _StreamResponse()

        def __exit__(self, *_exc: object) -> None:
            return None

    class _FakeClient:
        def __enter__(self) -> _FakeClient:
            return self

        def __exit__(self, *_exc: object) -> None:
            return None

        def post(self, path: str, *, json: dict[str, Any]) -> httpx.Response:
            posted.update(json)
            return httpx.Response(
                201,
                json={
                    "id": artifact_id,
                    "status": "ready",
                    "archive_filename": "subset.tar.gz",
                    "sha256": digest,
                    "download_url": (
                        f"/api/v1/batches/{_BATCH_ID}/delivery-export/"
                        f"{artifact_id}/download"
                    ),
                    "manifest": {
                        "mode": "lightweight",
                        "selection_rule": "explicit_trial_ids",
                        "trial_count": 2,
                    },
                },
            )

        def stream(self, method: str, path: str) -> _StreamContext:
            return _StreamContext()

    monkeypatch.setattr(
        "loom_cli.eval_cmd.authed_client",
        lambda _cfg, *, timeout=30.0: _FakeClient(),
    )

    rc = main(
        [
            "eval",
            "batch",
            "delivery-bundle",
            _BATCH_ID,
            "--trial-id",
            trial_a,
            "--trial-id",
            trial_b,
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert posted == {"selection": {"trial_ids": [trial_a, trial_b]}}
    assert output.read_bytes() == b"subset-archive"


# ──────────────────────────────────────────────────────────────────────
# eval trial list / show
# ──────────────────────────────────────────────────────────────────────


def test_trial_list_with_task_id_filter(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/trials")] = httpx.Response(
        200,
        json={
            "items": [
                {
                    "id": _TRIAL_ID,
                    "task_id": "humaneval/HumanEval/0",
                    "team_id": "x",
                    "state": "succeeded",
                    "failure_reason": None,
                    "submitted_at": "2026-06-16T00:00:00Z",
                    "started_at": None,
                    "finished_at": None,
                    "attempt_count": 1,
                    "aggregate_reward": 0.875,
                    "total_prompt_tokens": 20,
                    "total_completion_tokens": 8,
                    "llm_calls_count": 1,
                    "agent_name": "litellm",
                    "model": {"provider": "openai", "name": "gpt-4o"},
                }
            ],
            "next_cursor": None,
        },
    )
    rc = main(
        [
            "eval",
            "trial",
            "list",
            "--task-id",
            "humaneval/HumanEval/0",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert _TRIAL_ID in out
    assert "0.875" in out
    assert mock_server[0].url.params.get("task_id") == "humaneval/HumanEval/0"


def test_trial_show_renders_public_download_commands_when_ready(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}")] = httpx.Response(
        200,
        json={
            "id": _TRIAL_ID,
            "task_id": "t",
            "team_id": "x",
            "state": "succeeded",
            "failure_reason": None,
            "submitted_at": "2026-06-16T00:00:00Z",
            "started_at": "2026-06-16T00:01:00Z",
            "finished_at": "2026-06-16T00:02:00Z",
            "attempt_count": 1,
            "aggregate_reward": 1.0,
            "total_prompt_tokens": 50,
            "total_completion_tokens": 12,
            "llm_calls_count": 1,
            "agent_name": "litellm",
            "model": {"provider": "openai", "name": "gpt-4o"},
            "atif_url": f"https://loom.test/api/v1/trials/{_TRIAL_ID}/atif",
            "trajectory_url": (f"https://loom.test/api/v1/trials/{_TRIAL_ID}/trajectory/download"),
            "atif_ready": True,
            "trajectory_ready": True,
            "materialization": {"canonical_ready": True},
            "artifacts": [
                {
                    "path": "result.txt",
                    "key": f"team/{_TRIAL_ID}/artifacts/result.txt",
                    "download_url": (
                        f"https://loom.test/api/v1/trials/{_TRIAL_ID}"
                        "/artifacts/download?key=team/result.txt"
                    ),
                    "share_status": "shared",
                    "blocked_reason": None,
                }
            ],
        },
    )
    rc = main(["eval", "trial", "show", _TRIAL_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert "downloads:" in out
    assert f"loom eval trial download {_TRIAL_ID} --kind atif" in out
    assert f"loom eval trial download {_TRIAL_ID} --kind trajectory" in out
    assert f"loom eval trial download {_TRIAL_ID} --kind bundle" in out
    assert "--kind artifact" in out
    assert "--artifact-key" in out


def test_trial_show_redacts_legacy_signed_urls_in_text_output(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}")] = httpx.Response(
        200,
        json={
            "id": _TRIAL_ID,
            "task_id": "t",
            "team_id": "x",
            "state": "succeeded",
            "failure_reason": None,
            "submitted_at": "2026-06-16T00:00:00Z",
            "started_at": "2026-06-16T00:01:00Z",
            "finished_at": "2026-06-16T00:02:00Z",
            "attempt_count": 1,
            "aggregate_reward": 1.0,
            "total_prompt_tokens": 50,
            "total_completion_tokens": 12,
            "llm_calls_count": 1,
            "agent_name": "litellm",
            "model": {"provider": "openai", "name": "gpt-4o"},
            "atif_url": ("https://minio.internal/atif.json?X-Amz-Signature=secret-sig"),
            "trajectory_url": ("https://minio.internal/events.jsonl?X-Amz-Signature=secret-sig"),
            "atif_ready": True,
            "trajectory_ready": True,
            "artifacts": [
                {
                    "path": "secret.txt",
                    "key": "team/trial/artifacts/secret.txt",
                    "download_url": (
                        "https://minio.internal/secret.txt?X-Amz-Signature=secret-sig"
                    ),
                    "share_status": "shared",
                    "blocked_reason": None,
                }
            ],
        },
    )

    rc = main(["eval", "trial", "show", _TRIAL_ID])

    assert rc == 0
    out = capsys.readouterr().out
    assert "minio.internal" not in out
    assert "X-Amz-Signature=secret-sig" not in out
    assert "loom eval trial download" in out


def test_trial_show_json_format(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "id": _TRIAL_ID,
        "task_id": "t",
        "team_id": "x",
        "state": "queued",
        "failure_reason": None,
        "submitted_at": "2026-06-16T00:00:00Z",
        "started_at": None,
        "finished_at": None,
        "attempt_count": 0,
        "aggregate_reward": None,
        "total_prompt_tokens": 0,
        "total_completion_tokens": 0,
        "llm_calls_count": 0,
        "agent_name": None,
        "model": None,
        "atif_url": "https://minio.internal/atif?X-Amz-Signature=abc",
        "trajectory_url": "https://minio.internal/traj?X-Amz-Signature=abc",
        "atif_ready": False,
        "trajectory_ready": False,
        "artifacts": [],
    }
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}")] = httpx.Response(
        200,
        json=payload,
    )
    rc = main(["eval", "trial", "show", _TRIAL_ID, "--format", "json"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["atif_url"] == "[REDACTED:signed-url]"
    assert parsed["trajectory_url"] == "[REDACTED:signed-url]"


def test_trial_debug_fetches_machine_readable_evidence(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "schema_version": "1",
        "entity": {"type": "trial", "id": _TRIAL_ID},
        "lifecycle": {
            "state": "failed",
            "submitted_at": "2026-06-16T00:00:00Z",
            "finished_at": "2026-06-16T00:01:00Z",
            "attempt_count": 2,
        },
        "failure": {
            "reason_code": "trial.verifier_error",
            "category": "verifier",
            "attribution": "benchmark",
            "message": "pytest missing tests",
        },
        "provider": {"llm_calls_count": 1, "models": ["openai/gpt-4o"]},
        "next_actions": ["Inspect verifier output and benchmark task assets."],
    }
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}/debug")] = httpx.Response(
        200, json=payload
    )

    rc = main(["eval", "trial", "debug", _TRIAL_ID, "--format", "json"])

    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["failure"]["reason_code"] == "trial.verifier_error"
    assert mock_server[0].url.path == f"/api/v1/trials/{_TRIAL_ID}/debug"


def test_diagnose_trial_fetches_machine_readable_report(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "schema_version": "1",
        "entity": {"type": "trial", "id": _TRIAL_ID},
        "summary": "The trial reached the verifier, but the verifier failed.",
        "primary_cause": {
            "reason_code": "trial.verifier_error",
            "category": "verifier",
            "attribution": "benchmark",
            "confidence": "high",
            "affected_trials": 1,
            "affected_ratio": 1.0,
        },
        "impact": "The reward is not reliable.",
        "evidence": ["1/1 affected trial(s) matched trial.verifier_error"],
        "next_actions": [
            {
                "label": "Inspect verifier output and benchmark task assets",
                "kind": "manual",
            }
        ],
        "reason_clusters": [],
    }
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}/diagnosis")] = httpx.Response(
        200, json=payload
    )

    rc = main(["eval", "diagnose", "trial", _TRIAL_ID, "--format", "json"])

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["primary_cause"] == (payload["primary_cause"])
    assert mock_server[0].url.path == (f"/api/v1/trials/{_TRIAL_ID}/diagnosis")


def test_trial_download_atif_writes_public_route_response(
    mock_server: MockServer,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}/atif")] = httpx.Response(
        200, content=b'{"atif":"ok"}'
    )
    output = tmp_path / "atif.json"

    rc = main(
        [
            "eval",
            "trial",
            "download",
            _TRIAL_ID,
            "--kind",
            "atif",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert output.read_bytes() == b'{"atif":"ok"}'
    assert mock_server[0].method == "GET"
    assert mock_server[0].url.path == f"/api/v1/trials/{_TRIAL_ID}/atif"
    out = capsys.readouterr().out
    assert "Downloaded atif" in out
    assert "minio" not in out


def test_trial_download_bundle_writes_complete_canonical_archive(
    mock_server: MockServer,
    tmp_path: Path,
) -> None:
    mock_server.canned[
        ("GET", f"/api/v1/trials/{_TRIAL_ID}/bundle/download")
    ] = httpx.Response(200, content=b"complete-bundle")
    output = tmp_path / "bundle.tar.gz"

    rc = main(
        [
            "eval",
            "trial",
            "download",
            _TRIAL_ID,
            "--kind",
            "bundle",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert output.read_bytes() == b"complete-bundle"
    assert mock_server[0].url.path == f"/api/v1/trials/{_TRIAL_ID}/bundle/download"


def test_trial_download_artifact_requires_key_and_uses_proxy_route(
    mock_server: MockServer,
    tmp_path: Path,
) -> None:
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}/artifacts/download")] = httpx.Response(
        200, content=b"artifact-bytes"
    )
    output = tmp_path / "result.txt"

    rc = main(
        [
            "eval",
            "trial",
            "download",
            _TRIAL_ID,
            "--kind",
            "artifact",
            "--artifact-key",
            "team/trial/artifacts/result.txt",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert output.read_bytes() == b"artifact-bytes"
    assert mock_server[0].method == "GET"
    assert mock_server[0].url.path == (f"/api/v1/trials/{_TRIAL_ID}/artifacts/download")
    assert mock_server[0].url.params.get("key") == ("team/trial/artifacts/result.txt")


def test_trial_download_artifact_without_key_errors_before_http(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "eval",
            "trial",
            "download",
            _TRIAL_ID,
            "--kind",
            "artifact",
        ]
    )

    assert rc == 2
    assert "requires --artifact-key" in capsys.readouterr().err
    assert len(mock_server.requests) == 0


def test_artifact_export_writes_run_library_jsonl_with_filters(
    mock_server: MockServer,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/run-library/artifacts/export")] = httpx.Response(
        200,
        content=(
            b'{"id":"artifact-1","artifact_type":"metric_table",'
            b'"content_hash":"sha256:abc"}\n'
        ),
        headers={"content-type": "application/x-ndjson"},
    )
    output = tmp_path / "artifacts.jsonl"

    rc = main(
        [
            "eval",
            "artifact",
            "export",
            "--scope",
            "all",
            "--artifact-type",
            "metric_table",
            "--owner-team-id",
            "00000000-0000-0000-0000-0000000000aa",
            "--source-trial-id",
            _TRIAL_ID,
            "--safety-state",
            "safe",
            "--provenance-relation",
            "produced_from",
            "--format",
            "jsonl",
            "--output",
            str(output),
        ]
    )

    assert rc == 0
    assert output.read_bytes().startswith(b'{"id":"artifact-1"')
    assert mock_server[0].method == "GET"
    assert mock_server[0].url.path == "/api/v1/run-library/artifacts/export"
    assert mock_server[0].url.params.get("scope") == "all"
    assert mock_server[0].url.params.get("artifact_type") == "metric_table"
    assert mock_server[0].url.params.get("owner_team_id") == (
        "00000000-0000-0000-0000-0000000000aa"
    )
    assert mock_server[0].url.params.get("source_trial_id") == _TRIAL_ID
    assert mock_server[0].url.params.get("safety_state") == "safe"
    assert mock_server[0].url.params.get("provenance_relation") == "produced_from"
    assert mock_server[0].url.params.get("format") == "jsonl"
    assert "Exported artifact metadata" in capsys.readouterr().out


def test_eval_usage_calls_public_usage_route(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/usage")] = httpx.Response(
        200,
        json={
            "degraded": False,
            "buckets": [
                {
                    "start_at": "2026-06-01T00:00:00+00:00",
                    "end_at": None,
                    "trial_count": 2,
                    "trials_currently_succeeded": 1,
                    "trials_currently_failed": 1,
                    "succeeded_count": 1,
                    "failed_count": 1,
                    "total_cost_usd": 0.125,
                    "llm_input_tokens": 100,
                    "llm_output_tokens": 50,
                    "modal_compute_seconds": 0.0,
                    "modal_cost_usd": 0.0,
                    "cloud_compute_seconds": 0.0,
                    "cloud_cost_usd": 0.0,
                }
            ],
        },
    )

    rc = main(
        [
            "eval",
            "usage",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-02",
            "--group-by",
            "day",
        ]
    )

    assert rc == 0
    assert mock_server[0].method == "GET"
    assert mock_server[0].url.path == "/api/v1/usage"
    assert mock_server[0].url.params.get("start") == "2026-06-01"
    assert mock_server[0].url.params.get("end") == "2026-06-02"
    out = capsys.readouterr().out
    assert "2026-06-01" in out


def test_eval_usage_include_batches_shows_token_only_cost_status(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/usage")] = httpx.Response(
        200,
        json={
            "degraded": False,
            "buckets": [
                {
                    "start_at": "2026-06-02T00:00:00+00:00",
                    "end_at": None,
                    "trial_count": 1,
                    "trials_currently_succeeded": 0,
                    "trials_currently_failed": 1,
                    "succeeded_count": 0,
                    "failed_count": 1,
                    "total_cost_usd": 0.0,
                    "estimated_cost_usd": None,
                    "cost_status": "not_applicable",
                    "cost_currency": None,
                    "pricing_modes": ["tokens-only"],
                    "partial_usage_llm_calls_count": 1,
                    "missing_usage_llm_calls_count": 0,
                    "usage_reporting_status": "partial",
                    "usage_estimate_confidence": "partial",
                    "llm_input_tokens": 77,
                    "llm_output_tokens": 11,
                    "batches": [
                        {
                            "batch_id": "batch-token-only",
                            "batch_name": "self-deployed token-only batch",
                            "team_id": "team-1",
                            "team_name": "Team One",
                            "trial_count": 1,
                            "llm_input_tokens": 77,
                            "llm_output_tokens": 11,
                            "estimated_cost_usd": None,
                            "cost_status": "not_applicable",
                            "cost_currency": None,
                            "pricing_modes": ["tokens-only"],
                            "partial_usage_llm_calls_count": 1,
                            "usage_estimate_confidence": "partial",
                        }
                    ],
                }
            ],
        },
    )

    rc = main(
        [
            "eval",
            "usage",
            "--start",
            "2026-06-02",
            "--end",
            "2026-06-02",
            "--include-batches",
        ]
    )

    assert rc == 0
    assert mock_server[0].url.params.get("include_batches") == "true"
    out = capsys.readouterr().out
    assert "not_applicable" in out
    assert "n/a" in out
    assert "self-deployed token-only batch" in out
    assert "partial" in out
    assert "0.125" not in out


def test_eval_usage_forwards_admin_filters_and_breakdown(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/usage")] = httpx.Response(
        200,
        json={
            "degraded": False,
            "buckets": [
                {
                    "start_at": "2026-06-02T00:00:00+00:00",
                    "breakdown_by": "pricing_mode",
                    "breakdown_key": "tokens-only",
                    "breakdown_label": "tokens-only",
                    "trial_count": 1,
                    "succeeded_count": 0,
                    "failed_count": 1,
                    "estimated_cost_usd": None,
                    "cost_status": "not_applicable",
                    "cost_currency": None,
                    "pricing_modes": ["tokens-only"],
                    "llm_input_tokens": 77,
                    "llm_output_tokens": 11,
                }
            ],
        },
    )

    rc = main(
        [
            "eval",
            "usage",
            "--start",
            "2026-06-02",
            "--end",
            "2026-06-02",
            "--team-id",
            "team-1",
            "--user-id",
            "user-1",
            "--provider-connection-id",
            "provider-1",
            "--model",
            "qwen3.6-35b-a3b",
            "--benchmark-id",
            "skilllearnbench",
            "--batch-id",
            "batch-1",
            "--status",
            "failed",
            "--pricing-mode",
            "tokens-only",
            "--breakdown-by",
            "pricing_mode",
        ]
    )

    assert rc == 0
    params = mock_server[0].url.params
    assert params.get("user_id") == "user-1"
    assert params.get("provider_connection_id") == "provider-1"
    assert params.get("model") == "qwen3.6-35b-a3b"
    assert params.get("benchmark_id") == "skilllearnbench"
    assert params.get("batch_id") == "batch-1"
    assert params.get("status") == "failed"
    assert params.get("pricing_mode") == "tokens-only"
    assert params.get("breakdown_by") == "pricing_mode"
    out = capsys.readouterr().out
    assert "tokens-only" in out
    assert "not_applicable" in out


def test_eval_usage_forwards_batch_family_filter(
    mock_server: MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/usage")] = httpx.Response(
        200,
        json={
            "degraded": False,
            "buckets": [
                {
                    "start_at": "2026-06-02T00:00:00+00:00",
                    "trial_count": 2,
                    "succeeded_count": 1,
                    "failed_count": 1,
                    "estimated_cost_usd": 0.3,
                    "cost_status": "estimated",
                    "cost_currency": "USD",
                    "usage_estimate_confidence": "high",
                    "llm_input_tokens": 300,
                    "llm_output_tokens": 30,
                }
            ],
        },
    )

    rc = main(
        [
            "eval",
            "usage",
            "--start",
            "2026-06-02",
            "--end",
            "2026-06-02",
            "--batch-id",
            "main-batch",
            "--include-batch-family",
        ]
    )

    assert rc == 0
    params = mock_server[0].url.params
    assert params.get("batch_id") == "main-batch"
    assert params.get("include_batch_family") == "true"
    assert "0.3" in capsys.readouterr().out


# ──────────────────────────────────────────────────────────────────────
# not-logged-in path
# ──────────────────────────────────────────────────────────────────────


def test_eval_run_not_logged_in(
    capsys: pytest.CaptureFixture[str],
) -> None:
    main(["auth", "logout"])
    capsys.readouterr()
    rc = main(
        [
            "eval",
            "run",
            "--provider",
            "n",
            "--model",
            "m",
            "--agent",
            "a",
            "--task",
            "t",
        ]
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "not logged in" in err


def test_eval_batch_list_not_logged_in(
    capsys: pytest.CaptureFixture[str],
) -> None:
    main(["auth", "logout"])
    capsys.readouterr()
    rc = main(["eval", "batch", "list"])
    assert rc == 2
    assert "not logged in" in capsys.readouterr().err


@pytest.mark.parametrize("current", [False, True])
def test_batch_rerun_failed_submits_linked_selection(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str], current: bool,
) -> None:
    path = f"/api/v1/batches/{_BATCH_ID}/rerun-failed"
    mock_server.canned[("POST", path)] = httpx.Response(201, json={
        "batch_id": "new-supplemental-batch", "expected_trial_count": 1,
        "rerun_of_batch_id": _BATCH_ID,
    })
    args = ["eval", "batch", "rerun-failed", _BATCH_ID,
            "--task-id", "task/failed", "--include-operator-approval", "--format", "json"]
    if current:
        args.append("--use-current-runtime")
    assert main(args) == 0
    assert json.loads(mock_server[0].content) == {
        "task_ids": ["task/failed"], "include_operator_approval": True,
        "use_current_runtime": current,
    }
    assert json.loads(capsys.readouterr().out)["rerun_of_batch_id"] == _BATCH_ID


@pytest.mark.parametrize("purpose", ["evaluation", "trajectory_generation"])
@pytest.mark.parametrize("from_file", [False, True])
def test_batch_create_complete_request_preserves_export(
    mock_server: MockServer, tmp_path: Path, purpose: str, from_file: bool,
) -> None:
    payload = {
        "purpose": purpose,
        "team_id": "team-b",
        "name_suffix": "a's $(literal)",
        "task_filter": {
            "benchmark_ids": ["benchmark-a", "benchmark-b"],
            "tag_filters": {"language": ["python", "C++"]},
            "subset_kind": "random_n", "n": 4, "seed": 72,
        },
        "combinations": [
            {
                "agent_name": "terminus-2", "agent_version": "harbor-v2",
                "agent_model": {"provider": "openai", "name": "model", "source": "api"},
                "provider_connection_id": _CONN_ID,
                "provider_model_id": "model", "n_per_task": 3, "label": "first",
            },
            {"agent_name": "oracle", "agent_model": None, "n_per_task": 2, "label": "second"},
        ],
        "trial_config": {
            "force_build": True, "override_agent_timeout_sec": 100,
            "retry": {"max_attempts": 2, "retry_on": ["worker_crash"]},
        },
        "budget_usd": 2.5, "budget_policy": "soft", "budget_confirmed": True,
    }
    if purpose == "trajectory_generation":
        payload["task_filter"] = {"subset_kind": "explicit", "task_ids": ["a/0", "b/1"]}
        payload["trial_config"]["skip_verifier"] = True
    request = json.dumps(payload)
    if from_file:
        path = tmp_path / "batch.json"
        path.write_text(request)
        request = f"@{path}"
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={"batch_id": _BATCH_ID, "state": "submitted"},
    )
    assert main(["eval", "batch", "create", "--request-json", request]) == 0
    assert len(mock_server) == 1
    assert json.loads(mock_server[0].content) == payload
    assert mock_server[0].headers["Authorization"] == "Bearer loom_admin_test123456"


@pytest.mark.parametrize("flags", [
    ["--agent", "oracle"], ["--team-id", "another-team"],
    ["--skip-verifier"], ["--name-suffix", "overwrite"],
    ["--combinations-json", '[{"agent_name":"oracle"}]'],
    ["--storage-preflight-evidence", "does-not-exist.json"],
    ["--n-per-task", "1"], ["--n-per-task=1"], ["--name-suffix", ""],
])
def test_batch_request_rejects_overrides_before_network(
    mock_server: MockServer, flags: list[str], capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["eval", "batch", "create", "--request-json", '{"purpose":"evaluation"}', *flags]) == 2
    assert "complete batch request; omit" in capsys.readouterr().err
    assert len(mock_server) == 0


@pytest.mark.parametrize("raw_request", ["[]", "null", "bad json", "@missing-export.json"])
def test_batch_request_requires_json_object(mock_server: MockServer, raw_request: str) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["eval", "batch", "create", "--request-json", raw_request])
    assert exc.value.code == 2
    assert len(mock_server) == 0


def test_batch_request_and_purpose_mutually_exclusive(mock_server: MockServer) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["eval", "batch", "create", "--request-json", "{}", "--purpose", "evaluation"])
    assert exc.value.code == 2
    assert len(mock_server) == 0


def test_batch_request_preserves_server_authority(mock_server: MockServer) -> None:
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        403, json={"detail": "no access to target team"},
    )
    assert main(["eval", "batch", "create", "--request-json", '{"purpose":"evaluation","team_id":"other-team"}']) != 0
    assert len(mock_server) == 1
