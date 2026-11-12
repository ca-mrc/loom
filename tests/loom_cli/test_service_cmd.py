"""`loom service {up,down,status}` argparse + dispatch.

The actual docker-compose / alembic / seed_test_data calls are
subprocess invocations against a real docker daemon — exercised by
manual smoke + tests/system/ (gated). Here we only verify the CLI
surface, missing-compose-file handling, and that the right argv
shapes flow into our subprocess wrappers.
"""

from __future__ import annotations

import io
import stat
import subprocess
import tomllib
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.service_cmd import _compose_args, _mutable_dev_images

_GENERIC_EXPECTED_DENIAL_ERROR = "error: expected hidden-resource denial was not observed\n"
_UPDATE_DENIAL_RECEIPT = (
    '{"error_code":"resource_hidden","http_method":"PUT",'
    '"schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,'
    '"target_phase":"target_update"}\n'
)


def _read_admin_token(secret_file: Path) -> str:
    data = tomllib.loads(secret_file.read_text(encoding="utf-8"))
    return data["admin"]["token"]


def test_help_lists_subcommands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["service", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "up" in out and "down" in out and "status" in out


def test_up_errors_when_compose_file_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "service",
            "up",
            "--compose-file",
            str(tmp_path / "nonexistent.yml"),
        ]
    )
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


def test_down_errors_when_compose_file_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "service",
            "down",
            "--compose-file",
            str(tmp_path / "nonexistent.yml"),
        ]
    )
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


def test_status_errors_when_compose_file_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "service",
            "status",
            "--compose-file",
            str(tmp_path / "nonexistent.yml"),
        ]
    )
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


@pytest.mark.parametrize("service_cmd", ["up", "down", "status"])
def test_service_commands_error_without_docker_cli(
    service_cmd: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing Docker should produce an actionable CLI error, not a traceback."""
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    monkeypatch.setattr(
        "shutil.which",
        lambda binary: None if binary == "docker" else "/bin/other",
    )

    with patch("loom_cli.service_cmd._run", side_effect=AssertionError("compose should not run")):
        argv = [
            "service",
            service_cmd,
            "--compose-file",
            str(compose),
            "--env-file",
            str(tmp_path / "absent.env"),
        ]
        if service_cmd == "up":
            argv.extend([])
        rc = main(argv)

    captured = capsys.readouterr()
    assert rc == 2
    assert "Docker CLI was not found" in captured.err
    assert "Docker Desktop for Mac" in captured.err
    assert "docker compose version" in captured.err
    assert "Traceback" not in captured.err


def test_service_status_errors_when_docker_compose_unavailable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing Compose plugin should be diagnosed before compose ps runs."""
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    monkeypatch.setattr(
        "shutil.which",
        lambda binary: "/usr/local/bin/docker" if binary == "docker" else None,
    )

    def _fake_run(argv, **kwargs):  # type: ignore[no-untyped-def]
        assert argv == ["docker", "compose", "version"]
        return subprocess.CompletedProcess(
            args=argv,
            returncode=1,
            stdout="",
            stderr="docker: compose is not a docker command\n",
        )

    monkeypatch.setattr("loom_cli.service_cmd.subprocess.run", _fake_run)
    rc = main(
        [
            "service",
            "status",
            "--compose-file",
            str(compose),
            "--env-file",
            str(tmp_path / "absent.env"),
        ]
    )

    captured = capsys.readouterr()
    assert rc == 2
    assert "Docker Compose is not available" in captured.err
    assert "docker compose version" in captured.err
    assert "docker: compose is not a docker command" in captured.err


def test_compose_args_includes_env_file_when_present(
    tmp_path: Path,
) -> None:
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env = tmp_path / ".env"
    env.write_text("FOO=bar\n")
    args = _compose_args(compose, env)
    assert "--env-file" in args
    assert str(env) in args
    assert "-f" in args
    assert str(compose) in args


def test_compose_args_omits_env_file_when_missing(
    tmp_path: Path,
) -> None:
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    missing_env = tmp_path / "does-not-exist.env"
    args = _compose_args(compose, missing_env)
    assert "--env-file" not in args
    assert "-f" in args
    assert str(compose) in args


def test_mutable_dev_image_detection_is_literal_and_local(tmp_path: Path) -> None:
    compose = tmp_path / "compose.yml"
    compose.write_text(
        """services:
  service:
    image: loom-service:dev
  worker:
    image: 'loom-worker:dev'
  release:
    image: loom-control-plane:v1.0.0
  external:
    image: postgres:16
  parameterized:
    image: loom-gateway:${LOOM_IMAGE_TAG:-dev}
  sandboxed:
    image: loom-control-plane:${LOOM_DEV_IMAGE_TAG:-dev}
""",
    )

    assert _mutable_dev_images(compose) == (
        "loom-control-plane:${LOOM_DEV_IMAGE_TAG:-dev}",
        "loom-service:dev",
        "loom-worker:dev",
    )


def test_dev_step_jwt_has_one_local_source() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose_text = (repo_root / "deploy" / "docker-compose.dev.yml").read_text()
    env_text = (repo_root / ".env.example").read_text()
    source = "${LOOM_CP_STEP_JWT_SIGNING_KEY:-dev-only-shared-jwt-key-do-not-use-in-prod}"

    assert f'LOOM_CP_STEP_JWT_SIGNING_KEY: "{source}"' in compose_text
    assert f'LOOM_GW_STEP_JWT_SIGNING_KEY: "{source}"' in compose_text
    assert "LOOM_CP_STEP_JWT_SIGNING_KEY=" in env_text
    assert "LOOM_TASK_IMAGE_BUILDER_TOKEN=placeholder" in env_text


def test_up_invokes_docker_compose_up(
    tmp_path: Path,
) -> None:
    """Storage comes up first; on postgres failure we bail before alembic."""
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run") as mock_run,
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=False) as mock_wait,
        patch("loom_cli.service_cmd._alembic_upgrade") as mock_alembic,
    ):
        # _run returns CompletedProcess-like; we need .returncode = 0
        from subprocess import CompletedProcess

        mock_run.return_value = CompletedProcess([], 0, "", "")
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(tmp_path / "absent.env"),
            ]
        )
        # postgres didn't go healthy → exit 1, no alembic call
        assert rc == 1
        # First call should be `docker compose ... up -d postgres minio`
        first_args = mock_run.call_args_list[0].args[0]
        assert "up" in first_args and "-d" in first_args
        assert first_args[-2:] == ["postgres", "minio"]
        assert mock_wait.called
        assert not mock_alembic.called


def test_up_builds_mutable_dev_images_before_start_and_migrations(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Fresh, cached, and stale ``:dev`` images share one safe contract:
    storage migrates first, then Compose builds before schema-gated start.
    """
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text(
        "services:\n  service:\n    image: loom-service:dev\n    build: .\n",
    )
    env_file = tmp_path / ".env"
    secret = "test-step-jwt-secret-that-must-not-be-printed"
    env_file.write_text(f"LOOM_CP_STEP_JWT_SIGNING_KEY={secret}\n")
    events: list[str] = []

    def _capture_run(argv, *_args, **_kwargs):
        if argv[-2:] == ["postgres", "minio"]:
            events.append("infra")
        else:
            events.append("up")
            assert "--build" in argv
        return CompletedProcess(argv, 0, "", "")

    def _alembic(_db_url: str) -> int:
        events.append("alembic")
        return 0

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", side_effect=_alembic),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(1, {})),
        patch(
            "loom_cli.service_cmd._ensure_dev_admin_secret",
            return_value="loom_admin_" + "A" * 43,
        ),
    ):
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 1
    assert events == ["infra", "alembic", "up"]
    captured = capsys.readouterr()
    assert secret not in captured.out
    assert secret not in captured.err


def test_up_does_not_force_build_for_immutable_images(tmp_path: Path) -> None:
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services:\n  service:\n    image: loom-service:v1.0.0\n")

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run") as mock_run,
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=False),
    ):
        mock_run.return_value = CompletedProcess([], 0, "", "")
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(tmp_path / "absent.env"),
            ]
        )

    assert rc == 1
    first_args = mock_run.call_args_list[0].args[0]
    assert first_args[-2:] == ["postgres", "minio"]
    assert "--build" not in first_args


def test_seed_test_data_parses_all_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--print all` emits `<label>: <token>` per line; the wrapper
    parses team/worker/builder tokens while admin comes from a secret file."""
    from subprocess import CompletedProcess

    from loom_cli.service_cmd import _seed_test_data

    fake_stdout = "team: loom_team_aaaaaa\nworker: loom_w_bbbbbb\nbuilder: loom_tib_cccccc\n"

    def _fake_run(*_args, **_kwargs):
        return CompletedProcess([], 0, fake_stdout, "")

    monkeypatch.setattr("loom_cli.service_cmd.subprocess.run", _fake_run)
    rc, tokens = _seed_test_data("postgresql://x/y")
    assert rc == 0
    assert tokens == {
        "team": "loom_team_aaaaaa",
        "worker": "loom_w_bbbbbb",
        "builder": "loom_tib_cccccc",
    }


def test_seed_test_data_invokes_dev_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`loom service up` must call the seed script with `--mode dev`
    so the Benchmarks table is populated and no placeholder
    (hello-world Task, card-e2e RateCard) is seeded into the dev
    stack."""
    from subprocess import CompletedProcess

    from loom_cli.service_cmd import _seed_test_data

    captured_argv: list[list[str]] = []

    def _fake_run(argv, *_args, **_kwargs):
        captured_argv.append(list(argv))
        return CompletedProcess([], 0, "team: t\nworker: w\n", "")

    monkeypatch.setattr("loom_cli.service_cmd.subprocess.run", _fake_run)
    rc, _tokens = _seed_test_data("postgresql://x/y")
    assert rc == 0
    assert captured_argv, "expected at least one subprocess.run call"
    argv = captured_argv[0]
    assert "--mode" in argv
    assert argv[argv.index("--mode") + 1] == "dev"
    assert "--print" in argv
    assert argv[argv.index("--print") + 1] == "all"


def test_print_summary_labels_admin_as_file_backed_dev_singleton(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The summary must not print the raw admin secret."""
    from loom_cli.service_cmd import _print_summary

    _print_summary({"team": "loom_team_x", "admin": "loom_admin_y"})
    out = capsys.readouterr().out
    assert "loom_team_x" in out
    assert "loom_admin_y" not in out
    assert "DEV-ONLY" in out
    assert "file-backed" in out
    assert "reveal-admin" in out


def test_ensure_dev_admin_secret_creates_0600_and_returns_token(
    tmp_path: Path,
) -> None:
    from loom_cli.service_cmd import _ensure_dev_admin_secret

    secret_file = tmp_path / ".loom" / "admin" / "secrets.toml"

    token = _ensure_dev_admin_secret(secret_file)

    assert token == _read_admin_token(secret_file)
    assert token.startswith("loom_admin_")
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600


def test_ensure_dev_admin_secret_preserves_existing_token(
    tmp_path: Path,
) -> None:
    from loom_cli.service_cmd import _ensure_dev_admin_secret

    secret_file = tmp_path / ".loom" / "admin" / "secrets.toml"
    existing = "loom_admin_" + "E" * 43
    secret_file.parent.mkdir(parents=True)
    secret_file.write_text(
        f'[admin]\ntoken = "{existing}"\n',
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    token = _ensure_dev_admin_secret(secret_file)

    assert token == existing


def test_write_env_tokens_creates_file_when_absent(tmp_path) -> None:
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    _write_env_tokens(
        env_file,
        {
            "team": "loom_team_aaa",
            "worker": "loom_w_bbb",
            "admin": "loom_admin_ccc",
        },
    )
    content = env_file.read_text()
    assert "LOOM_TEAM_TOKEN=loom_team_aaa" in content
    assert "LOOM_WORKER_TOKEN=loom_w_bbb" in content
    assert "LOOM_ADMIN_TOKEN=loom_admin_ccc" in content


def test_write_env_tokens_writes_builder_token(tmp_path: Path) -> None:
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    _write_env_tokens(
        env_file,
        {
            "team": "loom_team_t",
            "worker": "loom_w_w",
            "builder": "loom_tib_b",
            "admin": "loom_admin_a",
        },
    )
    content = env_file.read_text()
    assert "LOOM_TASK_IMAGE_BUILDER_TOKEN=loom_tib_b" in content


def test_write_env_tokens_replaces_existing_keys_preserving_others(
    tmp_path,
) -> None:
    """Idempotent overwrite: existing token lines get the new value,
    unrelated lines (comments, custom env vars) survive verbatim."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    env_file.write_text(
        "# Local dev tokens\n"
        "LOOM_WORKER_TOKEN=loom_w_old\n"
        "LOOM_TEAM_TOKEN=loom_team_old\n"
        "LOOM_ADMIN_TOKEN=loom_admin_old\n"
        "MY_CUSTOM_VAR=please-keep-me\n",
    )
    _write_env_tokens(
        env_file,
        {
            "team": "loom_team_new",
            "worker": "loom_w_new",
            "admin": "loom_admin_new",
        },
    )
    lines = env_file.read_text().splitlines()
    assert "# Local dev tokens" in lines
    assert "LOOM_TEAM_TOKEN=loom_team_new" in lines
    assert "LOOM_WORKER_TOKEN=loom_w_new" in lines
    assert "LOOM_ADMIN_TOKEN=loom_admin_new" in lines
    assert "MY_CUSTOM_VAR=please-keep-me" in lines
    # Old values are gone; no duplicates of any key.
    assert not any(line.endswith("=loom_team_old") for line in lines)
    keys = [
        line.split("=", 1)[0] for line in lines if "=" in line and not line.lstrip().startswith("#")
    ]
    assert len(keys) == len(set(keys))


def test_write_env_tokens_appends_missing_keys(tmp_path) -> None:
    """If only one key exists in .env, the other two must be appended
    rather than silently dropped."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    env_file.write_text("LOOM_TEAM_TOKEN=loom_team_only\n")
    _write_env_tokens(
        env_file,
        {
            "team": "loom_team_new",
            "worker": "loom_w_new",
            "admin": "loom_admin_new",
        },
    )
    content = env_file.read_text()
    assert "LOOM_TEAM_TOKEN=loom_team_new" in content
    assert "LOOM_WORKER_TOKEN=loom_w_new" in content
    assert "LOOM_ADMIN_TOKEN=loom_admin_new" in content


def test_up_recreates_worker_after_seeding_fresh_tokens(
    tmp_path: Path,
) -> None:
    """After `_seed_test_data` mints fresh tokens and `_write_env_tokens`
    persists them to .env, the worker container is still running with
    the STALE token it booted with — `docker restart` reuses the old env,
    only `up --force-recreate` re-reads .env. `loom service up` MUST issue
    that recreate, otherwise the worker keeps rejecting control-plane
    requests with 401 until the operator notices.
    """
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
        "admin": "loom_admin_db_ignored",
    }
    admin_secret_token = "loom_admin_" + "U" * 43

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", return_value=0),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(0, fake_tokens)),
        patch("loom_cli.service_cmd._ensure_dev_admin_secret", return_value=admin_secret_token),
        patch("loom_cli.service_cmd._mint_batch_runner_cp_token", return_value=None),
    ):
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 0
    assert env_file.exists(), "env_file should have been written"
    assert f"LOOM_ADMIN_TOKEN={admin_secret_token}" in env_file.read_text()
    assert "loom_admin_db_ignored" not in env_file.read_text()

    # Find the recreate call — must come after the initial `up -d` and
    # carry --force-recreate + --no-deps + worker target.
    recreate_calls = [
        argv for argv in captured_run_calls if "--force-recreate" in argv and "worker" in argv
    ]
    assert len(recreate_calls) == 1, (
        f"expected exactly one --force-recreate worker call; "
        f"got {len(recreate_calls)} of {len(captured_run_calls)} _run calls. "
        f"All argvs: {captured_run_calls!r}"
    )
    recreate_argv = recreate_calls[0]
    assert "up" in recreate_argv
    assert "-d" in recreate_argv
    assert "--no-deps" in recreate_argv

    # Order check: the worker recreate must come AFTER _write_env_tokens
    # has run. When mint returns None (no loom-service recreate), the
    # worker recreate is the last _run call.
    assert captured_run_calls[-1] == recreate_argv, (
        "worker recreate must be the last subprocess call, AFTER "
        "_write_env_tokens has persisted fresh tokens"
    )


def test_up_recreates_task_image_builder_after_seeding_builder_token(
    tmp_path: Path,
) -> None:
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"
    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
        "builder": "loom_tib_fresh",
        "admin": "loom_admin_db_ignored",
    }

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", return_value=0),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(0, fake_tokens)),
        patch(
            "loom_cli.service_cmd._ensure_dev_admin_secret", return_value="loom_admin_" + "U" * 43
        ),
        patch("loom_cli.service_cmd._mint_batch_runner_cp_token", return_value=None),
    ):
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 0
    assert "LOOM_TASK_IMAGE_BUILDER_TOKEN=loom_tib_fresh" in env_file.read_text()
    builder_recreates = [
        argv
        for argv in captured_run_calls
        if "--force-recreate" in argv and "task-image-builder" in argv
    ]
    assert len(builder_recreates) == 1
    assert "--no-deps" in builder_recreates[0]


def test_up_skips_worker_recreate_when_no_env_file(
    tmp_path: Path,
) -> None:
    """When operator runs without --env-file, _write_env_tokens is
    skipped — there's no .env to update, so there's no stale token to
    flush. Recreating the worker would be pointless work.
    """
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
    }

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", return_value=0),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(0, fake_tokens)),
        patch(
            "loom_cli.service_cmd._ensure_dev_admin_secret", return_value="loom_admin_" + "N" * 43
        ),
        patch("loom_cli.service_cmd._mint_batch_runner_cp_token", return_value="loom_br_unused"),
    ):
        # argparse defaults --env-file to <compose_dir>/.env if not
        # explicitly None, so we have to pass --env-file pointing
        # somewhere AND ensure the loader treats it as "absent".
        # The CLI doesn't actually have a "no env file" mode — it
        # defaults to .env next to the compose file. So this test
        # documents that "env_file is None" is actually an internal
        # branch reachable only via the API, not the CLI flag set.
        import argparse

        from loom_cli.service_cmd import _DEFAULT_CP_URL, _up_local

        args = argparse.Namespace(
            compose_file=compose,
            env_file=None,
            db_url="postgresql://x/y",
            admin_secret_file=tmp_path / ".loom" / "admin" / "secrets.toml",
            cp_url=_DEFAULT_CP_URL,
        )
        rc = _up_local(args)

    assert rc == 0
    # No recreate should have happened (env_file is None → nothing to write).
    recreate_calls = [argv for argv in captured_run_calls if "--force-recreate" in argv]
    assert recreate_calls == [], (
        f"unexpected --force-recreate when env_file is None: {recreate_calls!r}"
    )


def test_init_admin_secret_writes_0600_without_printing_token(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"

    rc = main(
        [
            "service",
            "init-admin",
            "--secret-file",
            str(secret_file),
        ]
    )

    assert rc == 0
    token = _read_admin_token(secret_file)
    assert token.startswith("loom_admin_")
    assert len(token.removeprefix("loom_admin_")) >= 32
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600
    captured = capsys.readouterr()
    assert token not in captured.out
    assert token not in captured.err
    assert str(secret_file) in captured.out


def test_reveal_admin_requires_confirmation_unless_yes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"
    token = "loom_admin_" + "R" * 43
    secret_file.write_text(
        f'[admin]\ntoken = "{token}"\n',
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    monkeypatch.setattr("sys.stdin", io.StringIO("no\n"))
    denied = main(
        [
            "service",
            "reveal-admin",
            "--secret-file",
            str(secret_file),
        ]
    )
    denied_output = capsys.readouterr()

    approved = main(
        [
            "service",
            "reveal-admin",
            "--secret-file",
            str(secret_file),
            "--yes",
        ]
    )
    approved_output = capsys.readouterr()

    assert denied == 2
    assert token not in denied_output.out
    assert token not in denied_output.err
    assert approved == 0
    assert token in approved_output.out


def test_rotate_admin_replaces_secret_without_printing_new_token(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"
    old_token = "loom_admin_" + "O" * 43
    secret_file.write_text(
        f'[admin]\ntoken = "{old_token}"\n',
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    rc = main(
        [
            "service",
            "rotate-admin",
            "--secret-file",
            str(secret_file),
        ]
    )

    assert rc == 0
    new_token = _read_admin_token(secret_file)
    assert new_token.startswith("loom_admin_")
    assert new_token != old_token
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600
    out = capsys.readouterr().out
    assert new_token not in out
    assert old_token not in out
    assert "restart" in out.lower()


# ---------------------------------------------------------------------------
# _mint_batch_runner_cp_token
# ---------------------------------------------------------------------------


def test_mint_batch_runner_cp_token_returns_token_on_201(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 201 response with a token field yields the raw token string."""

    from loom_cli.service_cmd import _mint_batch_runner_cp_token

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        assert "/admin/batch-runner-tokens" in url
        assert "Authorization" in headers
        return httpx.Response(
            201, json={"token": "loom_br_testtoken", "token_hash_prefix": "ab12cd34"}
        )

    monkeypatch.setattr("loom_cli.service_cmd.httpx.post", _fake_post)
    result = _mint_batch_runner_cp_token("loom_admin_" + "A" * 43)
    assert result == "loom_br_testtoken"


def test_mint_batch_runner_cp_token_returns_none_on_non_201(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A non-201 response yields None and prints a warning."""

    from loom_cli.service_cmd import _mint_batch_runner_cp_token

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        return httpx.Response(403, text="missing scope admin:tokens")

    monkeypatch.setattr("loom_cli.service_cmd.httpx.post", _fake_post)
    result = _mint_batch_runner_cp_token("loom_admin_" + "B" * 43)
    assert result is None
    err = capsys.readouterr().err
    assert "warning" in err.lower()
    assert "403" in err


def test_mint_batch_runner_cp_token_returns_none_on_network_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A network error yields None and prints a recoverable warning."""

    from loom_cli.service_cmd import _mint_batch_runner_cp_token

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr("loom_cli.service_cmd.httpx.post", _fake_post)
    result = _mint_batch_runner_cp_token("loom_admin_" + "C" * 43)
    assert result is None
    err = capsys.readouterr().err
    assert "warning" in err.lower()
    assert "connection refused" in err


# ---------------------------------------------------------------------------
# _up: batch-runner token integration
# ---------------------------------------------------------------------------


def test_up_mints_batch_runner_token_and_writes_to_env(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """After a successful seed, _up mints a batch-runner CP token, writes
    it to .env as LOOM_SVC_BATCH_RUNNER_CP_TOKEN, and prints a confirm line."""
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
    }
    admin_secret_token = "loom_admin_" + "U" * 43
    batch_runner_token = "loom_br_fakebatchtoken"

    def _capture_run(argv, *_args, **_kwargs):
        return CompletedProcess(argv, 0, "", "")

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", return_value=0),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(0, fake_tokens)),
        patch("loom_cli.service_cmd._ensure_dev_admin_secret", return_value=admin_secret_token),
        patch(
            "loom_cli.service_cmd._mint_batch_runner_cp_token", return_value=batch_runner_token
        ) as mock_mint,
    ):
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 0
    # mint was called with the admin token
    mock_mint.assert_called_once()
    call_args = mock_mint.call_args
    assert call_args.args[0] == admin_secret_token

    # token written to .env
    assert env_file.exists()
    env_content = env_file.read_text()
    assert f"LOOM_SVC_BATCH_RUNNER_CP_TOKEN={batch_runner_token}" in env_content

    # confirmation line printed
    out = capsys.readouterr().out
    assert "batch-runner CP token written to .env" in out


def test_up_recreates_loom_service_after_writing_batch_runner_token(
    tmp_path: Path,
) -> None:
    """loom-service must be force-recreated AFTER .env is updated so it
    reads the freshly-minted LOOM_SVC_BATCH_RUNNER_CP_TOKEN."""
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {"team": "loom_team_x", "worker": "loom_w_x"}
    admin_secret_token = "loom_admin_" + "V" * 43
    batch_runner_token = "loom_br_recreatetest"

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", return_value=0),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(0, fake_tokens)),
        patch("loom_cli.service_cmd._ensure_dev_admin_secret", return_value=admin_secret_token),
        patch("loom_cli.service_cmd._mint_batch_runner_cp_token", return_value=batch_runner_token),
    ):
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 0

    # loom-service recreate call must exist
    svc_recreate_calls = [
        argv for argv in captured_run_calls if "--force-recreate" in argv and "loom-service" in argv
    ]
    assert len(svc_recreate_calls) == 1, (
        f"expected exactly one --force-recreate loom-service call; "
        f"got {len(svc_recreate_calls)} of {len(captured_run_calls)} _run calls. "
        f"All argvs: {captured_run_calls!r}"
    )
    svc_recreate_argv = svc_recreate_calls[0]
    assert "up" in svc_recreate_argv
    assert "-d" in svc_recreate_argv
    assert "--no-deps" in svc_recreate_argv

    # The loom-service recreate must come AFTER the env file is written
    # (i.e., it must be a later _run call than the initial `up -d`).
    initial_up_idx = next(
        i
        for i, argv in enumerate(captured_run_calls)
        if "up" in argv and "-d" in argv and "--force-recreate" not in argv
    )
    svc_recreate_idx = captured_run_calls.index(svc_recreate_argv)
    assert svc_recreate_idx > initial_up_idx, (
        "loom-service recreate must come after initial compose up"
    )

    # env file must have the token before the recreate ran (we wrote it)
    assert f"LOOM_SVC_BATCH_RUNNER_CP_TOKEN={batch_runner_token}" in env_file.read_text()


def test_up_skips_loom_service_recreate_when_mint_fails(
    tmp_path: Path,
) -> None:
    """If minting the batch-runner token fails (returns None), _up must
    not attempt to recreate loom-service — that would be pointless."""
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {"team": "loom_team_x", "worker": "loom_w_x"}

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", return_value=0),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(0, fake_tokens)),
        patch(
            "loom_cli.service_cmd._ensure_dev_admin_secret", return_value="loom_admin_" + "W" * 43
        ),
        patch("loom_cli.service_cmd._mint_batch_runner_cp_token", return_value=None),
    ):
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 0
    # No loom-service recreate when mint failed
    svc_recreate_calls = [
        argv for argv in captured_run_calls if "--force-recreate" in argv and "loom-service" in argv
    ]
    assert svc_recreate_calls == [], (
        f"unexpected loom-service recreate when mint failed: {svc_recreate_calls!r}"
    )


# ---------------------------------------------------------------------------
# _write_env_tokens: batch_runner_cp key
# ---------------------------------------------------------------------------


def test_write_env_tokens_writes_batch_runner_cp_token(tmp_path: Path) -> None:
    """LOOM_SVC_BATCH_RUNNER_CP_TOKEN must be persisted alongside the
    other token keys when the batch_runner_cp label is present."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    _write_env_tokens(
        env_file,
        {
            "team": "loom_team_t",
            "worker": "loom_w_w",
            "admin": "loom_admin_a",
            "batch_runner_cp": "loom_br_b",
        },
    )
    content = env_file.read_text()
    assert "LOOM_SVC_BATCH_RUNNER_CP_TOKEN=loom_br_b" in content


def test_write_env_tokens_replaces_existing_batch_runner_cp_token(
    tmp_path: Path,
) -> None:
    """Idempotent overwrite: an existing LOOM_SVC_BATCH_RUNNER_CP_TOKEN
    line is replaced with the new value on re-seed."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    env_file.write_text(
        "LOOM_TEAM_TOKEN=loom_team_old\nLOOM_SVC_BATCH_RUNNER_CP_TOKEN=loom_br_old\n",
    )
    _write_env_tokens(
        env_file,
        {
            "team": "loom_team_new",
            "batch_runner_cp": "loom_br_new",
        },
    )
    lines = env_file.read_text().splitlines()
    assert "LOOM_TEAM_TOKEN=loom_team_new" in lines
    assert "LOOM_SVC_BATCH_RUNNER_CP_TOKEN=loom_br_new" in lines
    assert not any("loom_br_old" in line for line in lines)


# ---------------------------------------------------------------------------
# _mint_secret_store_master_key + _ensure_secret_store_master_key
# ---------------------------------------------------------------------------


def test_secret_store_master_key_is_32_bytes_base64() -> None:
    """The helper must produce a base64-encoded 32-byte (256-bit) key."""
    import base64

    from loom_cli.service_cmd import _mint_secret_store_master_key

    key = _mint_secret_store_master_key()
    raw = base64.b64decode(key)
    assert len(raw) == 32, f"expected 32 bytes, got {len(raw)}"


def test_up_generates_secret_store_master_key_if_absent(
    tmp_path: Path,
) -> None:
    """On a fresh .env (no pre-existing key), `_up` must write
    LOOM_SECRET_STORE_MASTER_KEY before docker compose starts."""
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"
    # env_file does NOT yet exist (fresh dev setup)

    def _capture_run(argv, *_args, **_kwargs):
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {"team": "loom_team_t", "worker": "loom_w_w"}
    admin_secret_token = "loom_admin_" + "K" * 43

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", return_value=0),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(0, fake_tokens)),
        patch("loom_cli.service_cmd._ensure_dev_admin_secret", return_value=admin_secret_token),
        patch("loom_cli.service_cmd._mint_batch_runner_cp_token", return_value=None),
    ):
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 0
    assert env_file.exists(), ".env should have been created"
    content = env_file.read_text()
    assert "LOOM_SECRET_STORE_MASTER_KEY=" in content
    # The value must be non-empty
    for line in content.splitlines():
        if line.startswith("LOOM_SECRET_STORE_MASTER_KEY="):
            value = line.split("=", 1)[1]
            assert value, "LOOM_SECRET_STORE_MASTER_KEY must have a non-empty value"
            break


def test_up_preserves_existing_secret_store_master_key(
    tmp_path: Path,
) -> None:
    """When LOOM_SECRET_STORE_MASTER_KEY already exists in .env, `_up` must
    NOT regenerate it — rotating the master key would invalidate all stored
    provider-connection secrets."""
    import base64
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"
    existing_key = base64.b64encode(b"existing-32-byte-key-padded-here").decode()
    env_file.write_text(f"LOOM_SECRET_STORE_MASTER_KEY={existing_key}\n")

    def _capture_run(argv, *_args, **_kwargs):
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {"team": "loom_team_t", "worker": "loom_w_w"}
    admin_secret_token = "loom_admin_" + "P" * 43

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", return_value=0),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(0, fake_tokens)),
        patch("loom_cli.service_cmd._ensure_dev_admin_secret", return_value=admin_secret_token),
        patch("loom_cli.service_cmd._mint_batch_runner_cp_token", return_value=None),
    ):
        rc = main(
            [
                "service",
                "up",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 0
    content = env_file.read_text()
    # The original key must be unchanged.
    assert f"LOOM_SECRET_STORE_MASTER_KEY={existing_key}" in content
    # No duplicate lines for the key.
    key_lines = [
        line for line in content.splitlines() if line.startswith("LOOM_SECRET_STORE_MASTER_KEY=")
    ]
    assert len(key_lines) == 1, (
        f"expected exactly one LOOM_SECRET_STORE_MASTER_KEY line; "
        f"got {len(key_lines)}: {key_lines!r}"
    )


def _clear_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "LOOM_DEV_POSTGRES_PORT", "LOOM_DEV_POSTGRES_USER", "LOOM_DEV_POSTGRES_PASSWORD",
        "LOOM_DEV_POSTGRES_DB", "LOOM_DEV_BIND_ADDR", "LOOM_DEV_CONTROL_PLANE_PORT",
        "LOOM_DEV_DOCKER_SOCKET", "DOCKER_HOST",
    ):
        monkeypatch.delenv(name, raising=False)


def test_local_defaults_follow_dev_port_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    """Compose publishes Postgres and the Control Plane on LOOM_DEV_* ports, so
    the host-side migration and token steps must use the same ones."""
    import argparse

    from loom_cli.service_cmd import _populate_local_defaults

    _clear_dev_env(monkeypatch)
    monkeypatch.setenv("LOOM_DEV_POSTGRES_PORT", "15432")
    monkeypatch.setenv("LOOM_DEV_POSTGRES_PASSWORD", "p@ss/word")
    monkeypatch.setenv("LOOM_DEV_CONTROL_PLANE_PORT", "28080")
    args = argparse.Namespace(compose_file=None, env_file=None, db_url=None, admin_secret_file=None, cp_url=None)

    _populate_local_defaults(args)

    assert args.db_url == "postgresql+psycopg://loom:p%40ss%2Fword@localhost:15432/loom"
    assert args.cp_url == "http://localhost:28080"


def test_local_defaults_keep_explicit_urls_and_plain_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    import argparse

    from loom_cli.service_cmd import _DEFAULT_CP_URL, _DEFAULT_DB_URL, _populate_local_defaults

    _clear_dev_env(monkeypatch)
    args = argparse.Namespace(compose_file=None, env_file=None, db_url=None, admin_secret_file=None, cp_url=None)
    _populate_local_defaults(args)
    assert (args.db_url, args.cp_url) == (_DEFAULT_DB_URL, _DEFAULT_CP_URL)

    monkeypatch.setenv("LOOM_DEV_POSTGRES_PORT", "15432")
    explicit = argparse.Namespace(
        compose_file=None, env_file=None, db_url="postgresql+psycopg://x@db:1/x",
        admin_secret_file=None, cp_url="http://cp:1",
    )
    _populate_local_defaults(explicit)
    assert (explicit.db_url, explicit.cp_url) == ("postgresql+psycopg://x@db:1/x", "http://cp:1")


@pytest.mark.parametrize(
    ("docker_host", "preset", "expected"),
    [
        ("unix:///run/user/2015/docker.sock", None, "/run/user/2015/docker.sock"),
        ("tcp://10.0.0.1:2376", None, None),
        (None, None, None),
        ("unix:///run/user/2015/docker.sock", "/custom.sock", "/custom.sock"),
    ],
)
def test_local_defaults_export_rootless_docker_socket(
    monkeypatch: pytest.MonkeyPatch, docker_host: str | None, preset: str | None, expected: str | None,
) -> None:
    import argparse
    import os

    from loom_cli.service_cmd import _populate_local_defaults

    _clear_dev_env(monkeypatch)
    if docker_host:
        monkeypatch.setenv("DOCKER_HOST", docker_host)
    if preset:
        monkeypatch.setenv("LOOM_DEV_DOCKER_SOCKET", preset)
    args = argparse.Namespace(compose_file=None, env_file=None, db_url=None, admin_secret_file=None, cp_url=None)

    _populate_local_defaults(args)

    assert os.environ.get("LOOM_DEV_DOCKER_SOCKET") == expected


def test_export_env_tokens_overrides_values_loaded_from_old_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """`loom` loads .env at startup and Compose prefers shell variables over
    --env-file, so recreated containers would get the old tokens."""
    import os

    from loom_cli.service_cmd import _export_env_tokens

    monkeypatch.setenv("LOOM_WORKER_TOKEN", "old-worker")
    monkeypatch.setenv("LOOM_SVC_BATCH_RUNNER_CP_TOKEN", "old-runner")
    monkeypatch.setenv("LOOM_TEAM_TOKEN", "keep-me")

    _export_env_tokens({"worker": "new-worker", "batch_runner_cp": "new-runner", "team": ""})

    assert os.environ["LOOM_WORKER_TOKEN"] == "new-worker"
    assert os.environ["LOOM_SVC_BATCH_RUNNER_CP_TOKEN"] == "new-runner"
    assert os.environ["LOOM_TEAM_TOKEN"] == "keep-me"
