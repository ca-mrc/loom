"""Explicit CLI contexts isolate credentials without a mutable global target."""

from __future__ import annotations

import os

import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, config_path, load_config, save_config


def test_named_auth_dispatch_keeps_default_login_and_resets_selection(tmp_xdg_home, monkeypatch, capsys):
    save_config(LoomConfig(server_url="https://management.example.com", auth_token="management-secret"))
    original = config_path().read_bytes()
    monkeypatch.setenv("TEST_CHILD_TOKEN", "child-secret")
    assert main(["--context", "dev-alice", "auth", "login", "--server", "https://alice.example.com",
                 "--token", "env:TEST_CHILD_TOKEN"]) == 0
    assert config_path().read_bytes() == original
    assert main(["--context", "dev-alice", "auth", "status"]) == 0
    assert "https://alice.example.com" in capsys.readouterr().out
    assert load_config().auth_token == "management-secret"
    assert main(["auth", "status"]) == 0
    assert "https://management.example.com" in capsys.readouterr().out


@pytest.mark.parametrize("name", ["../outside", "/tmp/outside", ".", "", "a/b", "a\\b", "a" * 97])
def test_invalid_context_cannot_escape_config_directory(tmp_xdg_home, name, capsys):
    assert main(["--context", name, "auth", "status"]) == 2
    assert "context" in capsys.readouterr().err.lower()
    assert not (tmp_xdg_home / "loom").exists()


def test_loaded_credentials_keep_their_storage_origin_across_selection(tmp_xdg_home):
    from loom_cli.contexts import selected_context

    save_config(LoomConfig(server_url="https://management.example.com", auth_token="management-secret"))
    with selected_context("alice"):
        save_config(LoomConfig(server_url="https://alice.example.com", auth_token="old-child-secret"))
        cfg = load_config()
        alice_path = config_path()
    cfg.auth_token = "rotated-child-secret"
    save_config(cfg)
    assert load_config().auth_token == "management-secret"
    with selected_context("alice"):
        assert load_config().auth_token == "rotated-child-secret"
    if os.name != "nt":
        assert alice_path.stat().st_mode & 0o777 == 0o600
        assert alice_path.parent.stat().st_mode & 0o777 == 0o700


def test_failed_atomic_replace_preserves_existing_credentials(tmp_xdg_home, monkeypatch):
    save_config(LoomConfig(server_url="https://management.example.com", auth_token="old-secret"))
    original = config_path().read_bytes()

    def fail_replace(*args, **kwargs):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated disk failure"):
        save_config(LoomConfig(server_url="https://management.example.com", auth_token="new-secret"))
    assert config_path().read_bytes() == original
    assert list(config_path().parent.iterdir()) == [config_path()]


def test_config_symlink_does_not_overwrite_another_file(tmp_xdg_home, tmp_path):
    target = tmp_path / "unrelated.toml"
    target.write_text('auth_token="unrelated-secret"')
    path = config_path()
    path.parent.mkdir(parents=True)
    path.symlink_to(target)
    with pytest.raises(ValueError, match="regular"):
        save_config(LoomConfig(auth_token="replacement"))
    assert target.read_text() == 'auth_token="unrelated-secret"'


def test_context_selection_resets_after_exception(tmp_xdg_home):
    from loom_cli.contexts import selected_context

    default_path = config_path()
    with pytest.raises(RuntimeError):
        with selected_context("alice"):
            assert config_path() != default_path
            raise RuntimeError("failed command")
    assert config_path() == default_path


def managed_binding(origin="https://alice.example.com", identity="20000000-0000-4000-8000-000000000001"):
    from loom_cli.contexts import ManagedEnvironmentBinding

    return ManagedEnvironmentBinding(environment_id=identity, incarnation=identity,
                                     management_origin="https://management.example.com", child_origin=origin)


def test_managed_context_refuses_changed_or_removed_binding_and_server(tmp_xdg_home):
    from dataclasses import replace

    from loom_cli.contexts import selected_context
    from loom_cli.server_client import authed_client

    binding = managed_binding()
    with selected_context("alice"):
        save_config(LoomConfig(server_url=binding.child_origin, auth_token="child", managed_environment=binding))
        original = config_path().read_bytes()
        for changes in ({"server_url": "https://foreign.example.com"}, {"managed_environment": None},
                        {"managed_environment": replace(binding, incarnation="20000000-0000-4000-8000-000000000002")}):
            cfg = load_config()
            for key, value in changes.items():
                setattr(cfg, key, value)
            with pytest.raises(ValueError, match="binding"):
                save_config(cfg)
            assert config_path().read_bytes() == original
        cfg = load_config()
        cfg.server_url = "https://foreign.example.com"
        with pytest.raises(ValueError, match="binding"):
            authed_client(cfg)


def test_management_or_unbound_existing_context_cannot_be_replaced_by_child(tmp_xdg_home):
    from loom_cli.contexts import selected_context

    binding = managed_binding()
    with pytest.raises(ValueError, match="named context"):
        save_config(LoomConfig(server_url=binding.child_origin, managed_environment=binding))
    with selected_context("alice"):
        save_config(LoomConfig(server_url=binding.child_origin, auth_token="preexisting-login"))
        with pytest.raises(ValueError, match="binding"):
            save_config(LoomConfig(server_url=binding.child_origin, managed_environment=binding))
        assert load_config().auth_token == "preexisting-login"


def test_competing_first_managed_context_writes_keep_exactly_one_binding(tmp_xdg_home):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from loom_cli.contexts import selected_context

    barrier = Barrier(2)

    def create(identity):
        with selected_context("same-name"):
            cfg = LoomConfig(server_url="https://alice.example.com", auth_token=identity,
                             managed_environment=managed_binding(identity=identity))
            barrier.wait()
            try:
                save_config(cfg)
                return "saved"
            except ValueError:
                return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, ["20000000-0000-4000-8000-000000000001", "20000000-0000-4000-8000-000000000002"]))
    assert sorted(results) == ["conflict", "saved"]
    with selected_context("same-name"):
        cfg = load_config()
        assert cfg.auth_token == cfg.managed_environment.environment_id


def test_application_binding_cannot_change_origin_identity_or_binding_kind(tmp_xdg_home):
    from dataclasses import replace

    from loom_cli.contexts import ManagedApplicationBinding, selected_context
    from loom_cli.server_client import authed_client

    legacy = managed_binding()
    binding = ManagedApplicationBinding(application_id=legacy.environment_id, incarnation=legacy.incarnation,
                                        management_origin=legacy.management_origin, child_origin=legacy.child_origin)
    with pytest.raises(ValueError, match="named context"):
        save_config(LoomConfig(server_url=binding.child_origin, managed_application=binding))
    with selected_context("application"):
        save_config(LoomConfig(server_url=binding.child_origin, auth_token="child", managed_application=binding))
        original = config_path().read_bytes()
        for changes in ({"server_url": "https://foreign.example.com"}, {"managed_application": None},
                        {"managed_application": replace(binding, application_id="20000000-0000-4000-8000-000000000002")},
                        {"managed_application": None, "managed_environment": legacy}, {"managed_environment": legacy}):
            cfg = load_config()
            for field, value in changes.items():
                setattr(cfg, field, value)
            with pytest.raises(ValueError, match="binding"):
                save_config(cfg)
            assert config_path().read_bytes() == original
        cfg = load_config()
        cfg.server_url = "https://foreign.example.com"
        with pytest.raises(ValueError, match="binding"):
            authed_client(cfg)


def test_competing_application_and_environment_context_writes_keep_one_binding(tmp_xdg_home):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from loom_cli.contexts import ManagedApplicationBinding, selected_context

    barrier = Barrier(2)
    legacy = managed_binding()
    application = ManagedApplicationBinding(application_id=legacy.environment_id, incarnation=legacy.incarnation,
                                            management_origin=legacy.management_origin, child_origin=legacy.child_origin)

    def create(kind):
        with selected_context("same-name"):
            cfg = LoomConfig(server_url=legacy.child_origin, auth_token=kind, **{
                "managed_environment" if kind == "environment" else "managed_application":
                    legacy if kind == "environment" else application,
            })
            barrier.wait()
            try:
                save_config(cfg)
                return "saved"
            except ValueError:
                return "conflict"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(create, ["environment", "application"]))
    assert sorted(results) == ["conflict", "saved"]
    with selected_context("same-name"):
        cfg = load_config()
        assert (cfg.managed_application is not None) == (cfg.auth_token == "application")
        assert (cfg.managed_environment is not None) == (cfg.auth_token == "environment")
