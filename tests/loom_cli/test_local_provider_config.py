"""LoomConfig — local_providers section + config_cmd `loom config set
local.<name>.{base_url,api_key}` round-trip + show + validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.config import LocalProvider, LoomConfig, load_config, save_config


def test_config_round_trip_with_local_providers(tmp_xdg_home: Path) -> None:
    cfg = LoomConfig(
        local_providers={
            "vllm": LocalProvider(base_url="http://localhost:8000/v1"),
            "ollama": LocalProvider(
                base_url="http://localhost:11434/v1",
                api_key="sk-foo",
            ),
        },
    )
    save_config(cfg)
    loaded = load_config()
    assert set(loaded.local_providers.keys()) == {"vllm", "ollama"}
    assert loaded.local_providers["vllm"].base_url == "http://localhost:8000/v1"
    assert loaded.local_providers["vllm"].api_key is None
    assert loaded.local_providers["ollama"].api_key == "sk-foo"


def test_set_local_base_url_creates_provider(tmp_xdg_home: Path) -> None:
    rc = main([
        "config", "set", "local.vllm.base_url",
        "http://localhost:8000/v1",
    ])
    assert rc == 0
    cfg = load_config()
    assert cfg.local_providers["vllm"].base_url == "http://localhost:8000/v1"
    assert cfg.local_providers["vllm"].api_key is None


def test_set_local_api_key_requires_existing_provider(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """Setting api_key on a provider that hasn't had its base_url set
    yet is a user error — fail with the fix suggestion."""
    rc = main(["config", "set", "local.foo.api_key", "sk-bar"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not registered" in err
    assert "base_url" in err


def test_set_local_api_key_after_base_url_works(tmp_xdg_home: Path) -> None:
    main(["config", "set", "local.vllm.base_url", "http://x:8000/v1"])
    rc = main(["config", "set", "local.vllm.api_key", "sk-bar"])
    assert rc == 0
    cfg = load_config()
    assert cfg.local_providers["vllm"].api_key == "sk-bar"
    assert cfg.local_providers["vllm"].base_url == "http://x:8000/v1"


def test_set_rejects_invalid_local_provider_name(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["config", "set", "local.Bad Name.base_url", "http://x/v1"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "invalid local provider name" in err


def test_set_rejects_malformed_local_key(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    # local.<name> alone (no field) → reject
    rc = main(["config", "set", "local.vllm", "http://x/v1"])
    assert rc == 2
    # local.<name>.<unknown> → reject
    rc2 = main(["config", "set", "local.vllm.foo", "bar"])
    assert rc2 == 2


def test_show_lists_local_providers_with_redacted_keys(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    main(["config", "set", "local.vllm.base_url", "http://localhost:8000/v1"])
    main(["config", "set", "local.vllm.api_key", "sk-supersecret-foo"])
    capsys.readouterr()
    rc = main(["config", "show"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "local_providers:" in out
    assert "vllm.base_url = http://localhost:8000/v1" in out
    assert "vllm.api_key" in out
    assert "sk-supersecret-foo" not in out
    assert "***" in out


def test_load_rejects_provider_without_base_url(
    tmp_xdg_home: Path,
) -> None:
    # Write a TOML by hand that has a local provider with no base_url
    from loom_cli.config import config_path
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[local_providers.broken]\napi_key = "sk-foo"\n',
    )
    with pytest.raises(ValueError, match="base_url is required"):
        load_config()


def test_set_rejects_empty_base_url(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["config", "set", "local.vllm.base_url", "   "])
    assert rc == 2
    err = capsys.readouterr().err
    assert "empty base_url rejected" in err


def test_set_warns_when_base_url_missing_v1(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    """Soft warning — most local servers serve under `/v1`. The set
    still succeeds (some setups legitimately omit it), but a warning
    on stderr nudges users to `loom models test` first."""
    rc = main(["config", "set", "local.vllm.base_url", "http://localhost:8000"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "warning" in err.lower()
    assert "/v1" in err
    assert "loom models test local/vllm" in err


def test_set_does_not_warn_when_v1_present(
    tmp_xdg_home: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["config", "set", "local.vllm.base_url", "http://localhost:8000/v1"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "warning" not in err.lower()


def test_local_provider_served_model_name_optional(tmp_xdg_home: Path) -> None:
    """A config block without served_model_name still loads (backward
    compat)."""
    cfg_path = tmp_xdg_home / "loom" / "config.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        '[local_providers.vllm]\n'
        'base_url = "http://localhost:8000/v1"\n',
    )
    from loom_cli.config import load_config
    cfg = load_config()
    assert cfg.local_providers["vllm"].base_url == "http://localhost:8000/v1"
    assert cfg.local_providers["vllm"].served_model_name is None


def test_local_provider_served_model_name_round_trip(
    tmp_xdg_home: Path,
) -> None:
    """A config block WITH served_model_name preserves it on round-trip."""
    cfg_path = tmp_xdg_home / "loom" / "config.toml"
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(
        '[local_providers.llama8b]\n'
        'base_url = "http://localhost:8234/v1"\n'
        'served_model_name = "meta-llama/Llama-3.1-8B-Instruct"\n',
    )
    from loom_cli.config import load_config
    cfg = load_config()
    entry = cfg.local_providers["llama8b"]
    assert entry.base_url == "http://localhost:8234/v1"
    assert entry.served_model_name == "meta-llama/Llama-3.1-8B-Instruct"


def test_set_local_provider_round_trips(tmp_xdg_home: Path) -> None:
    from loom_cli.config import load_config, set_local_provider
    set_local_provider(
        "llama8b",
        base_url="http://localhost:8234/v1",
        served_model_name="meta-llama/Llama-3.1-8B-Instruct",
    )
    cfg = load_config()
    assert cfg.local_providers["llama8b"].base_url == "http://localhost:8234/v1"
    assert cfg.local_providers["llama8b"].served_model_name == \
        "meta-llama/Llama-3.1-8B-Instruct"


def test_unset_local_provider_removes_entry(tmp_xdg_home: Path) -> None:
    from loom_cli.config import load_config, set_local_provider, unset_local_provider
    set_local_provider("doomed", base_url="http://example.com/v1")
    unset_local_provider("doomed")
    cfg = load_config()
    assert "doomed" not in cfg.local_providers


def test_unset_local_provider_is_noop_if_missing(tmp_xdg_home: Path) -> None:
    from loom_cli.config import unset_local_provider
    unset_local_provider("never-existed")  # must not raise


def test_native_dialect_survives_config_mutations(tmp_xdg_home):
    cfg = LoomConfig(local_providers={"native": LocalProvider(base_url="https://proxy.test/anthropic",
        api_dialect="anthropic", served_model_name="served")})
    save_config(cfg)
    assert main(["config", "set", "local.native.api_key", "secret"]) == 0
    assert main(["config", "set", "local.native.base_url", "https://proxy.test/prefix"]) == 0
    entry = load_config().local_providers["native"]
    assert entry.api_dialect == "anthropic" and entry.served_model_name == "served" and entry.api_key == "secret"
    assert main(["config", "set", "local.native.api_dialect", "openai_chat"]) == 0
    assert load_config().local_providers["native"].api_dialect == "openai_chat"
    assert main(["config", "set", "local.native.api_dialect", "unknown"]) == 2


def test_legacy_local_config_defaults_to_chat_and_invalid_dialect_fails(tmp_xdg_home):
    from loom_cli.config import config_path

    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('[local_providers.x]\nbase_url="http://localhost:8000/v1"\n')
    assert load_config().local_providers["x"].api_dialect == "openai_chat"
    path.write_text(path.read_text() + 'api_dialect="guess"\n')
    with pytest.raises(ValueError, match="api_dialect"):
        load_config()
