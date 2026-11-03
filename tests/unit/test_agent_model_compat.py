"""Validation matrix for agent ⇄ model compatibility.

The catalog declares each agent's `supported_providers` +
`supported_model_sources`; `validate_agent_model_compat` is the single
gate used by both the SPA-facing batch route and the trial route.
Tests pin every interesting combination so accidental drift in either
side breaks loudly."""

from __future__ import annotations

from loom.models.types import ModelSpec
from loom_service.agent_catalog import (
    get_agent,
    known_names,
    list_agents,
    validate_agent_model_compat,
)


def test_oracle_with_no_model_ok() -> None:
    assert validate_agent_model_compat("oracle", None) is None


def test_oracle_with_a_model_rejects() -> None:
    err = validate_agent_model_compat(
        "oracle",
        ModelSpec(provider="anthropic", name="claude-opus-4"),
    )
    assert err is not None
    assert "does not take a model" in err


def test_litellm_accepts_any_provider_api() -> None:
    assert (
        validate_agent_model_compat(
            "litellm",
            ModelSpec(provider="anthropic", name="claude-opus-4"),
        )
        is None
    )


def test_direct_completion_is_canonical_and_litellm_is_alias() -> None:
    canonical = get_agent("direct-completion")
    alias = get_agent("litellm")

    assert canonical is not None
    assert alias is canonical
    assert canonical.name == "direct-completion"
    assert canonical.aliases == ("litellm",)
    assert {"direct-completion", "litellm"}.issubset(known_names())
    assert "litellm" not in {entry.name for entry in list_agents()}
    assert (
        validate_agent_model_compat(
            "litellm",
            ModelSpec(provider="openai", name="gpt-4o"),
        )
        is None
    )


def test_retired_hf_source_rejects() -> None:
    err = validate_agent_model_compat(
        "litellm",
        ModelSpec(
            provider="hf",
            name="meta-llama/Llama-3-8B-Instruct",
            source="hf",
        ),
    )
    assert err is not None
    assert "retired" in err


def test_retired_local_server_source_rejects() -> None:
    err = validate_agent_model_compat(
        "litellm",
        ModelSpec(
            provider="local",
            name="llama3",
            source="local-server",
            local_server="ollama",
        ),
    )
    assert err is not None
    assert "retired" in err


def test_local_server_field_only_with_local_source() -> None:
    err = validate_agent_model_compat(
        "litellm",
        ModelSpec(
            provider="anthropic",
            name="claude-opus-4",
            source="api",
            local_server="ollama",
        ),
    )
    assert err is not None
    assert "local_server" in err


SUPPORTED_PRODUCT_ENTRIES = {"oracle", "direct-completion", "terminus-2", "openhands-sdk", "codex", "claude-code"}


def test_only_the_supported_product_entries_are_selectable() -> None:
    supported = {a.name for a in list_agents() if a.product_support == "supported"}
    assert supported == SUPPORTED_PRODUCT_ENTRIES
    for agent in list_agents():
        if agent.product_support == "deferred":
            assert agent.deferred_reason


def test_supported_model_backed_entries_accept_api_models() -> None:
    model = ModelSpec(provider="openai", name="gpt-4o")
    for name in SUPPORTED_PRODUCT_ENTRIES - {"oracle"}:
        assert validate_agent_model_compat(name, model) is None, name


def test_deferred_agent_rejects_with_reason() -> None:
    for name in ("opencode", "swe-agent"):
        err = validate_agent_model_compat(
            name,
            ModelSpec(provider="openai", name="gpt-4o"),
        )
        assert err is not None
        assert "not available for new submissions" in err
        assert "#2054" in err


def test_openhands_is_an_alias_of_openhands_sdk() -> None:
    canonical = get_agent("openhands-sdk")
    assert canonical is not None
    assert get_agent("openhands") == canonical
    assert canonical.aliases == ("openhands",)
    assert canonical.to_dict()["display_name"] == "openhands"
    assert "openhands" not in {entry.name for entry in list_agents()}
    assert "openhands" in known_names()
    assert (
        validate_agent_model_compat(
            "openhands",
            ModelSpec(provider="openai", name="gpt-4o"),
        )
        is None
    )


def test_codex_is_not_restricted_by_provider_name() -> None:
    """Wire-protocol compatibility comes from the Provider Connection type,
    not the model's provider namespace (#2054)."""
    codex = get_agent("codex")
    assert codex is not None
    assert codex.supported_providers == ("*",)
    assert (
        validate_agent_model_compat(
            "codex",
            ModelSpec(provider="deepseek", name="deepseek-chat"),
        )
        is None
    )


def test_needs_model_with_null_model_rejects() -> None:
    err = validate_agent_model_compat("litellm", None)
    assert err is not None
    assert "requires a model" in err


def test_unknown_agent_rejects() -> None:
    err = validate_agent_model_compat(
        "doesnt-exist",
        ModelSpec(provider="anthropic", name="claude-opus-4"),
    )
    assert err is not None
    assert "unknown agent_name" in err


def test_to_dict_includes_new_metadata() -> None:
    """SPA reads these fields from /api/v1/agents — make sure they're
    in the dict shape, not just on the dataclass."""
    a = get_agent("litellm")
    assert a is not None
    d = a.to_dict()
    assert d["supported_providers"] == ["*"]
    assert d["supported_model_sources"] == ["api"]
    assert d["product_support"] == "supported"
    assert d["deferred_reason"] is None
    assert d["display_name"] == "direct-completion"


def test_catalog_entries_include_service_mode_runtime_contract() -> None:
    """Every displayed agent needs enough runtime metadata for the SPA
    and direct API callers to understand whether service-mode can run it."""
    displayed = {
        "oracle",
        "direct-completion",
        "aider",
        "claude-code",
        "codex",
        "gemini-cli",
        "kimi-cli",
        "mini-swe-agent",
        "opencode",
        "openhands-sdk",
        "qwen-cli",
        "swe-agent",
    }
    by_name = {a.name: a.to_dict() for a in list_agents()}
    assert displayed.issubset(by_name)
    assert "hello" not in by_name

    for name in displayed:
        data = by_name[name]
        assert isinstance(data["service_mode_ready"], bool)
        assert data["readiness_status"] in {"ready", "unavailable"}
        contract = data["runtime_contract"]
        assert isinstance(contract, dict)
        assert "execution" in contract
        assert "capture" in contract
        assert "required_executables" in contract
        assert "required_python_modules" in contract
        assert "endpoint_dialect" in contract
        assert "api_key_env" in contract
        assert "base_url_env" in contract


def test_hello_is_an_internal_no_model_canary() -> None:
    assert get_agent("hello") is None

    hello = get_agent("hello", include_internal=True)
    assert hello is not None
    assert hello.catalog_visibility == "internal"
    assert hello.needs_model is False
    assert hello.supported_providers == ()
    assert hello.supported_model_sources == ()
    assert "no model call" in hello.description


def test_catalog_package_hints_use_verified_install_sources() -> None:
    by_name = {agent.name: agent.to_dict() for agent in list_agents()}

    assert by_name["opencode"]["runtime_contract"]["required_packages"] == [
        "opencode-ai",
    ]
    assert by_name["kimi-cli"]["runtime_contract"]["required_packages"] == [
        "@moonshot-ai/kimi-code",
    ]
    assert by_name["swe-agent"]["runtime_contract"]["required_packages"] == [
        "git+https://github.com/SWE-agent/SWE-agent",
    ]
    assert by_name["openhands-sdk"]["runtime_contract"]["required_python_modules"] == [
        "loom_launcher.openhands_sdk_runner",
        "openhands.sdk",
        "openhands.tools.terminal",
    ]
    assert by_name["openhands-sdk"]["runtime_contract"]["required_packages"] == [
        "openhands-sdk",
        "openhands-tools",
    ]
    assert by_name["openhands-sdk"]["runtime_contract"]["required_executables"] == [
        "tmux",
    ]



def test_terminus_2_accepts_openai_compatible_gateway_models() -> None:
    err = validate_agent_model_compat(
        "terminus-2",
        ModelSpec(provider="openai", name="deepseek-chat"),
    )
    assert err is None


def test_terminus_2_catalog_advertises_any_gateway_provider() -> None:
    terminus = get_agent("terminus-2")
    assert terminus is not None
    assert terminus.supported_providers == ("*",)
    assert terminus.supported_model_sources == ("api",)


def test_existing_modelspec_default_source_is_api() -> None:
    """Backwards-compat: callers that don't set `source` get the legacy
    catalog-backed API path."""
    m = ModelSpec(provider="anthropic", name="claude-opus-4")
    assert m.source == "api"
