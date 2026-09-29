"""XDG-aware config for the `loom` CLI.

Lives at `$XDG_CONFIG_HOME/loom/config.toml` (default
`~/.config/loom/config.toml`). Holds:

- `tokens` — upstream LLM provider API keys (anthropic / openai /
  google)
- `server_url` — optional Control Plane URL for additive
  result-posting from `loom run`
- `local_providers` — registered locally-served OpenAI-compatible
  LLM servers (vLLM, ollama, llama.cpp, lm-studio). Each server has
  a name + base_url + optional api_key. The name disambiguates
  between multiple local servers; the base_url is the
  OpenAI-compatible endpoint (typically ends in `/v1`).

All fields are optional — a fresh install has no file and
`load_config()` returns an empty `LoomConfig`.
"""

from __future__ import annotations

import os
import stat
import tempfile
import tomllib
from dataclasses import asdict, dataclass, field
from pathlib import Path

import tomli_w

from loom_cli.contexts import ManagedApplicationBinding, ManagedEnvironmentBinding, current_context

CONFIG_FILENAME = "config.toml"


def _xdg_config_home() -> Path:
    val = os.environ.get("XDG_CONFIG_HOME")
    if val:
        return Path(val)
    return Path(os.environ.get("HOME", str(Path.home()))) / ".config"


def config_path() -> Path:
    root = _xdg_config_home() / "loom"
    context = current_context()
    return root / "contexts" / (context + ".toml") if context is not None else root / CONFIG_FILENAME


@dataclass
class LocalProvider:
    """One registered locally-served OpenAI-compatible LLM server."""

    base_url: str
    api_key: str | None = None
    served_model_name: str | None = None


@dataclass
class LoomConfig:
    """Persisted CLI config. Mutable so callers can set fields then save."""

    tokens: dict[str, str] = field(default_factory=dict)
    server_url: str | None = None
    # Bearer token for `server_url` API calls (set by token-mode
    # `loom auth login`).
    auth_token: str | None = None
    # Username/password login stores the same session cookie + CSRF token the
    # web UI uses. Stored in the same owner-only config file as bearer tokens.
    auth_session_cookie: str | None = None
    auth_session_cookie_name: str = "loom_session"
    auth_csrf_token: str | None = None
    local_providers: dict[str, LocalProvider] = field(default_factory=dict)
    managed_environment: ManagedEnvironmentBinding | None = None
    managed_application: ManagedApplicationBinding | None = None
    # Session rotation saves to the config's source, even after a nested context
    # exits. Not serialized and not part of config value equality.
    _storage_path: Path | None = field(default=None, repr=False, compare=False)

    def to_toml_dict(self) -> dict[str, object]:
        out: dict[str, object] = {}
        self.validate_binding()
        if self.managed_environment is not None:
            out["managed_environment"] = asdict(self.managed_environment)
        if self.managed_application is not None:
            out["managed_application"] = asdict(self.managed_application)
        if self.tokens:
            out["tokens"] = dict(self.tokens)
        if self.server_url is not None:
            out["server_url"] = self.server_url
        if self.auth_token is not None:
            out["auth_token"] = self.auth_token
        if self.auth_session_cookie is not None:
            out["auth_session_cookie"] = self.auth_session_cookie
            out["auth_session_cookie_name"] = self.auth_session_cookie_name
        if self.auth_csrf_token is not None:
            out["auth_csrf_token"] = self.auth_csrf_token
        if self.local_providers:
            local: dict[str, dict[str, str]] = {}
            for name, p in self.local_providers.items():
                entry: dict[str, str] = {"base_url": p.base_url}
                if p.api_key is not None:
                    entry["api_key"] = p.api_key
                if p.served_model_name is not None:
                    entry["served_model_name"] = p.served_model_name
                local[name] = entry
            out["local_providers"] = local
        return out

    def validate_binding(self) -> None:
        if self.managed_environment is not None and self.managed_application is not None:
            raise ValueError("context cannot have both environment and application bindings")
        for binding in (self.managed_environment, self.managed_application):
            if binding is not None and self.server_url != binding.child_origin:
                raise ValueError("server URL conflicts with managed context binding")


def load_config() -> LoomConfig:
    path = config_path()
    if not path.exists():
        return LoomConfig(_storage_path=path)
    _check_regular_destination(path)
    raw = tomllib.loads(path.read_text())
    tokens_obj = raw.get("tokens", {})
    if not isinstance(tokens_obj, dict):
        raise ValueError(f"{path}: [tokens] must be a table")
    tokens = {str(k): str(v) for k, v in tokens_obj.items()}
    server_url = raw.get("server_url")
    if server_url is not None and not isinstance(server_url, str):
        raise ValueError(f"{path}: server_url must be a string")
    auth_token = raw.get("auth_token")
    if auth_token is not None and not isinstance(auth_token, str):
        raise ValueError(f"{path}: auth_token must be a string")
    auth_session_cookie = raw.get("auth_session_cookie")
    if auth_session_cookie is not None and not isinstance(auth_session_cookie, str):
        raise ValueError(f"{path}: auth_session_cookie must be a string")
    auth_session_cookie_name = raw.get("auth_session_cookie_name", "loom_session")
    if auth_session_cookie_name not in ("loom_session", "__Host-loom_session"):
        raise ValueError(f"{path}: unsupported auth_session_cookie_name")
    auth_csrf_token = raw.get("auth_csrf_token")
    if auth_csrf_token is not None and not isinstance(auth_csrf_token, str):
        raise ValueError(f"{path}: auth_csrf_token must be a string")
    local_raw = raw.get("local_providers", {})
    if not isinstance(local_raw, dict):
        raise ValueError(f"{path}: [local_providers] must be a table")
    local_providers: dict[str, LocalProvider] = {}
    for name, entry in local_raw.items():
        if not isinstance(entry, dict):
            raise ValueError(
                f"{path}: [local_providers.{name}] must be a table "
                f"with at minimum a base_url",
            )
        base_url = entry.get("base_url")
        if not isinstance(base_url, str) or not base_url:
            raise ValueError(
                f"{path}: [local_providers.{name}].base_url is required "
                f"(string, non-empty)",
            )
        api_key = entry.get("api_key")
        if api_key is not None and not isinstance(api_key, str):
            raise ValueError(
                f"{path}: [local_providers.{name}].api_key must be a string",
            )
        served_model_name = entry.get("served_model_name")
        if served_model_name is not None and not isinstance(served_model_name, str):
            raise ValueError(
                f"{path}: [local_providers.{name}].served_model_name must be a string",
            )
        local_providers[str(name)] = LocalProvider(
            base_url=base_url,
            api_key=api_key,
            served_model_name=served_model_name,
        )
    cfg = LoomConfig(
        tokens=tokens,
        server_url=server_url,
        auth_token=auth_token,
        auth_session_cookie=auth_session_cookie,
        auth_session_cookie_name=auth_session_cookie_name,
        auth_csrf_token=auth_csrf_token,
        local_providers=local_providers,
        managed_environment=ManagedEnvironmentBinding.from_dict(raw.get("managed_environment")),
        managed_application=ManagedApplicationBinding.from_dict(raw.get("managed_application")),
        _storage_path=path,
    )
    cfg.validate_binding()
    return cfg


def _check_regular_destination(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISREG(mode):
        raise ValueError("CLI config must be a regular file, not a symlink")


def save_config(cfg: LoomConfig) -> None:
    path = cfg._storage_path or config_path()
    cfg.validate_binding()
    if (cfg.managed_environment is not None or cfg.managed_application is not None) and path.parent.name != "contexts":
        raise ValueError("a managed deployment requires a separate named context")
    if path.parent.is_symlink():
        raise ValueError("CLI config parent must be a regular directory")
    path.parent.mkdir(parents=True, exist_ok=True)
    _check_regular_destination(path)
    if os.name != "nt":
        path.parent.chmod(0o700)
    descriptor, name = tempfile.mkstemp(prefix=".config-", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(tomli_w.dumps(cfg.to_toml_dict()))
            stream.flush()
            os.fsync(stream.fileno())
        try:
            # Exclusive first publication closes the two-writers-create-the-
            # same-name race. Losers must verify the winning binding before
            # replacing credentials; they cannot replace another environment.
            os.link(temporary, path)
        except FileExistsError:
            _check_regular_destination(path)
            existing = tomllib.loads(path.read_text())
            binding = ManagedEnvironmentBinding.from_dict(existing.get("managed_environment"))
            application = ManagedApplicationBinding.from_dict(existing.get("managed_application"))
            if (binding, application) != (cfg.managed_environment, cfg.managed_application):
                raise ValueError("existing context binding cannot be changed or removed") from None
            os.replace(temporary, path)
        cfg._storage_path = path
    finally:
        temporary.unlink(missing_ok=True)


def set_local_provider(
    name: str,
    *,
    base_url: str,
    api_key: str | None = None,
    served_model_name: str | None = None,
) -> None:
    """Persist a `[local_providers.<name>]` entry to
    `~/.config/loom/config.toml`. Overwrites any existing entry with
    the same name."""
    cfg = load_config()
    cfg.local_providers[name] = LocalProvider(
        base_url=base_url,
        api_key=api_key,
        served_model_name=served_model_name,
    )
    save_config(cfg)


def unset_local_provider(name: str) -> None:
    """Remove a `[local_providers.<name>]` entry. Silent no-op if
    the entry doesn't exist."""
    cfg = load_config()
    if name in cfg.local_providers:
        del cfg.local_providers[name]
        save_config(cfg)
