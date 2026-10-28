"""Non-secret runtime environment bounds shared with the isolated controller."""

import re

MAX_ENV_VALUE_BYTES = 4096
_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]{0,127}$")
_SECRET_ENV = re.compile(r"(?:KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|KUBECONFIG)")


def validate_runtime_environment(value: dict[str, str]) -> dict[str, str]:
    if len(value) > 64:
        raise ValueError("process environment exceeds 64 entries")
    for name, item in value.items():
        # Official Python images publish this public signing-key fingerprint.
        public_fingerprint = name == "GPG_KEY" and re.fullmatch(r"[A-Fa-f0-9]{40}|[A-Fa-f0-9]{64}", item)
        # VNC keyboard event pacing is a public numeric setting, not key material.
        public_key_delay = name == "VNC_KEY_DELAY_MS" and re.fullmatch(r"[0-9]{1,6}", item)
        if _ENV_NAME.fullmatch(name) is None or (_SECRET_ENV.search(name) and not (public_fingerprint or public_key_delay)):
            raise ValueError("process environment contains a forbidden name")
        if "\x00" in item or len(item.encode("utf-8")) > MAX_ENV_VALUE_BYTES:
            raise ValueError("process environment value is invalid")
    return value


def validate_task_environment(value: dict[str, str], *, allow_runtime_controls: bool = False) -> dict[str, str]:
    """Authored fixture values stay inside the sandbox, outside controller authority."""
    if len(value) > 64:
        raise ValueError("task environment exceeds 64 entries")
    for name, item in value.items():
        platform_control = allow_runtime_controls and name == "LOOM_WORKSPACE_MAX_ARCHIVE_BYTES"
        if (_ENV_NAME.fullmatch(name) is None or (name.startswith("LOOM_") and not platform_control)
                or name in {"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "KUBECONFIG"}):
            raise ValueError("task environment contains a reserved name")
        if "\x00" in item or len(item.encode("utf-8")) > MAX_ENV_VALUE_BYTES:
            raise ValueError("task environment value is invalid")
    return value


# A publish diagnostic may name the rejected variable. It must not carry the value.
TASK_IMAGE_ENVIRONMENT_REJECTION = re.compile(
    r"^task image environment rejected(?: [A-Z_][A-Z0-9_]{0,127}| invalid-name)?$"
)


def rejected_task_image_environment(value: dict[str, str]) -> str | None:
    """Safe rejection for authored image defaults, or None when they are accepted."""

    if len(value) > 64:
        return "task image environment rejected"
    for name, item in value.items():
        try:
            validate_task_environment({name: item})
        except ValueError:
            shown = name if _ENV_NAME.fullmatch(name) else "invalid-name"
            return f"task image environment rejected {shown}"
    return None
