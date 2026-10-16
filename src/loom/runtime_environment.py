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
        if _ENV_NAME.fullmatch(name) is None or (_SECRET_ENV.search(name) and not public_fingerprint):
            raise ValueError("process environment contains a forbidden name")
        if "\x00" in item or len(item.encode("utf-8")) > MAX_ENV_VALUE_BYTES:
            raise ValueError("process environment value is invalid")
    return value
