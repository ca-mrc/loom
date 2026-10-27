"""Reviewed product support metadata, separate from source validation and admission.

Only changes to this platform-owned catalog policy can change product support.
Task payloads and uploaded profile descriptors cannot override these decisions.
Unknown/new adapters retain the ordinary supported policy; their requirements,
source authority and admission are validated by the common execution boundaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal


@dataclass(frozen=True)
class CatalogSupportPolicy:
    state: Literal["supported", "unsupported_runtime", "deferred_support", "not_v1_supported"]
    message: str | None = None


_SUPPORTED = CatalogSupportPolicy("supported")
_OUTSIDE_V1 = CatalogSupportPolicy(
    "not_v1_supported",
    "This benchmark is outside the current v1.0 benchmark support set. "
    "It is visible for catalog transparency but cannot be selected yet.",
)

# These are policy records, not lifecycle branches. Preserve the existing
# support scope independently of an adapter's validation or qualification.
CATALOG_SUPPORT_POLICY = MappingProxyType({
    'aime-24': _SUPPORTED,
    'aime-25': _SUPPORTED,
    'humaneval': _SUPPORTED,
    'livecodebench': _SUPPORTED,
    'mbpp': _SUPPORTED,
    'mmlu-pro': _SUPPORTED,
    'math-500': _SUPPORTED,
    'gpqa': _SUPPORTED,
    'skillflow': _SUPPORTED,
    'skilllearnbench': _SUPPORTED,
    'swe-bench-verified': _SUPPORTED,
    'terminal-bench-2': _SUPPORTED,
    'terminal-bench-2@tb2.1-r6': _SUPPORTED,
    'aime-22': _OUTSIDE_V1,
    'aime-23': _OUTSIDE_V1,
    'bfcl': _OUTSIDE_V1,
    'browsecomp': _OUTSIDE_V1,
    'hendrycks-math': _OUTSIDE_V1,
    'swe-bench': _OUTSIDE_V1,
    'swe-bench-multimodal': _OUTSIDE_V1,
    'tau2-bench': _OUTSIDE_V1,
    "osworld": CatalogSupportPolicy(
        "unsupported_runtime",
        "OSWorld requires a UI benchmark runtime with desktop VM/DesktopEnv "
        "support before it can be selected.",
    ),
    "webarena": CatalogSupportPolicy(
        "unsupported_runtime",
        "WebArena requires a UI benchmark runtime with browser-agent control, "
        "self-hosted sites, auth reset, and URL/HTML evaluators before it can "
        "be selected.",
    ),
    "gaia": CatalogSupportPolicy(
        "deferred_support",
        "GAIA requires a GAIA-authorized Hugging Face token and a published "
        "staging task bundle before it can be selected.",
    ),
})


def catalog_support_policy(profile_id: str | None) -> CatalogSupportPolicy:
    return CATALOG_SUPPORT_POLICY.get(profile_id or "", _SUPPORTED)


CURRENTLY_UNSUPPORTED_BENCHMARK_IDS = frozenset(
    profile_id for profile_id, policy in CATALOG_SUPPORT_POLICY.items()
    if policy.state != "supported"
)
