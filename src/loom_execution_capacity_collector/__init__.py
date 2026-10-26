"""Read-only Nebius/Kubernetes capacity observation collector.

Importing contracts or the control-plane client must not construct the
collector's settings and provider adapters. Resolve the public collector only
when a caller requests it, retaining the original callable and signature.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from loom_execution_capacity_collector.collector import collect_capacity_observation

__all__ = ["collect_capacity_observation"]


def __getattr__(name: str) -> Any:
    if name == "collect_capacity_observation":
        from loom_execution_capacity_collector.collector import collect_capacity_observation

        globals()[name] = collect_capacity_observation
        return collect_capacity_observation
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
