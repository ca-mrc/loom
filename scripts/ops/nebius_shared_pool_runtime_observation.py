"""Existing runtime probes with only anchored shared-handoff successors.

Nested settings/database probes call inspect themselves. Bind that inventory
here too; accepting the namespace only in an outer observer is insufficient.
"""
from __future__ import annotations

import copy
import ssl
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_runtime_probes import HTTPSDevelopmentRuntimeProbes
from scripts.ops.nebius_shared_pool_handoff_live import SharedPoolGuardBindings


def shared_forward_runtime_view(*, bindings: SharedPoolGuardBindings, state_dir: Path
        ) -> tuple[dict[str, Any], dict[str, dict[str, str]], dict[str, tuple[dict[str, Any], ...]]]:
    """Bind both dev successors without widening the dev reader to staging.

    Live writer authority is checked by the separate revision-bound census;
    this inventory retains only the namespace and gateway workload projections.
    All relevant receipt bytes and the shared parent bracket remote reads.
    """
    from scripts.ops.nebius_shared_pool_build_activation import shared_forward_build_namespace_view
    from scripts.ops.nebius_shared_pool_gateway import shared_forward_gateway_view

    build = shared_forward_build_namespace_view(bindings=bindings, state_dir=state_dir)
    gateway = shared_forward_gateway_view(bindings=bindings, state_dir=state_dir)
    if (build[0] != gateway[0] or build[2].keys() & gateway[2].keys()
            or shared_forward_build_namespace_view(bindings=bindings, state_dir=state_dir) != build):
        raise ValueError('shared runtime successor history changed')
    return build[0], {'forward-build-policy': build[1], 'forward-gateway': gateway[1]}, {**build[2], **gateway[2]}


class HTTPSSharedPoolRuntimeProbes(HTTPSDevelopmentRuntimeProbes):
    def __init__(self, *, bindings: SharedPoolGuardBindings, api_server: str,
            ssl_context: ssl.SSLContext, token: str,
            private_files: dict[Path, bytes] | None = None):
        self.shared_bindings = copy.deepcopy(bindings)
        super().__init__(request=self.shared_bindings.plan.development.runtime.request,
            api_server=api_server, ssl_context=ssl_context, token=token, private_files=private_files)

    def inspect(self, *, state_dir: Path, _prechild_phase: str | None = None,
            _expected_record: dict[str, Any] | None = None,
            _shared_handoff: SharedPoolGuardBindings | None = None) -> dict[str, dict[str, Any]]:
        if _shared_handoff is not None and _shared_handoff != self.shared_bindings:
            raise ValueError('shared runtime observation binding differs')
        return super().inspect(state_dir=state_dir, _prechild_phase=_prechild_phase,
            _expected_record=_expected_record, _shared_handoff=self.shared_bindings)
