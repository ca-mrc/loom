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
