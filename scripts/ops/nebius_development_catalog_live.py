"""Fixed catalog transport after the qualified closed-control-plane transition.

The internal parent supplies phase-aware live qualification. The initial database
adapter's original-workload check is deliberately not replayed after replacement.
Historical file pins and regenerated fixed documents remain mandatory throughout.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Callable
from pathlib import Path
from typing import Any

from scripts.ops.nebius_development_catalog_runtime import (
    catalog_runtime_documents,
    validate_catalog_runtime_proof,
)
from scripts.ops.nebius_development_runtime_database_live import HTTPSDevelopmentRuntimeDatabaseAPI
from scripts.ops.nebius_development_runtime_job import read_runtime_job
from scripts.ops.nebius_development_runtime_setup import DevelopmentDatabaseRuntime
from scripts.ops.nebius_management_material import ManagementBinding


class HTTPSDevelopmentCatalogAPI(HTTPSDevelopmentRuntimeDatabaseAPI):
    """Only the regenerated catalog ConfigMap/Job can pass the write allowlist."""

    phase = 'development-runtime-catalog'

    def __init__(self, *, request: DevelopmentDatabaseRuntime, api_server: str,
                 ssl_context: ssl.SSLContext, qualify_runtime: Callable[[DevelopmentDatabaseRuntime], None],
                 token: str | None = None, private_files: dict[Path, bytes] | None = None):
        documents = catalog_runtime_documents(request)
        if not callable(qualify_runtime):
            raise ValueError('development catalog runtime qualifier missing')
        super().__init__(request=request, api_server=api_server, ssl_context=ssl_context,
            token=token, private_files=private_files)
        self.documents = documents
        self.qualify_runtime = qualify_runtime

    def verify_identity(self, binding: ManagementBinding) -> None:
        try:
            if binding != self.binding:
                raise ValueError
            self._private_inputs()
            self.qualify_runtime(copy.deepcopy(self.runtime))
            self._private_inputs()
        except Exception:
            raise ValueError('development catalog prerequisites unqualified') from None

    def catalog_report(self, state_dir: Path) -> dict[str, Any] | None:
        try:
            return read_runtime_job(client=self.client, read=lambda path: self._request('GET', path),
                recorded=lambda: self._recorded(state_dir), private_inputs=self._private_inputs,
                region=self.runtime.foundation.inputs.config['region'], report_field='catalog',
                validate=lambda proof: validate_catalog_runtime_proof(self.runtime, state_dir, proof))
        except Exception:
            raise ValueError('development catalog execution unqualified; preserve evidence') from None
