"""Fixed collector ConfigMap and CronJob HTTPS transport for the closed parent.

Reuse the runtime's cross-namespace create, UID/resourceVersion CAS and descendant
drain mechanics. No arbitrary document, namespace, credential or cloud mutation
is selected here. The parent supplies continuously checked phase authority.
"""
from __future__ import annotations

import copy
import ssl
from collections.abc import Callable
from typing import Any

from scripts.ops.nebius_development_runtime_live import (
    HTTPSDevelopmentRuntimeResources,
    HTTPSDevelopmentRuntimeWorkloads,
)
from scripts.ops.nebius_management_transport import ManagementKubernetesTransport
from scripts.ops.nebius_pool_preactivation_operation import _unchanged
from scripts.ops.nebius_pool_storage_operation import (
    RetainedClosedStorage,
    load_closed_storage,
    storage_configuration_identity,
    storage_transition_inputs,
)


class HTTPSStorageCollectorAPI(HTTPSDevelopmentRuntimeResources, HTTPSDevelopmentRuntimeWorkloads):
    def __init__(self, *, retained: RetainedClosedStorage, phase: str, api_server: str,
            ssl_context: ssl.SSLContext, qualify: Callable[[], None], token: str | None = None):
        selected = load_closed_storage(runtime=retained.inputs.runtime, reference=retained.reference)
        if (selected != retained or phase not in {'pause', 'configuration', 'reconfigure', 'resume'}
                or api_server.rstrip('/') != retained.runtime.database.foundation.inputs.config['kubernetes_api_server'].rstrip('/')):
            raise ValueError('storage collector connection differs')
        self.retained = copy.deepcopy(retained)
        self.origin = copy.deepcopy(retained)
        self.request = self.retained.runtime
        self.phase, self.check = phase, qualify
        self.binding = retained.registration.binding
        self.documents = storage_configuration_identity(retained)[0] if phase == 'configuration' else {}
        self.originals, self.targets = self._transitions()
        ManagementKubernetesTransport.__init__(self, api_server=api_server, ssl_context=ssl_context, token=token)

    def _transitions(self) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        if self.phase == 'configuration':
            return {}, {}
        originals, targets, _ = storage_transition_inputs(self.retained, self.phase)
        return originals, targets

    def _private_inputs(self) -> None:
        if self.retained != self.origin or self.request != self.origin.runtime or self.binding != self.origin.registration.binding:
            raise ValueError('storage collector selection changed')
        _unchanged(self.origin.files)
        documents = storage_configuration_identity(self.retained)[0] if self.phase == 'configuration' else {}
        if self.documents != documents or (self.originals, self.targets) != self._transitions():
            raise ValueError('storage collector documents changed')

    def _qualify(self, *, writing: bool) -> None:
        self._private_inputs()
        self.check()
        self._private_inputs()

    def observe_ready(self, *args: object) -> bool:
        # The inherited workload reader already proves exact CAS readback and,
        # while suspended, terminal Jobs/Pods. Fresh aggregate publication is a
        # separate terminal condition of the connected operation, not this child.
        self._qualify(writing=False)
        return True
