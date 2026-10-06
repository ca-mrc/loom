"""Image-only specialization of the existing exact manager CAS/drain transport."""
from __future__ import annotations

import copy
from typing import Any

from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_manager_image_history import (
    ManagerImageRepairBinding,
    manager_image_entry,
)
from scripts.ops.nebius_pool_manager_image_stage import qualify_image_entry_closed
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI
from scripts.ops.nebius_pool_startup_repair_live import HTTPSPoolStartupRepairAPI


class HTTPSPoolManagerImageAPI(HTTPSPoolStartupRepairAPI):
    """No configuration writer; retain the existing parent's authority and client."""

    def __init__(self, *, parent: HTTPSPoolCutoverAPI, binding: ManagerImageRepairBinding):
        HTTPSPoolStartupAPI.__init__(self, parent=parent)
        self.image_binding = ManagerImageRepairBinding.model_validate(binding.model_dump())
        self.documents = manager_image_entry(self.request, self.image_binding,
            state=self.state, anchor=self.anchor).documents

    def _qualify_binding(self) -> dict[str, Any] | None:
        self._scope()
        entry = manager_image_entry(self.request, self.image_binding, state=self.state, anchor=self.anchor)
        if entry.documents != self.documents:
            raise ValueError('pool_manager_image_entry_changed')
        qualify_image_entry_closed(entry, state=self.state, anchor=self.anchor)
        return entry.record

    def _replacement_template(self, before: dict[str, Any]) -> dict[str, Any]:
        template = copy.deepcopy(before['spec']['template'])
        pod = template['spec']
        for container in (*pod['containers'], *pod.get('initContainers', [])):
            container['image'] = self.image_binding.candidate['images']['service']['image_ref']
        return template
