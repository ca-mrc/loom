"""Image-only specialization of the existing exact manager CAS/drain transport."""
from __future__ import annotations

import copy
from typing import Any

from scripts.ops.nebius_ingress_stage import _key
from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_manager_image_history import (
    IMAGE_MARKER,
    STEPS,
    ManagerImageRepairBinding,
    RuntimeImageRepairBinding,
    manager_image_entry,
    parse_image_binding,
)
from scripts.ops.nebius_pool_manager_image_stage import qualify_image_entry_closed
from scripts.ops.nebius_pool_runtime_image import runtime_image_component, runtime_image_template
from scripts.ops.nebius_pool_startup_live import HTTPSPoolStartupAPI
from scripts.ops.nebius_pool_startup_repair_live import HTTPSPoolStartupRepairAPI


class HTTPSPoolManagerImageAPI(HTTPSPoolStartupRepairAPI):
    """No configuration writer; retain the existing parent's authority and client."""

    steps = STEPS
    drain_slice = slice(2, 4)

    def __init__(self, *, parent: HTTPSPoolCutoverAPI, binding: ManagerImageRepairBinding):
        HTTPSPoolStartupAPI.__init__(self, parent=parent)
        self.image_binding = parse_image_binding(binding.model_dump())
        self.documents = manager_image_entry(self.request, self.image_binding,
            state=self.state, anchor=self.anchor).documents

    def _repair_workload(self) -> dict[str, Any]:
        return self.closed[_key(self.documents[0])]

    def _qualify_binding(self) -> dict[str, Any] | None:
        self._scope()
        entry = manager_image_entry(self.request, self.image_binding, state=self.state, anchor=self.anchor)
        if entry.documents != self.documents:
            raise ValueError('pool_manager_image_entry_changed')
        qualify_image_entry_closed(entry, state=self.state, anchor=self.anchor)
        return entry.record

    def _replacement_template(self, before: dict[str, Any]) -> dict[str, Any]:
        template = copy.deepcopy(runtime_image_template(before))
        pod = template['spec']
        component = (runtime_image_component(self.image_binding.target)
            if isinstance(self.image_binding, RuntimeImageRepairBinding) else 'service')
        for container in (*pod['containers'], *pod.get('initContainers', [])):
            container['image'] = self.image_binding.candidate['images'][component]['image_ref']
        return template

    def _repair_changes(self, phase: str, before: dict[str, Any], desired: dict[str, Any]
            ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        from scripts.ops.nebius_ingress_stage import _snapshot

        if phase == 'isolate':
            proposed = _snapshot(before)
            proposed['metadata'].setdefault('annotations', {})[IMAGE_MARKER] = str(self.image_binding.operation_id)
            return proposed, [{'op': 'add', 'path': '/metadata/annotations',
                'value': proposed['metadata']['annotations']}]
        if before['kind'] == 'CronJob':
            proposed = _snapshot(before)
            if phase == 'template':
                template = self._replacement_template(before)
                proposed['spec']['jobTemplate']['spec']['template'] = template
                changes = [{'op': 'replace', 'path': '/spec/jobTemplate/spec/template', 'value': template}]
            else:
                proposed['spec']['suspend'] = desired['spec']['suspend']
                changes = [{'op': 'replace', 'path': '/spec/suspend', 'value': desired['spec']['suspend']}]
        else:
            proposed, changes = super()._repair_changes(phase, before, desired)
        if phase == 'start':
            del proposed['metadata']['annotations'][IMAGE_MARKER]
            changes.append({'op': 'remove', 'path': '/metadata/annotations/loom.nebius~1manager-image-repair'})
            if not proposed['metadata']['annotations']:
                del proposed['metadata']['annotations']
        return proposed, changes
