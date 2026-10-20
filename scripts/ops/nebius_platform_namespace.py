"""Versioned explicit platform namespace identity; legacy bindings stay unchanged."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal
from uuid import UUID

from scripts.ops.nebius_management_material import ManagementBinding


@dataclass(frozen=True)
class PlatformNamespaceBindingV2(ManagementBinding):
    schema_version: Literal['loom.platform-namespace.v2'] = field(default='loom.platform-namespace.v2', init=False)

    def __post_init__(self) -> None:
        if (any(str(UUID(value)) != value or not UUID(value).int
                for value in (self.installation_id, self.namespace_uid, self.kube_system_uid))
                or not isinstance(self.namespace, str) or len(self.namespace) > 63
                or re.fullmatch(r'[a-z0-9](?:[a-z0-9-]*[a-z0-9])?', self.namespace) is None):
            raise ValueError('platform_namespace_binding_unqualified')
