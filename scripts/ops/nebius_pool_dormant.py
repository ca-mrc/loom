"""Explicit closed remote consumers; never active pool participants or aliases."""
from __future__ import annotations

import copy
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_pool_migration import PoolMigrationRequest
from scripts.ops.nebius_pool_runtime import _environment


@dataclass(frozen=True, repr=False)
class DormantPoolConsumer:
    participant_id: UUID
    actuator: dict[str, Any]
    collector: dict[str, Any]


def dormant_retirement_documents(*, migration: PoolMigrationRequest,
        actuators: tuple[dict[str, Any], ...], consumers: tuple[DormantPoolConsumer, ...]) -> dict[str, dict[str, Any]]:
    """Bind only already-closed local roots using the retained participant DB.

    Their remote namespace is not operation scope. The parent must still qualify
    complete live inventory, process drain and effective read-only permissions;
    being dormant does not exempt an identity from those barriers.
    """
    try:
        if len(consumers) > 128:
            raise ValueError
        participants = {row.participant_id: row for row in migration.registration.spec.participants}
        local_namespaces = {migration.registration.binding.namespace, *(row.namespace for row in migration.guards),
            *(namespace.name for row in participants.values() for namespace in (row.execution_namespace, row.build_namespace))}
        result: dict[str, dict[str, Any]] = {}
        identities: set[str] = set()
        for consumer in consumers:
            participant = participants[consumer.participant_id]
            namespace = participant.execution_namespace.name
            primary, = (row for row in actuators if row["metadata"]["namespace"] == namespace
                and row["metadata"]["name"] == "loom-execution-actuator")
            primary_container, = primary["spec"]["template"]["spec"]["containers"]
            primary_db = _environment(primary_container)["LOOM_EXECUTION_ACTUATOR_DB_URL"]
            actuator, collector = consumer.actuator, consumer.collector
            pod = actuator["spec"]["template"]["spec"]
            container, = pod["containers"]
            settings = _environment(container)
            target = settings["LOOM_EXECUTION_ACTUATOR_TARGET_ID"].get("value")
            remote_namespace = settings["LOOM_EXECUTION_ACTUATOR_NAMESPACE"].get("value")
            if (not isinstance(target, str) or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,52}[a-z0-9])?", target) is None
                    or target in {row.target_id for row in participant.targets}
                    or not isinstance(remote_namespace, str)
                    or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", remote_namespace) is None
                    or remote_namespace in local_namespaces
                    or container["name"] != "actuator"
                    or settings["LOOM_EXECUTION_ACTUATOR_DB_URL"] != primary_db
                    or not primary_db.get("valueFrom", {}).get("secretKeyRef")):
                raise ValueError
            for document, kind, version, suffix in ((actuator, "Deployment", "apps/v1", "actuator"),
                    (collector, "CronJob", "batch/v1", "collector")):
                metadata = document["metadata"]
                name = target + "-" + suffix
                template = (document["spec"]["template"] if kind == "Deployment"
                    else document["spec"]["jobTemplate"]["spec"]["template"])
                uid, key = _uid(document), _key(document)
                if (document.get("apiVersion") != version or document.get("kind") != kind
                        or metadata.get("name") != name or metadata.get("namespace") != namespace
                        or metadata.get("deletionTimestamp")
                        or not isinstance(metadata.get("resourceVersion"), str) or not metadata["resourceVersion"]
                        or template["spec"].get("serviceAccountName") != name
                        or key in result or uid in identities):
                    raise ValueError
                if kind == "Deployment":
                    if (type(document["spec"].get("replicas")) is not int or document["spec"]["replicas"] != 0
                            or document["spec"]["selector"] != {"matchLabels": {"app.kubernetes.io/name": name}}
                            or template["metadata"].get("labels", {}).get("app.kubernetes.io/name") != name):
                        raise ValueError
                elif document["spec"].get("suspend") is not True or document["spec"].get("concurrencyPolicy") != "Forbid":
                    raise ValueError
                _snapshot(document)
                result[key] = copy.deepcopy(document)
                identities.add(uid)
        return result
    except Exception:
        raise ValueError("pool_dormant_consumers_unqualified") from None
