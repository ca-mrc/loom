"""Fixed scalar startup transport on the connected cutover's existing authority.

No new operator credentials, arbitrary manifests, creates, deletes or admission
opening. The parent owns this adapter's HTTP client and read-only SQL transports.
"""
from __future__ import annotations

import copy
import json
from typing import Any

from scripts.ops import nebius_certificates as private_state
from scripts.ops.nebius_ingress_stage import _key, _snapshot, _uid
from scripts.ops.nebius_management_switch import _matches, _stable
from scripts.ops.nebius_pool_cutover_live import HTTPSPoolCutoverAPI
from scripts.ops.nebius_pool_migration import _hash
from scripts.ops.nebius_pool_retirement_live import _patch_result
from scripts.ops.nebius_pool_role_fencing import role_fence_documents
from scripts.ops.nebius_pool_startup import _startup_record, closed_startup_documents


class HTTPSPoolStartupAPI:
    def __init__(self, *, parent: HTTPSPoolCutoverAPI):
        if parent.state_dir is None or parent.anchor_dir is None:
            raise ValueError("pool_startup_parent_journal_required")
        self.parent, self.request = parent, parent.request
        self.state, self.anchor = parent.state_dir, parent.anchor_dir
        self.closed, self.targets = closed_startup_documents(self.request, state_dir=self.state, anchor_dir=self.anchor)
        self.closure_sha256 = _hash(self.state / "cutover.json")

    def _scope(self) -> None:
        if self.parent.request != self.request or _hash(self.state / "cutover.json") != self.closure_sha256:
            raise ValueError("pool_startup_parent_changed")
        self.parent._scope()

    def qualify_closed(self) -> None:
        """Current closed SQL, guards, restricted writers and retained material."""
        try:
            self._scope()
            if closed_startup_documents(self.request, state_dir=self.state, anchor_dir=self.anchor) != (self.closed, self.targets):
                raise ValueError
            parent = self.parent
            parent.preflight(self.request)
            parent.history.qualify_closed_pool()
            for guard in parent.guards.request.guards:
                if parent.guards.guard(guard, "observe").get("status") != "held":
                    raise ValueError
                parent.qualify_runtime_access(guard.participant_id, "observe")
            roles = role_fence_documents(self.request.fencing)
            for original in self.request.fencing.originals:
                if not _matches(parent.fencing.read_role(_key(original)), roles[_key(original)], _uid(original)):
                    raise ValueError
            parent.fencing.verify_readonly()
            # Gateway runtime may now be starting. Every other staged resource
            # remains exactly the installed child receipt, never repaired here.
            for phase in ("material", "configuration", "authority"):
                child = json.loads(private_state._private_read(self.state / phase / "stage.json", limit=4 * 1024**2))
                for item in child["resources"].values():
                    if item["status"] != "created":
                        raise ValueError
                    actual = parent.resources.get_resource(item["desired"])
                    if actual is None or _uid(actual) != item["uid"] or _snapshot(actual) != item["observed"]:
                        raise ValueError
            self._scope()
            parent.history.qualify_closed_pool()
        except Exception:
            raise ValueError("pool_startup_live_closure_unqualified") from None

    def _path(self, key: str) -> str:
        original = self.closed[key]
        resource = "cronjobs" if original["kind"] == "CronJob" else "deployments"
        return ("/apis/" + str(original["apiVersion"]) + "/namespaces/" + str(original["metadata"]["namespace"])
            + "/" + resource + "/" + str(original["metadata"]["name"]))

    def read_workload(self, key: str) -> dict[str, Any]:
        try:
            original = self.closed[key]
            self._scope()
            actual = self.parent._request("GET", self._path(key))
            if (actual is None or _uid(actual) != _uid(original) or _key(actual) != key
                    or any(actual.get(field) != original[field] for field in ("apiVersion", "kind"))):
                raise ValueError
            _snapshot(actual)
            return actual
        except Exception:
            raise ValueError("pool_startup_read_unqualified") from None

    def _patch(self, key: str, before: dict[str, Any], desired: dict[str, Any], *, preview: bool) -> bool:
        try:
            original = self.closed[key]
            version = before["metadata"]["resourceVersion"]
            if (key not in self.targets or not _matches(before, original, _uid(original))
                    or _stable(desired) != _stable(self.targets[key])
                    or not isinstance(version, str) or not 0 < len(version) <= 128):
                raise ValueError
            _, record = _startup_record(self.request, state=self.state, anchor=self.anchor, closed=self.closed, targets=self.targets)
            if record is None:
                raise ValueError
            item = record["workloads"][key]
            if (item["phase"] != ("prepared" if preview else "intent")
                    or item["before_resource_version"] != (None if preview else version)):
                raise ValueError
            self.qualify_closed()
            field = "suspend" if original["kind"] == "CronJob" else "replicas"
            patches = [{"op": "test", "path": "/metadata/uid", "value": _uid(original)},
                {"op": "test", "path": "/metadata/resourceVersion", "value": version},
                {"op": "test", "path": "/spec", "value": before["spec"]},
                {"op": "replace", "path": "/spec/" + field, "value": self.targets[key]["spec"][field]}]
            with self.parent.client.stream("PATCH", self._path(key) + ("?dryRun=All" if preview else ""),
                    json=patches, headers={"Content-Type": "application/json-patch+json"}) as response:
                return _patch_result(response, desired=desired, uid=_uid(original))
        except Exception:
            raise ValueError("pool_startup_update_unconfirmed") from None

    def preview_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> dict[str, Any] | None:
        return copy.deepcopy(desired) if self._patch(key, before, desired, preview=True) else None

    def start_workload(self, key: str, before: dict[str, Any], desired: dict[str, Any]) -> bool:
        return self._patch(key, before, desired, preview=False)
