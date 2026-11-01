"""Private from-file material for an already approved regional builder installer.

This does not read online Secret values or grant credentials. The owning material
operator supplies private files and their exact primary Secret UID/role refs.
Only the existing protected installer may invoke apply; the standalone CLI plans.
"""
from __future__ import annotations

import argparse
import base64
import fcntl
import json
import os
import re
import stat
import sys
import tempfile
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.nebius_regional_rollout_gateway import _run, validate_scope  # noqa: E402

from loom.nebius_platform_render import (  # noqa: E402
    _regional_config,
    _task_image_builder_settings,
    validate_environment,
)

_LIMIT = 65_536


class MaterialError(ValueError):
    """Fixed, payload-free diagnostic; never serialize a file or process error."""


def _uid(value: Any) -> str:
    try:
        if not isinstance(value, str) or str(UUID(value)) != value or not UUID(value).int:
            raise ValueError
        return value
    except Exception:
        raise MaterialError("material identity is invalid") from None


def _private_stat(value: Any) -> tuple[int, ...]:
    try:
        path = Path(value)
        info = path.lstat()
        if (not path.is_absolute() or path.resolve(strict=True) != path or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) not in {0o400, 0o600} or info.st_nlink != 1
                or not 0 < info.st_size <= _LIMIT):
            raise ValueError
        return (info.st_dev, info.st_ino, info.st_uid, info.st_mode, info.st_size,
                info.st_mtime_ns, info.st_ctime_ns)
    except Exception:
        raise MaterialError("private material file handles are unavailable") from None


def _private_bytes(value: Any, expected: tuple[int, ...]) -> bytes:
    descriptor = None
    try:
        if _private_stat(value) != expected:
            raise ValueError
        descriptor = os.open(value, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
        info = os.fstat(descriptor)
        identity = (info.st_dev, info.st_ino, info.st_uid, info.st_mode, info.st_size,
                    info.st_mtime_ns, info.st_ctime_ns)
        if identity != expected:
            raise ValueError
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = None
            result = stream.read(_LIMIT + 1)
            after = os.fstat(stream.fileno())
        if (not result or len(result) != expected[4]
                or (after.st_mtime_ns, after.st_ctime_ns) != expected[-2:]):
            raise ValueError
        return result
    except Exception:
        raise MaterialError("private material file handles changed") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _handle_path(value: Any, key: str) -> Any:
    if isinstance(value, dict):
        if set(value) != {"path", "json_key"} or key not in {"access-key", "secret-key"} or value["json_key"] != key:
            raise MaterialError("private material file handle differs from its approved key")
        return value["path"]
    return value


def _material_bytes(value: Any, key: str, expected: tuple[int, ...]) -> bytes:
    body = _private_bytes(_handle_path(value, key), expected)
    if not isinstance(value, dict):
        return body
    try:
        document = json.loads(body)
        if (not isinstance(document, dict) or set(document) != {"access-key", "secret-key"}
                or any(not isinstance(v, str) or not v for v in document.values())):
            raise ValueError
        return document[key].encode()
    except Exception:
        raise MaterialError("private material JSON handle unavailable") from None


def _roles(settings: Any) -> dict[str, tuple[tuple[str, ...], tuple[str, ...]]]:
    grouped: dict[str, tuple[set[str], set[str]]] = {}
    for role in ("source", "cache", "archive", "registry"):
        name = getattr(settings, role + "_secret_name")
        if name is None:
            continue
        if re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", name) is None:
            raise MaterialError("primary material resource names differ")
        keys = {"credentials.json"} if role == "registry" else {"access-key", "secret-key"}
        roles, required = grouped.setdefault(name, (set(), set()))
        roles.add(role)
        required.update(keys)
    return {name: (tuple(sorted(roles)), tuple(sorted(keys))) for name, (roles, keys) in grouped.items()}


@dataclass(frozen=True)
class MaterialPlan:
    candidate_sha: str
    target_id: str
    source_namespace: str
    source_namespace_uid: str
    destination_namespace: str
    destination_namespace_uid: str
    destination_context: str = field(repr=False)
    references: tuple[tuple[str, str, tuple[str, ...], tuple[str, ...]], ...]
    file_identities: dict[tuple[str, str], tuple[int, ...]] = field(repr=False)

    def summary(self, *, status: str = "material_planned") -> dict[str, Any]:
        return {"status": status, "candidate_sha": self.candidate_sha, "target_id": self.target_id,
                "source_namespace": self.source_namespace, "source_namespace_uid": self.source_namespace_uid,
                "destination_namespace": self.destination_namespace,
                "destination_namespace_uid": self.destination_namespace_uid,
                "required_secret_keys": [{"name": name, "source_secret_uid": uid,
                    "roles": list(roles), "keys": list(keys)} for name, uid, roles, keys in self.references],
                "controllers_started": False}


def _files(material_files: Any, expected: dict[str, tuple[tuple[str, ...], tuple[str, ...]]]) -> tuple:
    if not isinstance(material_files, dict) or set(material_files) != set(expected):
        raise MaterialError("material references differ from the existing primary roles")
    references, identities = [], {}
    for name, (roles, keys) in sorted(expected.items()):
        item = material_files[name]
        if (not isinstance(item, dict) or set(item) != {"source_secret_uid", "roles", "files"}
                or not isinstance(item["roles"], list) or sorted(item["roles"]) != list(roles)
                or not isinstance(item["files"], dict) or set(item["files"]) != set(keys)):
            raise MaterialError("material role or key declarations differ")
        references.append((name, _uid(item["source_secret_uid"]), roles, keys))
        for key in keys:
            identities[name, key] = _private_stat(_handle_path(item["files"][key], key))
    return tuple(references), identities


def plan_material(scope: dict[str, Any], config: dict[str, Any], candidate: dict[str, Any], *,
                  source_sha: str, target_id: str, source_namespace_uid: str,
                  material_files: dict[str, Any]) -> MaterialPlan:
    """Only inspect supplied metadata and stat private handles; perform no I/O to K8s."""
    try:
        validate_scope(scope)
        validate_environment(config)
        if (not isinstance(source_sha, str) or re.fullmatch(r"[0-9a-f]{40}", source_sha) is None
                or candidate.get("candidate_sha") != source_sha or config.get("registration")):
            raise ValueError
        target, = (row for row in config.get("regional_execution_targets", []) if row["target_id"] == target_id)
        region, = (row for row in scope["regions"] if row["namespace"] == target["execution_namespace"])
        if (target["region"] != "eu-west1" or region["api_server"] != target["kubernetes_api_server"]
                or not region["cluster_name"].endswith(target["cluster_id"].removeprefix("mk8s"))
                or region.get("builder_documents", {}).get("candidate_sha", source_sha) != source_sha):
            raise ValueError
        image = candidate["images"]["service"]["image_ref"]
        primary = _task_image_builder_settings(config, service_image=image)
        destination = _task_image_builder_settings(_regional_config(config, target), service_image=image, storage_config=config)
        if (primary is None or destination is None or _roles(primary) != _roles(destination)
                or any(getattr(primary, key) != getattr(destination, key) for key in (
                    "storage_endpoint", "storage_region", "source_bucket", "cache_bucket", "registry_repository"))
                or primary.registry_auth_kind != "nebius" or destination.registry_auth_kind != "nebius"
                or primary.namespace == destination.namespace):
            raise ValueError
        references, identities = _files(material_files, _roles(primary))
        return MaterialPlan(source_sha, target_id, primary.namespace, _uid(source_namespace_uid),
            destination.namespace, _uid(region["namespace_uids"][destination.namespace]),
            region["context"], references, identities)
    except MaterialError:
        raise
    except Exception:
        raise MaterialError("regional builder material binding differs") from None


def _metadata_template(keys: tuple[str, ...]) -> str:
    return ('go-template={{.metadata.uid}}{{"\\n"}}{{.metadata.resourceVersion}}{{"\\n"}}' + ''.join(
        '{{if index .data ' + json.dumps(key) + '}}' + key + '{{"\\n"}}{{end}}' for key in keys))


_ANNOTATIONS = (
    "material-operation", "material-candidate", "material-source-namespace-uid",
    "material-destination-namespace-uid", "material-source-secret-uid", "material-target",
)


def _destination_template(keys: tuple[str, ...]) -> str:
    return ('go-template={{.metadata.uid}}{{"\\n"}}{{.metadata.resourceVersion}}{{"\\n"}}{{.immutable}}{{"\\n"}}{{.type}}{{"\\n"}}'
        + ''.join('{{index .metadata.annotations ' + json.dumps("loom.nebius/builder-" + key) + '}}{{"\\n"}}'
                  for key in _ANNOTATIONS)
        + ''.join('{{if index .data ' + json.dumps(key) + '}}' + key + '{{"\\n"}}{{end}}' for key in keys))


def _journal_save(path: Path, value: dict[str, Any]) -> None:
    fd, temporary = tempfile.mkstemp(prefix=".builder-material-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(value, stream, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


@contextmanager
def _journal_lock(path: Path):
    """Private normal installer state, locked before the first create intent."""
    if not path.is_absolute() or path.resolve() != path:
        raise MaterialError("private material journal unavailable")
    ancestor, missing = path.parent, []
    while not ancestor.exists():
        missing.append(ancestor)
        ancestor = ancestor.parent
    info = ancestor.lstat()
    if ancestor.resolve() != ancestor or not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o077:
        raise MaterialError("private material journal unavailable")
    for directory in reversed(missing):
        directory.mkdir(mode=0o700)
    lock = path.with_suffix(path.suffix + ".lock")
    descriptor = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
            raise MaterialError("private material journal unavailable")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)


def apply_material(*, plan: MaterialPlan, run: Callable[..., Any], material_files: dict[str, Any],
                   journal_path: Path | None = None) -> dict[str, Any]:
    """Create only absent destinations over the installer's fixed operator route.

    Source Secret responses contain only UID/resourceVersion/approved key names.
    Secret values are read solely from owner files, kept in local memory and
    supplied as opaque stdin. Neither data nor file paths appear in the result.
    """
    try:
        if not isinstance(plan, MaterialPlan) or journal_path is None:
            raise ValueError
        expected = {name: (roles, keys) for name, _, roles, keys in plan.references}
        references, identities = _files(material_files, expected)
        if references != plan.references or identities != plan.file_identities:
            raise MaterialError("private material file handles changed")
        source_prefix = ["--request-timeout=10s"]
        destination_prefix = ["--context", plan.destination_context, "--request-timeout=10s"]

        def call(args: list[str], payload: bytes | None = None) -> bytes:
            result = run(args, payload=payload, timeout=15)
            if result.returncode or not isinstance(result.stdout, bytes) or len(result.stdout) > _LIMIT:
                raise MaterialError("regional material operation unconfirmed; preserve existing destinations")
            return result.stdout

        def namespaces() -> None:
            for prefix, name, uid in ((source_prefix, plan.source_namespace, plan.source_namespace_uid),
                    (destination_prefix, plan.destination_namespace, plan.destination_namespace_uid)):
                observed = json.loads(call([*prefix, "get", "namespace", name, "-o", "json"]))
                if (observed.get("kind") != "Namespace" or observed.get("metadata", {}).get("name") != name
                        or observed["metadata"].get("uid") != uid or observed["metadata"].get("deletionTimestamp")):
                    raise MaterialError("regional material namespace identity differs")

        def source_metadata() -> dict[str, tuple[str, ...]]:
            result = {}
            for name, uid, _, keys in plan.references:
                fields = call([*source_prefix, "get", "secret", name, "-n", plan.source_namespace,
                    "-o", _metadata_template(keys)]).decode().splitlines()
                if (len(fields) != len(keys) + 2 or fields[0] != uid or not fields[1]
                        or sorted(fields[2:]) != list(keys)):
                    raise MaterialError("primary material UID or required key names differ")
                result[name] = tuple(fields)
            return result

        def annotation_values(operation: str, uid: str) -> tuple[str, ...]:
            return (operation, plan.candidate_sha, plan.source_namespace_uid, plan.destination_namespace_uid,
                    uid, plan.target_id)

        namespaces()
        original = source_metadata()
        path = Path(journal_path)
        with _journal_lock(path):
            binding = plan.summary()
            versions = {name: list(fields) for name, fields in original.items()}
            if path.exists():
                journal = json.loads(_private_bytes(path, _private_stat(path)))
                if (not isinstance(journal, dict) or set(journal) != {"schema", "binding", "operation", "source_versions", "secrets"}
                        or journal["schema"] != "loom.builder-material-install.v1" or journal["binding"] != binding
                        or journal["source_versions"] != versions or not isinstance(journal["secrets"], dict)
                        or set(journal["secrets"]) - set(expected)):
                    raise MaterialError("material journal or primary source identity changed")
                _uid(journal["operation"])
                for entry in journal["secrets"].values():
                    if not isinstance(entry, dict) or set(entry) != {"uid"}:
                        raise MaterialError("material journal identity differs")
                    if entry["uid"] is not None:
                        _uid(entry["uid"])
            else:
                journal = {"schema": "loom.builder-material-install.v1", "binding": binding,
                    "operation": str(uuid4()), "source_versions": versions, "secrets": {}}
                _journal_save(path, journal)

            def destination(name: str, uid: str, keys: tuple[str, ...]) -> str | None:
                fields = call([*destination_prefix, "get", "secret", name, "-n", plan.destination_namespace,
                    "--ignore-not-found", "-o", _destination_template(keys)]).decode().splitlines()
                entry = journal["secrets"].get(name)
                if not fields:
                    if entry and entry["uid"] is not None:
                        raise MaterialError("recorded destination material disappeared")
                    return None
                if (entry is None or len(fields) != 10 + len(keys) or not fields[1] or fields[2:4] != ["true", "Opaque"]
                        or tuple(fields[4:10]) != annotation_values(journal["operation"], uid)
                        or sorted(fields[10:]) != list(keys)
                        or (entry["uid"] is not None and fields[0] != entry["uid"])):
                    raise MaterialError("destination material already exists; refusing adoption or rotation")
                return _uid(fields[0])

            # Qualify all existing destinations before reading any owner bytes.
            observed = {name: destination(name, uid, keys) for name, uid, _, keys in plan.references}
            values = {name: {key: _material_bytes(material_files[name]["files"][key], key, identities[name, key])
                             for key in keys} for name, _, _, keys in plan.references if observed[name] is None}
            created, resumed = {}, {}
            for name, source_uid, _, keys in plan.references:
                namespaces()
                if source_metadata() != original:
                    raise MaterialError("primary material metadata changed; preserve existing destinations")
                if observed[name] is not None:
                    created[name] = resumed[name] = observed[name]
                    journal["secrets"][name]["uid"] = observed[name]
                    _journal_save(path, journal)
                    continue
                journal["secrets"][name] = {"uid": None}
                _journal_save(path, journal)
                document = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque", "immutable": True,
                    "metadata": {"name": name, "namespace": plan.destination_namespace,
                        "annotations": {"loom.nebius/builder-" + key: value
                            for key, value in zip(_ANNOTATIONS, annotation_values(journal["operation"], source_uid), strict=True)}},
                    "data": {key: base64.b64encode(value).decode() for key, value in values[name].items()}}
                uid = call([*destination_prefix, "create", "-f", "-", "-o", 'go-template={{.metadata.uid}}'],
                    json.dumps(document, separators=(",", ":")).encode()).decode().strip()
                created[name] = _uid(uid)
                journal["secrets"][name]["uid"] = uid
                _journal_save(path, journal)
                namespaces()
                if destination(name, source_uid, keys) != uid:
                    raise MaterialError("created destination material metadata differs")
            namespaces()
            if source_metadata() != original:
                raise MaterialError("primary material metadata changed; preserve existing destinations")
            return {**plan.summary(status="material_installed"), "created_secret_uids": created,
                    "resumed_secret_uids": resumed, "material_operation_id": journal["operation"]}
    except MaterialError:
        raise
    except Exception:
        raise MaterialError("regional material operation unconfirmed; preserve existing destinations") from None


def supply_builder_material(scope: dict[str, Any], config: dict[str, Any], candidate: dict[str, Any], *,
                            source_sha: str, target_id: str, source_namespace_uid: str,
                            material_files: dict[str, Any], apply: bool = False, run: Callable[..., Any] = _run,
                            journal_path: Path | None = None) -> dict[str, Any]:
    """Called only after the protected installer qualifies saved intent and scope."""
    plan = plan_material(scope, config, candidate, source_sha=source_sha, target_id=target_id,
        source_namespace_uid=source_namespace_uid, material_files=material_files)
    if not apply:
        return plan.summary()
    return apply_material(plan=plan, material_files=material_files, journal_path=journal_path,
        run=lambda args, **kwargs: run(scope, args, **kwargs))


def main(argv: list[str] | None = None) -> int:
    class Parser(argparse.ArgumentParser):
        def error(self, _message: str) -> None:
            raise MaterialError("regional material plan arguments are invalid")

    try:
        parser = Parser(description="Plan private builder file references; apply belongs to the protected installer.")
        parser.add_argument("operation", choices=("plan",))
        parser.add_argument("--request-file", required=True)
        parser.add_argument("--material-refs", required=True)
        args = parser.parse_args(argv)
        request = json.loads(_private_bytes(args.request_file, _private_stat(args.request_file)))
        refs = json.loads(_private_bytes(args.material_refs, _private_stat(args.material_refs)))
        if set(request) != {"scope", "config", "candidate", "source_sha", "target_id", "source_namespace_uid"}:
            raise MaterialError("regional material plan binding differs")
        print(json.dumps(supply_builder_material(**request, material_files=refs), sort_keys=True))
        return 0
    except Exception:
        print("Regional builder material plan unavailable; private inputs were not disclosed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
