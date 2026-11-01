"""Plan/apply a narrow regional extension while preserving retained SSH grants."""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import stat
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any

from scripts.ops.nebius_regional_rollout_gateway import (
    CLUSTER_KINDS,
    SECRET_KEY_TEMPLATE,
    _json,
    _run,
    projected_contexts,
    validate_scope,
)

BEGIN = "# BEGIN loom regional rollout compatibility\n"
END = "# END loom regional rollout compatibility\n"


def qualify_retained_scope(scope: dict[str, Any]) -> None:
    """Read the installed identities; only reviewed future build objects may be absent."""
    projected_contexts(scope)
    for region in scope["regions"]:
        reviewed = region.get("builder_documents", {}).get("documents", [])
        future = {(d["kind"], d["metadata"]["name"], d["metadata"].get("namespace"))
                  for d in reviewed if d["kind"] != "Namespace"}
        for resource in region["resources"]:
            identity = (resource["kind"], resource["name"], resource["namespace"])
            if identity in future:
                continue
            command = ["--context", region["context"], "--request-timeout=10s", "get", resource["kind"], resource["name"]]
            if resource["kind"] not in CLUSTER_KINDS:
                command.extend(["-n", resource["namespace"]])
            observed = _json(_run(scope, [*command, "-o", "json"], timeout=15))
            if observed.get("metadata", {}).get("name") != resource["name"]:
                raise ValueError("only already installed resource identities may be refreshed")
            if resource["kind"] == "Namespace" and observed["metadata"].get("uid") != region["namespace_uids"][resource["name"]]:
                raise ValueError("retained regional namespace identity differs")
        for secret in region["secret_key_reads"]:
            observed = _run(scope, ["--context", region["context"], "--request-timeout=10s", "get", "secret", secret["name"], "-n", secret["namespace"], "-o", SECRET_KEY_TEMPLATE], timeout=15)
            if observed.returncode or not set(secret["required_keys"]) <= set(observed.stdout.decode().splitlines()):
                raise ValueError("retained build secret key names unavailable")


def _namespace_observation(scope: dict[str, Any], prefix: list[str], name: str) -> dict[str, Any]:
    observed = _run(scope, [*prefix, "get", "namespace", name, "--ignore-not-found", "-o", "json"], timeout=15)
    if observed.returncode:
        raise ValueError("regional build namespace read unavailable")
    return _json(observed) if observed.stdout.strip() else {}


def prepare_builder_scope(
    scope: dict[str, Any], config: dict[str, Any], candidate: dict[str, Any], *,
    source_sha: str, repo_root: Path, apply: bool = False,
    material_files: dict[str, Any] | None = None,
    material_journal_root: Path | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    """Bootstrap only an existing region's fixed build sibling through its operator.

    Candidate source renders the allowed documents; callers cannot submit a
    manifest or a new connection. Namespace bootstrap stays restricted. The
    ordinary protected credential channel must populate the scoped Secrets.
    """
    from scripts.ops.deploy_nebius_platform import _build_secret_requirements

    from loom.nebius_platform_render import (
        _regional_config,
        _task_image_builder_settings,
        build_regional_execution,
        task_image_builder_resource_ceiling,
    )

    validate_scope(scope)
    if material_files is not None and apply and material_journal_root is None:
        raise ValueError("private installer material state is required")
    if (not re.fullmatch(r"[0-9a-f]{40}", source_sha)
            or candidate.get("candidate_sha") != source_sha or config.get("registration")):
        raise ValueError("builder installation requires the reviewed standalone fixed candidate")
    rendered = build_regional_execution(config, candidate, repo_root=repo_root)
    targets = [target for target in config.get("regional_execution_targets", [])
               if target.get("task_image_builder") is not None]
    if not targets:
        raise ValueError("no explicitly configured regional builder")
    saved = _json(_run(scope, ["get", "configmap", "loom-platform-config", "-n", config["namespace"], "-o", "json"], timeout=15))
    if json.loads(saved.get("data", {}).get("environment.json", "null")) != config:
        raise ValueError("reviewed builder configuration differs from saved platform intent")
    updated = deepcopy(scope)
    # Qualify all old identities before any bootstrap write. This keeps the old
    # namespace, connection and grants independently authoritative.
    qualify_retained_scope(scope)
    bindings = []
    for target in targets:
        regions = [region for region in updated["regions"] if region["namespace"] == target["execution_namespace"]]
        if (len(regions) != 1 or regions[0]["api_server"] != target["kubernetes_api_server"]
                or not regions[0]["cluster_name"].endswith(target["cluster_id"].removeprefix("mk8s"))):
            raise ValueError("builder configuration differs from an existing reviewed region")
        region = regions[0]
        projected_contexts(scope, selected=region["context"])
        binding = {"kind": "RoleBinding", "name": "loom-execution-actuator", "namespace": region["namespace"]}
        if binding not in region["resources"]:
            raise ValueError("existing regional actuator binding is not retained")
        observed_binding = _json(_run(scope, ["--context", region["context"], "--request-timeout=10s",
            "get", "RoleBinding", binding["name"], "-n", region["namespace"], "-o", "json"], timeout=15))
        if observed_binding.get("subjects") != [{"kind": "User", "apiGroup": "rbac.authorization.k8s.io",
                                                 "name": target["service_account_ids"]["actuator"]}]:
            raise ValueError("regional builder must retain the installed actuator identity")
        bindings.append((target, region))
    source_namespace_uid = None
    if material_files is not None:
        source_namespace = _namespace_observation(scope, [], config["execution_namespace"] + "-build")
        source_namespace_uid = source_namespace.get("metadata", {}).get("uid")
        if not source_namespace_uid:
            raise ValueError("primary builder namespace identity unavailable for material references")
    result: dict[str, Any] = {"candidate_sha": source_sha, "bootstrap_namespaces": [], "materials": [],
                              "missing_secret_keys": [], "required_secret_keys": [],
                              "regional_targets": [], "controllers_started": False}
    for target, region in bindings:
        build = region["namespace"] + "-build"
        documents = [d for d in rendered[target["target_id"]]
                     if d["metadata"].get("namespace") == build or (d["kind"] == "Namespace" and d["metadata"]["name"] == build)]
        namespace, = (d for d in documents if d["kind"] == "Namespace")
        prefix = ["--context", region["context"], "--request-timeout=10s"]
        required = _build_secret_requirements(target)
        result["regional_targets"].append(target["target_id"])
        result["required_secret_keys"].extend({"namespace": ns, "name": name, "keys": sorted(keys)}
                                              for (ns, name), keys in sorted(required.items()))

        observed = _namespace_observation(scope, prefix, build)
        if not observed:
            result["bootstrap_namespaces"].append(build)
            if not apply:
                continue
            bootstrap = deepcopy(namespace)
            bootstrap["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] = "restricted"
            created = _run(scope, [*prefix, "create", "-f", "-"], payload=json.dumps(bootstrap).encode(), timeout=15)
            if created.returncode:
                raise ValueError("restricted regional build namespace bootstrap unconfirmed")
            observed = _namespace_observation(scope, prefix, build)
        metadata = observed.get("metadata", {})
        if (metadata.get("name") != build or not metadata.get("uid")
                or metadata.get("labels", {}).get("loom.nebius/platform") != "true"
                or metadata.get("labels", {}).get("pod-security.kubernetes.io/enforce") not in {"restricted", "privileged"}
                or (build in region["namespace_uids"] and metadata["uid"] != region["namespace_uids"][build])):
            raise ValueError("regional build namespace ownership or retained UID differs")
        region["namespace_uids"][build] = metadata["uid"]
        # Only the renderer's six fixed build objects may be installed later;
        # every other old resource still had to exist in the read above.
        for doc in documents:
            identity = {"kind": doc["kind"], "name": doc["metadata"]["name"], "namespace": doc["metadata"].get("namespace")}
            if identity not in region["resources"]:
                region["resources"].append(identity)
        settings = _task_image_builder_settings(_regional_config(config, target),
            service_image=candidate["images"]["service"]["image_ref"], storage_config=config)
        region["builder_documents"] = {"candidate_sha": source_sha, "documents": documents,
            "resource_ceiling": task_image_builder_resource_ceiling(target["task_image_builder"], settings),
            "max_concurrent": settings.max_concurrent}
        region["secret_key_reads"] = [{"namespace": ns, "name": name, "required_keys": sorted(keys)}
                                      for (ns, name), keys in sorted(required.items())]
        if material_files is not None:
            from scripts.ops.nebius_regional_builder_material import supply_builder_material

            result["materials"].append(supply_builder_material(updated, config, candidate,
                source_sha=source_sha, target_id=target["target_id"], source_namespace_uid=source_namespace_uid,
                material_files=material_files, apply=apply, run=_run,
                journal_path=None if material_journal_root is None else material_journal_root / source_sha / (target["target_id"] + ".json")))
        for secret in region["secret_key_reads"]:
            keys = _run(scope, [*prefix, "get", "secret", secret["name"], "-n", build, "-o", SECRET_KEY_TEMPLATE], timeout=15)
            populated = set(keys.stdout.decode().splitlines()) if not keys.returncode else set()
            missing = sorted(set(secret["required_keys"]) - populated)
            if missing:
                result["missing_secret_keys"].append({"namespace": build, "name": secret["name"], "keys": missing})
    if result["bootstrap_namespaces"] and not apply:
        return None, {**result, "status": "planned_builder_bootstrap"}
    if result["missing_secret_keys"]:
        return None, {**result, "status": "blocked_build_secret_keys"}
    validate_scope(updated)
    return updated, result


def _private(path: Path, *, directory: bool = False) -> None:
    info = path.lstat()
    if path.resolve() != path or info.st_uid != os.geteuid() or info.st_mode & 0o077 or not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
        raise ValueError("gateway path must be private, owned, and not a symlink")


def _write_once(path: Path, data: bytes) -> None:
    if path.exists() or path.is_symlink():
        _private(path)
        if path.read_bytes() != data:
            raise ValueError("retained regional extension differs; preserve it for review")
        return
    with path.open("xb") as stream:
        os.fchmod(stream.fileno(), 0o600)
        stream.write(data)


def _prefix(program: Path, scope: Path) -> bytes:
    # Do not load this extension for legacy commands: primary kubectl, the
    # completion reader and all existing exact-bundle grants stay independent.
    return f'''{BEGIN}import os as _regional_os, shlex as _regional_shlex, sys as _regional_sys
try:
    _regional_args = _regional_shlex.split(_regional_os.environ.get('SSH_ORIGINAL_COMMAND', ''))
except ValueError:
    _regional_args = []
_regional_full = _regional_args[3:] in (["config", "view", "-o", "json"], ["--request-timeout=30s", "config", "view", "-o", "json"])
if _regional_full or any(a.startswith('--context') for a in _regional_args):
    try:
        _regional_namespace = {{'__name__': 'installed_regional_rollout_gateway'}}
        with open({str(program)!r}, 'rb') as _regional_source:
            exec(compile(_regional_source.read(), {str(program)!r}, 'exec'), _regional_namespace)
        _regional_code = _regional_namespace['entry']({str(scope)!r})
    except Exception:
        print('Regional rollout command outside the installed scope', file=_regional_sys.stderr)
        _regional_code = 126
    if _regional_code is not None:
        _regional_sys.exit(_regional_code)
{END}'''.encode()


def install(root: Path, source: bytes, scope: dict[str, Any], revision: str, *, apply: bool = False) -> dict[str, Any]:
    validate_scope(scope)
    if not re.fullmatch(r"[0-9a-f]{40}", revision) or not source or len(source) > 65536:
        raise ValueError("reviewed integrated source revision required")
    compile(source, "regional_gateway.py", "exec")
    _private(root, directory=True)
    wrapper = root / "github-kubectl.py"
    _private(wrapper)
    before = wrapper.read_bytes()
    original = before
    if before.startswith(BEGIN.encode()):
        if before.count(END.encode()) != 1:
            raise ValueError("regional extension prefix requires reconciliation")
        original = before.split(END.encode(), 1)[1]
    elif BEGIN.encode() in before or END.encode() in before:
        raise ValueError("regional extension prefix is not at the owning boundary")
    # Bind this extension to the existing wrapper's retained connection, rather
    # than granting another kubeconfig or arbitrary executable via the scope file.
    tree = ast.parse(original)
    expected = [n.value.value for n in ast.walk(tree) if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "expected" for t in n.targets)
                and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str)]
    executables = [n.args[0].value for n in ast.walk(tree) if isinstance(n, ast.Call)
                   and isinstance(n.func, ast.Attribute) and n.func.attr == "execv"
                   and n.args and isinstance(n.args[0], ast.Constant)
                   and isinstance(n.args[0].value, str) and n.args[0].value.endswith("/kubectl")]
    if set(expected) != {scope["kubeconfig"]} or set(executables) != {scope["kubectl"]}:
        raise ValueError("scope must preserve the retained kubectl and kubeconfig")
    directory = root / "regional-compatibility" / revision
    program = directory / "gateway.py"
    configuration = directory / "scope.json"
    desired = _prefix(program, configuration) + original
    compile(desired, str(wrapper), "exec")
    scope_bytes = (json.dumps(scope, sort_keys=True, indent=2) + "\n").encode()
    result = {"status": "planned", "regional_target_count": len(scope["regions"]), "primary_grants_preserved": True, "source_revision": revision}
    if not apply:
        return result
    for path in (root / "regional-compatibility", directory):
        path.mkdir(mode=0o700, exist_ok=True)
        _private(path, directory=True)
    _write_once(program, source)
    _write_once(configuration, scope_bytes)
    backup = directory / "previous-github-kubectl.py"
    if before == desired:
        _private(backup)
        return {**result, "status": "unchanged"}
    _write_once(backup, before)
    fd, name = tempfile.mkstemp(prefix=".regional-wrapper-", dir=root)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), stat.S_IMODE(wrapper.stat().st_mode))
            stream.write(desired)
            stream.flush()
            os.fsync(stream.fileno())
        if wrapper.read_bytes() != before:
            raise ValueError("existing gateway changed during installation")
        os.replace(temporary, wrapper)
        if wrapper.read_bytes() != desired:
            raise ValueError("installed wrapper readback differs")
    finally:
        temporary.unlink(missing_ok=True)
    return {**result, "status": "installed"}


def restore(root: Path, revision: str) -> dict[str, str]:
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("reviewed integrated source revision required")
    _private(root, directory=True)
    wrapper = root / "github-kubectl.py"
    _private(wrapper)
    current = wrapper.read_bytes()
    directory = root / "regional-compatibility" / revision
    backup = directory / "previous-github-kubectl.py"
    _private(backup)
    if not current.startswith(BEGIN.encode()) or current.count(END.encode()) != 1 or _prefix(directory / "gateway.py", directory / "scope.json") not in current:
        raise ValueError("current wrapper is not this retained extension")
    original = backup.read_bytes()
    if current.split(END.encode(), 1)[1] != original.split(END.encode(), 1)[-1]:
        raise ValueError("later gateway grants differ; preserve them for review")
    fd, name = tempfile.mkstemp(prefix=".regional-wrapper-restore-", dir=root)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchmod(stream.fileno(), stat.S_IMODE(wrapper.stat().st_mode))
            stream.write(original)
            stream.flush()
            os.fsync(stream.fileno())
        if wrapper.read_bytes() != current:
            raise ValueError("existing gateway changed during restoration")
        os.replace(temporary, wrapper)
    finally:
        temporary.unlink(missing_ok=True)
    return {"status": "restored"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--scope", type=Path, required=True, help="Private, reviewed existing-target scope JSON.")
    parser.add_argument("--builder-platform-config", type=Path, help="Private reviewed saved standalone configuration; enables only its existing regional build siblings.")
    parser.add_argument("--builder-material-refs", type=Path, help="Optional owner-private current primary Secret UID/roles and private from-file references; never Secret values.")
    parser.add_argument("--candidate", type=Path, help="Published fixed candidate matching --source-sha; used by the source-owned renderer.")
    parser.add_argument("--root", type=Path, default=Path.home() / ".loom/nebius-auto-rollout")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--restore-previous", action="store_true")
    args = parser.parse_args()
    try:
        if args.restore_previous:
            if not args.apply:
                raise ValueError("restoration requires explicit --apply")
            result = restore(args.root, args.source_sha)
        else:
            _private(args.scope)
            scope = validate_scope(json.loads(args.scope.read_text()))
            details = {}
            if args.builder_platform_config is not None or args.candidate is not None or args.builder_material_refs is not None:
                if args.builder_platform_config is None or args.candidate is None:
                    raise ValueError("builder configuration and fixed candidate are both required")
                _private(args.builder_platform_config)
                material_files = None
                if args.builder_material_refs is not None:
                    _private(args.root, directory=True)
                    _private(args.builder_material_refs)
                    material_files = json.loads(args.builder_material_refs.read_text())
                scope, details = prepare_builder_scope(scope, json.loads(args.builder_platform_config.read_text()),
                    json.loads(args.candidate.read_text()), source_sha=args.source_sha,
                    repo_root=Path(__file__).resolve().parents[2], apply=args.apply, material_files=material_files,
                    material_journal_root=args.root / "builder-material")
                if scope is None:
                    print(json.dumps(details, sort_keys=True))
                    return 1 if details["status"].startswith("blocked") else 0
            else:
                qualify_retained_scope(scope)
            source = Path(__file__).with_name("nebius_regional_rollout_gateway.py").read_bytes()
            result = {**details, **install(args.root, source, scope, args.source_sha, apply=args.apply)}
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"status": "blocked", "reason": "regional gateway scope or retained installation requires reconciliation"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
