"""Plan/apply a narrow regional extension while preserving retained SSH grants."""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import Any

from scripts.ops.nebius_regional_rollout_gateway import (
    CLUSTER_KINDS,
    _json,
    _run,
    projected_contexts,
    validate_scope,
)

BEGIN = "# BEGIN loom regional rollout compatibility\n"
END = "# END loom regional rollout compatibility\n"


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
            # A plan proves the exact retained context mapping is still present;
            # it changes neither kubeconfig nor cluster authorization.
            projected_contexts(scope)
            for region in scope["regions"]:
                for resource in region["resources"]:
                    command = ["--context", region["context"], "--request-timeout=10s", "get", resource["kind"], resource["name"]]
                    if resource["kind"] not in CLUSTER_KINDS:
                        command.extend(["-n", region["namespace"]])
                    observed = _json(_run(scope, [*command, "-o", "json"], timeout=15))
                    if observed.get("metadata", {}).get("name") != resource["name"]:
                        raise ValueError("only already installed resource identities may be refreshed")
                    if resource["kind"] == "Namespace" and observed["metadata"].get("uid") != region["namespace_uid"]:
                        raise ValueError("retained regional namespace identity differs")
            source = Path(__file__).with_name("nebius_regional_rollout_gateway.py").read_bytes()
            result = install(args.root, source, scope, args.source_sha, apply=args.apply)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print(json.dumps({"status": "blocked", "reason": "regional gateway scope or retained installation requires reconciliation"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
