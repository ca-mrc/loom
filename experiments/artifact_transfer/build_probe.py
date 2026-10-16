#!/usr/bin/env python3
"""Disposable OCI build timing probe; no registry writes or candidate signing.

Producer hot runs deliberately use the same SHA and builder: they measure an
optimistic no-op cache bound, not normal between-commit CI performance. A new
consumer without an imported cache is reported as cold. Source stays read-only.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import time
import tomllib
import uuid

sys.dont_write_bytecode = True
GIB = 1024 ** 3
BUILDKIT = "docker.io/moby/buildkit:v0.33.0@sha256:6c2fa84a6b61ccd72899dde4239f8d5717f05f9a8ca6f3cad185fb1a95a94de3"
IMAGES = {
    "service": "loom-service", "control_plane": "loom-control-plane",
    "web": "loom-web", "gateway": "loom-llm-gateway",
    "execution_runtime": "loom-execution-runtime",
    "execution_actuator": "loom-execution-actuator", "harbor_runtime": "loom-harbor-runtime",
}


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def require_disk(root: Path, minimum: int = 2 * GIB) -> int:
    free = shutil.disk_usage(root).free
    if free < minimum:
        raise RuntimeError(f"insufficient probe disk: {free} bytes free, require {minimum}; no unrelated files cleaned")
    return free


def log_counts(path: Path) -> dict:
    text = path.read_text(errors="replace")
    cached = set(re.findall(r"(?m)^#(\d+) CACHED\s*$", text))
    run_steps = set(re.findall(r"(?m)^#(\d+) \[[^\n]+\] RUN\b", text))
    return {"cached_steps": len(cached), "run_steps": len(run_steps),
        "actual_run_steps": len(run_steps - cached),
        "cache_import_log": [line for line in text.splitlines()
            if "importing cache manifest" in line or "local cache importer" in line]}


def available_memory() -> int:
    meminfo = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    available = int(meminfo["MemAvailable"].split()[0]) * 1024
    limit = Path("/sys/fs/cgroup/memory.max")
    current = Path("/sys/fs/cgroup/memory.current")
    if limit.exists() and current.exists() and limit.read_text().strip() != "max":
        available = min(available, max(0, int(limit.read_text()) - int(current.read_text())))
    return available


def _capture(command: list[str], *, timeout: int = 120, cwd: Path | None = None) -> str:
    return subprocess.run(command, cwd=cwd, check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout).stdout.strip()


def start(source: Path, root: Path) -> dict:
    """Start one bounded, pinned disposable BuildKit; return its identity/budget."""
    if platform.machine().lower() not in {"x86_64", "amd64"}:
        raise ValueError("probe requires native AMD64")
    source, root = Path(source).resolve(), Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    require_disk(root, 5 * GIB)
    cpus = min(8, os.cpu_count() or 1)
    memory = min(20 * GIB, available_memory() - 2 * GIB)
    if memory < 5 * GIB:
        raise ValueError("insufficient memory: require 5 GiB builder plus 2 GiB reserve")
    name = "loom-artifact-probe-" + uuid.uuid4().hex[:16]
    budget = {"name": name, "image": BUILDKIT, "cpus": cpus,
        "memory_bytes": memory, "host_cpu_count": os.cpu_count()}
    command = ["docker", "run", "--detach", "--name", name, "--privileged",
        "--cpus", str(cpus), "--memory", str(memory), "--memory-swap", str(memory),
        "--mount", f"type=bind,src={source},dst=/source,readonly",
        "--mount", f"type=bind,src={root},dst=/probe", BUILDKIT]
    write_json(root / "metrics" / "builder.json", {**budget, "command": command})
    try:
        _capture(command, timeout=240)
        deadline = time.monotonic() + 60
        while True:
            try:
                _capture(["docker", "exec", name, "buildctl", "debug", "workers"], timeout=10)
                break
            except (subprocess.SubprocessError, OSError):
                if time.monotonic() >= deadline:
                    raise RuntimeError("BuildKit readiness deadline exceeded")
                time.sleep(1)
        return budget
    except BaseException:
        subprocess.run(["docker", "rm", "--force", "--volumes", name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        raise


def image_rows(source: Path) -> list[dict]:
    manifest = tomllib.loads((source / "config/component-ownership.toml").read_text())
    result = []
    for component, image in IMAGES.items():
        rows = [row for row in manifest["components"]
            if row.get("kind") == "release-image" and row.get("release_digest") == image]
        if len(rows) != 1:
            raise ValueError(f"expected exactly one Dockerfile owner for {component}")
        row = rows[0]
        for key in ("dockerfile", "build_context"):
            if not (source / row[key]).resolve().is_relative_to(source):
                raise ValueError("build input outside source")
        result.append({"component": component, "image_name": image,
            "dockerfile": row["dockerfile"], "context": row["build_context"]})
    return result


def inspect_archive(path: Path, source_sha: str) -> dict:
    """Check native OCI manifest/config identity; full scanning is separate."""
    with tarfile.open(path) as archive:
        def read(name: str) -> bytes:
            member = archive.getmember(name)
            if not member.isfile():
                raise ValueError("OCI metadata must be a regular file")
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("OCI metadata missing")
            return stream.read()
        def blob(descriptor: dict) -> bytes:
            digest = descriptor["digest"]
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
                raise ValueError("invalid OCI digest")
            payload = read("blobs/sha256/" + digest.split(":", 1)[1])
            if len(payload) != descriptor["size"] or hashlib.sha256(payload).hexdigest() != digest[7:]:
                raise ValueError("OCI metadata content mismatch")
            return payload
        index = json.loads(read("index.json"))
        if len(index["manifests"]) != 1:
            raise ValueError("expected exactly one image manifest")
        descriptor = index["manifests"][0]
        manifest = json.loads(blob(descriptor))
        config = json.loads(blob(manifest["config"]))
        if config.get("architecture") != "amd64" or config.get("os") != "linux":
            raise ValueError("unexpected OCI platform")
        if config.get("config", {}).get("Labels", {}).get("org.opencontainers.image.revision") != source_sha:
            raise ValueError("OCI source label mismatch")
        return {"source_sha": source_sha, "platform": "linux/amd64",
            "digest": descriptor["digest"], "archive": path.name,
            "archive_bytes": path.stat().st_size, "bytes": path.stat().st_size}


def build_set(*, builder: str, source: Path, root: Path, source_sha: str,
        rows: list[dict], phase: str, workers: int,
        import_cache: Path | None = None) -> dict:
    """Build a bounded image set into root/phase; return per-image/wall timing."""
    started = time.monotonic()
    output = root / phase
    output.mkdir(parents=True, exist_ok=True)
    image_dir = output if phase in {"images", "rebuilt-images"} else output / "images"
    image_dir.mkdir(parents=True, exist_ok=True)
    log_dir = root / "metrics" / phase
    log_dir.mkdir(parents=True, exist_ok=True)

    def one(row: dict) -> tuple[str, dict]:
        component = row["component"]
        host_archive = image_dir / f"{component}.tar"
        if host_archive.exists():
            raise ValueError(f"refusing existing archive: {host_archive}")
        container_archive = "/probe/" + host_archive.relative_to(root).as_posix()
        dockerfile = Path(row["dockerfile"])
        tag = f"localhost/loom-probe/{component}:candidate-{source_sha}"
        command = ["docker", "exec", builder, "buildctl", "build", "--progress", "plain",
            "--frontend", "dockerfile.v0", "--local", f"context=/source/{row['context']}",
            "--local", f"dockerfile=/source/{dockerfile.parent.as_posix()}",
            "--opt", f"filename={dockerfile.name}", "--opt", "platform=linux/amd64",
            "--opt", f"label:org.opencontainers.image.revision={source_sha}",
            "--opt", f"build-arg:LOOM_BUILD_SHA={source_sha}",
            "--opt", "build-arg:LOOM_SOURCE_REF=refs/heads/dev"]
        if component == "harbor_runtime":
            command.extend(["--opt", f"build-arg:LOOM_AGENT_VERSION=nebius-{source_sha}"])
        cache_used = None
        if import_cache is not None:
            cache = import_cache.resolve()
            if not (cache / "index.json").is_file() or not cache.resolve().is_relative_to(root):
                raise ValueError("import cache must contain index.json and be mounted below probe root")
            cache_map = json.loads((cache / "component-digests.json").read_text())
            if set(cache_map) != set(IMAGES) or not re.fullmatch(r"sha256:[0-9a-f]{64}", cache_map.get(component, "")):
                raise ValueError("cache must record all seven native component manifest digests")
            cache_used = "/probe/" + cache.resolve().relative_to(root).as_posix()
            command.extend(["--import-cache", f"type=local,src={cache_used},digest={cache_map[component]}"])
        command.extend(["--output", f"type=oci,oci-mediatypes=true,name={tag},dest={container_archive}"])
        write_json(log_dir / f"{component}.command.json", {"command": command})
        free_before = require_disk(root)
        item_started = time.monotonic()
        with (log_dir / f"{component}.log").open("w") as stream:
            process = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, timeout=3600, check=False)
        build_seconds = time.monotonic() - item_started
        if process.returncode:
            write_json(log_dir / f"{component}.result.json", {"returncode": process.returncode,
                "build_seconds": build_seconds})
            raise RuntimeError(f"{component} build failed; inspect its build log")
        checked = time.monotonic()
        image = inspect_archive(host_archive, source_sha)
        image.update(build_seconds=round(build_seconds, 6),
            inspect_seconds=round(time.monotonic() - checked, 6), import_cache=cache_used,
            disk_free_before_bytes=free_before, **log_counts(log_dir / f"{component}.log"))
        write_json(log_dir / f"{component}.result.json", image)
        return component, image

    images = {}
    errors = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(one, row): row["component"] for row in rows}
        for future in as_completed(futures):
            try:
                component, image = future.result()
                images[component] = image
            except Exception as error:
                errors.append({"component": futures[future], "error": type(error).__name__})
    document = {"source_sha": source_sha, "phase": phase, "workers": max(1, workers),
        "wall_seconds": round(time.monotonic() - started, 6), "images": images,
        "errors": errors, "complete": not errors and len(images) == len(rows),
        "cache_note": "explicit local import" if import_cache else "no external cache import; same builder may retain earlier phase layers"}
    write_json(output / "manifest.json", document)
    write_json(root / "metrics" / f"{phase}.json", document)
    if phase == "images":
        write_json(root / "manifest.json", document)
    if errors:
        raise RuntimeError(f"{phase}: {len(errors)} image builds failed")
    return document


def export_cache(*, builder: str, root: Path, source_sha: str, rows: list[dict]) -> dict:
    """Sequential min exports share blobs; retain each overwritten index digest.

    BuildKit v0.33.0 README documents local import digest=sha256:... and
    reset=false by default, preserving older blobs while index.json advances.
    https://github.com/moby/buildkit/blob/v0.33.0/README.md#local-directory-1
    """
    started = time.monotonic()
    cache = root / "cache"
    cache.mkdir(parents=True, exist_ok=True)
    logs = root / "metrics" / "cache-export"
    logs.mkdir(parents=True, exist_ok=True)
    digests = {}
    timings = {}
    for row in rows:
        component = row["component"]
        command = json.loads((root / "metrics" / "images" / f"{component}.command.json").read_text())["command"]
        # Original producer command has one OCI exporter; cache-only does not
        # create another archive or publish an image.
        output_index = command.index("--output")
        command = command[:output_index] + command[output_index + 2:]
        command.extend(["--export-cache", "type=local,dest=/probe/cache,mode=min"])
        require_disk(root)
        write_json(logs / f"{component}.command.json", {"command": command})
        item_started = time.monotonic()
        log = logs / f"{component}.log"
        with log.open("w") as stream:
            subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, timeout=3600, check=True)
        index = json.loads((cache / "index.json").read_text())
        digest = index["manifests"][0]["digest"]
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
            raise ValueError("local exporter returned invalid native manifest digest")
        digests[component] = digest
        write_json(cache / "component-digests.json", digests)
        timings[component] = {"seconds": round(time.monotonic() - item_started, 6),
            "digest": digest, **log_counts(log)}
    files = [path for path in cache.rglob("*") if path.is_file()]
    result = {"source_sha": source_sha, "mode": "min", "complete": len(digests) == len(IMAGES),
        "wall_seconds": round(time.monotonic() - started, 6), "bytes": sum(path.stat().st_size for path in files),
        "file_count": len(files), "components": timings,
        "cache_note": "cross-run transferable exact-same-SHA min cache; not prior-commit max-cache evidence"}
    write_json(root / "metrics" / "cache-export.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--phase", choices=("producer", "cache-rebuild"), required=True)
    parser.add_argument("--workers", type=int, default=min(7, os.cpu_count() or 1))
    parser.add_argument("--import-cache", type=Path)
    args = parser.parse_args()
    source, root = args.source.resolve(), args.root.resolve()
    if args.workers < 1 or args.workers > 7:
        parser.error("workers must be between 1 and 7")
    source_sha = _capture(["git", "rev-parse", "HEAD"], cwd=source)
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("source must be an exact Git commit")
    if _capture(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=source):
        raise ValueError("source checkout is not clean")
    rows = image_rows(source)
    started = time.monotonic()
    budget = start(source, root)
    summary = {"phase": args.phase, "source_sha": source_sha, "runs": {},
        "cache_note": "same-SHA hot phases are an optimistic no-op cache bound", "complete": False}
    try:
        versions = {"python": sys.version, "docker": _capture(["docker", "version", "--format", "{{.Client.Version}}"]),
            "buildkit": _capture(["docker", "exec", budget["name"], "buildctl", "--version"])}
        write_json(root / "metrics" / "versions.json", versions)
        phases = [("images", args.workers), ("hot-parallel", args.workers), ("hot-serial", 1)] if args.phase == "producer" else [("rebuilt-images", args.workers)]
        for phase, workers in phases:
            summary["runs"][phase] = build_set(builder=budget["name"], source=source,
                root=root, source_sha=source_sha, rows=rows, phase=phase, workers=workers,
                import_cache=args.import_cache if args.phase == "cache-rebuild" else None)
            if phase in {"hot-parallel", "hot-serial"}:
                # Only archives produced by this successful phase are removed;
                # the native manifests, timing records and build logs remain.
                for image in summary["runs"][phase]["images"].values():
                    (root / phase / "images" / image["archive"]).unlink()
                summary["runs"][phase]["archives_removed_after_measurement"] = True
                write_json(root / "metrics" / f"{phase}.json", summary["runs"][phase])
        if args.phase == "producer":
            summary["cache_export"] = export_cache(builder=budget["name"], root=root,
                source_sha=source_sha, rows=rows)
        summary["complete"] = True
    finally:
        with (root / "metrics" / "builder-final.log").open("w") as stream:
            subprocess.run(["docker", "logs", "--tail", "100", budget["name"]], stdout=stream, stderr=subprocess.STDOUT, check=False)
        subprocess.run(["docker", "rm", "--force", "--volumes", budget["name"]], check=False,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        summary["wall_seconds"] = round(time.monotonic() - started, 6)
        if args.phase == "cache-rebuild" and args.import_cache is None:
            summary["cache_note"] = "fresh builder without imported cache: cold rebuild, not cache-reuse evidence"
        write_json(root / "metrics" / "summary.json", summary)
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
