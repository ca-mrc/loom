"""Assemble a Linux/amd64 guest payload from checksum-locked upstream archives.

Run only in the disposable build stage. Debian packages are extracted, never
installed; their postinst scripts cannot configure the build host's kernel.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def download(item: dict[str, object], directory: Path) -> Path:
    expected = str(item["sha256"])
    destination = directory / expected
    if destination.exists() and digest(destination) == expected:
        return destination
    partial = destination.with_suffix(".partial")
    url = str(item["url"])
    if not url.startswith("https://"):
        raise ValueError(f"archive URL must use HTTPS: {url}")
    try:
        for attempt in range(1, 4):
            try:
                with urlopen(url, timeout=120) as response, partial.open("wb") as output:
                    shutil.copyfileobj(response, output)
            except HTTPError as error:
                error.close()
                partial.unlink(missing_ok=True)
                print(
                    f"guest archive {expected}: HTTP {error.code}, attempt {attempt}/3",
                    file=sys.stderr,
                    flush=True,
                )
                if error.code not in {502, 503, 504} or attempt == 3:
                    raise
                time.sleep(attempt)
            else:
                break
        if partial.stat().st_size != item["bytes"] or digest(partial) != expected:
            raise ValueError(f"archive content does not match source lock: {url}")
        partial.replace(destination)
        return destination
    finally:
        partial.unlink(missing_ok=True)


def run(*args: str | Path) -> str:
    return subprocess.check_output([str(arg) for arg in args], text=True).strip()


def copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination, follow_symlinks=True)


def build(lock_path: Path, buildx: Path, downloads: Path, work: Path, output: Path) -> None:
    if platform.machine() != "x86_64":
        raise ValueError("the guest payload currently supports Linux/amd64 only")
    lock = json.loads(lock_path.read_text())
    if lock["format_version"] != 1 or lock["architecture"] != "amd64":
        raise ValueError("unsupported payload source lock")
    downloads.mkdir(parents=True, exist_ok=True)
    work.mkdir(parents=True, exist_ok=False)
    output.mkdir(parents=True, exist_ok=False)
    root = work / "root"
    packages = lock["packages"]
    with ThreadPoolExecutor(max_workers=8) as pool:
        archives = list(pool.map(lambda item: download(item, downloads), packages))
    for package, archive in zip(packages, archives, strict=True):
        actual = run("dpkg-deb", "--field", archive, "Package", "Version", "Architecture")
        expected = "\n".join(
            f"{key}: {package[field]}"
            for key, field in (
                ("Package", "name"),
                ("Version", "version"),
                ("Architecture", "architecture"),
            )
        )
        if actual != expected:
            raise ValueError(f"package metadata does not match source lock: {actual}")
        subprocess.run(["dpkg-deb", "--extract", str(archive), str(root)], check=True)

    libdir = root / "usr/lib/x86_64-linux-gnu"
    loader = libdir / "ld-linux-x86-64.so.2"
    library_path = f"{libdir}:{root / 'lib/x86_64-linux-gnu'}"

    def dynamic(executable: Path, *args: str | Path) -> str:
        return run(loader, "--inhibit-cache", "--library-path", library_path, executable, *args)

    def bundle_elf(source: Path, destination: Path) -> None:
        copy_file(source, destination)
        dependencies = run(
            loader, "--inhibit-cache", "--library-path", library_path, "--list", source
        )
        for line in dependencies.splitlines():
            if "not found" in line:
                raise ValueError(f"unresolved ELF dependency: {line}")
            match = re.search(r"(?:=>\s+)?(/\S+)\s+\(", line)
            if match:
                dependency = Path(match[1])
                # A closure must never silently incorporate builder-image libc.
                if not dependency.is_relative_to(root):
                    raise ValueError(f"dependency escaped locked package root: {dependency}")
                target = output / "lib" / dependency.name
                if target.exists() and digest(target) != digest(dependency):
                    raise ValueError(f"colliding library names: {dependency.name}")
                copy_file(dependency, target)

    copy_file(loader, output / "lib/ld-linux-x86-64.so.2")
    for name, path in {
        "qemu-system-x86_64": "usr/bin/qemu-system-x86_64",
        "mke2fs": "usr/sbin/mke2fs",
        "kmod": "usr/bin/kmod",
        "xtables-legacy-multi": "usr/sbin/xtables-legacy-multi",
        "xtables-nft-multi": "usr/sbin/xtables-nft-multi",
    }.items():
        bundle_elf(root / path, output / "bin" / name)
    for directory in ("qemu", "xtables"):
        for module in sorted((libdir / directory).glob("*.so")):
            bundle_elf(module, output / "lib" / directory / module.name)

    # Dereference distro ROM symlinks: no /usr/share path exists in a task root.
    for name in ("qemu", "seabios", "ipxe"):
        source = root / "usr/share" / name
        if source.exists():
            for file in source.rglob("*"):
                if file.is_symlink():
                    link = file.readlink()
                    if link.is_absolute():
                        file.unlink()
                        file.symlink_to(root / str(link).lstrip("/"))
            shutil.copytree(source, output / "share" / name, symlinks=False)

    copy_file(root / "etc/mke2fs.conf", output / "etc/mke2fs.conf")
    copy_file(root / "usr/bin/busybox", output / "bin/busybox")
    release = lock["kernel_release"]
    copy_file(root / "boot" / f"vmlinuz-{release}", output / "kernel")
    copy_file(root / "boot" / f"config-{release}", output / "kernel.config")
    (output / "kernel-release").write_text(f"{release}\n")

    module_root = work / "module-root"
    module_tree = module_root / "lib/modules" / release
    shutil.copytree(root / "lib/modules" / release, module_tree)
    for compressed in sorted(module_tree.rglob("*.ko.zst")):
        subprocess.run(
            [
                str(loader),
                "--inhibit-cache",
                "--library-path",
                library_path,
                str(root / "usr/bin/zstd"),
                "-d",
                "--rm",
                str(compressed),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    dynamic(root / "usr/sbin/depmod", "-b", module_root, release)
    shutil.move(str(module_root / "lib/modules"), output / "modules")
    early_modules = ["netfs", "9pnet", "9pnet_virtio", "9p", "overlay"]
    for name in early_modules:
        matches = list((output / "modules" / release).rglob(f"{name}.ko"))
        if len(matches) != 1:
            raise ValueError(f"boot module {name}: expected exactly one match")
        copy_file(matches[0], output / "boot-modules" / f"{name}.ko")
    (output / "boot-modules/load-order").write_text("\n".join(early_modules) + "\n")

    docker_archive = download(lock["docker"], downloads)
    with tarfile.open(docker_archive) as archive:
        archive.extractall(output, filter="data")
    if (
        buildx.stat().st_size != lock["buildx"]["bytes"]
        or digest(buildx) != lock["buildx"]["sha256"]
    ):
        raise ValueError("buildx content does not match source lock")
    copy_file(buildx, output / "docker/cli-plugins/docker-buildx")
    versions = {
        name: run(output / "docker" / name, "--version")
        for name in ("docker", "dockerd", "containerd", "runc")
    }
    versions["buildx"] = run(output / "docker/cli-plugins/docker-buildx", "version")
    versions["qemu"] = dynamic(root / "usr/bin/qemu-system-x86_64", "--version")
    versions["kernel_release"] = release
    (output / "versions.json").write_text(json.dumps(versions, indent=2) + "\n")
    copy_file(lock_path, output / "sources.lock.json")
    for package in packages:
        copyright_file = root / "usr/share/doc" / package["name"] / "copyright"
        if copyright_file.is_file():
            copy_file(copyright_file, output / "licenses" / f"{package['name']}.copyright")

    # Self-contained regular files avoid broken absolute symlinks after relocation.
    files = sorted(file for file in output.rglob("*") if file.is_file())
    for file in files:
        if file.is_symlink():
            raise ValueError(f"unexpected payload symlink: {file}")
        # Distro kernels are root-only; the enclosing QEMU runs without root.
        file.chmod(0o755 if file.stat().st_mode & 0o111 else 0o644)
    (output / "SHA256SUMS").write_text(
        "".join(f"{digest(file)}  {file.relative_to(output)}\n" for file in files)
    )
    print(
        json.dumps(
            {
                "payload_bytes": sum(file.stat().st_size for file in files),
                "file_count": len(files),
                "versions": versions,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ("lock", "buildx", "downloads", "work", "output"):
        parser.add_argument(f"--{option}", required=True, type=Path)
    args = parser.parse_args()
    build(args.lock, args.buildx, args.downloads, args.work, args.output)
