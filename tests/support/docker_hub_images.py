"""Prepare selected cold Docker Hub fixtures before pytest's test deadlines."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from tests.support.minio_images import selected_fixture_images

# Preserve the references already used by the PostgreSQL and sandbox fixtures.
FIXTURE_IMAGES = ("postgres:16", "python:3.11-slim")


def selected_images(paths: list[str], *, root: Path | None = None) -> tuple[str, ...]:
    return selected_fixture_images(paths, root=root, known_images=FIXTURE_IMAGES)


def prepare_images(images: tuple[str, ...]) -> None:
    if any(image not in FIXTURE_IMAGES for image in images):
        raise ValueError("Unknown Docker Hub fixture image")
    for image in images:
        cached = subprocess.run(
            ["docker", "image", "inspect", image], check=False, timeout=10,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        if cached.returncode == 0:
            continue
        print(f"Preparing cold fixture image: {image}", flush=True)
        # Docker verifies layer digests. Use the runner's configured registry
        # mirror and leave pull errors visible; no fallback or retry loop.
        subprocess.run(["docker", "pull", image], check=True, timeout=180)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-paths-file", type=Path, required=True)
    arguments = parser.parse_args()
    prepare_images(selected_images(arguments.test_paths_file.read_text().splitlines()))
