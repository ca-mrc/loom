#!/usr/bin/env python3
"""Build the #2054 response-only acceptance TaskSet (direct-completion/litellm)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from loom.agent_model_acceptance_taskset import (
    AgentModelAcceptanceTaskSetError,
    build_response_only_taskset,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--task-image-ref",
        help=(
            "Pin an exact digest image instead of the platform runner image; only "
            "needed for deployments that predate runner-image tasks."
        ),
    )
    args = parser.parse_args(argv)
    try:
        evidence = build_response_only_taskset(
            output_dir=args.output.resolve(), task_image_ref=args.task_image_ref,
        )
    except AgentModelAcceptanceTaskSetError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    print(json.dumps(evidence, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
