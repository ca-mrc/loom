#!/usr/bin/env python3
"""Build a #2054 acceptance TaskSet: `response-only` (direct-completion/litellm)
or `workspace` (OpenHands, Terminus-2, Codex and Oracle)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from loom.agent_model_acceptance_taskset import (
    AgentModelAcceptanceTaskSetError,
    build_response_only_taskset,
    build_workspace_taskset,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--task", choices=("response-only", "workspace"), default="response-only")
    parser.add_argument(
        "--task-image-ref",
        help=(
            "response-only: pin an exact digest image instead of the platform runner "
            "image; only needed for deployments that predate runner-image tasks."
        ),
    )
    args = parser.parse_args(argv)
    if args.task == "workspace" and args.task_image_ref:
        parser.error("--task-image-ref applies only to --task response-only")
    try:
        evidence = (
            build_workspace_taskset(output_dir=args.output.resolve())
            if args.task == "workspace"
            else build_response_only_taskset(
                output_dir=args.output.resolve(), task_image_ref=args.task_image_ref,
            )
        )
    except AgentModelAcceptanceTaskSetError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    print(json.dumps(evidence, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
