"""Exercise installed Loom modules and the runtime assets shipped in its wheel."""

from __future__ import annotations

import argparse
from importlib import import_module, resources
from importlib.resources.abc import Traversable
from pathlib import Path


def verify_asset(source: Path, installed: Traversable) -> None:
    """Report which packaged resource drifted without printing file contents."""
    try:
        expected, actual = source.read_bytes(), installed.read_bytes()
    except OSError as error:
        raise ValueError(f"cannot read wheel asset {source} -> {installed}: {error.strerror}") from None
    if expected != actual:
        offset = next((i for i, pair in enumerate(zip(expected, actual, strict=False)) if pair[0] != pair[1]),
                      min(len(expected), len(actual)))
        raise ValueError(
            f"wheel asset mismatch: {source} -> {installed}; "
            f"source={len(expected)} bytes, installed={len(actual)} bytes, first difference at byte {offset}"
        )


def verify_installed_wheel(repo_root: Path, test_scope: str) -> None:
    repo_root = repo_root.resolve()
    for name in ("loom_cli", "loom.nebius_platform_render", "loom_execution_actuator.task_image_controller"):
        module = import_module(name)
        if module.__file__ is None:
            raise ValueError(f"installed module has no source location: {name}")
        module_path = Path(module.__file__).resolve()
        if module_path.is_relative_to(repo_root / "src"):
            raise ValueError(f"wheel smoke imported editable source: {name} from {module_path}")
    runtime_assets: dict[Path, tuple[str, ...]] = {
        Path("config/loom-schema.toml"): ("loom-schema.toml",),
    }
    if test_scope == "all":
        runtime_assets.update({
            Path(f"deploy/grafana/dashboards/{name}.json"): ("grafana", f"{name}.json")
            for name in ("operator-overview", "control-plane", "llm-gateway", "loom-service", "worker")
        })
        runtime_assets[Path("deploy/envoy/egress-proxy.yaml")] = ("envoy", "egress-proxy.yaml")
    package_root = resources.files("loom_cli.data")
    for source_path, resource_parts in runtime_assets.items():
        verify_asset(repo_root / source_path, package_root.joinpath(*resource_parts))
    if test_scope == "all":
        template = resources.files("loom_cli.templates.k8s").joinpath("_env.j2")
        if not template.read_text(encoding="utf-8").strip():
            raise ValueError(f"empty installed Kubernetes template: {template}")
        render_manifests = import_module("loom_cli.cluster_cmd").render_manifests
        cluster_config = import_module("loom_cli.cluster_config").ClusterConfig
        inspect_migration_plan = import_module("loom_cli.migration_readiness").inspect_migration_plan

        if not callable(inspect_migration_plan):
            raise ValueError("installed migration readiness entry point is not callable")
        rendered = render_manifests(cluster_config())
        if "kind: ConfigMap" not in rendered or "name: loom-grafana-dashboards" not in rendered:
            raise ValueError("installed wheel did not render the Grafana dashboard ConfigMap")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--test-scope", choices=("all", "nebius"), default="all")
    args = parser.parse_args()
    try:
        verify_installed_wheel(args.repo_root, args.test_scope)
    except (ValueError, OSError, ImportError) as error:
        parser.exit(1, f"FAIL: {error}\n")
    print("PASS: installed wheel imports and runtime assets")


if __name__ == "__main__":
    main()
