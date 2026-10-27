"""Fresh platform inputs shared without importing renderer test cases."""
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def platform_inputs() -> tuple[dict, dict, dict]:
    config = json.loads((ROOT / "deploy/nebius/integration.platform.json.example").read_text())
    for key in (
        "public_allocation_id",
        "project_id",
        "quota_parent_id",
        "execution_node_group_id",
        "cluster_id",
    ):
        config[key] = "test-" + key.replace("_", "-")
    config["project_id"] = "project-test"
    config["quota_parent_id"] = "tenant-test"
    config["kubernetes_api_server"] = "https://api.cluster.test"
    config["execution_price"]["vcpu_microusd_per_hour"] = 1000
    for key in ("postgres_image", "backup_image"):
        config[key] = "cr.eu-north1.nebius.cloud/test/postgres@sha256:" + "a" * 64
    config["buckets"] = {key: "loom-integration-" + key for key in config["buckets"]}
    images = {
        key: {"image_ref": "cr.eu-north1.nebius.cloud/test/" + key + "@sha256:" + "b" * 64}
        for key in (
            "service",
            "control_plane",
            "web",
            "gateway",
            "execution_runtime",
            "execution_actuator",
        )
    }
    profile = {
        "candidate_sha": "c" * 40,
        "task_image_ref": images["service"]["image_ref"],
        "runtime_image_ref": images["execution_runtime"]["image_ref"],
    }
    candidate = {
        "candidate_sha": "c" * 40,
        "source_ref": "refs/heads/codex/nebius-main",
        "repository": "qianyi-sun/loom",
        "images": images,
    }
    return config, candidate, profile


@pytest.fixture
def regional_inputs(platform_inputs: tuple) -> tuple:
    from copy import deepcopy

    config, candidate, profile = platform_inputs
    regional_example = json.loads(
        (ROOT / "deploy/nebius/regional.platform.json.example").read_text()
    )
    regional = deepcopy(regional_example["regional_execution_targets"][0])
    for key in ("project_id", "quota_parent_id", "cluster_id", "execution_node_group_id"):
        regional[key] = config[key] + "-west"
    regional["kubernetes_api_server"] = "https://api.west.test"
    regional["execution_price"] = dict(
        config["execution_price"],
        region="eu-west1",
        sku=regional["execution_price"]["sku"],
        source_version="test-west",
    )
    config["public_gateway_ipv4"] = "8.8.4.4"
    config["regional_execution_targets"] = [regional]
    return config, candidate, profile
