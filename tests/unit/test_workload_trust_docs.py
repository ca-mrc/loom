from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RENDER_FIXTURES = (
    "tests/fixtures/cluster-render/staging.cluster.toml",
    "tests/fixtures/cluster-render/staging.multinode.cluster.toml",
    "tests/fixtures/cluster-render/production.cluster.toml",
)


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


def _normalized(text: str) -> str:
    return " ".join(text.split())


def test_current_workload_trust_docs_record_the_supported_tuple() -> None:
    operator_runbook = _read("docs/runbooks/operator-runbook.md")
    normalized = _normalized(operator_runbook)

    for fragment in (
        'workload_trust_mode = "internal_trusted"',
        "taskset_transforms_enabled = false",
        "taskset_transform_network_isolated = false",
        "untrusted_workload_isolation = false",
    ):
        assert fragment in normalized
        for profile in RENDER_FIXTURES:
            assert fragment in _read(profile)


def test_current_workload_trust_docs_preserve_the_fail_closed_boundary() -> None:
    tasksets = _read("docs/architecture/user-brought-tasksets.md")
    sandbox = _read("docs/architecture/sandbox-isolation.md")
    domain_model = _read("docs/agent/domain-model.md")
    operator_runbook = _read("docs/runbooks/operator-runbook.md")
    normalized_operator = _normalized(operator_runbook).lower()

    assert "transform_unavailable_in_internal_trusted" in domain_model
    assert "before any transform/source/verifier blob fetch" in _normalized(domain_model)
    assert (
        "declaring one fails before source, verifier, or\ntransform blobs are fetched" in tasksets
    )
    assert "do not make arbitrary uploaded code safe to execute" in _normalized(sandbox)
    assert "`--skip-preflight` does not bypass the contract" in operator_runbook
    assert "protected namespace is authoritative target evidence" in normalized_operator
    assert (
        "manual rollout validates the cluster and namespace identity before evidence "
        "collection or disposable local work"
    ) in normalized_operator


def test_current_workload_trust_docs_are_discoverable() -> None:
    architecture_index = _read("docs/architecture/README.md")
    domain_model = _read("docs/agent/domain-model.md")
    docs_index = _read("docs/index.md")

    assert "sandbox-isolation.md" in architecture_index
    assert "user-brought-tasksets.md" in architecture_index
    assert "sandbox-isolation.md#workload-trust-mode" in domain_model
    assert "Workload Trust Mode" in domain_model
    assert "domain-model.md" in docs_index
