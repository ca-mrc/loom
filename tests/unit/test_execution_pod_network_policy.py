"""The execution pod stays closed when public HTTP uses the gateway dialer."""

from pathlib import Path

_POLICY = Path("deploy/k8s/nebius-execution-actuator.yaml").read_text()


def test_execution_pod_stays_dns_and_gateway_only() -> None:
    start = _POLICY.index("name: loom-execution-attempt-default-deny")
    end = _POLICY.index("apiVersion: apps/v1", start)
    execution = _POLICY[start:end]
    assert "ingress: []" in execution
    assert "0.0.0.0/0" not in execution
    assert "port: 53" in execution
    assert "port: 9100" in execution
    assert "port: 80" not in execution
    assert "port: 443" not in execution
