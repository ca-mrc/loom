"""`loom cluster status` unit tests.

Use fake k8s client classes (not the real kubernetes lib) so the
tests don't need a cluster or the python `kubernetes` package
installed in the test env. The CLI module's `collect_status`
function takes the API clients as args specifically for this seam.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import (
    ClusterStatus,
    ComponentStatus,
    DeploymentImageCheck,
    IngressEndpoint,
    _format_json,
    _format_table,
    collect_status,
    rendered_image_checks,
    rendered_image_drifts,
)

# ──────────────────────────────────────────────────────────────────────
# Fake k8s clients
# ──────────────────────────────────────────────────────────────────────


class _FakeApiException(Exception):  # noqa: N818 — name must end "Exception" so `_exception_to_note` matches on type name
    """Mimics kubernetes.client.exceptions.ApiException for the
    `_exception_to_note` 404 branch."""

    def __init__(self, status: int, body: str = "") -> None:
        super().__init__(body or f"status={status}")
        self.status = status


# Override the class name so `_exception_to_note` matches on "ApiException".
_FakeApiException.__name__ = "ApiException"


class _Spec:
    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


class _Workload:
    def __init__(
        self,
        *,
        metadata: object | None = None,
        spec: object | None = None,
        status: object | None = None,
    ) -> None:
        self.metadata = metadata
        self.spec = spec
        self.status = status


class _FakeAppsV1:
    """In-memory deployments / daemonsets / statefulsets. Tests
    populate the dicts with the workloads they want to exist;
    missing entries surface as ApiException(404)."""

    def __init__(self) -> None:
        self.deployments: dict[str, _Workload] = {}
        self.daemonsets: dict[str, _Workload] = {}
        self.statefulsets: dict[str, _Workload] = {}

    def read_namespaced_deployment(
        self,
        *,
        name: str,
        namespace: str,
    ) -> _Workload:
        if name not in self.deployments:
            raise _FakeApiException(404)
        return self.deployments[name]

    def read_namespaced_daemon_set(
        self,
        *,
        name: str,
        namespace: str,
    ) -> _Workload:
        if name not in self.daemonsets:
            raise _FakeApiException(404)
        return self.daemonsets[name]

    def read_namespaced_stateful_set(
        self,
        *,
        name: str,
        namespace: str,
    ) -> _Workload:
        if name not in self.statefulsets:
            raise _FakeApiException(404)
        return self.statefulsets[name]


class _IngressList:
    def __init__(self, items: Iterable[Any]) -> None:
        self.items = list(items)


class _FakeNetworkingV1:
    def __init__(self, ingresses: list[Any] | None = None) -> None:
        self.ingresses = ingresses or []

    def list_namespaced_ingress(self, *, namespace: str) -> _IngressList:
        return _IngressList(self.ingresses)


class _FakeCoreV1:
    def __init__(self, secrets: set[str] | None = None) -> None:
        self.secrets = secrets or set()
        self.pods_by_namespace: dict[str, list[Any]] = {}
        self.events_by_namespace: dict[str, list[Any]] = {}

    def read_namespaced_secret(self, *, name: str, namespace: str) -> object:
        if name not in self.secrets:
            raise _FakeApiException(404)
        return _Spec(name=name)

    def list_namespaced_pod(self, *, namespace: str) -> object:
        return _Spec(items=self.pods_by_namespace.get(namespace, []))

    def list_namespaced_event(self, *, namespace: str) -> object:
        return _Spec(items=self.events_by_namespace.get(namespace, []))


class _FakeStorageV1:
    """Stub for `_load_clients`'s 4-tuple. Status doesn't read from
    it; preflight does. Tests in this file pass an empty fake."""

    def list_storage_class(self) -> object:
        return _Spec(items=[])


# ──────────────────────────────────────────────────────────────────────
# Helpers to build a "happy" cluster snapshot
# ──────────────────────────────────────────────────────────────────────


def _make_deployment(
    ready: int,
    desired: int,
    available: int | None = None,
    *,
    generation: int = 1,
    observed_generation: int = 1,
    updated: int | None = None,
    image: str = "loom-service:current",
) -> _Workload:
    return _Workload(
        metadata=_Spec(generation=generation),
        spec=_Spec(
            replicas=desired,
            template=_Spec(
                spec=_Spec(containers=[_Spec(name="app", image=image)]),
            ),
        ),
        status=_Spec(
            ready_replicas=ready,
            available_replicas=available if available is not None else ready,
            observed_generation=observed_generation,
            updated_replicas=updated if updated is not None else ready,
        ),
    )


def _make_statefulset(ready: int, desired: int) -> _Workload:
    return _Workload(
        spec=_Spec(replicas=desired),
        status=_Spec(ready_replicas=ready),
    )


def _fully_ready_apps() -> _FakeAppsV1:
    apps = _FakeAppsV1()
    apps.deployments["loom-service"] = _make_deployment(2, 2)
    apps.deployments["loom-control-plane"] = _make_deployment(2, 2)
    apps.deployments["loom-llm-gateway"] = _make_deployment(2, 2)
    apps.deployments["loom-web"] = _make_deployment(0, 0)  # paused by default
    apps.deployments["loom-worker"] = _make_deployment(3, 3)
    # Keyed by the ACTUAL k8s resource name, not the display name.
    # The status code looks up via k8s names (`loom-postgres`,
    # `loom-minio`) and renders rows under the display name
    # (`postgres`, `minio`).
    apps.statefulsets["loom-postgres"] = _make_statefulset(1, 1)
    apps.statefulsets["loom-minio"] = _make_statefulset(1, 1)
    return apps


# ──────────────────────────────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────────────────────────────


def test_collect_status_happy_path_marks_all_healthy() -> None:
    """Default config: web=0 (paused), all others at desired count.
    `desired=0` components are healthy by definition (operator
    intentionally scaled them down), so `all_ready` is True.
    Before #128's staging-smoke caught it, this required
    `desired > 0` which made the default config never reach
    `all_ready` → `loom cluster up --wait` could never succeed."""
    apps = _fully_ready_apps()
    net = _FakeNetworkingV1()
    core = _FakeCoreV1(secrets={"loom-secrets"})
    status = collect_status(apps, net, core, "loom", context=None)
    assert status.namespace == "loom"
    assert status.warnings == []
    # Web 0/0 IS healthy now — operator intent.
    web = next(c for c in status.components if c.name == "loom-web")
    assert web.healthy
    assert status.all_ready
    # Non-zero components are also healthy.
    svc = next(c for c in status.components if c.name == "loom-service")
    assert svc.healthy


def test_collect_status_with_web_scaled_up_is_all_ready() -> None:
    """When operator scales web up, it's still healthy + all_ready
    stays True."""
    apps = _fully_ready_apps()
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    net = _FakeNetworkingV1()
    core = _FakeCoreV1(secrets={"loom-secrets"})
    status = collect_status(apps, net, core, "loom", context=None)
    assert status.all_ready


def test_collect_status_with_web_partially_up_is_not_ready() -> None:
    """desired=2 but only 1 ready is NOT healthy. The 0/0=healthy
    rule only applies when desired is literally 0."""
    apps = _fully_ready_apps()
    apps.deployments["loom-web"] = _make_deployment(1, 2)
    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )
    web = next(c for c in status.components if c.name == "loom-web")
    assert not web.healthy
    assert not status.all_ready


def test_collect_status_stale_deployment_generation_is_not_ready() -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-service"] = _make_deployment(
        2,
        2,
        generation=12,
        observed_generation=11,
        updated=2,
    )

    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )

    svc = next(c for c in status.components if c.name == "loom-service")
    assert not svc.healthy
    assert svc.note == "stale-generation: observed 11 < generation 12"
    assert not status.all_ready


def test_collect_status_stale_updated_replicas_is_not_ready() -> None:
    """#203 regression — the old-pods-still-serving state.

    Observed on staging-7c0e222 rollout: Deployment desired=1, ready=1
    (old pod still Ready), updated=0 (new pod not yet Ready). Previously
    the healthy check treated ready==desired as sufficient because it
    didn't gate on updated>=desired when updated is 0 (not None). That
    made `loom cluster up` return success while the new image wasn't
    yet serving.
    """
    apps = _fully_ready_apps()
    apps.deployments["loom-service"] = _Workload(
        metadata=_Spec(generation=12),
        spec=_Spec(
            replicas=1,
            template=_Spec(
                spec=_Spec(
                    containers=[_Spec(name="app", image="loom-service:new")],
                ),
            ),
        ),
        status=_Spec(
            ready_replicas=1,  # old pod still Ready
            available_replicas=1,  # old pod still Available
            updated_replicas=0,  # NO pod on the new template yet
            replicas=1,  # total pods across old + new
            observed_generation=12,
        ),
    )

    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )

    svc = next(c for c in status.components if c.name == "loom-service")
    assert not svc.healthy, (
        "desired=1 ready=1 updated=0 must NOT be healthy — a new pod hasn't "
        "reached Ready yet; the old pod is masking the new-image readiness"
    )
    assert svc.note is not None
    assert "updated-replicas" in svc.note
    assert not status.all_ready


def test_collect_status_stale_old_pods_still_around_is_not_ready() -> None:
    """#203 — new pod is Ready but old pod hasn't terminated yet.

    Deployment desired=1, ready=2 (both old + new are Ready — briefly
    possible under a rolling update with maxSurge=1), updated=1,
    replicas=2 (both alive). Not converged: the old pod is still around
    and could still be serving traffic behind the Service selector.
    """
    apps = _fully_ready_apps()
    apps.deployments["loom-service"] = _Workload(
        metadata=_Spec(generation=12),
        spec=_Spec(
            replicas=1,
            template=_Spec(
                spec=_Spec(
                    containers=[_Spec(name="app", image="loom-service:new")],
                ),
            ),
        ),
        status=_Spec(
            ready_replicas=2,
            available_replicas=2,
            updated_replicas=1,
            replicas=2,  # OLD pod still counted
            observed_generation=12,
        ),
    )

    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )

    svc = next(c for c in status.components if c.name == "loom-service")
    assert not svc.healthy, (
        "replicas=2 while updated=1 means an old pod hasn't terminated; "
        "the rollout is not fully converged"
    )
    assert svc.note is not None
    assert "stale-pods" in svc.note or "old" in svc.note.lower()
    assert not status.all_ready


def test_collect_status_updated_reached_but_not_available_is_not_ready() -> None:
    """#203 — updated pods exist but aren't Available (past minReadySeconds).

    Ready>=desired is a weaker signal than Available>=desired. A pod can
    be Ready (its containers report Ready) but not yet Available if the
    Deployment's minReadySeconds hasn't elapsed. Rollout not yet safe.
    """
    apps = _fully_ready_apps()
    apps.deployments["loom-service"] = _Workload(
        metadata=_Spec(generation=12),
        spec=_Spec(
            replicas=1,
            template=_Spec(
                spec=_Spec(
                    containers=[_Spec(name="app", image="loom-service:new")],
                ),
            ),
        ),
        status=_Spec(
            ready_replicas=1,
            available_replicas=0,  # not yet past minReadySeconds
            updated_replicas=1,
            replicas=1,
            observed_generation=12,
        ),
    )

    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )

    svc = next(c for c in status.components if c.name == "loom-service")
    assert not svc.healthy, (
        "available_replicas < desired means the new pod hasn't cleared "
        "minReadySeconds yet; not safe to declare rollout done"
    )
    assert not status.all_ready


def test_collect_status_missing_updated_replicas_is_not_ready() -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-service"] = _Workload(
        metadata=_Spec(generation=12),
        spec=_Spec(
            replicas=2,
            template=_Spec(
                spec=_Spec(
                    containers=[_Spec(name="app", image="loom-service:expected")],
                ),
            ),
        ),
        status=_Spec(
            ready_replicas=2,
            available_replicas=2,
            observed_generation=12,
        ),
    )

    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )

    svc = next(c for c in status.components if c.name == "loom-service")
    assert not svc.healthy
    assert svc.note == "updated-replicas: unknown/2"
    assert not status.all_ready


def test_collect_status_visible_kube_system_crashloop_blocks_ready() -> None:
    apps = _fully_ready_apps()
    core = _FakeCoreV1(secrets={"loom-secrets"})
    core.pods_by_namespace["kube-system"] = [
        _Spec(
            metadata=_Spec(name="kube-controller-manager-staging-control-plane"),
            status=_Spec(
                conditions=[
                    _Spec(type="Ready", status="False"),
                ],
                container_statuses=[
                    _Spec(
                        state=_Spec(
                            waiting=_Spec(reason="CrashLoopBackOff"),
                        ),
                    ),
                ],
            ),
        ),
    ]

    status = collect_status(apps, _FakeNetworkingV1(), core, "loom", context=None)

    assert not status.all_ready
    assert any("kube-controller-manager" in warning for warning in status.warnings)
    assert any("CrashLoopBackOff" in warning for warning in status.warnings)


def test_collect_status_managed_pod_crashloop_blocks_ready() -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-service"] = _make_deployment(
        2,
        2,
        generation=13,
        observed_generation=13,
        updated=2,
    )
    core = _FakeCoreV1(secrets={"loom-secrets"})
    core.pods_by_namespace["loom"] = [
        _Spec(
            metadata=_Spec(
                name="loom-service-abc123",
                labels={"app": "loom-service"},
            ),
            spec=_Spec(
                containers=[
                    _Spec(name="loom-service", image="loom-service:current"),
                ],
            ),
            status=_Spec(
                conditions=[_Spec(type="Ready", status="False")],
                container_statuses=[
                    _Spec(
                        name="loom-service",
                        state=_Spec(
                            waiting=_Spec(reason="CrashLoopBackOff"),
                        ),
                        last_state=_Spec(
                            terminated=_Spec(
                                reason="Error",
                                message="SchemaNotAtHeadError",
                            ),
                        ),
                    ),
                ],
            ),
        ),
    ]

    status = collect_status(apps, _FakeNetworkingV1(), core, "loom", context=None)

    svc = next(c for c in status.components if c.name == "loom-service")
    assert not svc.healthy
    assert svc.note == "pod-health: loom-service-abc123 CrashLoopBackOff"
    assert not status.all_ready


def test_collect_status_classifies_pod_sandbox_deadline_events() -> None:
    """#206 regression: containerd sandbox deadline stalls are
    node-runtime failures, not application CrashLoop/image/config failures.

    The staging failure had both target pods stuck creating their pod
    sandbox and old pods stuck terminating their pod sandbox. The status path
    must inspect events and include old-template terminating pods for this
    failure class; otherwise rollout tooling only sees generic readiness lag.
    """
    apps = _fully_ready_apps()
    apps.deployments["loom-worker"] = _Workload(
        metadata=_Spec(generation=16),
        spec=_Spec(
            replicas=6,
            selector=_Spec(match_labels={"app": "loom-worker"}),
            template=_Spec(
                spec=_Spec(
                    containers=[_Spec(name="worker", image="loom-worker:target")],
                ),
            ),
        ),
        status=_Spec(
            ready_replicas=5,
            available_replicas=5,
            updated_replicas=5,
            replicas=7,
            observed_generation=16,
        ),
    )
    core = _FakeCoreV1(secrets={"loom-secrets"})
    core.pods_by_namespace["loom"] = [
        _Spec(
            metadata=_Spec(
                name="loom-worker-new",
                labels={"app": "loom-worker"},
            ),
            spec=_Spec(
                containers=[_Spec(name="worker", image="loom-worker:target")],
            ),
            status=_Spec(
                phase="Pending",
                conditions=[_Spec(type="Ready", status="False")],
                container_statuses=[
                    _Spec(
                        name="worker",
                        state=_Spec(
                            waiting=_Spec(
                                reason="ContainerCreating",
                                message="waiting for pod sandbox",
                            ),
                        ),
                    ),
                ],
            ),
        ),
        _Spec(
            metadata=_Spec(
                name="loom-worker-old",
                labels={"app": "loom-worker"},
                deletion_timestamp="2026-06-30T16:44:56Z",
            ),
            spec=_Spec(
                containers=[_Spec(name="worker", image="loom-worker:previous")],
            ),
            status=_Spec(phase="Running", container_statuses=[]),
        ),
    ]
    core.events_by_namespace["loom"] = [
        _Spec(
            involved_object=_Spec(name="loom-worker-new"),
            reason="FailedCreatePodSandBox",
            message=(
                "Failed to create pod sandbox: rpc error: code = "
                "DeadlineExceeded desc = context deadline exceeded"
            ),
        ),
        _Spec(
            involved_object=_Spec(name="loom-worker-old"),
            reason="FailedKillPod",
            message=(
                "KillPodSandboxError: rpc error: code = DeadlineExceeded "
                "desc = context deadline exceeded"
            ),
        ),
    ]

    status = collect_status(apps, _FakeNetworkingV1(), core, "loom", context=None)

    worker = next(c for c in status.components if c.name == "loom-worker")
    assert not worker.healthy
    assert worker.failure_class == "node_runtime_sandbox_deadline"
    assert worker.runtime_recovery == "bounded_pod_cleanup_retry"
    assert worker.runtime_failure_diagnostics == [
        {
            "pod": "loom-worker-new",
            "reason": "FailedCreatePodSandBox",
            "operation": "create",
            "target_generation": True,
        },
        {
            "pod": "loom-worker-old",
            "reason": "FailedKillPod",
            "operation": "kill",
            "target_generation": False,
        },
    ]
    assert worker.note is not None
    assert "node-runtime-sandbox-deadline" in worker.note
    assert "loom-worker-new FailedCreatePodSandBox" in worker.note
    assert "loom-worker-old FailedKillPod" in worker.note
    assert not status.all_ready


def test_collect_status_ignores_old_template_pod_failure() -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-service"] = _make_deployment(
        2,
        2,
        generation=14,
        observed_generation=14,
        updated=2,
        image="loom-service:target",
    )
    core = _FakeCoreV1(secrets={"loom-secrets"})
    core.pods_by_namespace["loom"] = [
        _Spec(
            metadata=_Spec(
                name="loom-service-old",
                labels={"app": "loom-service"},
            ),
            spec=_Spec(
                containers=[
                    _Spec(name="loom-service", image="loom-service:previous"),
                ],
            ),
            status=_Spec(
                conditions=[_Spec(type="Ready", status="False")],
                container_statuses=[
                    _Spec(
                        name="loom-service",
                        state=_Spec(
                            waiting=_Spec(reason="CrashLoopBackOff"),
                        ),
                    ),
                ],
            ),
        ),
    ]

    status = collect_status(apps, _FakeNetworkingV1(), core, "loom", context=None)

    svc = next(c for c in status.components if c.name == "loom-service")
    assert svc.healthy
    assert svc.note is None
    assert status.all_ready


def test_collect_status_recovered_pod_last_state_does_not_block_ready() -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-service"] = _make_deployment(
        2,
        2,
        generation=15,
        observed_generation=15,
        updated=2,
    )
    core = _FakeCoreV1(secrets={"loom-secrets"})
    core.pods_by_namespace["loom"] = [
        _Spec(
            metadata=_Spec(
                name="loom-service-recovered",
                labels={"app": "loom-service"},
            ),
            spec=_Spec(
                containers=[
                    _Spec(name="loom-service", image="loom-service:current"),
                ],
            ),
            status=_Spec(
                conditions=[_Spec(type="Ready", status="True")],
                container_statuses=[
                    _Spec(
                        name="loom-service",
                        state=_Spec(running=_Spec(started_at="now")),
                        last_state=_Spec(
                            terminated=_Spec(reason="Error"),
                        ),
                    ),
                ],
            ),
        ),
    ]

    status = collect_status(apps, _FakeNetworkingV1(), core, "loom", context=None)

    svc = next(c for c in status.components if c.name == "loom-service")
    assert svc.healthy
    assert svc.note is None
    assert status.all_ready


def test_collect_status_pod_list_failure_blocks_ready() -> None:
    class _PodListForbiddenCore(_FakeCoreV1):
        def list_namespaced_pod(self, *, namespace: str) -> object:
            raise _FakeApiException(403, "forbidden")

    apps = _fully_ready_apps()
    core = _PodListForbiddenCore(secrets={"loom-secrets"})

    status = collect_status(apps, _FakeNetworkingV1(), core, "loom", context=None)

    assert not status.all_ready
    assert any("cannot inspect managed pods" in warning for warning in status.warnings)
    assert any("k8s 403" in warning for warning in status.warnings)


def test_rendered_image_checks_capture_live_release_evidence() -> None:
    apps = _FakeAppsV1()
    apps.deployments["loom-worker"] = _make_deployment(
        3,
        3,
        image="loom-worker:staging-expected",
    )
    rendered = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-worker
spec:
  template:
    spec:
      containers:
        - name: app
          image: loom-worker:staging-expected
"""

    checks = rendered_image_checks(apps, "loom", rendered)

    assert checks == [
        DeploymentImageCheck(
            deployment="loom-worker",
            container="app",
            expected_image="loom-worker:staging-expected",
            live_image="loom-worker:staging-expected",
        ),
    ]
    assert not checks[0].drifted
    assert checks[0].evidence_line() == (
        "  - loom-worker/app: "
        "rendered=loom-worker:staging-expected "
        "live=loom-worker:staging-expected"
    )


def test_rendered_image_drifts_report_rendered_and_live_tags() -> None:
    apps = _FakeAppsV1()
    apps.deployments["loom-worker"] = _make_deployment(
        3,
        3,
        image="loom-worker:debug-tip",
    )
    rendered = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-worker
spec:
  template:
    spec:
      containers:
        - name: app
          image: loom-worker:staging-expected
"""

    drifts = rendered_image_drifts(apps, "loom", rendered)

    assert drifts == [
        "Deployment loom-worker container app image drift: "
        "rendered loom-worker:staging-expected, live loom-worker:debug-tip",
    ]


def test_collect_status_missing_component_surfaces_as_not_found() -> None:
    """A deployment that hasn't been applied yet appears in the
    output with ready=0/desired=0 and a 'not-found' note. Operators
    expect to see what's missing, not have it silently hidden."""
    apps = _FakeAppsV1()
    # Only loom-service is deployed.
    apps.deployments["loom-service"] = _make_deployment(2, 2)
    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )
    cp = next(c for c in status.components if c.name == "loom-control-plane")
    assert cp.note == "not-found"
    assert cp.ready == 0
    assert cp.desired == 0


def test_collect_status_warns_when_loom_secrets_missing() -> None:
    """The Secret 'loom-secrets' carries DB URLs + minio creds + JWT
    keys. If it's missing, every component fails to start. Surface
    this as a warning so operators don't waste time reading pod
    events one by one."""
    apps = _fully_ready_apps()
    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets=set()),
        "loom",
        context=None,
    )
    assert any("loom-secrets" in w for w in status.warnings)


def test_collect_status_renders_ingress_endpoints_with_tls() -> None:
    """The status output should surface ingress URLs so operators
    immediately know where to point a browser. https:// when the
    host appears in spec.tls; http:// otherwise."""
    ingress = _Spec(
        spec=_Spec(
            tls=[_Spec(hosts=["loom.example.com"])],
            rules=[
                _Spec(host="loom.example.com", http=_Spec(paths=[_Spec(path="/")])),
            ],
        ),
    )
    apps = _fully_ready_apps()
    status = collect_status(
        apps,
        _FakeNetworkingV1([ingress]),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )
    assert len(status.ingresses) == 1
    ep = status.ingresses[0]
    assert ep.host == "loom.example.com"
    assert ep.tls is True
    assert ep.paths == ["/"]


def test_format_table_includes_every_expected_component_row() -> None:
    """The table renderer should produce one row per expected
    component, even when some are missing — operators rely on the
    presence of the row to know what wasn't deployed."""
    apps = _FakeAppsV1()
    apps.deployments["loom-service"] = _make_deployment(1, 1)
    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )
    table = _format_table(status)
    for name in (
        "loom-service",
        "loom-control-plane",
        "loom-llm-gateway",
        "loom-worker",
        "postgres",
        "minio",
    ):
        assert name in table


def test_format_json_is_stable_and_parseable() -> None:
    status = ClusterStatus(
        namespace="loom",
        context="prod",
        components=[
            ComponentStatus(
                name="loom-service",
                kind="Deployment",
                ready=2,
                desired=2,
                available=True,
                generation=1,
                observed_generation=1,
                updated=2,
            )
        ],
        ingresses=[
            IngressEndpoint(
                host="loom.example.com",
                paths=["/"],
                tls=True,
            )
        ],
        warnings=["sample warning"],
    )
    body = _format_json(status)
    parsed = json.loads(body)
    assert parsed["namespace"] == "loom"
    assert parsed["all_ready"] is True
    assert parsed["components"][0]["healthy"] is True
    assert parsed["components"][0]["generation"] == 1
    assert parsed["components"][0]["observed_generation"] == 1
    assert parsed["components"][0]["updated"] == 2
    assert parsed["ingresses"][0]["tls"] is True
    assert parsed["warnings"] == ["sample warning"]
    assert parsed["blocking_warnings"] == []


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch
# ──────────────────────────────────────────────────────────────────────


def test_cli_status_returns_2_when_kubernetes_lib_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Lazy-import path: if the kubernetes package isn't installed,
    the CLI surfaces a friendly install hint and exits 2."""

    def _raise_module_not_found(_: str | None) -> object:
        raise RuntimeError(
            "the 'kubernetes' package is required for `loom cluster` "
            "commands. install it with `pip install loom[cluster]`.",
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        _raise_module_not_found,
    )
    rc = main(["cluster", "status"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "pip install loom[cluster]" in err


def test_cli_status_returns_2_when_cluster_unreachable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A kubeconfig error → exit 2 with a `cannot connect to
    cluster` message."""

    def _raise(_: str | None) -> object:
        raise OSError("kubeconfig not found")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _raise)
    rc = main(["cluster", "status"])
    assert rc == 2
    assert "cannot connect to cluster" in capsys.readouterr().err


def test_cli_status_returns_0_when_all_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (
            apps,
            _FakeNetworkingV1(),
            _FakeCoreV1(secrets={"loom-secrets"}),
            _FakeStorageV1(),
        ),
    )
    rc = main(["cluster", "status"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "loom-service" in out
    assert "ready" in out


def test_cli_status_returns_1_when_a_component_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Exit code 1 specifically when the cluster is reachable but
    not fully ready — distinguishes 'cluster down' (2) from
    'cluster present but broken' (1) for CI scripts."""
    apps = _fully_ready_apps()
    # Postgres down. Keyed by the k8s resource name (`loom-postgres`),
    # not the display name (`postgres`).
    apps.statefulsets["loom-postgres"] = _make_statefulset(0, 1)
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (
            apps,
            _FakeNetworkingV1(),
            _FakeCoreV1(secrets={"loom-secrets"}),
            _FakeStorageV1(),
        ),
    )
    rc = main(["cluster", "status"])
    assert rc == 1
    out = capsys.readouterr().out
    assert "not-ready" in out


def test_cli_status_json_format(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    apps = _fully_ready_apps()
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (
            apps,
            _FakeNetworkingV1(),
            _FakeCoreV1(secrets={"loom-secrets"}),
            _FakeStorageV1(),
        ),
    )
    rc = main(["cluster", "status", "--format", "json"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed["all_ready"] is True


def test_cli_status_namespace_flag_passed_to_collector(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """`--namespace` should flow through to the collector so a
    multi-tenant cluster can be queried for different deployments."""
    captured_ns: dict[str, str] = {}

    class _CapturingApps:
        def read_namespaced_deployment(self, *, name: str, namespace: str) -> Any:
            captured_ns["ns"] = namespace
            raise _FakeApiException(404)

        def read_namespaced_daemon_set(self, *, name: str, namespace: str) -> Any:
            raise _FakeApiException(404)

        def read_namespaced_stateful_set(self, *, name: str, namespace: str) -> Any:
            raise _FakeApiException(404)

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (
            _CapturingApps(),
            _FakeNetworkingV1(),
            _FakeCoreV1(secrets=set()),
            _FakeStorageV1(),
        ),
    )
    main(["cluster", "status", "--namespace", "loom-stage"])
    assert captured_ns["ns"] == "loom-stage"


def test_collect_status_looks_up_statefulsets_by_k8s_name_not_display_name() -> None:
    """Regression: the StatefulSet entries in _COMPONENT_STATEFULSETS
    are `(display_name, k8s_resource_name)` tuples with the two
    fields DIFFERING for postgres + minio. The status API lookup
    MUST use the k8s resource name (`loom-postgres`), not the
    display name (`postgres`). A fake k8s populated only with the
    real resource name + a status snapshot reporting `1/1 ready`
    proves the lookup uses the right key.

    Caught by staging-smoke run #7 — earlier the lookup used the
    wrong field and every postgres/minio status was `not-found`
    even though the StatefulSet was actually running.
    """
    from loom_cli.cluster_cmd import collect_status

    apps = _FakeAppsV1()
    apps.deployments["loom-service"] = _make_deployment(2, 2)
    apps.deployments["loom-control-plane"] = _make_deployment(2, 2)
    apps.deployments["loom-llm-gateway"] = _make_deployment(2, 2)
    apps.deployments["loom-web"] = _make_deployment(2, 2)
    apps.deployments["loom-worker"] = _make_deployment(3, 3)
    # ONLY the k8s-name key; the display name is NOT present.
    apps.statefulsets["loom-postgres"] = _make_statefulset(1, 1)
    apps.statefulsets["loom-minio"] = _make_statefulset(1, 1)
    status = collect_status(
        apps,
        _FakeNetworkingV1(),
        _FakeCoreV1(secrets={"loom-secrets"}),
        "loom",
        context=None,
    )
    by_display = {c.name: c for c in status.components}
    assert by_display["postgres"].kind == "StatefulSet"
    assert by_display["postgres"].ready == 1
    assert by_display["postgres"].note is None
    assert by_display["minio"].ready == 1
    assert by_display["minio"].note is None
