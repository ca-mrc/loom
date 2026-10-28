import logging
import re

from prometheus_client import Counter, Gauge, Histogram

KUBERNETES_API_SECONDS = Histogram(
    "loom_execution_actuator_kubernetes_api_seconds",
    "Kubernetes API latency by bounded operation.",
    labelnames=("operation",),
)
KUBERNETES_API_ERRORS_TOTAL = Counter(
    "loom_execution_actuator_kubernetes_api_errors_total",
    "Kubernetes API errors by bounded operation/status class.",
    labelnames=("operation", "status_class"),
)
KUBERNETES_PENDING_TOTAL = Counter(
    "loom_execution_actuator_pending_reasons_total",
    "Observed pending/failure reasons by normalized bounded reason.",
    labelnames=("reason",),
)
KUBERNETES_ORPHAN_COUNT = Gauge(
    "loom_execution_actuator_orphan_count",
    "Managed-scope Jobs without exactly one matching durable lease.",
)
KUBERNETES_CLEANUP_RETRIES_TOTAL = Counter(
    "loom_execution_actuator_cleanup_retries_total",
    "Cleanup API retries by bounded cause.",
    labelnames=("cause",),
)
KUBERNETES_RECONCILE_CONVERGED = Gauge(
    "loom_execution_actuator_reconcile_converged",
    "Whether the last full reconciliation found no drift.",
)
KUBERNETES_WATCH_RESTARTS_TOTAL = Counter(
    "loom_execution_actuator_watch_restarts_total",
    "Watch restarts by bounded outcome.",
    labelnames=("outcome",),
)

RESOURCE_USAGE_ERRORS_TOTAL = Counter(
    "loom_execution_actuator_resource_usage_errors_total",
    "Non-fatal native telemetry errors by bounded collection/parse/persist operation.",
    labelnames=("operation",),
)

# Native build loop attribution (#2625). Phase time excludes the poll sleep.
TASK_IMAGE_BUILD_CYCLE_SECONDS = Histogram(
    "loom_execution_actuator_task_image_build_cycle_seconds",
    "Native build loop time by controller and bounded phase.",
    labelnames=("controller", "phase"),
)
TASK_IMAGE_BUILD_OUTSTANDING = Gauge(
    "loom_execution_actuator_task_image_build_outstanding",
    "Builds reconciled before the last admission opportunity.",
    labelnames=("controller",),
)
TASK_IMAGE_BUILD_ADMISSIONS_TOTAL = Counter(
    "loom_execution_actuator_task_image_build_admissions_total",
    "Admission opportunities by controller and bounded outcome or blocked reason.",
    labelnames=("controller", "outcome"),
)

_LOG = logging.getLogger(__name__)
_REASON = re.compile(r"[a-z][a-z0-9_]{0,79}")


def blocked_outcome(reason: object) -> str:
    """Blocked reasons are code identifiers; anything else stays one label."""
    return reason if isinstance(reason, str) and _REASON.fullmatch(reason) else "blocked_other"


def record_build_cycle(controller: str, *, outstanding: int, reconcile_seconds: float,
                       outcome: str, admission_seconds: float) -> None:
    TASK_IMAGE_BUILD_OUTSTANDING.labels(controller).set(outstanding)
    TASK_IMAGE_BUILD_CYCLE_SECONDS.labels(controller, "reconcile").observe(reconcile_seconds)
    TASK_IMAGE_BUILD_CYCLE_SECONDS.labels(controller, "admission").observe(admission_seconds)
    TASK_IMAGE_BUILD_ADMISSIONS_TOTAL.labels(controller, outcome).inc()
    # Idle cycles repeat every poll; keep logs to cycles that carry work.
    if outstanding or outcome == "claimed":
        _LOG.info("Native build cycle controller=%s outstanding=%d reconcile_s=%.3f admission=%s admission_s=%.3f",
                  controller, outstanding, reconcile_seconds, outcome, admission_seconds)
