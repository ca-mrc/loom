"""Readable application version and schema diagnostics, without readiness claims."""
from __future__ import annotations

from loom.nebius_application_contract import ApplicationReleaseV1
from loom.nebius_application_versions import (
    ApplicationReleaseCompatibilityV1,
    ApplicationVersionsV1,
)


def _release(label: str, release: ApplicationReleaseV1) -> None:
    print(f"{label}: {release.release_id}")
    print(f"  Source: {release.source_digest}")
    print(f"  API image: {release.service_image_ref}")
    print(f"  Frontend image: {release.web_image_ref}")
    print(f"  Schema revision: {release.schema_revision}")


def print_release_compatibility(report: ApplicationReleaseCompatibilityV1) -> None:
    _release("Release", report.release)
    print(f"configured shared schema: {report.shared_schema_revision}")
    print(f"Schema compatibility: {report.compatibility}")
    if report.compatibility == "schema_mismatch":
        print("Use a release matching the shared schema, or a disposable local database for migration experiments. "
              "Shared migrations require the platform operator.")
    print("Checks configured schema compatibility only; live database state, deployment and task execution are not checked.")


def print_application_versions(report: ApplicationVersionsV1) -> None:
    row, operation = report.status.registration, report.status.operation
    print(f"Application: {row.slug} ({row.application_id})")
    print(f"Desired state: {row.desired_state}; generation {row.deployment_generation}; "
          f"operation {operation.phase if operation is not None else 'unavailable'}")
    _release("Requested release", report.requested_release)
    previous = report.last_completed_deployment
    if previous is None:
        print("Last completed deployment: none recorded")
    else:
        print(f"Last completed deployment: generation {previous.deployment_generation} at {previous.completed_at.isoformat()}")
        _release("  Recorded release", previous.release)
    print(f"Configured shared schema: {report.shared_schema_revision}; {report.schema_compatibility}")
    print("Reports the deployment journal, not a live readiness check. "
          "Personal releases change frontend/API code; shared controllers and task images have separate versions.")
    print("Use loom eval trial show TRIAL_ID in the application context to inspect that trial's execution images.")
