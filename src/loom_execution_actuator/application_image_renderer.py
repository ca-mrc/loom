"""Fixed personal service/web adapter over the shared isolated native Job."""
from __future__ import annotations

from typing import Any

from loom.application_image_build import ApplicationImageBuildClaimV1, application_image_components
from loom_execution_actuator.renderer import ExecutionTargetRuntime
from loom_execution_actuator.task_image_renderer import TaskImageJobConfig, render_native_image_job


def render_application_image_job(*, claim: ApplicationImageBuildClaimV1, target: ExecutionTargetRuntime,
                                  config: TaskImageJobConfig) -> tuple[dict[str, Any], dict[str, Any]]:
    claim = ApplicationImageBuildClaimV1.model_validate_json(claim.model_dump_json())
    recipe = claim.recipe
    if ((config.service_image, config.buildkit_image, config.snapshotter, config.export_cache_mode, config.oci_export_format) != (
            recipe.trusted_image_ref, recipe.buildkit_image_ref, recipe.snapshotter, recipe.export_cache_mode, recipe.oci_export_format)
            or (config.cache_secret_name is None) != (claim.cache_bucket is None)):
        raise ValueError("application image recipe differs from protected Job settings")
    components = application_image_components()
    arguments = {
        # A dirty/untracked snapshot is never presented as its base Git commit.
        "LOOM_BUILD_SHA": "unknown", "LOOM_SOURCE_REF": "personal", "LOOM_BUILD_KIND": "personal",
        "LOOM_SOURCE_DIGEST": claim.source.source_digest,
        "LOOM_SOURCE_BASE_COMMIT": claim.source.base_commit or "unknown",
    }
    return render_native_image_job(workload="application", work_id=claim.build_id, attempt=claim.attempt,
        claim={**claim.model_dump(mode="json"), "cpu_arch": recipe.cpu_arch},
        components=components, target=target, config=config,
        component_build_args={row.name: dict(arguments) for row in components})
