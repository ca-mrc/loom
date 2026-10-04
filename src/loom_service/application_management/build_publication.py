"""Read exact publisher evidence, never logs or owner-supplied image references."""
from __future__ import annotations

from typing import Any, Literal

from loom.application_image_build import ApplicationImageBuildClaimV1, ApplicationImagePublicationV1
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom_execution_actuator.pool_native_observation import qualify_native_observation


def qualify_publication(claim: ApplicationImageBuildClaimV1, publication: ApplicationImagePublicationV1) -> None:
    """Bind trusted publisher output to the retained attempt, including on replay."""
    for field in ("build_id", "attempt", "upload_id", "installation_id", "owner_user_id", "owner_team_id",
                  "data_environment_id", "cluster_id"):
        if getattr(publication, field) != getattr(claim, field):
            raise ValueError("application_build_publication_conflict")
    if ((publication.source_digest, publication.recipe_digest, publication.schema_revision, publication.cpu_arch) != (
            claim.source.source_digest, claim.recipe.digest, claim.recipe.schema_revision, claim.recipe.cpu_arch)
            or any(ref.split("@", 1)[0] != claim.registry_repository for ref in publication.registry_images.values())):
        raise ValueError("application_build_publication_conflict")


def observed_publication(request: PoolApplicationImagePrepareV1, runtime: PoolNativeRuntimeV1,
                         observed: dict[str, Any]) -> tuple[Literal["pending", "ready", "failed"], ApplicationImagePublicationV1 | None]:
    # A foreign Job is not a failed build: refuse the observation entirely.
    qualify_native_observation(observed, runtime)
    try:
        status = observed.get("status", {})
        conditions = {row["type"] for row in status.get("conditions", []) if row.get("status") == "True"}
        pods = observed.get("pods", [])
        if "Failed" in conditions or any(pod.get("status", {}).get("phase") == "Failed" for pod in pods):
            return "failed", None
        if "Complete" not in conditions and status.get("succeeded", 0) == 0:
            return "pending", None
        if ("Complete" not in conditions or type(status.get("succeeded")) is not int
                or status["succeeded"] != 1 or len(pods) != 1 or pods[0]["status"]["phase"] != "Succeeded"):
            raise ValueError
        pod_status = pods[0]["status"]

        def completed(rows: Any, names: set[str]) -> dict[str, Any]:
            if not isinstance(rows, list) or len(rows) != len(names) or {row["name"] for row in rows} != names:
                raise ValueError
            result = {}
            for row in rows:
                state = row["state"]
                if (type(row["restartCount"]) is not int or row["restartCount"] != 0
                        or set(state) != {"terminated"} or type(state["terminated"]["exitCode"]) is not int
                        or state["terminated"]["exitCode"] != 0):
                    raise ValueError
                result[row["name"]] = state["terminated"]
            return result

        completed(pod_status["initContainerStatuses"], {"prepare", "build"})
        publisher = completed(pod_status["containerStatuses"], {"publish"})["publish"]
        message = publisher["message"]
        if not isinstance(message, str) or len(message.encode()) > 4096:
            raise ValueError
        publication = ApplicationImagePublicationV1.model_validate_json(message)
        qualify_publication(request.build, publication)
        return "ready", publication
    except (ValueError, KeyError, TypeError, AttributeError):
        return "failed", None
