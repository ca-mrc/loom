"""Read only retained native authority, independent of current profile availability."""
from __future__ import annotations

import json

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.application_image_build import application_image_components
from loom.db.nebius_pool_schema import NebiusPoolEffect
from loom.nebius_pool_application_image import PoolApplicationImagePrepareV1
from loom.nebius_pool_contract import PoolNamespaceBindingV1, PoolRequestActionV1
from loom.nebius_pool_native_runtime import PoolNativeRuntimeV1
from loom.nebius_pool_task_image import PoolTaskImagePrepareV1
from loom_service.pool_management.auth import PoolPrincipal
from loom_service.pool_management.capacity import digest
from loom_service.pool_management.control import PoolControlError, _locked_request
from loom_service.pool_management.registry import _receipt


async def native_build_runtime(session: AsyncSession, principal: PoolPrincipal, action: PoolRequestActionV1) -> PoolNativeRuntimeV1:
    async with _locked_request(session, principal, action) as (row, _):
        if (row is None or row.workload_kind not in {"task_image_build", "application_image_build"} or row.plan_json is None
                or digest(row.plan_json) != row.plan_sha256):
            raise PoolControlError
        plan = row.plan_json
        job, configmap = plan["job"], plan["configmap"]
        claim = json.loads(configmap["data"]["claim.json"])
        if plan["request_sha256"] != row.request_sha256:
            raise PoolControlError
        if row.workload_kind == "application_image_build":
            application = PoolApplicationImagePrepareV1.model_validate(row.request_json)
            epoch = application.build.attempt
            components = [value.model_dump(mode="json") for value in application_image_components()]
            if application.build.recipe.oci_export_format == "directory":
                for component in components:
                    component["oci_output_path"] = component["oci_output_path"].removesuffix(".tar")
            expected = {**application.build.model_dump(mode="json"), "components": components,
                "cpu_arch": application.build.recipe.cpu_arch}
            if (claim != expected or application.build.build_id != row.local_work_id or epoch != row.generation
                    or job["metadata"]["labels"]["loom.application-build-id"] != str(row.local_work_id)
                    or job["metadata"]["labels"]["loom.build-attempt"] != str(epoch)):
                raise PoolControlError
        else:
            request = PoolTaskImagePrepareV1.model_validate(row.request_json)
            epoch = request.build.expected_lease_epoch + 1
            if (claim["id"] != str(row.local_work_id)
                or claim["lease_epoch"] != epoch or claim["materialization_key"] != request.build.materialization_key
                or job["metadata"]["labels"]["loom.materialization-id"] != str(row.local_work_id)
                or job["metadata"]["labels"]["loom.lease-epoch"] != str(epoch)):
                raise PoolControlError
        effect_id = None
        if row.job_uid is not None:
            effect = await session.scalar(select(NebiusPoolEffect).where(
                NebiusPoolEffect.request_id == row.request_id, NebiusPoolEffect.effect_key == "create:job"))
            if (effect is None or effect.phase != "observed" or effect.observed_uid != row.job_uid
                    or effect.plan_sha256 != row.plan_sha256 or effect.namespace_uid != row.namespace_uid):
                raise PoolControlError
            effect_id = effect.effect_id
        return PoolNativeRuntimeV1(receipt=_receipt(row), target_id=row.target_id,
            namespace=PoolNamespaceBindingV1(name=job["metadata"]["namespace"], uid=row.namespace_uid),
            job_name=job["metadata"]["name"], lease_epoch=epoch, deadline_at=row.deadline_at,
            registry_repository=claim["registry_repository"], job_effect_id=effect_id)
