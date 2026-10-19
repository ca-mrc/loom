"""Admission checks actual frozen Dockerfile bytes, not a claimed base name."""
import hashlib
import io
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError, ParamValidationError, ReadTimeoutError

from loom.models.task import TaskConfig
from loom.service_execution_materialization import ServiceExecutionInputManifestV1
from loom_service.task_image_admission import task_image_preparation_rejection
from tests.unit.test_guest_execution_materialization import _guest_inputs


def inputs(dockerfile=b"FROM opensuse/leap:15.5\n"):
    task, _, _ = _guest_inputs()
    task = TaskConfig.model_validate({**task.model_dump(mode="json"), "environment": {
        **task.environment.model_dump(mode="json"), "docker_image": None,
        "dockerfile": "environment/Dockerfile",
    }})
    manifest = ServiceExecutionInputManifestV1(task_revision_sha256="sha256:" + "a" * 64,
        files=[dict(relative_path="environment/Dockerfile", mode="0644", size_bytes=len(dockerfile),
                    sha256="sha256:" + hashlib.sha256(dockerfile).hexdigest())])
    payload = manifest.canonical_bytes()
    provenance = {"service_execution_input": dict(manifest_uri="s3://artifacts/inputs/task.json",
        manifest_sha256="sha256:" + hashlib.sha256(payload).hexdigest(), file_count=1, total_bytes=len(dockerfile))}
    objects = {"inputs/task.json": payload, "inputs/task/environment/Dockerfile": dockerfile}
    streams = []
    def get_object(**kwargs):
        assert kwargs["Bucket"] == "artifacts"
        value = objects[kwargs["Key"]]
        body = io.BytesIO(value)
        streams.append(body)
        return dict(Body=body, ContentLength=len(value))
    client = SimpleNamespace(get_object=Mock(side_effect=get_object))
    return task, provenance, objects, client, streams


@pytest.mark.parametrize("base,expected", [
    ("opensuse/leap:15.5", None), ("debian:bookworm", None),
    ("opensuse/leap:15.6", "image_preparation_base_unsupported"),
    ("centos:7", "image_preparation_base_unsupported"),
])
def test_shared_classifier_checks_frozen_primary_dockerfile(base, expected):
    task, provenance, _, client, streams = inputs(f"FROM {base}\n".encode())
    assert task_image_preparation_rejection(client, "artifacts", task, "s3://artifacts/inputs/task/",
                                            provenance, "a" * 64) == expected
    assert all(body.closed for body in streams)


@pytest.mark.parametrize("change", ["manifest", "dockerfile", "revision"])
def test_changed_registered_inputs_fail_closed(change):
    task, provenance, objects, client, _ = inputs()
    checksum = "a" * 64
    if change == "manifest":
        objects["inputs/task.json"] += b" "
    elif change == "dockerfile":
        objects["inputs/task/environment/Dockerfile"] = b"FROM debian:bookworm\n"
    else:
        checksum = "b" * 64
    assert task_image_preparation_rejection(client, "artifacts", task, "s3://artifacts/inputs/task/",
                                            provenance, checksum) == "image_preparation_input_invalid"


@pytest.mark.parametrize("code,expected", [
    ("NoSuchKey", "image_preparation_source_missing"),
    ("AccessDenied", "image_preparation_source_denied"),
    ("SlowDown", "image_preparation_source_unavailable"),
    ("InvalidAccessKeyId", "image_preparation_source_denied"),
    ("SignatureDoesNotMatch", "image_preparation_source_denied"),
    ("NotFound", "image_preparation_source_missing"),
    ("UnexpectedError", "image_preparation_internal_error"),
])
def test_storage_failure_categories(code, expected):
    task, provenance, _, client, _ = inputs()
    client.get_object.side_effect = ClientError({"Error": {"Code": code}}, "GetObject")
    assert task_image_preparation_rejection(client, "artifacts", task, "s3://artifacts/inputs/task/",
                                            provenance, "a" * 64) == expected


def test_network_timeout_is_temporary_without_exposing_sdk_details():
    task, provenance, _, client, _ = inputs()
    client.get_object.side_effect = ReadTimeoutError(endpoint_url="https://private.invalid")
    assert task_image_preparation_rejection(client, "artifacts", task, "s3://artifacts/inputs/task/",
                                            provenance, "a" * 64) == "image_preparation_source_unavailable"


def test_foreign_bucket_not_read():
    task, provenance, _, client, _ = inputs()
    assert task_image_preparation_rejection(client, "artifacts", task, "s3://foreign/inputs/task/",
                                            provenance, "a" * 64) == "image_preparation_input_invalid"
    client.get_object.assert_not_called()


def test_sdk_parameter_error_is_not_a_temporary_storage_outage():
    task, provenance, _, client, _ = inputs()
    client.get_object.side_effect = ParamValidationError(report="private input detail")
    assert task_image_preparation_rejection(client, "artifacts", task, "s3://artifacts/inputs/task/",
                                            provenance, "a" * 64) == "image_preparation_internal_error"
