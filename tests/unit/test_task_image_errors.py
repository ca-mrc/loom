import pytest
from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ParamValidationError,
    ReadTimeoutError,
)

from loom.task_image_errors import TaskImagePreparationError, preparation_failure_from_receipt
from loom_execution_actuator.task_image_runtime import classify_preparation_failure


@pytest.mark.parametrize("error,code,retryable", [
    (EndpointConnectionError(endpoint_url="https://private.invalid"), "image_preparation_source_unavailable", True),
    (ReadTimeoutError(endpoint_url="https://private.invalid"), "image_preparation_source_unavailable", True),
    (ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject"), "image_preparation_source_missing", False),
    (ClientError({"Error": {"Code": "AccessDenied"}}, "GetObject"), "image_preparation_source_denied", False),
    (ClientError({"Error": {"Code": "SlowDown"}}, "GetObject"), "image_preparation_source_unavailable", True),
    (ParamValidationError(report="private-input"), "image_preparation_internal_error", False),
    (RuntimeError("connection timeout unsupported base"), "image_preparation_internal_error", False),
])
def test_typed_preparation_classification_never_reads_prose(error, code, retryable):
    classified = classify_preparation_failure(error)
    assert classified.code == code
    assert classified.retryable is retryable
    assert "private" not in str(classified)
    assert preparation_failure_from_receipt(classified.receipt()) == (code, str(classified), retryable)


def test_preparation_receipt_cannot_grant_retry_authority():
    receipt = TaskImagePreparationError("image_preparation_base_unsupported", "Unsupported base").receipt()
    receipt["retryable"] = True
    assert preparation_failure_from_receipt(receipt) is None


def test_only_structured_package_guard_diagnostic_can_classify_build_failure():
    from loom.task_image_errors import package_guard_failure

    receipt = TaskImagePreparationError('image_preparation_package_change_required', 'guard').receipt()
    import json
    frame = 'LOOM_IMAGE_PREPARATION_ERROR=' + json.dumps(receipt)
    assert package_guard_failure('#6 1.23 ' + frame)[0] == 'image_preparation_package_change_required'
    assert package_guard_failure('would upgrade authored packages') is None
    assert package_guard_failure('RUN echo ' + frame) is None
    receipt['retryable'] = True
    assert package_guard_failure('LOOM_IMAGE_PREPARATION_ERROR=' + json.dumps(receipt)) is None
