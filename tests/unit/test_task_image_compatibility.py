import pytest

from loom.task_image_compatibility import classify_terminus_image
from loom.task_image_errors import TaskImagePreparationError


@pytest.mark.parametrize("base,manager", [
    ("opensuse/leap:15.5", "zypper"), ("ubuntu:22.04", "apt"),
    ("alpine:3.20", "apk"), ("archlinux:base", "pacman"),
])
def test_supported_final_base_and_stage_alias(base, manager):
    actual = classify_terminus_image(f"FROM {base} AS authored\nFROM authored\n")
    assert (actual.base_image, actual.package_manager) == (base, manager)


@pytest.mark.parametrize("source,code", [
    ("FROM opensuse/leap:15.6\n", "image_preparation_base_unsupported"),
    ("FROM opensuse/tumbleweed:latest\n", "image_preparation_base_unsupported"),
    ("FROM scratch\n", "image_preparation_base_unsupported"),
    ("FROM ${BASE}\n", "image_preparation_base_unsupported"),
    ("FROM ubuntu:22.04 AS a\nFROM a AS a\n", "image_preparation_dockerfile_invalid"),
])
def test_unsupported_or_malformed_input_has_deterministic_code(source, code):
    with pytest.raises(TaskImagePreparationError) as caught:
        classify_terminus_image(source)
    assert caught.value.code == code
    assert caught.value.retryable is False
