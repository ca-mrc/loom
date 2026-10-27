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


@pytest.mark.parametrize('base', [
    'python:3.13-slim-bookworm', 'docker.io/library/ubuntu:24.04',
    'oven/bun:1.2.15-debian', 'eclipse-temurin:17-jdk-jammy',
    'mcr.microsoft.com/playwright/python:v1.52.0-noble',
    'mambaorg/micromamba:1.5', 'coqorg/coq:8.18',
    'condaforge/miniforge3:24.9.2-0', 'vllm/vllm-openai-cpu:v0.21.0',
    'postgres:16',
])
def test_native_debian_family_preserves_pinned_reference(base):
    reference = base + '@sha256:' + 'a' * 64
    actual = classify_terminus_image(f'FROM {reference}\n')
    assert actual.base_image == reference
    assert actual.package_manager == 'apt'


@pytest.mark.parametrize('base', ['evil.example/ubuntu:24.04', 'python:3.13-alpine',
                                  'nvidia/cuda:12.6.3-base-centos7', 'ubuntu:24.04@sha256:bad'])
def test_registry_and_distribution_are_not_inferred_from_suffix(base):
    with pytest.raises(TaskImagePreparationError):
        classify_terminus_image(f'FROM {base}\n')


def test_fedora_has_its_own_package_manager():
    assert classify_terminus_image('FROM fedora:42\n').package_manager == 'dnf'
