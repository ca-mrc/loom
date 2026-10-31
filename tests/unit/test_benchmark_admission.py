"""Neutral contracts and lifecycle decisions without optional adapter SDKs."""

from copy import deepcopy

import pytest
from pydantic import ValidationError

from loom.benchmark_admission import (
    BenchmarkAdmissionError,
    publication_execution_state,
    snapshot_profile,
    snapshot_task,
    trusted_adapter_contract,
)
from loom.db.schema import Benchmark, Task
from loom.models.benchmark_profile import BenchmarkProfileSpec


def descriptor(**changes):
    values = dict(
        id="arbitrary-new-suite@release-7",
        origin=dict(kind="json", locator="https://example.test/source.json",
                    revision="sha256:" + "a" * 64, release="7", importer="loom.json.v1"),
        expected_task_count=2, selected_task_ids=("one", "two"),
    )
    values.update(changes)
    return values


def test_neutral_descriptor_does_not_require_harbor_or_benchmark_name_registration():
    spec = BenchmarkProfileSpec(**descriptor())
    assert spec.origin.kind == "json"
    assert spec.validation_contract == "canonical.v1"


@pytest.mark.parametrize("changes", [
    {"selected_task_ids": ("one", "one")},
    {"selected_task_ids": ("one",)},
    {"selected_task_ids": ("one", "two", "three")},
    {"selected_task_ids": ("../one", "two")},
    {"verified": True},
    {"validation_code": "pass"},
])
def test_exact_census_and_data_only_contract(changes):
    with pytest.raises(ValidationError):
        BenchmarkProfileSpec(**descriptor(**changes))


def test_subset_selection_binds_the_exact_selected_names():
    values = descriptor(selection="subset", selected_task_ids=("two",))
    values["origin"]["subset"] = ["one"]
    with pytest.raises(ValidationError, match="exact source subset"):
        BenchmarkProfileSpec(**values)
    values["origin"]["subset"] = ["two"]
    assert BenchmarkProfileSpec(**values).selected_task_ids == ("two",)


@pytest.mark.parametrize("previous,changed,expected", [
    (None, False, "pending"), (None, True, "pending"),
    ("pending", False, "pending"), ("runnable", False, "runnable"),
    ("runnable", True, "pending"),
])
def test_publication_is_pending_until_shared_admission(previous, changed, expected):
    assert publication_execution_state(previous, execution_changed=changed) == expected


def test_historical_profiles_cannot_be_republished_even_identically():
    with pytest.raises(BenchmarkAdmissionError, match="profile_historical"):
        publication_execution_state("historical", execution_changed=False)


def test_unknown_required_adapter_cannot_fall_back_to_canonical_checks():
    profile = Benchmark(id="arbitrary-new-suite", profile_provenance={"validation_contract": "unknown.v1"})
    with pytest.raises(BenchmarkAdmissionError, match="validation_contract_unknown"):
        trusted_adapter_contract(profile)


def test_observed_adapter_facts_freeze_nested_config_and_provenance():
    profile = Benchmark(id="sample", upstream_kind="json", upstream_locator="source",
                        upstream_revision="7", profile_provenance={"required": {"private": True}})
    row = Task(id="sample/one", benchmark_id="sample", checksum="a" * 64,
               config={"task": {"id": "sample/one"}}, source=None, source_provenance={})
    profile_facts, task_facts = snapshot_profile(profile), snapshot_task(row)
    original_profile, original_task = deepcopy(profile_facts), deepcopy(task_facts)
    profile.profile_provenance["required"]["private"] = False
    row.config["task"]["id"] = "sample/two"
    assert profile_facts == original_profile
    assert task_facts == original_task


@pytest.mark.parametrize("provenance", [
    ["untrusted"], {"upstream_origin": "untrusted"},
    {"upstream_origin": {"importer": "loom.harbor-native.v1"},
     "validation_contract": "unreviewed.v2"},
])
def test_malformed_or_unknown_required_profile_contract_fails_closed(provenance):
    profile = Benchmark(id="arbitrary-new-suite", profile_provenance=provenance)
    with pytest.raises(BenchmarkAdmissionError) as failure:
        trusted_adapter_contract(profile)
    assert failure.value.findings[0].code in {"profile_metadata_invalid", "validation_contract_unknown"}


def test_invalid_descriptor_diagnostics_do_not_echo_embedded_credentials():
    values = descriptor()
    values["origin"]["locator"] = "https://operator:private-key@example.test/tasks.json"
    with pytest.raises(ValidationError) as failure:
        BenchmarkProfileSpec(**values)
    assert "private-key" not in str(failure.value)


@pytest.mark.parametrize("kind", ["git", "huggingface"])
@pytest.mark.parametrize("revision", ["dev", "v1", "release/next", "a" * 39, "A" * 40])
def test_git_and_huggingface_origins_require_resolved_commit_identity(kind, revision):
    from loom.models.benchmark_profile import ProfileOrigin

    with pytest.raises(ValidationError, match="resolved 40-character commit"):
        ProfileOrigin(kind=kind, locator="https://example.test/tasks", revision=revision,
                      release="1", importer="loom.canonical.v1")


@pytest.mark.parametrize("kind", ["git", "huggingface"])
def test_neutral_git_and_huggingface_origins_accept_pinned_commits(kind):
    from loom.models.benchmark_profile import ProfileOrigin

    origin = ProfileOrigin(kind=kind, locator="https://example.test/tasks", revision="a" * 40,
                           release="1", importer="loom.canonical.v1")
    assert origin.revision == "a" * 40


def test_canonical_legacy_source_policy_still_checks_profile_origin_and_census():
    from loom.benchmark_admission_contracts import CANONICAL, validate_profile_contract

    spec = BenchmarkProfileSpec(**descriptor())
    benchmark = Benchmark(id=spec.id, profile_provenance={
        "upstream_origin": spec.origin.model_dump(mode="json"), "source_task_count": 3,
    })
    assert CANONICAL.allow_legacy_source
    with pytest.raises(BenchmarkAdmissionError, match="profile_census_drift"):
        validate_profile_contract(CANONICAL, benchmark, spec)
    benchmark.profile_provenance = {"upstream_origin": {
        **spec.origin.model_dump(mode="json"), "release": "other-release",
    }}
    with pytest.raises(BenchmarkAdmissionError, match="profile_origin_drift"):
        validate_profile_contract(CANONICAL, benchmark, spec)


@pytest.mark.parametrize("kind", ["s3-folder", "local-folder"])
def test_reviewed_local_source_kinds_can_use_content_identity_without_upstream_revision(kind):
    from loom.models.benchmark_profile import ProfileOrigin

    origin = ProfileOrigin(kind=kind, locator="s3://fixtures/catalog/", revision="",
                           release="local", importer="loom.local.v1")
    assert origin.revision == ""


@pytest.mark.parametrize("kind", ["json", "harbor-package", "other-format", "fixture"])
def test_nonlocal_source_kinds_cannot_omit_revision(kind):
    from loom.models.benchmark_profile import ProfileOrigin

    with pytest.raises(ValidationError, match="explicit revision"):
        ProfileOrigin(kind=kind, locator="https://example.test/tasks", revision="",
                      release="1", importer="loom.adapter.v1")


def test_explicit_catalog_mapping_can_preserve_a_local_root_task_identity():
    values = descriptor(expected_task_count=1, selected_task_ids=("root",),
                        catalog_task_ids=("arbitrary-new-suite@release-7",))
    spec = BenchmarkProfileSpec(**values)
    assert spec.resolved_catalog_task_ids == (spec.id,)
    assert BenchmarkProfileSpec(**descriptor()).resolved_catalog_task_ids == (
        spec.id + "/one", spec.id + "/two",
    )


@pytest.mark.parametrize("mapping", [
    (), ("arbitrary-new-suite@release-7/one",),
    ("arbitrary-new-suite@release-7/one", "arbitrary-new-suite@release-7/one"),
    ("another-profile/one", "arbitrary-new-suite@release-7/two"),
    ("arbitrary-new-suite@release-7/../one", "arbitrary-new-suite@release-7/two"),
])
def test_catalog_mapping_cannot_change_census_or_cross_physical_identity(mapping):
    with pytest.raises(ValidationError, match="exact unique identities"):
        BenchmarkProfileSpec(**descriptor(catalog_task_ids=mapping))


@pytest.mark.parametrize("metadata,code", [
    ({"source_task_count": True}, "profile_metadata_invalid"),
    ({"source_task_count": "2"}, "profile_metadata_invalid"),
    ({"selection": "subset"}, "profile_selection_drift"),
])
def test_published_census_and_selection_cannot_be_weakened(metadata, code):
    from types import SimpleNamespace

    from loom.benchmark_admission_contracts import CANONICAL, validate_profile_contract
    from loom.models.benchmark_profile import BenchmarkProfileSpec
    spec = BenchmarkProfileSpec(
        id="arbitrary", origin={"kind": "json", "locator": "fixture:json",
                                "revision": "7", "release": "7", "importer": "loom.json.v1"},
        expected_task_count=2, selected_task_ids=("one", "two"),
    )
    with pytest.raises(ValueError, match=code):
        validate_profile_contract(CANONICAL, SimpleNamespace(profile_provenance=metadata), spec)
