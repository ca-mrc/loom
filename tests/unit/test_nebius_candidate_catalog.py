"""Publication authority is GitHub metadata plus immutable bytes, not JSON labels."""

from __future__ import annotations

import copy
import hashlib
import io
import json
import zipfile
from uuid import UUID

import httpx
import pytest
from scripts.ops.nebius_candidate import create_candidate

from tests.ops.test_nebius_candidate import inputs

IDENTITY = UUID("16dc7c6c-69cd-4b77-b959-f484b8a7692f")
SHA = "a" * 40
HEAD = "b" * 40
GATES = ("repository-checks", "images-gate", "cluster-smoke-gate", "staging-smoke-gate")


@pytest.fixture
def publication(tmp_path):
    record, private, keyring = inputs(tmp_path)
    candidate, profile = create_candidate(record, signing_key=private, signing_key_id="publisher", keyring_json=keyring)
    run_id = candidate["run_id"]
    artifact = io.BytesIO()
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("candidate.json", json.dumps(candidate))
        archive.writestr("runtime-profile.json", json.dumps(profile))
    payload = artifact.getvalue()
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    reference = dict(candidate_id=IDENTITY, source_sha=SHA, run_id=run_id, run_attempt=1,
                     artifact_id=123, artifact_sha256=digest, pull_request=40)
    repo = {"full_name": "qianyi-sun/loom", "id": 1281629473}
    responses = {
        f"actions/runs/{run_id}/attempts/1": {
            "id": run_id, "run_attempt": 1, "head_sha": SHA, "head_branch": "dev",
            "repository": repo, "head_repository": repo, "status": "completed", "conclusion": "success",
            "event": "push", "path": ".github/workflows/nebius-candidate.yml",
        },
        "pulls/40": {
            "number": 40, "merged": True, "state": "closed", "merge_commit_sha": SHA,
            "base": {"ref": "dev", "repo": repo}, "head": {"sha": HEAD, "repo": repo},
        },
        f"commits/{HEAD}/check-runs": {"total_count": 4, "check_runs": [
            {"id": n + 100, "name": name, "head_sha": HEAD, "status": "completed", "conclusion": "success",
             "app": {"id": 15368, "slug": "github-actions"}} for n, name in enumerate(GATES)
        ]},
        "actions/artifacts/123": {
            "id": 123, "name": f"nebius-candidate-{SHA}-{run_id}-1", "expired": False,
            "size_in_bytes": len(payload), "digest": digest,
            "workflow_run": {"id": run_id, "head_sha": SHA, "head_branch": "dev",
                             "repository_id": repo["id"], "head_repository_id": repo["id"]},
        },
    }
    return reference, responses, payload, keyring, candidate


def github_transport(responses, payload, *, location="https://store.blob.core.windows.net/artifact?sig=private"):
    def respond(request):
        if request.url.host == "api.github.com":
            assert request.headers["Authorization"] == "Bearer test-github-secret"
            # GitHub rejects otherwise valid requests without a User-Agent.
            assert request.headers.get("User-Agent", "").startswith("loom-")
            path = request.url.path.removeprefix("/repositories/1281629473/")
            if path == "actions/artifacts/123/zip":
                return httpx.Response(302, headers={"Location": location})
            return httpx.Response(200, json=responses[path])
        assert request.url.host == "store.blob.core.windows.net"
        # This assertion models the cloud boundary, not the manager: GitHub's
        # credential must not follow its signed artifact redirect to blob storage.
        assert "Authorization" not in request.headers
        return httpx.Response(200, content=payload)
    return httpx.MockTransport(respond)


async def resolve(publication, *, location=None):
    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management.candidates import (
        GitHubCandidateCatalog,
        ProtectedPublication,
    )

    reference, responses, payload, keyring, candidate = publication
    transport = github_transport(responses, payload, **({"location": location} if location else {}))
    async with httpx.AsyncClient(transport=transport) as http:
        catalog = GitHubCandidateCatalog(
            http, token="test-github-secret", publications=[ProtectedPublication.model_validate(reference)],
            keyring=ImageAdmissionKeyring.from_json(keyring), registry_prefix=candidate["registry_prefix"],
        )
        return await catalog.resolve(IDENTITY)


async def test_protected_publication_resolves_actual_squash_not_nonexistent_dev_push_checks(publication):
    bundle = await resolve(publication)
    assert bundle.candidate_id == IDENTITY
    assert bundle.candidate["candidate_sha"] == SHA
    assert bundle.profile["candidate_sha"] == SHA


@pytest.mark.parametrize("candidate_repository", ["qianyi-sun/loom", "ca-mrc/loom"])
async def test_transferred_repository_resolves_old_and_new_publications(publication, candidate_repository):
    reference, responses, _, keyring, candidate = copy.deepcopy(publication)
    run = responses[f"actions/runs/{reference['run_id']}/attempts/1"]
    run["repository"]["full_name"] = "ca-mrc/loom"
    candidate["repository"] = candidate_repository
    # Metadata changes do not rewrite a historical publication's source identity.
    with zipfile.ZipFile(io.BytesIO(publication[2])) as source:
        profile = source.read("runtime-profile.json")
    artifact = io.BytesIO()
    with zipfile.ZipFile(artifact, "w") as target:
        target.writestr("candidate.json", json.dumps(candidate))
        target.writestr("runtime-profile.json", profile)
    payload = artifact.getvalue()
    reference["artifact_sha256"] = "sha256:" + hashlib.sha256(payload).hexdigest()
    responses["actions/artifacts/123"].update(
        digest=reference["artifact_sha256"], size_in_bytes=len(payload),
    )
    bundle = await resolve((reference, responses, payload, keyring, candidate))
    assert bundle.candidate["repository"] == candidate_repository


@pytest.mark.parametrize("mutation", [
    "failed-publication", "wrong-branch", "wrong-workflow", "wrong-repository", "wrong-repository-id", "wrong-attempt",
    "unmerged", "other-squash", "other-base", "failed-check", "missing-check", "forged-check-app",
    "newer-failed-check", "expired-artifact", "wrong-artifact-run", "wrong-artifact-digest", "tampered-bytes",
])
async def test_metadata_or_artifact_mismatch_cannot_authorize_candidate(publication, mutation):
    from loom_service.environment_management.registry import ManagementError

    reference, responses, payload, keyring, candidate = copy.deepcopy(publication)
    run = responses[f"actions/runs/{reference['run_id']}/attempts/1"]
    pr = responses["pulls/40"]
    checks = responses[f"commits/{HEAD}/check-runs"]
    artifact = responses["actions/artifacts/123"]
    if mutation == "failed-publication":
        run["conclusion"] = "failure"
    elif mutation == "wrong-branch":
        run["head_branch"] = "feature/unapproved"
    elif mutation == "wrong-workflow":
        run["path"] = ".github/workflows/untrusted.yml"
    elif mutation == "wrong-repository":
        run["head_repository"] = {"full_name": "fork/loom", "id": 1}
    elif mutation == "wrong-repository-id":
        run["head_repository"] = {"full_name": "ca-mrc/loom", "id": 1}
    elif mutation == "wrong-attempt":
        run["run_attempt"] = 2
    elif mutation == "unmerged":
        pr["merged"] = False
    elif mutation == "other-squash":
        pr["merge_commit_sha"] = "c" * 40
    elif mutation == "other-base":
        pr["base"]["ref"] = "main"
    elif mutation == "failed-check":
        checks["check_runs"][0]["conclusion"] = "failure"
    elif mutation == "missing-check":
        checks["check_runs"].pop()
        checks["total_count"] = 3
    elif mutation == "forged-check-app":
        checks["check_runs"][0]["app"]["id"] = 42
    elif mutation == "newer-failed-check":
        checks["check_runs"].append({**checks["check_runs"][0], "id": 200, "conclusion": "failure"})
        checks["total_count"] = 5
    elif mutation == "expired-artifact":
        artifact["expired"] = True
    elif mutation == "wrong-artifact-run":
        artifact["workflow_run"]["id"] += 1
    elif mutation == "wrong-artifact-digest":
        artifact["digest"] = "sha256:" + "0" * 64
    elif mutation == "tampered-bytes":
        payload += b"tampered"
    with pytest.raises(ManagementError, match="candidate_publication_invalid"):
        await resolve((reference, responses, payload, keyring, candidate))


@pytest.mark.parametrize("location", [
    "http://store.blob.core.windows.net/file", "https://api.github.com.evil.example/file",
    "https://127.0.0.1/file", "https://user:pass@store.blob.core.windows.net/file",
    "https://store.blob.core.windows.net:8443/file",
])
async def test_redirect_is_bounded_to_https_artifact_storage(publication, location):
    from loom_service.environment_management.registry import ManagementError

    with pytest.raises(ManagementError, match="candidate_publication_invalid"):
        await resolve(publication, location=location)


async def test_profile_signature_rechecked_against_current_trust(publication):
    from loom_service.environment_management.registry import ManagementError

    reference, responses, payload, _, candidate = publication
    with pytest.raises(ManagementError, match="candidate_publication_invalid"):
        await resolve((reference, responses, payload, '{"schema_version":1,"keys":[]}', candidate))
