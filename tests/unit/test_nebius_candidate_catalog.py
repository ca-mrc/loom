"""Publication authority is GitHub metadata plus immutable bytes, not JSON labels."""

from __future__ import annotations

import copy
import gzip
import hashlib
import io
import json
import tarfile
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


@pytest.mark.parametrize('status,current,merge_base,ahead,behind,want', [
    ('ahead', HEAD, HEAD, 12, 0, True),
    ('identical', SHA, SHA, 0, 0, True),
    ('behind', HEAD, SHA, 0, 3, False),
    ('diverged', HEAD, 'c' * 40, 2, 3, False),
    ('ahead', HEAD, 'c' * 40, 12, 0, None),
    ('ahead', HEAD, HEAD, True, 0, None),
    ('identical', HEAD, SHA, 0, 0, None),
    ('unknown', HEAD, HEAD, 0, 0, None),
])
async def test_exact_candidate_ancestry_uses_scoped_github_reader(publication, status, current, merge_base, ahead, behind, want):
    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management.candidates import (
        GitHubCandidateCatalog,
        ProtectedPublication,
    )

    reference, responses, payload, keyring, candidate = publication
    responses[f'compare/{current}...{SHA}'] = {'status': status,
        'base_commit': {'sha': current}, 'merge_base_commit': {'sha': merge_base},
        'ahead_by': ahead, 'behind_by': behind}
    async with httpx.AsyncClient(transport=github_transport(responses, payload)) as http:
        catalog = GitHubCandidateCatalog(http, token='test-github-secret',
            publications=[ProtectedPublication.model_validate(reference)],
            keyring=ImageAdmissionKeyring.from_json(keyring), registry_prefix=candidate['registry_prefix'])
        assert callable(getattr(catalog, 'candidate_follows', None)), 'protected source comparison still requires Git'
        if want is None:
            with pytest.raises(ValueError):
                await catalog.candidate_follows(candidate_id=IDENTITY, current_sha=current)
        else:
            assert await catalog.candidate_follows(candidate_id=IDENTITY, current_sha=current) is want


@pytest.mark.parametrize('status', [401, 403, 404, 429, 500])
async def test_unavailable_commit_comparison_never_authorizes_a_release(publication, status):
    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management.candidates import (
        GitHubCandidateCatalog,
        ProtectedPublication,
    )

    reference, _, _, keyring, candidate = publication

    def respond(request):
        assert request.url.path == f'/repositories/1281629473/compare/{HEAD}...{SHA}'
        assert request.headers['Authorization'] == 'Bearer test-github-secret'
        return httpx.Response(status)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        catalog = GitHubCandidateCatalog(http, token='test-github-secret',
            publications=[ProtectedPublication.model_validate(reference)],
            keyring=ImageAdmissionKeyring.from_json(keyring), registry_prefix=candidate['registry_prefix'])
        with pytest.raises(httpx.HTTPStatusError):
            await catalog.candidate_follows(candidate_id=IDENTITY, current_sha=HEAD)


def rendering_archive(*, damage=None):
    data = io.BytesIO()
    prefix = 'ca-mrc-loom-' + SHA[:7]
    selected = {'deploy/k8s/web.yaml': b'selected template',
        'src/loom/__init__.py': b'',
        'src/loom/nebius_platform_render.py': b'selected module',
        'scripts/ops/render_nebius_platform.py': b'selected renderer'}
    if damage == 'missing-package':
        del selected['src/loom/__init__.py']
    with tarfile.open(fileobj=data, mode='w', format=tarfile.PAX_FORMAT,
            pax_headers={'comment': HEAD if damage == 'commit' else SHA}) as archive:
        for name, raw in {**selected, 'README.md': b'not renderer input'}.items():
            member = tarfile.TarInfo(prefix + '/' + name)
            member.size = len(raw)
            if damage == 'symlink' and name == 'deploy/k8s/web.yaml':
                member.type, member.linkname, member.size = tarfile.SYMTYPE, '/etc/passwd', 0
            if damage == 'hardlink' and name == 'deploy/k8s/web.yaml':
                member.type, member.linkname, member.size = tarfile.LNKTYPE, prefix + '/README.md', 0
            archive.addfile(member, io.BytesIO(raw))
            if damage == 'duplicate':
                archive.addfile(member, io.BytesIO(raw))
        # The real repository has this non-rendering symlink. It must not be
        # extracted, but must not make legitimate rendering source unavailable.
        ignored = tarfile.TarInfo(prefix + '/deploy/.env')
        ignored.type, ignored.linkname = tarfile.SYMTYPE, '../../private.env'
        archive.addfile(ignored)
        if damage in {'traversal', 'absolute', 'foreign-root'}:
            name = {'traversal': prefix + '/src/loom/../../escape',
                'absolute': '/tmp/escape', 'foreign-root': 'foreign/src/loom/extra.py'}[damage]
            archive.addfile(tarfile.TarInfo(name))
    return gzip.compress(data.getvalue()), selected


@pytest.mark.parametrize('damage', [None, 'commit', 'symlink', 'hardlink', 'duplicate', 'traversal',
    'absolute', 'foreign-root', 'download-size', 'expanded-size', 'missing-source', 'invalid-gzip', 'missing-package'])
async def test_render_source_is_exact_bounded_and_never_extracts_unselected_files(publication, monkeypatch, damage):
    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management import candidates as module

    reference, _, _, keyring, candidate = publication
    payload, selected = rendering_archive(damage=damage)
    if damage == 'download-size':
        monkeypatch.setattr(module, '_MAX_SOURCE_DOWNLOAD', len(payload) - 1, raising=False)
    elif damage == 'expanded-size':
        monkeypatch.setattr(module, '_MAX_SOURCE_EXPANDED', 100, raising=False)
    elif damage == 'missing-source':
        payload = gzip.compress(b'\0' * 10240)
    elif damage == 'invalid-gzip':
        payload = b'invalid archive'

    def respond(request):
        if request.url.host == 'api.github.com':
            assert request.url.path == f'/repositories/1281629473/tarball/{SHA}'
            assert request.headers['Authorization'] == 'Bearer test-github-secret'
            return httpx.Response(302, headers={'Location': f'https://codeload.github.com/ca-mrc/loom/legacy.tar.gz/{SHA}?token=private'})
        assert request.url.host == 'codeload.github.com'
        assert 'Authorization' not in request.headers and 'Cookie' not in request.headers
        return httpx.Response(200, content=payload)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond), auth=('default', 'credential'),
            headers={'Authorization': 'other-secret'}, cookies={'session': 'secret'}) as http:
        catalog = module.GitHubCandidateCatalog(http, token='test-github-secret',
            publications=[module.ProtectedPublication.model_validate(reference)],
            keyring=ImageAdmissionKeyring.from_json(keyring), registry_prefix=candidate['registry_prefix'])
        assert callable(getattr(catalog, 'render_source', None)), 'selected rendering source is not retrieved'
        if damage is None:
            assert await catalog.render_source(candidate_id=IDENTITY) == selected
        else:
            with pytest.raises(ValueError):
                await catalog.render_source(candidate_id=IDENTITY)


@pytest.mark.parametrize('location', [
    f'http://codeload.github.com/ca-mrc/loom/legacy.tar.gz/{SHA}',
    f'https://codeload.github.com.evil.test/ca-mrc/loom/legacy.tar.gz/{SHA}',
    f'https://user:secret@codeload.github.com/ca-mrc/loom/legacy.tar.gz/{SHA}',
    f'https://codeload.github.com:444/ca-mrc/loom/legacy.tar.gz/{SHA}',
    f'https://codeload.github.com/fork/loom/legacy.tar.gz/{SHA}',
    f'https://codeload.github.com/ca-mrc/loom/legacy.tar.gz/{HEAD}',
    'https://codeload.github.com/ca-mrc/loom/legacy.tar.gz/dev',
])
async def test_render_source_refuses_redirect_outside_exact_repository_commit(publication, location):
    from loom.execution_image_admission import ImageAdmissionKeyring
    from loom_service.environment_management.candidates import (
        GitHubCandidateCatalog,
        ProtectedPublication,
    )

    reference, _, _, keyring, candidate = publication

    def respond(request):
        assert request.url.host == 'api.github.com', 'unqualified redirect must never be contacted'
        return httpx.Response(302, headers={'Location': location})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        catalog = GitHubCandidateCatalog(http, token='test-github-secret',
            publications=[ProtectedPublication.model_validate(reference)],
            keyring=ImageAdmissionKeyring.from_json(keyring), registry_prefix=candidate['registry_prefix'])
        assert callable(getattr(catalog, 'render_source', None)), 'source redirect validation is missing'
        with pytest.raises(ValueError):
            await catalog.render_source(candidate_id=IDENTITY)


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


@pytest.mark.parametrize("change", ["valid", "missing-admission", "tampered-signature", "undeclared-image"])
async def test_protected_publication_qualifies_only_explicit_signed_task_pins(publication, tmp_path, change):
    from scripts.ops.nebius_candidate import _private_key, _sign_admission

    from loom_service.environment_management.registry import ManagementError

    reference, responses, payload, keyring, candidate = copy.deepcopy(publication)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        profile = json.loads(archive.read("runtime-profile.json"))
    source = "ghcr.io/terminal-bench/task:rev6"
    image = "ghcr.io/terminal-bench/task@sha256:" + "9" * 64
    original = profile["image_admission"]["admissions"][0]["statement"]
    signed = _sign_admission({**original, "image_ref": image}, original["policy_sha256"],
        original["provenance_sha256"], key=_private_key(tmp_path / "signer.pem"), signing_key_id="publisher").model_dump(mode="json")
    if change != "undeclared-image":
        profile["prebuilt_image_pins"] = {source: image}
    if change != "missing-admission":
        profile["image_admission"]["admissions"].append(signed)
    if change == "tampered-signature":
        signed["signature_base64"] = "A" * 88
    artifact = io.BytesIO()
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr("candidate.json", json.dumps(candidate))
        archive.writestr("runtime-profile.json", json.dumps(profile))
    payload = artifact.getvalue()
    reference["artifact_sha256"] = "sha256:" + hashlib.sha256(payload).hexdigest()
    responses["actions/artifacts/123"].update(digest=reference["artifact_sha256"], size_in_bytes=len(payload))
    prepared = (reference, responses, payload, keyring, candidate)
    if change == "valid":
        assert (await resolve(prepared)).profile["prebuilt_image_pins"] == {source: image}
    else:
        with pytest.raises(ManagementError, match="candidate_publication_invalid"):
            await resolve(prepared)
