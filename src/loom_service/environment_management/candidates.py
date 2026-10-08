"""Resolve only operator-pinned, protected GitHub publications.

Candidate JSON is data, never approval. The successful exact publication attempt,
merged squash identity, four Actions-app-bound PR-head gates and artifact digest
must agree before its images can enter a new environment plan. Signed runtime
evidence is checked against current installation trust, without an evidence TTL.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
import stat
import zipfile
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field

from loom.execution_image_admission import ImageAdmissionKeyring, verify_execution_image_admission
from loom.nebius_candidate_contract import NEBIUS_PLATFORM_IMAGES
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1
from loom_service.environment_management.manager import CandidateBundle
from loom_service.environment_management.registry import ManagementError

_REPO = "qianyi-sun/loom"
_API = "https://api.github.com/repos/" + _REPO + "/"
_GATES = frozenset(("repository-checks", "images-gate", "cluster-smoke-gate", "staging-smoke-gate"))
_MAX_ARTIFACT = 64 * 1024 * 1024
_MAX_DOCUMENT = 1024 * 1024


class ProtectedPublication(BaseModel):
    """Installation-owned immutable selection, not accepted from an owner request."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    candidate_id: UUID
    source_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    run_id: int = Field(gt=0, strict=True)
    run_attempt: int = Field(gt=0, strict=True)
    artifact_id: int = Field(gt=0, strict=True)
    artifact_sha256: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    pull_request: int = Field(gt=0, strict=True)


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("invalid publication")


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        _require(key not in result)
        result[key] = value
    return result


def _json(payload: bytes) -> dict[str, Any]:
    result = json.loads(payload, object_pairs_hook=_object)
    _require(isinstance(result, dict))
    return dict(result)


class GitHubCandidateCatalog:
    def __init__(
        self, http: httpx.AsyncClient, *, token: str, publications: list[ProtectedPublication],
        keyring: ImageAdmissionKeyring, registry_prefix: str,
    ):
        _require(bool(token) and len(publications) <= 1000)
        _require(re.fullmatch(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9]+", registry_prefix) is not None)
        self.http = http
        self._token = token
        self.publications = {row.candidate_id: row for row in publications}
        _require(len(self.publications) == len(publications))
        self.keyring = keyring
        self.registry_prefix = registry_prefix
        self._concurrency = asyncio.Semaphore(2)

    async def _read(self, url: str, *, api: bool, limit: int) -> tuple[bytes, str | None]:
        # Build a standalone Request instead of inheriting client's credentials,
        # cookies or default auth on the signed blob-storage request.
        headers = {"User-Agent": "loom-environment-management"}
        if api:
            _require(url.startswith(_API))
            headers.update({"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})
            headers["Authorization"] = "Bearer " + self._token
        request = httpx.Request("GET", url, headers=headers, extensions={
            "timeout": {"connect": 10.0, "read": 30.0, "write": 10.0, "pool": 10.0},
        })
        response = await self.http.send(request, stream=True, auth=None, follow_redirects=False)
        try:
            if response.status_code == 302 and api and url.endswith("/zip"):
                return b"", response.headers["Location"]
            response.raise_for_status()
            _require(response.status_code == 200)
            if "Content-Length" in response.headers:
                _require(0 <= int(response.headers["Content-Length"]) <= limit)
            content = bytearray()
            async for chunk in response.aiter_bytes():
                _require(len(content) + len(chunk) <= limit)
                content.extend(chunk)
            return bytes(content), None
        finally:
            await response.aclose()

    async def _api(self, path: str) -> dict[str, Any]:
        payload, redirect = await self._read(_API + path, api=True, limit=2 * _MAX_DOCUMENT)
        _require(redirect is None)
        return _json(payload)

    async def _checks(self, head_sha: str) -> None:
        _require(re.fullmatch(r"[0-9a-f]{40}", head_sha) is not None)
        latest: dict[str, dict[str, Any]] = {}
        seen = 0
        for page in range(1, 21):
            data = await self._api(f"commits/{head_sha}/check-runs?filter=all&per_page=100&page={page}")
            rows = data["check_runs"]
            _require(isinstance(rows, list) and len(rows) <= 100)
            for row in rows:
                name = row["name"]
                if name not in _GATES:
                    continue
                _require(row["head_sha"] == head_sha)
                if row["app"]["id"] != 15368 or row["app"]["slug"] != "github-actions":
                    continue
                if name not in latest or row["id"] > latest[name]["id"]:
                    latest[name] = row
            seen += len(rows)
            if seen >= data["total_count"]:
                break
            _require(bool(rows))
        else:
            raise ValueError("check inventory too large")
        _require(set(latest) == _GATES and all(
            row["status"] == "completed" and row["conclusion"] == "success" for row in latest.values()
        ))

    async def resolve(self, candidate_id: UUID) -> CandidateBundle:
        reference = self.publications.get(candidate_id)
        if reference is None:
            raise ManagementError("candidate_not_available", 404)
        try:
            async with self._concurrency, asyncio.timeout(120):
                return await self._resolve(reference)
        except (httpx.HTTPError, TimeoutError) as exc:
            # Never serialize provider exception strings/URLs or credentials.
            raise ManagementError("candidate_publication_unavailable", 503) from exc
        except (ValueError, KeyError, TypeError, AttributeError, zipfile.BadZipFile, OSError, RuntimeError) as exc:
            raise ManagementError("candidate_publication_invalid", 503) from exc

    async def _resolve(self, reference: ProtectedPublication) -> CandidateBundle:
        run, pr, artifact = await asyncio.gather(
            self._api(f"actions/runs/{reference.run_id}/attempts/{reference.run_attempt}"),
            self._api(f"pulls/{reference.pull_request}"),
            self._api(f"actions/artifacts/{reference.artifact_id}"),
        )
        _require(
            run["id"] == reference.run_id and run["run_attempt"] == reference.run_attempt
            and run["head_sha"] == reference.source_sha and run["head_branch"] == "dev"
            and run["repository"]["full_name"] == _REPO and run["head_repository"]["full_name"] == _REPO
            and run["repository"]["id"] == run["head_repository"]["id"]
            and run["path"] == ".github/workflows/nebius-candidate.yml"
            and run["event"] in {"push", "workflow_dispatch"}
            and run["status"] == "completed" and run["conclusion"] == "success"
        )
        _require(
            pr["number"] == reference.pull_request and pr["merged"] is True and pr["state"] == "closed"
            and pr["base"]["ref"] == "dev" and pr["base"]["repo"]["full_name"] == _REPO
            and pr["merge_commit_sha"] == reference.source_sha
        )
        await self._checks(pr["head"]["sha"])
        origin = artifact["workflow_run"]
        _require(
            artifact["id"] == reference.artifact_id and artifact["expired"] is False
            and artifact["name"] == f"nebius-candidate-{reference.source_sha}-{reference.run_id}-{reference.run_attempt}"
            and artifact["digest"] == reference.artifact_sha256
            and 0 < artifact["size_in_bytes"] <= _MAX_ARTIFACT
            and origin["id"] == reference.run_id and origin["head_sha"] == reference.source_sha
            and origin["head_branch"] == "dev" and origin["repository_id"] == run["repository"]["id"]
            and origin["head_repository_id"] == run["repository"]["id"]
        )
        payload, redirect = await self._read(
            _API + f"actions/artifacts/{reference.artifact_id}/zip", api=True, limit=_MAX_ARTIFACT,
        )
        if redirect is not None:
            url = httpx.URL(redirect)
            _require(url.scheme == "https" and not url.userinfo and url.port in {None, 443}
                     and url.host.endswith((".blob.core.windows.net", ".actions.githubusercontent.com")))
            payload, second_redirect = await self._read(str(url), api=False, limit=_MAX_ARTIFACT)
            _require(second_redirect is None)
        _require("sha256:" + hashlib.sha256(payload).hexdigest() == reference.artifact_sha256)
        with zipfile.ZipFile(io.BytesIO(payload)) as archive:
            _require(len(archive.infolist()) <= 100)
            documents = {}
            for name in ("candidate.json", "runtime-profile.json"):
                matches = [row for row in archive.infolist() if row.filename == name]
                _require(len(matches) == 1)
                info = matches[0]
                _require(0 < info.file_size <= _MAX_DOCUMENT and not info.flag_bits & 1
                         and not stat.S_ISLNK(info.external_attr >> 16))
                with archive.open(info) as source:
                    raw = source.read(_MAX_DOCUMENT + 1)
                    _require(len(raw) <= _MAX_DOCUMENT)
                    documents[name] = _json(raw)
        candidate, profile = documents["candidate.json"], documents["runtime-profile.json"]
        _require(
            candidate["schema_version"] == "loom.nebius-candidate.v1"
            and candidate["repository"] == _REPO and candidate["source_ref"] == "refs/heads/dev"
            and candidate["workflow_path"] == run["path"] and candidate["run_id"] == reference.run_id
            and candidate["candidate_sha"] == reference.source_sha
            and candidate["registry_prefix"] == self.registry_prefix
            and set(candidate["images"]) == set(NEBIUS_PLATFORM_IMAGES)
        )
        images = {name: row["image_ref"] for name, row in candidate["images"].items()}
        for name, repository in NEBIUS_PLATFORM_IMAGES.items():
            _require(re.fullmatch(re.escape(self.registry_prefix + "/" + repository + "@")
                                  + r"sha256:[0-9a-f]{64}", images[name]) is not None)
        runtime = ServiceExecutionRuntimeProfileV1.model_validate(profile)
        _require(runtime.candidate_sha == reference.source_sha
                 and runtime.task_image_ref == images["service"]
                 and runtime.runtime_image_ref == images["execution_runtime"]
                 and runtime.agent_image_ref == images["harbor_runtime"])
        verify_execution_image_admission(runtime.image_admission, keyring=self.keyring,
                                         required_image_refs=runtime.published_image_refs())
        return CandidateBundle(reference.candidate_id, candidate, profile)
