"""Trusted personal-source preparation, separate from untrusted native builds.

Source Python is parsed as data only. These helpers neither migrate a database
nor attest to the behavior of arbitrary developer application code.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Literal

from loom.application_image_build import (
    ApplicationImageBuildClaimV1,
    ApplicationImagePublicationV1,
    application_image_components,
)
from loom.application_source_archive import extract_application_source_archive
from loom_execution_actuator.task_image_oci import (
    validate_native_oci_archive,
    validate_native_oci_directory,
)
from loom_execution_actuator.task_image_runtime import (
    BuildPreparationError,
    _client,
    _download,
    _output_path,
    _publish_cache_blobs,
    _try_import_cache,
)

_REVISION = re.compile(r"[a-zA-Z0-9_]{1,64}\Z")
_SCHEMA_FIELDS = {"revision", "down_revision", "branch_labels", "depends_on"}


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise BuildPreparationError("duplicate application build claim field")
        result[key] = value
    return result


def load_claim(path: Path) -> ApplicationImageBuildClaimV1:
    """Parse the protected renderer envelope, not arbitrary owner build options."""
    with path.open("rb") as stream:
        body = stream.read(256 * 1024 + 1)
    if len(body) > 256 * 1024:
        raise BuildPreparationError("application build claim is too large")
    document = json.loads(body, object_pairs_hook=_object)
    if not isinstance(document, dict):
        raise BuildPreparationError("application build claim must be an object")
    components, architecture = document.pop("components", None), document.pop("cpu_arch", None)
    claim = ApplicationImageBuildClaimV1.model_validate(document)
    expected = [row.model_dump(mode="json") for row in application_image_components()]
    if claim.recipe.oci_export_format == "directory":
        for row in expected:
            row["oci_output_path"] = row["oci_output_path"].removesuffix(".tar")
    if components != expected or architecture != claim.recipe.cpu_arch:
        raise BuildPreparationError("application build envelope differs from protected recipe")
    return claim


def _migration_metadata(path: Path) -> tuple[str, tuple[str, ...]]:
    # Bound parser work independently of the much larger source archive budget.
    with path.open("rb") as stream:
        body = stream.read(1024 * 1024 + 1)
    if len(body) > 1024 * 1024:
        raise BuildPreparationError("source migration exceeds metadata parsing limit")
    fields: dict[str, str | tuple[str, ...] | None] = {}
    try:
        tree = ast.parse(body)
        for node in tree.body:
            targets: list[ast.expr] = []
            value: ast.expr | None = None
            if isinstance(node, ast.Assign):
                targets, value = node.targets, node.value
            elif isinstance(node, ast.AnnAssign):
                targets, value = [node.target], node.value
            for target in targets:
                if not isinstance(target, ast.Name) or target.id not in _SCHEMA_FIELDS:
                    continue
                if target.id in fields:
                    raise BuildPreparationError("source migration metadata must be unique literals")
                if isinstance(value, ast.Constant) and (value.value is None or isinstance(value.value, str)):
                    fields[target.id] = value.value
                elif (target.id == "down_revision" and isinstance(value, (ast.Tuple, ast.List))
                        and value.elts and all(isinstance(item, ast.Constant) and isinstance(item.value, str)
                                              for item in value.elts)):
                    fields[target.id] = tuple(item.value for item in value.elts
                        if isinstance(item, ast.Constant) and isinstance(item.value, str))
                else:
                    raise BuildPreparationError("source migration metadata must be unique literals")
    except (SyntaxError, ValueError, RecursionError) as error:
        raise BuildPreparationError("source migration metadata is invalid") from error
    revision, parent = fields.get("revision"), fields.get("down_revision")
    parents = () if parent is None else (parent,) if isinstance(parent, str) else parent
    if (not isinstance(revision, str) or not _REVISION.fullmatch(revision)
            or "down_revision" not in fields or len(parents) != len(set(parents))
            or any(not _REVISION.fullmatch(item) for item in parents)
            or fields.get("branch_labels") is not None or fields.get("depends_on") is not None):
        raise BuildPreparationError("source migration metadata is incompatible")
    return revision, parents


def qualify_source_schema(context: Path, *, expected_revision: str) -> None:
    """Require one complete acyclic metadata history at the installed schema head.

    Never import migrations or invoke Alembic on developer-controlled code.
    This checks declared schema compatibility, not application semantics or DDL.
    """
    try:
        versions = _output_path(context, "database/migrations/versions", directory=True)
        paths = sorted(versions.iterdir())
        if not 1 <= len(paths) <= 2000:
            raise BuildPreparationError("source migration inventory is unbounded or empty")
        parents: dict[str, tuple[str, ...]] = {}
        for path in paths:
            if path.name in {"__init__.py", ".gitkeep"}:
                _output_path(context, path.relative_to(context).as_posix())
                continue
            if path.suffix != ".py":
                raise BuildPreparationError("source migration inventory contains unsupported entries")
            regular = _output_path(context, path.relative_to(context).as_posix())
            revision, parent = _migration_metadata(regular)
            if revision in parents:
                raise BuildPreparationError("source migration revision is duplicated")
            parents[revision] = parent
        if set(parents) - {value for values in parents.values() for value in values} != {expected_revision}:
            raise BuildPreparationError("source schema head differs from installed schema")
        visited: set[str] = set()
        active: set[str] = set()
        pending = [(expected_revision, False)]
        while pending:
            current, closing = pending.pop()
            if closing:
                active.remove(current)
                visited.add(current)
                continue
            if current in visited:
                continue
            if current in active or current not in parents:
                raise BuildPreparationError("source migration history is incomplete or cyclic")
            active.add(current)
            pending.append((current, True))
            pending.extend((parent, False) for parent in parents[current])
        if visited != set(parents):
            raise BuildPreparationError("source migration history is disconnected")
    except OSError as error:
        raise BuildPreparationError("source migration inventory is unavailable") from error


def prepare(claim: ApplicationImageBuildClaimV1, work: Path, secrets: Path) -> None:
    """Download exact immutable source and qualify it before exposing build inputs."""
    claim = ApplicationImageBuildClaimV1.model_validate_json(claim.model_dump_json())
    context = work / "context"
    if context.exists() or context.is_symlink():
        raise BuildPreparationError("application build context must be new")
    if work != work.resolve(strict=True):
        raise BuildPreparationError("application build root must not contain links")
    source = _client(claim.model_dump(mode="json"), secrets / "source")
    try:
        with tempfile.TemporaryDirectory(prefix="loom-app-source-") as directory:
            archive = Path(directory) / "source.tar"
            size = _download(source, bucket=claim.source_bucket, key=claim.source_key,
                destination=archive, limit=claim.source.archive_size_bytes)
            with archive.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
                if size != claim.source.archive_size_bytes or digest != claim.source.archive_sha256:
                    raise BuildPreparationError("application source archive differs from verified upload")
                context.mkdir(mode=0o700)
                extract_application_source_archive(stream, expected_digest=claim.source.source_digest, destination=context)
        qualify_source_schema(context, expected_revision=claim.recipe.schema_revision)
        for component in application_image_components():
            _output_path(context, component.dockerfile_path)
        (work / "oci").mkdir()
    finally:
        source.close()
    if claim.cache_bucket is not None:
        binding = _cache_binding(claim)
        cache = _client(binding, secrets / "cache")
        try:
            verified: dict[str, tuple[Path, int]] = {}
            for index in range(2):
                _try_import_cache(cache, binding, index=index, work=work, verified=verified)
        finally:
            cache.close()


def _cache_binding(claim: ApplicationImageBuildClaimV1) -> dict[str, Any]:
    # These cache primitives need only a content key and storage identity, not
    # a TaskConfig/task claim. Preserve the existing shared prefix, GC and bounds.
    return {"storage_endpoint": claim.storage_endpoint, "storage_region": claim.storage_region,
        "cache_bucket": claim.cache_bucket, "materialization_key": claim.cache_key, "cache_transfer": "blobs"}


def _publication_binding(claim: ApplicationImageBuildClaimV1) -> dict[str, Any]:
    return {**claim.model_dump(mode="json", include={"build_id", "attempt", "upload_id", "installation_id",
        "owner_user_id", "owner_team_id", "data_environment_id", "cluster_id"}),
        "source_digest": claim.source.source_digest, "recipe_digest": claim.recipe.digest,
        "schema_revision": claim.recipe.schema_revision, "cpu_arch": claim.recipe.cpu_arch}


def publish(claim: ApplicationImageBuildClaimV1, work: Path, secrets: Path, *,
            receipt_path: Path = Path("/dev/termination-log")) -> ApplicationImagePublicationV1:
    """Validate read-only local outputs, publish once, and verify immutable readback.

    Unknown copy outcomes are never retried here. Exact build/attempt tags allow
    the management lifecycle to reconcile them without accepting another attempt.
    """
    claim = ApplicationImageBuildClaimV1.model_validate_json(claim.model_dump_json())
    directory = claim.recipe.oci_export_format == "directory"
    oci = _output_path(work, "oci", directory=True)
    names = {f"{index:04d}" + ("" if directory else ".tar") for index in range(2)}
    if {path.name for path in oci.iterdir()} != names:
        raise BuildPreparationError("application publication requires exactly service and web outputs")
    outputs = []
    architecture: Literal["amd64", "arm64"] = "amd64" if claim.recipe.cpu_arch == "x86_64" else "arm64"
    # Check both components before any registry write. The build has terminated
    # and the renderer mounts this volume read-only in the publisher.
    for component in application_image_components():
        relative = component.oci_output_path.removesuffix(".tar") if directory else component.oci_output_path
        output = _output_path(work, relative, directory=directory)
        if directory:
            validate_native_oci_directory(output, architecture=architecture)
        else:
            validate_native_oci_archive(output, architecture=architecture)
        outputs.append((component.name, f"{'oci' if directory else 'oci-archive'}:{output}"))
    images: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="loom-app-publish-") as temporary:
        private = Path(temporary)
        registry_auth = secrets / "registry/config.json"
        credentials = secrets / "registry/credentials.json"
        if credentials.is_file():
            from loom.nebius_registry_auth import mint_registry_auth

            registry_auth = private / "config.json"
            mint_registry_auth(credentials, "/".join(claim.registry_repository.split("/")[:2]), registry_auth)
        for index, (name, source) in enumerate(outputs):
            tag = f"{claim.registry_repository}:app-{claim.build_id.hex}-a{claim.attempt}-{index}"
            digest_file = private / f"digest-{index}"
            subprocess.run(["skopeo", "--tmpdir", temporary, "copy", "--authfile", str(registry_auth),
                "--preserve-digests", "--digestfile", str(digest_file), source, f"docker://{tag}"],
                check=True, timeout=300, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            with digest_file.open() as stream:
                digest = stream.read(128).strip()
            if re.fullmatch(r"sha256:[0-9a-f]{64}", digest) is None:
                raise BuildPreparationError("application registry digest is invalid")
            ref = f"{claim.registry_repository}@{digest}"
            with (private / f"manifest-{index}").open("w+b") as manifest_stream:
                subprocess.run(["skopeo", "--tmpdir", temporary, "inspect", "--authfile", str(registry_auth),
                    "--raw", f"docker://{ref}"], check=True, timeout=60, stdout=manifest_stream, stderr=subprocess.DEVNULL)
                if not 0 < manifest_stream.tell() <= 4 * 1024 * 1024:
                    raise BuildPreparationError("application registry readback exceeds metadata limit")
                manifest_stream.seek(0)
                observed = hashlib.file_digest(manifest_stream, "sha256").hexdigest()
            if "sha256:" + observed != digest:
                raise BuildPreparationError("application registry readback differs from published digest")
            images[name] = ref
            receipt_path.write_text(json.dumps({"schema_version": "loom.application-image-publication-progress.v1",
                **_publication_binding(claim), "registry_images": images}))
        if claim.cache_bucket is not None:
            binding = _cache_binding(claim)
            cache = _client(binding, secrets / "cache")
            try:
                for index in range(2):
                    candidate = work / f"cache-out/{index}"
                    if candidate.exists() or candidate.is_symlink():
                        cache_dir = _output_path(work, f"cache-out/{index}", directory=True)
                        _publish_cache_blobs(cache, binding, index=index, cache_dir=cache_dir)
            finally:
                cache.close()
    receipt = ApplicationImagePublicationV1.model_validate({**_publication_binding(claim), "registry_images": images})
    body = receipt.model_dump_json()
    if len(body.encode()) > 4000:
        raise BuildPreparationError("application publication exceeds termination message limit")
    receipt_path.write_text(body)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=("prepare", "publish"))
    parser.add_argument("--claim", type=Path, required=True)
    args = parser.parse_args()
    try:
        claim = load_claim(args.claim)
        work, secrets = Path("/loom/build"), Path("/var/run/loom-task-build")
        if args.phase == "prepare":
            prepare(claim, work, secrets)
        else:
            publish(claim, work, secrets)
    except Exception as error:
        # Do not expose SDK credentials or arbitrary source/registry error text.
        receipt_path = Path("/dev/termination-log")
        receipt: dict[str, Any] = {}
        try:
            previous = json.loads(receipt_path.read_text())
            if isinstance(previous, dict):
                receipt = previous
        except (OSError, ValueError):
            pass
        receipt.update(phase=args.phase, error=type(error).__name__)
        receipt_path.write_text(json.dumps(receipt))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
