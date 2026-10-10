"""Anonymous Harbor tasks retain source-relative identity through ordinary TaskSet intake."""

import io
import json
import tarfile
from uuid import uuid4

import pytest

from loom.models.taskset import UserTaskSetManifest
from loom.models.task_checksum import task_checksum
from loom.taskset.materialize import materialize_task_set


class ObjectStore:
    """Replace only network storage; run the actual materializer and publisher."""

    def __init__(self, archive):
        self.objects = {"tasksets/user/team/slice/bundle.tar.gz": archive}

    def get_object(self, *, Bucket, Key):  # noqa: N803 - boto3 protocol
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, *, Bucket, Key, Body, ContentType):  # noqa: N803 - boto3 protocol
        self.objects[Key] = Body


@pytest.mark.parametrize("bundle_root,expected_id", [("tasks/alpha", "alpha"), ("", "slice")])
@pytest.mark.parametrize("include_transport_metadata", [False, True])
def test_taskset_intake_uses_stable_source_identity_and_preserves_authored_bytes(
    tmp_path, bundle_root, expected_id, include_transport_metadata,
):
    authored = b'version = "1.0"\n[metadata]\ntags = ["shell"]\n[environment]\ncpus = 2\nmemory = "2G"\nstorage = "5G"\n'
    files = {
        "task.toml": authored,
        "instruction.md": b"Do the task.\n",
        "environment/Dockerfile": b"FROM ubuntu:24.04\nWORKDIR /app\n",
        ".authored": b"keep dotfiles\n",
        "inputs/.loom-bundle-files.v1.json": b"keep nested authored input\n",
    }
    authored_dir = tmp_path / "authored"
    for name, content in files.items():
        path = authored_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    expected_checksum = task_checksum(authored_dir)
    archive_files = dict(files)
    if include_transport_metadata:
        archive_files[".loom-bundle-files.v1.json"] = b'{"files":{},"schema_version":1}'
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive, mode="w:gz") as stream:
        for name, content in archive_files.items():
            entry = tarfile.TarInfo(f"{bundle_root}/{name}" if bundle_root else name)
            entry.size = len(content)
            stream.addfile(entry, io.BytesIO(content))
    store = ObjectStore(archive.getvalue())
    manifest = UserTaskSetManifest.model_validate({
        "apiVersion": "loom.taskset/v1", "kind": "UserTaskSet",
        "metadata": {"name": "slice", "display_name": "Slice"},
        "intents": ["trajectory_generation"],
        "source": {"type": "bundle-upload", "locator": "bundle.tar.gz"},
    })

    result = materialize_task_set(
        manifest=manifest, task_set_id="ts/team/slice", owning_team_id="team",
        materialization_job_id=uuid4(), materialization_epoch=1,
        intents=["trajectory_generation"], verifier_blob_uri=None,
        minio_client=store, artifacts_bucket="artifacts", upstream_cache_root=tmp_path,
    )

    assert result.status == "ready", result.error_summary
    assert result.task_count == 1
    task = result.task_rows[0]
    assert task.id == f"ts/team/slice/tasks/{expected_id}"
    assert task.config["task"]["id"] == expected_id
    assert task.config["environment"]["memory_mb"] == 2048
    assert task.config["environment"]["cpus"] == 2
    stored_key = task.source.removeprefix("s3://artifacts/") + "task.toml"
    assert store.objects[stored_key] == authored
    binding = task.source_provenance["service_execution_input"]
    manifest = json.loads(store.objects[binding["manifest_uri"].removeprefix("s3://artifacts/")])
    assert [item["relative_path"] for item in manifest["files"]] == sorted(files)
    assert task.checksum == expected_checksum
    assert manifest["task_revision_sha256"] == "sha256:" + expected_checksum
    assert binding["file_count"] == len(files)
    assert binding["total_bytes"] == sum(map(len, files.values()))
    prefix = task.source.removeprefix("s3://artifacts/")
    assert {key.removeprefix(prefix) for key in store.objects if key.startswith(prefix)} == set(files)
