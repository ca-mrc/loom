"""Publish a minimal frozen Dockerfile fixture to a disposable object store."""
import hashlib

from loom.service_execution_materialization import ServiceExecutionInputManifestV1


def publish_dockerfile(client, *, bucket, prefix, checksum, dockerfile=b"FROM debian:bookworm\n"):
    if not any(item["Name"] == bucket for item in client.list_buckets()["Buckets"]):
        client.create_bucket(Bucket=bucket)
    manifest = ServiceExecutionInputManifestV1(task_revision_sha256="sha256:" + checksum,
        files=[dict(relative_path="environment/Dockerfile", mode="0644", size_bytes=len(dockerfile),
                    sha256="sha256:" + hashlib.sha256(dockerfile).hexdigest())])
    body = manifest.canonical_bytes()
    client.put_object(Bucket=bucket, Key=prefix + "manifest.json", Body=body)
    client.put_object(Bucket=bucket, Key=prefix + "task/environment/Dockerfile", Body=dockerfile)
    return f"s3://{bucket}/{prefix}task/", {"service_execution_input": dict(
        manifest_uri=f"s3://{bucket}/{prefix}manifest.json",
        manifest_sha256="sha256:" + hashlib.sha256(body).hexdigest(),
        file_count=1, total_bytes=len(dockerfile))}
