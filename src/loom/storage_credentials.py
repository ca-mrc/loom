"""Object-store credentials provider.

A single ``build_s3_client`` factory that honors the ``auth_kind``
discriminator from ``config/loom-schema.toml`` (#250). Every component
that holds an S3 client constructs it through this module — that way
the IRSA path (or eventually Workload Identity, Vault dynamic
credentials, etc.) lights up automatically across the service,
control plane, worker, and CLI without per-call-site changes.

Design
------

The factory takes individual parameters rather than a settings object
because:

- ``LoomServiceSettings``, ``ControlPlaneSettings``, ``WorkerSettings``
  are codegen-produced and structurally similar but not interchangeable
  (different class identities). A Protocol would force every caller
  to import that Protocol; the function signature does the same job
  with less ceremony.
- The CLI staging-catalog caller doesn't have a Settings object at
  all; it builds the client from CLI arguments.

Caller pattern:

.. code:: python

    from loom.storage_credentials import build_s3_client

    client = build_s3_client(
        endpoint_url=settings.minio_endpoint,
        auth_kind=settings.storage_auth_kind,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )

For ``auth_kind="irsa"``, ``access_key`` and ``secret_key`` are
ignored — boto3 walks its standard provider chain.
"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.config import Config

# Implemented authentication strategies for S3-compatible clients.
SUPPORTED_AUTH_KINDS: frozenset[str] = frozenset({"static_keys", "irsa"})


class UnsupportedAuthKindError(ValueError):
    """Raised when ``auth_kind`` is structurally valid in schema but
    not yet implemented in the S3 client factory.

    Catching this separately from ``ValueError`` lets callers
    distinguish "operator misconfigured" (raw ValueError on a totally
    unknown value) from "feature not yet implemented" (caller should
    surface a pointer at the tracking issue).
    """


def build_s3_client(
    *,
    endpoint_url: str,
    auth_kind: str = "static_keys",
    access_key: str | None = None,
    secret_key: str | None = None,
    region: str = "us-east-1",
    config: Config | None = None,
) -> Any:
    """Build a boto3 S3 client per the configured ``auth_kind``.

    ``static_keys`` (default): explicit ``access_key`` + ``secret_key``
    required. Mirrors today's behavior for every existing MinIO
    deployment byte-identically.

    ``irsa``: omit credentials so boto3 walks its standard provider
    chain (env vars → shared credentials → STS
    ``AssumeRoleWithWebIdentity`` via the projected
    ``serviceAccountToken`` on EKS). ``access_key`` and ``secret_key``
    are ignored if passed.

    Other values raise ``UnsupportedAuthKindError`` and list the
    supported strategies.
    """
    if auth_kind == "static_keys":
        if not access_key or not secret_key:
            raise ValueError(
                "auth_kind=static_keys requires access_key + "
                "secret_key. Set LOOM_SVC_MINIO_ACCESS_KEY + "
                "LOOM_SVC_MINIO_SECRET_KEY (or pass them explicitly).",
            )
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(signature_version="s3v4").merge(config or Config()),
        )
    if auth_kind == "irsa":
        # boto3's default provider chain: env vars → shared credentials
        # → AssumeRoleWithWebIdentity (the IRSA path on EKS, via the
        # AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE env vars that the
        # EKS-injected projected serviceAccountToken volume populates).
        return boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            config=Config(signature_version="s3v4").merge(config or Config()),
        )
    raise UnsupportedAuthKindError(
        f"auth_kind={auth_kind!r} not supported by build_s3_client. "
        f"Supported: {sorted(SUPPORTED_AUTH_KINDS)}.",
    )
