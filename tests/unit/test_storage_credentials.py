"""Unit tests for the storage credentials factory (#251).

Covers the auth_kind dispatch. Each path is exercised against a
captured boto3.client call so we test the WIRE CONTRACT, not just the
arguments — both static_keys and irsa paths must produce boto3
clients with the correct kwargs.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from loom.storage_credentials import (
    SUPPORTED_AUTH_KINDS,
    UnsupportedAuthKindError,
    build_s3_client,
)


def test_static_keys_passes_explicit_credentials() -> None:
    """The static_keys path must pass aws_access_key_id +
    aws_secret_access_key explicitly. Today's deployments depend on
    byte-identical behavior to the pre-factory code."""
    with patch("loom.storage_credentials.boto3.client") as mock_client:
        build_s3_client(
            endpoint_url="http://minio:9000",
            auth_kind="static_keys",
            access_key="ak-123",
            secret_key="sk-456",
            region="us-east-1",
        )
    mock_client.assert_called_once()
    args, kwargs = mock_client.call_args
    assert args == ("s3",)
    assert kwargs["endpoint_url"] == "http://minio:9000"
    assert kwargs["aws_access_key_id"] == "ak-123"
    assert kwargs["aws_secret_access_key"] == "sk-456"
    assert kwargs["region_name"] == "us-east-1"
    # SigV4 is the only signature version MinIO + AWS S3 share; the
    # factory must keep it.
    assert kwargs["config"].signature_version == "s3v4"


def test_static_keys_rejects_missing_credentials() -> None:
    """Better to fail loudly than have boto3 try to discover (which
    would succeed unexpectedly via env vars or instance metadata
    and use the wrong identity)."""
    with pytest.raises(ValueError, match="requires access_key"):
        build_s3_client(
            endpoint_url="http://minio:9000",
            auth_kind="static_keys",
            access_key=None,
            secret_key=None,
        )


def test_static_keys_rejects_empty_access_key() -> None:
    """Empty string credentials are as bad as None."""
    with pytest.raises(ValueError, match="requires access_key"):
        build_s3_client(
            endpoint_url="http://minio:9000",
            auth_kind="static_keys",
            access_key="",
            secret_key="sk-456",
        )


def test_irsa_omits_explicit_credentials() -> None:
    """The IRSA path is identified by the ABSENCE of explicit
    credentials in the boto3.client call. boto3's default chain
    discovers via env vars / shared credentials / STS
    AssumeRoleWithWebIdentity (the EKS IRSA path)."""
    with patch("loom.storage_credentials.boto3.client") as mock_client:
        build_s3_client(
            endpoint_url="https://s3.us-east-1.amazonaws.com",
            auth_kind="irsa",
            region="us-east-1",
        )
    _, kwargs = mock_client.call_args
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs
    assert kwargs["endpoint_url"] == "https://s3.us-east-1.amazonaws.com"
    assert kwargs["region_name"] == "us-east-1"


def test_irsa_ignores_passed_credentials() -> None:
    """If the caller still passes access_key/secret_key under
    auth_kind=irsa (e.g. from a settings object that always has the
    fields), they MUST be ignored — boto3 should discover the IAM
    role, not use a static key that happens to be lying around."""
    with patch("loom.storage_credentials.boto3.client") as mock_client:
        build_s3_client(
            endpoint_url="https://s3.us-east-1.amazonaws.com",
            auth_kind="irsa",
            access_key="stale-static-key",
            secret_key="stale-secret",
        )
    _, kwargs = mock_client.call_args
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs


@pytest.mark.parametrize("auth_kind", ["workload_identity", "sa_json"])
def test_non_s3_auth_kind_is_rejected(auth_kind: str) -> None:
    with pytest.raises(UnsupportedAuthKindError, match="not supported"):
        build_s3_client(
            endpoint_url="https://storage.example.com", auth_kind=auth_kind,
        )


def test_unknown_auth_kind_raises() -> None:
    """Operator misconfiguration — wrong env var value — should
    surface clearly, not get silently treated as one of the known
    paths."""
    with pytest.raises(UnsupportedAuthKindError, match="not supported"):
        build_s3_client(
            endpoint_url="http://minio:9000",
            auth_kind="totally-bogus",
            access_key="x",
            secret_key="y",
        )


def test_supported_auth_kinds_exposed() -> None:
    """Downstream code (the CLI, tests, future docs) needs to
    introspect the supported set."""
    assert SUPPORTED_AUTH_KINDS == frozenset({"static_keys", "irsa"})


@pytest.mark.parametrize("kind", ["irsa", "static_keys"])
def test_admission_can_bound_storage_requests_without_changing_authority(kind):
    from botocore.config import Config
    with patch("loom.storage_credentials.boto3.client") as mock_client:
        build_s3_client(endpoint_url="http://minio:9000", auth_kind=kind,
            access_key="test-access", secret_key="test-secret", config=Config(
                connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1}))
    config = mock_client.call_args.kwargs["config"]
    assert config.signature_version == "s3v4"
    assert config.connect_timeout == 3 and config.read_timeout == 5
    assert config.retries == {"total_max_attempts": 1}
