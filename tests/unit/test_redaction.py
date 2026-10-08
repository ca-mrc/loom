from __future__ import annotations

import hashlib
import logging

import pytest

import loom.security.redaction as redaction
from loom.security.redaction import (
    RedactionDecision,
    contains_secret_like_content,
    redact_mapping,
    redact_text,
)


def test_redact_text_covers_staging_secret_shapes() -> None:
    raw = (
        "Authorization: Bearer loom_api_abcdefghijklmnopqrstuvwxyz012345 "
        "Cookie: loom_session=loom_session_secret123; loom_csrf=loom_csrf_abc "
        "invite=loom_invite_invitationsecret "
        "external=loom_baseline_dedicatedsecret "
        "provider=sk-live-super-secret "
        "hf=hf_abcdefghijklmnopqrstuvwxyz1234567890 "
        "signed=https://minio.internal:9000/artifacts/team/trial/out.txt?"
        "X-Amz-Signature=abcdef&X-Amz-Credential=minio%2F20260622 "
        "ref=loom://provider-connection/123e4567-e89b-12d3-a456-426614174000 "
        "internal=http://loom-control-plane:8080/trials "
        "database=postgresql://dev-role:database-secret@postgres.internal/dev"
    )

    redacted = redact_text(raw)

    for leaked in (
        "loom_api_abcdefghijklmnopqrstuvwxyz012345",
        "loom_session_secret123",
        "loom_csrf_abc",
        "loom_invite_invitationsecret",
        "loom_baseline_dedicatedsecret",
        "sk-live-super-secret",
        "hf_abcdefghijklmnopqrstuvwxyz1234567890",
        "X-Amz-Signature=abcdef",
        "X-Amz-Credential=minio%2F20260622",
        "loom://provider-connection/123e4567-e89b-12d3-a456-426614174000",
        "http://loom-control-plane:8080/trials",
        "database-secret",
    ):
        assert leaked not in redacted
    assert "[REDACTED:" in redacted


def test_redact_mapping_preserves_shape_but_redacts_sensitive_values() -> None:
    payload = {
        "status": "failed",
        "headers": {
            "authorization": "Bearer loom_admin_supersecret",
            "x-loom-csrf": "loom_csrf_supersecret",
        },
        "provider": {
            "api_key": "sk-provider-secret",
            "base_url": "https://api.openai.com/v1",
        },
        "errors": [
            {
                "detail": (
                    "upstream 403 from http://loom-llm-gateway:9100/chat for key sk-provider-secret"
                ),
            },
        ],
    }

    redacted = redact_mapping(payload)

    assert redacted["status"] == "failed"
    assert redacted["provider"]["base_url"] == "https://api.openai.com/v1"
    assert redacted["headers"]["authorization"] == "[REDACTED:authorization]"
    assert redacted["headers"]["x-loom-csrf"] == "[REDACTED:x-loom-csrf]"
    assert redacted["provider"]["api_key"] == "[REDACTED:api_key]"
    assert "sk-provider-secret" not in str(redacted)
    assert "loom-llm-gateway" not in str(redacted)


def test_contains_secret_like_content_blocks_shared_artifacts() -> None:
    safe = b"reward: 0.82\nsummary: solved with public benchmark data\n"
    unsafe = (
        b"OPENAI_API_KEY=sk-artifact-secret\n"
        b"curl -H 'Authorization: Bearer loom_api_artifactsecret'\n"
    )

    assert contains_secret_like_content(safe) == RedactionDecision(
        status="shared",
        reason=None,
    )
    assert contains_secret_like_content(unsafe) == RedactionDecision(
        status="blocked",
        reason="secret-like content detected",
    )


def test_redact_environment_mapping_uses_digest_fingerprints_for_secret_keys() -> None:
    token = "loom_api_batch_runner_synthetic_token_value_123"
    password = "synthetic-db-password"
    env = {
        "LOOM_TEST_APIKEY": "synthetic-api-key",
        "LOOM_TEST_PUBLIC_BASE_URL": "https://loom.example.test",
        "LOOM_TEST_SERVICE_PASSWORD": "synthetic-service-password",
        "LOOM_TEST_SVC_BATCH_RUNNER_CP_TOKEN": token,
        "LOOM_TEST_DATABASE_URL": f"postgresql://loom:{password}@db/loom",
        "LOOM_TEST_MONKEY_PATCH": "visible-non-secret",
    }

    assert hasattr(redaction, "redact_environment_mapping")
    entries = {
        entry.name: entry.to_json()
        for entry in redaction.redact_environment_mapping(env, prefixes=("LOOM_TEST_",))
    }

    token_entry = entries["LOOM_TEST_SVC_BATCH_RUNNER_CP_TOKEN"]
    apikey_entry = entries["LOOM_TEST_APIKEY"]
    password_entry = entries["LOOM_TEST_SERVICE_PASSWORD"]
    db_entry = entries["LOOM_TEST_DATABASE_URL"]
    public_entry = entries["LOOM_TEST_PUBLIC_BASE_URL"]
    monkey_entry = entries["LOOM_TEST_MONKEY_PATCH"]

    assert token_entry["value"] == "[REDACTED]"
    assert token_entry["fingerprint"] == f"sha256:{hashlib.sha256(token.encode()).hexdigest()[:12]}"
    assert token_entry["length"] == len(token)
    assert token not in str(token_entry)
    assert token[:12] not in str(token_entry)

    assert db_entry["value"] == "[REDACTED]"
    assert db_entry["fingerprint"].startswith("sha256:")
    assert password not in str(db_entry)
    assert apikey_entry["value"] == "[REDACTED]"
    assert "synthetic-api-key" not in str(apikey_entry)
    assert password_entry["value"] == "[REDACTED]"
    assert "synthetic-service-password" not in str(password_entry)

    assert public_entry["value"] == "https://loom.example.test"
    assert public_entry["fingerprint"] is None
    assert monkey_entry["value"] == "visible-non-secret"


def test_redacted_environment_entries_are_safe_for_logs_and_exceptions(
    caplog: pytest.LogCaptureFixture,
) -> None:
    raw_token = "loom_api_log_exception_synthetic_token_value_123"
    entries = redaction.redact_environment_mapping(
        {
            "LOOM_TEST_BATCH_RUNNER_CP_TOKEN": raw_token,
            "LOOM_TEST_MODE": "staging",
        },
        prefixes=("LOOM_TEST_",),
    )

    logger = logging.getLogger("loom.tests.redaction")
    with caplog.at_level(logging.INFO):
        logger.info("env diagnostics: %s", entries)

    try:
        raise RuntimeError(f"env diagnostics failed: {entries}")
    except RuntimeError as exc:
        exception_text = str(exc)

    combined = caplog.text + exception_text
    assert "LOOM_TEST_BATCH_RUNNER_CP_TOKEN" in combined
    assert "[REDACTED]" in combined
    assert "sha256:" in combined
    assert raw_token not in combined
    assert raw_token[:12] not in combined
