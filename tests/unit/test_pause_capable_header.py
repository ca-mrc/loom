"""The pause header is read only at the authenticated dispatch factory."""

from types import SimpleNamespace
from uuid import uuid4

from starlette.requests import Request

from loom.auth import AuthContext
from loom_llm_gateway.dispatch_audit import request_dispatch_audit


def _request(header: str | None, *, attempt: bool = False) -> Request:
    headers = []
    if header is not None:
        headers.append((b"x-loom-pause-capable", header.encode()))
    trial_id = None if attempt else uuid4()
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/openai/v1/chat/completions",
        "headers": headers,
        "app": SimpleNamespace(state=SimpleNamespace(session_factory=None, settings=None)),
        "_loom_dispatch_auth": AuthContext(
            token_hash=b"x" * 32,
            type="step",
            scopes=["llm:call"],
            team_id=uuid4(),
            expires_at=None,
            trial_id=trial_id,
            execution_attempt_id=uuid4() if attempt else None,
            step_id="agent",
        ),
    }
    return Request(scope)


def test_exact_pause_header_marks_an_authenticated_trial() -> None:
    audit = request_dispatch_audit(_request("1"), "facade_openai")
    assert audit is not None and audit.state.pause_capable is True


def test_missing_or_forged_pause_header_stays_off() -> None:
    for header in (None, "", "true", "1 ", "0"):
        audit = request_dispatch_audit(_request(header), "facade_openai")
        assert audit is not None and audit.state.pause_capable is False


def test_pipeline_attempt_does_not_take_the_trial_queue_path() -> None:
    assert request_dispatch_audit(_request("1", attempt=True), "facade_openai") is None


def test_unauthenticated_request_is_not_pause_capable() -> None:
    request = _request("1")
    request.scope.pop("_loom_dispatch_auth")
    assert request_dispatch_audit(request, "facade_openai") is None
