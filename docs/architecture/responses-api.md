# OpenAI Responses API routing

The LLM Gateway exposes `POST /v1/responses` and
`POST /openai/v1/responses`. Requests authenticated with a provider connection
use that connection's OpenAI-shaped upstream; legacy requests without a
connection use the configured OpenAI API key.

## Provider-connection dispatch

For `openai-compatible` and `custom` connections, the gateway determines
whether the upstream implements `/responses`:

1. A fresh cached `responses_api_supported` value is used directly.
2. A missing or older-than-24-hours result triggers an inline five-second probe
   through the same egress client used by normal traffic.
3. The probe posts a minimal request with a nonexistent sentinel model. Status
   `200`, `400`, or `401` means the route exists; `404`, `501`, a `5xx`, or a
   transport failure means it is unavailable. Other `4xx` responses are
   inconclusive and retain the native path.
4. The result, probe timestamp, and redacted error classification are stored on
   the provider connection. Persistence is best-effort; a failed write causes a
   later request to probe again.

When the route is unavailable, Loom converts the Responses request to Chat
Completions, calls `/chat/completions`, and converts the result back to a
Responses-shaped body. A native request can also fall back when its response
matches the compatibility classifier.

## Authentication and attribution

The normal step-JWT checks apply. The gateway resolves the provider connection
within the authenticated team, decrypts its key server-side, and records the
call with team, trial, execution-attempt, step, model, provider, request
parameters, token usage, retry attempt, cost, and rate-card hash.

API keys and upstream response excerpts are redacted before errors are exposed.
Failed probes do not create LLM-call rows; the translated or native model call
does.

For ordinary calls, a non-JSON or non-object HTTP-success body, or a Chat
fallback body that lacks the required choice, returns `502` and records a failed
call with `upstream_invalid_response`. The record retains the upstream HTTP
status and attempt number with missing usage; it does not establish zero spend.

## Streaming and fidelity

Native Responses bodies and streams are passed through. The Chat compatibility
path maps supported message, tool, tool-choice, content, usage, and streaming
shapes through `responses_chat_compat.py`. It is a compatibility path rather
than a claim that every Responses feature has an equivalent Chat Completions
representation.

For direct OpenAI calls, a successful response without token usage is rejected
with `502` because cost cannot be attributed. Provider-connection calls attach
the connection's current cost-status metadata when exact pricing is
unavailable.

Native SSE streams without a usage-bearing event carry the same missing-usage
metadata as JSON responses without a usage block. Explicit pricing configurations
therefore report unknown cost; a provider-reported zero-token usage block remains
distinguishable from missing usage.
