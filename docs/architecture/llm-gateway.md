# LLM Gateway

Service-mode-only FastAPI process that proxies all upstream LLM calls
so every trajectory carries faithful token usage + cost attribution
the host doesn't have to reconstruct.

CLI mode skips the Gateway entirely — `UpstreamDirectGatewayClient`
in `loom_cli/upstream_gateway.py` re-implements the same dispatch
contract against provider SDKs in-process. See [`cli-mode.md`](cli-mode.md).

## Responsibilities

1. **Multi-dialect routing** — accept the *same* logical request and
   forward via the right native dialect (`/v1/messages` for
   Anthropic, `/v1/chat/completions` + `/v1/responses` for OpenAI,
   `:generateContent` for Gemini). Other adapters (incl. LiteLLM)
   for tail providers go through `loom_llm_gateway.litellm_wrapper`.
2. **Token-usage extraction** — every dialect puts cache + reasoning
   counters in a different shape; `DialectAdapter` normalises them
   into a `TokenUsage` dataclass with a verbatim `provider_extras`
   dict.
3. **Cost attribution** — looks up the rate card for `(provider,
   model)` at request time, computes `cost_usd` from the extracted
   `TokenUsage`, writes a row to the `llm_calls` table with
   `(team_id, trial_id, step_id)` keys so any aggregation slices
   cleanly.
4. **Request-parameter audit** — stores a redacted normalized
   `request_params` JSON object with non-sensitive generation
   controls such as `temperature`, `top_p`, `seed`, max output limits,
   reasoning effort, tool-choice mode, and provider decoding extras.
   Prompt bodies, messages, headers, API keys, and raw credentials are
   omitted.
   Service-mode `TrialConfig.request_params` is the user-facing input
   for those controls on the `direct-completion` worker path; the worker filters
   that object through the same allowlist before forwarding it to the
   Gateway.
5. **Raw provider handoff logs** — provider-facade success paths also
   store a redacted raw request/response record under
   `llm_calls.provider_extras._loom_raw_provider_log` for production
   trajectory exports. Unlike `request_params`, this intentionally keeps
   prompt and assistant payloads for downstream training/audit bundles,
   but it redacts bearer values, provider API keys, secret-looking
   fields, and known secret text before persistence. The stable object
   ref is `llm_calls/<id>/provider_extras/_loom_raw_provider_log`.
6. **Bearer auth + per-team RPM** — every call carries a team
   token; the Gateway gates RPM per `(team, provider)` and rejects
   when over.
7. **License allowlists** — task-level allow/deny of model ids; the
   Gateway denies a request if its `model` field isn't in the team's
   allowlist.

## Retained Pipeline dispatch accounting

Pipeline execution-attempt calls retain a separate durable dispatch ledger.
Before any upstream call, the Gateway reserves the Attempt's remaining provider
cost bound and a request slot, then commits `dispatched` before opening the
network request. Settlement correlates one `LLMCall` with the dispatch and updates
both the Attempt budget and run ledger in the same transaction.

Every 15 seconds, the Gateway sweeps up to 100 old dispatches, using the configured
upstream timeout (at least one second) plus a five-second grace period. Age starts
at creation for `reserved` rows and at send marking for `dispatched` rows. A
reserved row proves no upstream send: recovery releases its allowance and cost
as `not_dispatched` without creating an `LLMCall`. A dispatched row has an uncertain
provider outcome: recovery charges its full reserved cost as `uncertain` and
creates one correlated `LLMCall`. Replays cannot send or settle a recovered row
again. Recovery and send marking serialize on database row locks; recovery skips
Attempts and dispatches currently locked by live senders or worker cleanup.
It also skips a busy run ledger inside the candidate's savepoint. Ordinary
settlement acquires the ledger without waiting while it holds an Attempt lock;
on contention, it rolls back first, waits for the ledger alone for at most one
second, and retries the whole transaction. It makes at most three attempts. A
wait timeout or exhausted retries raises `provider_dispatch_accounting_busy`
with accounting unchanged for later settlement or recovery. The existing Responses
HTTP boundary returns `502` when an upstream result cannot be durably accounted;
replaying its dispatch does not authorize another upstream send. These boundaries
accommodate both worker cleanup's Attempt-before-ledger order and terminal-cause
cancellation's ledger-before-Attempt order; they do not change either authority.

Cancellation and worker-loss cleanup preserve dispatch-owned reservations for
ordinary Gateway settlement or this sweep. Accounting can therefore converge
after worker cleanup, and run terminal projection continues to wait for active
reservations. The sweep isolates recognized accounting drift in a per-candidate
savepoint, logs the dispatch ID and stable error category, and lets healthy
candidates in the same bounded batch progress. It leaves corrupt accounting
unchanged for investigation; unrelated failures still roll back the sweep.

These retained ledger guarantees are tested against disposable PostgreSQL. They
do not enable the retired hosted Pipeline execution path or establish live
provider or deployment acceptance.

## Deadline dispatch audit

Ordinary Trial requests have a non-billing `gateway_dispatch_receipts` audit
alongside, not in place of, `llm_calls` (#1858). After normal authentication,
provider authorization and routing, the Gateway commits a receipt before each
direct upstream send. It rechecks the signed attempt deadline after that commit
and before transport. Audit admission failure returns a secret-free
`503 / dispatch_audit_unavailable` and sends nothing upstream.
Admission failure logs contain the opaque request ID and only a fixed error
category (`timeout`, `database`, or `other`), never driver messages, SQL binds,
or exception tracebacks. The category diagnoses failure without relaxing admission.
Observation failure logs use the same fixed categories and identify the
`provider` or `gateway` write and its unfinished phase (`session`, `execute`,
`commit`, or `session_exit`). The execute phase includes connection checkout;
these fields locate the failed boundary without establishing a physical cause.
The existing one-second audit budget and durable unknown outcomes still apply.

The internal join is `trial_id / step_id / agent_attempt_id / step_jwt_id`.
The worker supervisor creates a new opaque attempt UUID for each real agent
retry; CP returns a new signed grant UUID (`jti`) for each mint. The worker
records `agent_attempt_start` and `agent_step_token_grant` in its audit spine
before provider work. This is a mint-time binding under the existing CP
authorization, **not** a new scheduler, an idempotent mint API, or perpetual
claim/lease authority. The legacy worker lane still has its existing
worker-scope and Trial/team checks; protected lanes retain their existing
claim fencing. Older tokens without these UUIDs remain explicitly unjoined.
Neither `Trial.attempt_count`, Gateway retry `attempt`, nor Harbor reclaim
tenure is the internal agent-attempt identity. CLI local-only issuers do not
invent CP grants.

The rollout must migrate the application database and upgrade CP before new
workers request the additive attempt-identity fields. A new worker against an
old CP fails closed with request validation failure; it must not retry without
identity. Older workers can use the new CP, but their missing attempt UUID is
still legacy/unjoined and cannot satisfy the fixed-candidate canary contract.

Receipts carry a server-generated request UUID and a unique per-request
dispatch ordinal. Gateway-internal retries get separate receipts and retain
their existing one-based retry counter. `purpose=capability_probe` separates
Responses endpoint probing from model work. `purpose=adapter_call` identifies
one LiteLLM invocation, not its opaque SDK-internal physical HTTP retries;
direct httpx calls use `model_call`. Pipeline execution-attempt Responses
keeps its existing budget reservation/dispatch/settlement authority and is not
duplicated into this ordinary-Trial path. Non-step legacy body-attributed
requests are not upgraded to trusted Trial identity from their payload.

Two observations must be read independently:

| Observation | Meaning |
| --- | --- |
| `provider_outcome=admitted` | Durable permission to try sending; completion is unknown, not proof the provider received bytes. |
| `not_dispatched` | The deadline/cancellation prevented send after admission. |
| `response_received` / `stream_completed` | A response or the complete stream was observed, not proof of task success. Later caller/accounting failure must not overwrite this fact. |
| `deadline` / `cancelled` / `transport_error` | Transport ended without complete observation; provider completion and cost may remain unknown. |
| `gateway_outcome`, `gateway_http_status` | Separate caller-side completion, error, cancellation or observed deadline. SSE headers can already be HTTP200 when the stream subsequently expires. |

Terminal updates are conditional and use independent sessions with a one-second
audit-write bound; provider executable time is never extended for persistence.
There are no detached, indefinitely shielded finalizers. A process crash,
commit-ack loss, database outage or repeated cancellation can leave an
`admitted`/`pending` observation; a bounded, secret-free log identifies its
request (and admission receipt where available). Operators must report it as
unknown and retain that evidence, not infer zero cost or fabricate completion.
Rows follow the existing authorized Trial/team deletion lifecycle.

No JWT, request content, credentials, private endpoint or new content digest is
stored in these receipts or forwarded to third-party providers. Existing
successful `llm_calls` usage/cost writes remain the accounting authority;
receipts do not become typed model/terminal execution events. They cannot
repair the historical successful-provider-call/typed-event gap owned by #743.
Local HTTP/Postgres regression evidence does not close #1748: #1857 still owns
the formally isolated, installed Case A/B collection and live acceptance.

## Endpoint shape (dialects)

| Route                    | Dialect             | Forwarded to                       |
|--------------------------|---------------------|------------------------------------|
| `POST /v1/messages`      | Anthropic native    | Direct httpx passthrough using the Gateway's configured Anthropic credential |
| `POST /v1/chat/completions` | OpenAI Chat / BYO chat | Provider connection resolved from the authoritative step-JWT claim, with any header/body values required to match; `openai-compatible` / `custom` use `EgressClientPool`, while native Anthropic / Google connections use their LiteLLM transport |
| `POST /v1/responses`     | OpenAI Responses / BYO OpenAI-compatible Responses | Native OpenAI path or direct httpx through `EgressClientPool` when the step JWT or `x-loom-provider-connection-id` resolves to `openai-compatible` / `custom`; if a BYO endpoint exposes only Chat Completions and returns the chat-style "messages required" 400 from `/responses`, the provider facade retries once through `/chat/completions` and synthesizes a Responses body/SSE for Codex |
| `POST /v1beta/models/{model}:generateContent` | Gemini  | Direct httpx passthrough using the Gateway's configured Google credential; streaming actions are rejected |
| `POST /v1/chat/completions` *(via litellm)* | Tail-provider shim | non-BYO and non-OpenAI-compatible fallback providers |
| `POST /openai/v1/chat/completions`, `/openai/v1/responses`, `/anthropic/v1/messages`, `/google/v1beta/...` | Sandbox provider facade | Direct httpx through `EgressClientPool` |

### Provider-connection authority

`POST /v1/chat/completions` resolves an optional provider connection across the
step JWT, `x-loom-provider-connection-id` header, and
`loom.provider_connection_id` body field. A provider claim in the verified
step JWT is authoritative. Every non-empty header or body value must match that
claim; a mismatch is rejected before provider lookup or upstream dispatch. For
legacy non-JWT callers, header and body are equivalent transports and must
match when both are present. When all three sources are empty, the request uses
the platform-credentialed path.

The family-run orchestrator does not choose a team by writing `loom.team_id`
and does not use its long-lived credential at the Gateway. For every
`skill_patcher_llm` call it presents its dedicated, teamless family-orchestrator
worker credential (scoped only to `family:evolve`) to Control Plane
`/admin/step-tokens`, explicitly supplies the
configured evolver provider (including null), and receives a short-lived
`llm:call` step JWT bound to the real completed trial, its represented team,
`step_id="family_evolver"`, and the authorized provider. The Control Plane
loads the trial team and repeats the provider owner/share check before minting
the JWT. The Gateway then authorizes provider lookup against the JWT team and
rejects caller-controlled attribution or routing that disagrees with it.

Provider API keys and secret references remain inside the SecretStore/provider
connection boundary. Family-run adapter parameters containing secret-like keys
fail closed before persistence, and Gateway failures must not log or echo
credentials, authorization values, secret references, or decryption exception
text.

The Gateway is **not** a single unified shape — each dialect's
response is forwarded verbatim. Agents see exactly the native
upstream shape, so client-side parsing stays standard. The
normalisation (TokenUsage → cost) happens *next to* the forward, not
*after* it.

Streaming is supported on the OpenAI Responses routes, the OpenAI Chat
provider facade, and on both Anthropic Messages routes. The Gateway accepts
`stream=true` on `/v1/messages`, `/v1/responses`, `/openai/v1/responses`,
`/openai/v1/chat/completions`, and `/anthropic/v1/messages`. Responses and
Anthropic routes forward native SSE streams and record usage from the terminal
usage-bearing SSE event. The OpenAI Chat provider facade preserves usage by
calling the upstream OpenAI-compatible `/chat/completions` endpoint with
`stream=false`, recording usage from the full JSON response, and returning a
synthetic OpenAI `chat.completion.chunk` SSE stream to streaming clients such
as opencode or aider. Other native routes (`/v1/chat/completions`) still reject
`stream=true` because their cost path expects the full JSON body.

For provider connections that are OpenAI-chat-compatible but not
Responses-compatible, the Responses facade has a narrow compatibility
fallback. It triggers only after an upstream `/responses` call returns
the chat-only missing-`messages` signature. The fallback converts the
Responses `instructions`/`input`/function-tool shape to a
non-streaming `/chat/completions` request, then converts the chat
message or tool calls back into Responses JSON or SSE. Unsupported
Responses-only item types are not treated as first-class ground truth;
operators should prefer a native `/responses` provider for production
score evidence when one is available.

## Dispatch contract

```
client                       Gateway                upstream
  │  POST /v1/messages          │
  │  Bearer <team_token>        │
  │  X-Loom-Trial: <uuid>       │
  │  X-Loom-Step:  <step>       │
  ├────────────────────────────►│
  │                             │  verify_bearer_token
  │                             │  rate-limit per (team, provider)
  │                             │  license-allowlist gate
  │                             │
  │                             │  messages.create(...)
  │                             ├──────────────────────►│
  │                             │◄──────────────────────│
  │                             │
  │                             │  DialectAdapter.extract(response) → TokenUsage
  │                             │  RateCardTable.lookup(provider, model)
  │                             │  compute_cost_usd(usage, rate) → cost_usd
  │                             │  llm_calls.insert(team_id, trial_id, step_id,
  │                             │                   input_tokens, output_tokens,
  │                             │                   provider_extras, request_params,
  │                             │                   cost_usd, ...)
  │                             │
  │                             │  forward response verbatim
  │◄────────────────────────────│
```

Trial + step IDs ride as request headers (`X-Loom-Trial`,
`X-Loom-Step`) because the wire response is the unmodified upstream
shape — there's nowhere to inject them into the body without
breaking client parsing. Workers populate the headers from
`bind_trial_context` (see [`cli-mode.md`](cli-mode.md) for the
contextvars helper).

For `direct-completion` service-mode trials, callers may include
`trial_config.request_params` with safe generation controls, for
example:

```json
{
  "agent_name": "direct-completion",
  "agent_model": {"provider": "openai", "name": "gpt-4o-mini"},
  "request_params": {
    "temperature": 0,
    "top_p": 0.5,
    "seed": 1234,
    "extra_body": {"top_k": 40}
  }
}
```

The worker drops prompt/message payloads, headers, credentials, and
unknown provider fields before the request is sent. The Gateway then
records the effective non-sensitive controls in `llm_calls.request_params`
and the trial/batch debug evidence surfaces.

Provider-facade raw handoff logs are a separate export contract from
`request_params`. They live in `provider_extras` so the normal debug
surfaces can continue to show only safe generation controls, while
`loom eval batch delivery-bundle --mode raw-harbor` can package the
redacted full request/response bodies into `provider_logs/`.
Versioned training handoff profiles such as
`--mode raw-harbor-tb2-v1` reconstruct SFT rows from those provider logs
plus reward/metrics joins; the Loom event stream remains an audit spine rather
than the SFT source of truth for that profile. For `terminus-2` trials,
`--mode raw-harbor-tb2-v2` projects execution from typed `terminus2_*` events
and embeds hash-verified native Harbor checkpoint artifacts.
See [`terminus2-runtime.md`](terminus2-runtime.md).

Codex subprocess alignment runs use a separate adapter-specific path
because Codex constructs its own Responses request. The worker converts
sanitized per-trial `trial_config.request_params` into
`LOOM_CODEX_SETTINGS_JSON` for Codex subprocesses; deployment-level
`LOOM_CODEX_SETTINGS_JSON` remains only a fallback. The Codex launcher
sanitizes the JSON again and encodes it as
`model_providers.loom.query_params.loom_request_params`. The Responses
facade accepts that namespaced query param, sanitizes it again, merges
the safe controls into the upstream provider payload, and records the
same merged payload through the normal request-parameter audit.

## Central gateway boundary

Service-mode agents call providers through the Gateway rather than writing
provider usage directly to their trajectories. The boundary provides:

1. **Honesty** — agents are subprocesses that crash, misreport, or
   silently swallow errors. A Gateway-side count is observably
   correct even if the agent process disappears mid-call.
2. **Attribution** — `(team, trial, step)` cost slicing needs a
   *single* writer to the `llm_calls` table. With per-agent writes,
   teams have to trust every adapter to spell the keys right.
3. **Policy** — rate limits and per-team budget accounting need a chokepoint.
   Gateway is that chokepoint.

This adds one network hop and requires the service stack. CLI mode runs the
same `Trial.run()` without that stack and therefore has weaker centralized
attribution.

## What `litellm_wrapper` is for

LiteLLM provides a single `acompletion(model=..., ...)` call that
dispatches to ~100 providers. Loom uses it as the **tail-provider
adapter** — anything not covered by Anthropic / OpenAI Chat /
Responses / Gemini routes through LiteLLM. BYO OpenAI-compatible
connections are also excluded from LiteLLM on the service-mode gateway
path for both Chat Completions and Responses: the route owns that wire
shape and forwards with the pooled httpx client from `EgressClientPool`,
so Envoy sees the
`x-loom-connection-id` CONNECT header for the selected
`provider_connection_id`. The top-four providers and BYO
OpenAI-compatible dispatch bypass LiteLLM so:

- Native dialect responses are verbatim — LiteLLM's normalised
  response shape would obscure provider-specific fields.
- Token-usage extraction remains per-dialect, preserving fields such as
  `cache_creation_input_tokens` through the provider-specific path.
- LiteLLM does not provide a stable per-call hook for proxy CONNECT
  headers; the egress proxy's per-connection allowlist depends on that
  header.

## Failure modes + behavior

| Failure                                | Response                              |
|----------------------------------------|---------------------------------------|
| Upstream timeout (>200s)               | 504, failed-attempt `llm_calls` row inserted with zero tokens and `provider_extras._loom_failure_category=upstream_timeout` |
| Upstream non-2xx                       | Forwarded or surfaced with a redacted excerpt; failed-attempt `llm_calls` row inserted with zero tokens and `provider_extras._loom_failure_category=upstream_http_4xx` or `upstream_http_5xx` |
| LiteLLM adapter exception or malformed upstream response | 502 with sanitized diagnostic text; failed-attempt `llm_calls` row inserted with `provider_extras._loom_failure_category=upstream_transport`; provider API keys and `Authorization: Bearer` values are redacted before logs or responses |
| BYO `/responses` endpoint is actually chat-only | One fallback POST to `/chat/completions`; success returns synthetic Responses JSON/SSE and records normal `openai_responses` usage from chat `usage` tokens |
| Rate-card missing                      | 422 `RateCardNotFoundError`; no row inserted |
| Bearer invalid / over RPM              | 401 / 429; no upstream call          |
| `stream=true`                          | Allowed for OpenAI Responses, OpenAI Chat provider-facade, and Anthropic provider-facade calls; other v1 dialect paths reject it where final usage cannot be attributed |
| Model not in team allowlist            | 403; no upstream call                 |

A failed call still produces an `llm_calls` row when the upstream
was contacted or the provider client attempted transport. The row
uses `input_tokens=0`, `output_tokens=0`, `cost_usd=0`, and
`rate_card_hash=failed-upstream`; `provider_extras` carries
`_loom_call_status=failed`, `_loom_failure_category`, and optional
status/error metadata. Failed rows also carry
`_loom_usage_status=missing` because no successful provider usage block
was observed. This makes debug evidence distinguish "no request
attempted" from "request attempted and failed upstream" while keeping
prompt bodies, messages, headers, bearer tokens, API keys, and
credentials out of persistence. Pre-upstream failures (auth, rate-card,
license) leave no row. Subprocess agents may surface some transport drops only
as stderr text after the SDK exits; the worker classifies those terminal
messages as `provider_transport_disconnect`, applies the trial retry policy,
and exposes that distinct reason in debug evidence instead of grouping them
with generic `internal_error` or agent logic failures.

## Persistence schema

`llm_calls` columns of note:

| Column           | Notes                                                    |
|------------------|----------------------------------------------------------|
| `team_id`        | from bearer token                                        |
| `trial_id`       | from `X-Loom-Trial`                                      |
| `step_id`        | from `X-Loom-Step`                                       |
| `dialect`        | route/dialect label such as `openai_chat`, `openai_responses`, `anthropic`, or `gemini` |
| `model`          | exact model id the request used                          |
| `input_tokens`   | from DialectAdapter                                      |
| `output_tokens`  | from DialectAdapter                                      |
| `provider_extras`| JSONB — cache + reasoning counters verbatim on success; `_loom_call_status=failed` plus failure metadata on failed upstream attempts |
| `request_params` | JSONB — redacted normalized generation controls; `NULL` means the data is unavailable |
| `cost_usd`       | computed at insert; cached for usage metrics/audits     |
| `rate_card_hash` | rate-card table hash or sentinel such as `failed-upstream` |
| `attempt`        | gateway-internal attempt number that produced the success or final failed response |

The frozen `cost_usd` column is a fast index for `/api/v1/usage` and
Gateway cost metrics; trial and batch detail responses expose token
totals, call counts, request-parameter summaries, and failed-call
status counts for local debugging.

## See also

- [`cost-and-rate-cards.md`](cost-and-rate-cards.md) — the rate-card
  model + cost formula
- [`service-mode.md`](service-mode.md) — where the Gateway sits in
  the cluster
- [`cli-mode.md`](cli-mode.md) — the CLI's in-process replacement
