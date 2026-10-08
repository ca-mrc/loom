# Restricted native Harbor baseline

Native Harbor can use a short-lived, dedicated OpenAI chat bearer without
receiving a provider key or impersonating a Loom Trial. The service creates and
revokes the grant; the existing Gateway decrypts the selected connection's key,
uses its existing egress client, and records a separate baseline subject in
`llm_calls`. Trial and ExecutionAttempt attribution remain exclusive.

## Operator setup

Use the existing authenticated CLI context. Select the exact team, connection
and model before creating a grant. A member needs `submit` to create/revoke and
`read:own` to inspect; an admin must select the target team explicitly. Provider
ownership/sharing is checked both at creation and before each call.

A visible, upstream-present GPT chat model needs an explicitly declared context
length. Discovery alone often omits this. The connection owner or admin with
provider management permission can use:

```http
PATCH /api/v1/provider-connections/CONNECTION/models/gpt-5.4/metadata
Content-Type: application/json

{"context_length":1050000,"specification_url":"https://developers.openai.com/api/docs/models/gpt-5.4"}
```

This changes only the context declaration and its source evidence, preserving
discovery, visibility and existing preflight results. It does not call a model,
claim entitlement, insert a missing model, or silently modify shared metadata.
For another model, verify that model's supplier contract first. The dedicated
endpoint currently qualifies only GPT text Chat Completions with the
`max_completion_tokens` output contract, which includes reasoning tokens.

All usage limits are explicit; there is no paid default or automatic renewal:

```sh
loom harbor-baselines create \
  --provider-connection-id CONNECTION_UUID --model gpt-5.4 \
  --team-id TEAM_UUID --label tb21-native-pilot \
  --ttl-seconds 21600 --max-calls CALL_LIMIT \
  --max-output-tokens OUTPUT_LIMIT --max-total-tokens TOTAL_TOKEN_LIMIT \
  --budget-usd CONFIGURED_PRICE_QUOTA --token-file /private/path/harbor.bearer
loom harbor-baselines show BASELINE_UUID
loom harbor-baselines calls BASELINE_UUID
loom harbor-baselines revoke BASELINE_UUID
```

Choose limits and obtain paid-run authorization before substituting these
placeholders. `--budget-usd` is optional; a configured USD catalog/custom price
is required when it is supplied. Singleton admin credentials also require
`--admin-actor`. The CLI exclusively creates a mode-0600 bearer file and prints
only non-secret metadata. An existing file is never overwritten. The HTTP
create endpoint returns the bearer once; inspect/call-evidence endpoints never
return it.

The Gateway base URL ends in `/harbor-baseline/v1`. Gateway is internal-only;
use the deployment's authorized private access or SSH/port-forward path. Do not
expose another listener or forward a provider credential. Provide the dedicated
bearer to native Harbor through `OPENAI_API_KEY`, and the base URL through
`--ak api_base=...`. Avoid placing the bearer in argv, `llm_kwargs`, job config,
trajectory or logs. Pin the same explicit `max_completion_tokens` and
`reasoning_effort` on the Loom and Harbor sides. Native Harbor accepts these as
`--ak 'llm_kwargs={"max_completion_tokens":8192}'` and
`--ak reasoning_effort=high`; the shown 8192 is an example, not a pilot default.
Use one concurrent native trial per bearer (`-n 1`); the grant admits one
in-flight model request at a time. Freeze the model, task package and agent
settings separately in the comparison protocol.

## Limits and evidence

Before provider I/O, a database row lock reserves one call, the entire declared
model context plus the requested output ceiling, and its conservative price
under the current configured rate card. A successful response with complete,
bounded usage releases unused token/cost reservation; the call remains counted.
The price basis, including catalog revision, is retained with each call. An
insufficient remaining balance rejects the next request before provider I/O,
even if its likely prompt would be smaller. The legacy `max_tokens` field is
normalized to `max_completion_tokens`; supplying both is rejected.

The optional USD value is a **configured-price quota, not a supplier invoice
cap**. It does not model unlisted supplier fees, price tiers or incorrect
supplier metadata. Token limits likewise depend on the supplier honoring the
declared context and output contract. An observed overrun blocks further calls
and retains the reservation; it cannot undo an upstream charge already made.
Unpriced usage is identified as tokens-only/unavailable, never asserted to be a
verified free call. No token estimate is used to claim a monetary hard cap.

Timeouts, missing/malformed usage, upstream HTTP failures and cancellation retain
the full reservation and block the grant. A process crash leaves its durable
in-flight reservation closed to new dispatches. These are infrastructure,
authorization or budget failures to retain in the paired acceptance analysis;
they are not benchmark reward zero and must not be dropped. Inspect the
baseline's dispatch/call evidence alongside Harbor artifacts, diagnose the
failure, then decide explicitly whether another grant is warranted. Expiry and
revocation prevent new calls; an already accepted call may finish and settle.

The gateway rejects streaming, tools, image/audio inputs, multiple completions,
arbitrary extra fields and provider/model overrides. A baseline bearer cannot
use Trial gateway routes or ordinary service APIs. There is no trial creation,
lease bypass, credential export, automatic retry, renewal or background monitor.

## Local verification

The HTTP tests use disposable PostgreSQL and the real encrypted SecretStore,
with a mocked provider. Set `LOOM_HARBOR_SMOKE_PYTHON` to an interpreter with the
pinned Harbor revision to opt into real Harbor/LiteLLM request construction:

```sh
uv run --no-sync pytest tests/integration/test_harbor_baseline.py \
  tests/loom_cli/test_harbor_baselines_cmd.py \
  tests/unit/terminus2/test_runtime_loop.py
```

The native smoke calls only the loopback Gateway and mocked provider. It also
reproduces Harbor's duplicate `reasoning_effort` constructor failure and verifies
the corrected top-level argument. These checks prove the integration contract;
they do not constitute paid TB2.1 accuracy acceptance.
