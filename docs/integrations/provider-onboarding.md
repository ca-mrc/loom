# Provider Onboarding

Loom supports two provider setup paths in service mode:

1. A hosted third-party API where the user already has a provider URL and API
   key.
2. A self-hosted checkpoint on a GPU cluster, exposed as an OpenAI-compatible
   HTTP service and then registered in Loom.

Both paths end in the same Loom object: a team-scoped provider connection. Team
owners create it, Loom encrypts the API key at rest, the LLM Gateway uses it
server-side, and users select the connection plus model id when creating a
batch.

Loom does not host model inference for teams. The deployed platform runs
the evaluation, gateway, scheduling, storage, and monitoring surfaces; the
model-serving endpoint is supplied and operated by the team, whether it is a
third-party API or a self-hosted OpenAI-compatible service such as vLLM.

## Credential retention

Rotating an API key changes the connection's active encrypted secret reference.
Deleting a connection disables and soft-deletes it while retaining attribution
for historical results. Both actions start a durable 24-hour grace period;
the service then reclaims the encrypted value once no active provider or retained
historical consumer references it. Existing soft-deleted connections get a fresh
grace period when discovered. Historical rotation orphans without a retained
owner remain stored because their ownership cannot be established safely.
See [provider key retirement](../architecture/service-mode.md#provider-api-key-retirement)
for collection limits and concurrency guarantees.

Loom does not revoke keys at the inference provider. Revoke superseded keys
through that provider once in-flight work no longer needs them.

## Hosted Third-Party API

Use this path for OpenAI-compatible hosts such as Together, Fireworks, vLLM
behind your own ingress, and provider-native endpoints such as Anthropic or
Google when those types are enabled.

1. In the web app, switch to the team that will own execution, cost, provider
   credentials, members, and API tokens.
2. Open **Providers** -> **New provider connection**.
3. Choose the provider type.
4. Enter a short display name.
5. Enter the provider API root:
   - OpenAI-compatible services should end in `/v1`, for example
     `https://api.together.xyz/v1`.
   - Anthropic and Google should use their documented API root.
6. Paste the API key into the password field. Do not paste keys into issue
   comments, browser URLs, docs, or chat transcripts.
7. Leave **Allowed models** blank to allow all discovered agent-capable models,
   or enter one model id per line to restrict the picker.
8. Create the connection.
9. Open the provider detail page and click **Test connection**.
10. Open **Models**, click **Refresh**, and hide noisy non-chat entries if the
    upstream catalog includes embeddings, rerankers, or tool-only models.
11. Click **Preflight** for the model you plan to use. Refresh only proves the
    upstream advertises a model; preflight sends one minimal generation request
    and records whether this connection/key can actually call it.
12. Open **New batch**, choose this provider connection, choose a discovered
    model, and submit a small smoke batch before starting large runs.

The CLI path uses secret indirection so keys do not land in shell history:

```bash
export LOOM_PASSWORD=...
loom auth login --server https://loom.example.com --username USER --password env:LOOM_PASSWORD

export PROVIDER_API_KEY=...
loom providers create \
  --name together-prod \
  --type openai-compatible \
  --base-url https://api.together.xyz/v1 \
  --api-key env:PROVIDER_API_KEY \
  --pricing-mode usage_only

loom providers test together-prod
loom providers models together-prod --refresh
loom providers models together-prod --preflight gpt-4o-mini
loom providers models together-prod
```

For catalog or per-model custom estimates, see [Provider pricing](../architecture/cost-and-rate-cards.md#provider-connection-pricing). Protocol compatibility does not identify a supplier.

The Providers list, New provider form, provider detail Overview, Models tab,
and New Batch page show the same hosted-API quickstart and smoke-batch snippets
inline. Prefer copying from the page when you are working against a specific
public server, because those snippets derive the current server URL.

Common failures:

- `401` or `403`: the provider key is invalid, expired, missing entitlement for
  that model, or not propagated yet after rotation.
- Invalid base URL: use the API root, not a provider dashboard URL. For
  OpenAI-compatible services the root should end in `/v1`.
- SSRF or private endpoint denial: expose the service through an approved
  public/tunnel path. If the endpoint uses a non-standard port such as a GPU
  cluster bastion forward, ask an operator to add a
  `provider_egress_allowlist` entry before testing the connection. Loom
  re-checks the current DNS/IP policy before connection tests, model refresh,
  and model preflight, so a hostname that later resolves to loopback,
  link-local, metadata, or an unapproved private range is blocked before any
  provider request is sent.
- Empty model list: run `loom providers models NAME --refresh`; if the endpoint
  does not implement useful discovery, add model ids manually from the web
  Models tab or the provider model API.
- Model missing from the picker or batch submission: batch creation rejects
  provider model ids that are absent from the connection's cached model catalog.
  Refresh, add the id manually, or choose a cached model before submitting.
- Model advertised but not runnable: run
  `loom providers models NAME --preflight MODEL`. A failed preflight is stored
  on that connection/model and New Batch warns before submit; the API rejects
  new batches for models with a known failed preflight until the model passes
  or another model is selected.
- Noisy model list: hide non-agent models in the Models tab.

### Harness Compatibility Matrix

Before submitting an agent/provider smoke matrix, generate the fixed repository
harness/provider acceptance matrix:

```bash
loom qa matrix \
  --compatibility-plan \
  --output provider-harness-compatibility.md \
  --json-output provider-harness-compatibility.json
```

This command does not log in, contact a provider, or submit batches. Its
endpoint taxonomy, output schema, and provenance fields are fixed in the CLI;
it does not discover arbitrary provider endpoint types or establish general
provider support. It covers the five supported product entries: Oracle,
`direct-completion`, Terminus-2, OpenHands and Codex (#2054). Deferred
harnesses such as `opencode`, `aider`, Claude Code and Gemini CLI stay in the
catalog for historical records but are not tested. The JSON schema records each agent harness x
provider endpoint cell as `supported`, `skipped`, or `blocked`, with protocol
surface (`chat`, `responses`, `messages`, or `gemini`), streaming, tool-use,
request-param, max-token, usage, diagnostic, and redaction status. Generic
`supported_providers=["*"]` agents are still emitted as per-agent rows. Use the
matrix to reject unsupported harness/provider combinations before spending
live provider calls.

If live-smoke evidence is available, merge it from a local JSON file:

```bash
loom qa matrix \
  --compatibility-plan \
  --compatibility-evidence provider-harness-evidence.json \
  --json-output provider-harness-compatibility.json
```

Evidence files may include sanitized `live_smoke` fields such as status,
checked time, `llm_calls_count`, usage, diagnostics, redaction status, and an
evidence URL. The CLI rejects raw-looking bearer tokens, provider API keys,
or signed URLs in evidence, notes, URLs, and serialized output; keep credentials
as safe references such as `env:PROVIDER_API_KEY`.

Combine that compatibility JSON with an offline catalog snapshot to run the
fixed agent/benchmark pre-submit planner before spending live provider calls:

```bash
loom qa matrix \
  --preflight-plan \
  --catalog-snapshot qa-catalog-snapshot.json \
  --provider-compatibility-plan provider-harness-compatibility.json \
  --output agent-benchmark-preflight-plan.md \
  --json-output agent-benchmark-preflight-plan.json
```

The catalog snapshot is a local JSON object with `agents.items[]` and
`benchmarks.items[]`, plus the offline evidence needed to select a deterministic
representative task and avoid doomed submissions: license evidence,
capability evidence, and worker architecture evidence. The preflight output
uses `planned_submit`, `blocked`, and `skipped` rows. Provider mismatches come
from the compatibility matrix, no-model agents are emitted once per
benchmark as provider endpoint `no-model`, and supported-but-unsmoked provider
cells stay blocked as `pending_live_evidence`.

The planner consumes the CLI's fixed compatibility schema and endpoint
taxonomy; it is not a general compatibility detector. Its offline output does
not prove live compatibility. The command does not log in, call `/api/v1/*`,
call a model provider, submit a batch, read artifact storage, or require live
secrets.

## User-operated inference endpoints

A user-operated model server can be registered as an OpenAI-compatible provider
using the same provider connection flow above. Supply a reachable HTTPS base URL,
model identifier and provider credential. Loom does not schedule or operate the
external model server. The former Slurm/vLLM bundle generator is retired.

## Reading readiness in the Web app

Ready describes the last successful connection test, with its age shown alongside
it. It is not a continuous availability probe. The Models tab distinguishes
cached discovery/manual registration, models that have not been preflight-tested,
and the last successful or failed generation preflight. Refreshing discovery does
not verify generation access. Model help and CLI opens the supported
`loom providers models NAME --refresh` command on demand; connection names are
shell-quoted so names containing spaces or apostrophes remain a single argument.
