# Cost & rate cards

How Loom turns token counts into dollar amounts, why the user-facing
model is *usage frozen, cost derived*, and where the rate cards live in
each mode.

## The model: usage frozen, cost derived

Every LLM call freezes its raw token counts (input, output, cache
reads, cache writes, plus any dialect-specific extras) verbatim into
storage at the moment the response lands. Priced calls also freeze the
per-call `cost_usd` snapshot and `rate_card_hash` used at emit time.
Trial, batch, and usage read APIs project these rows into token totals,
estimated spend, and cost diagnostics; they do not hide token-only or
missing-rate-card calls behind a zero-dollar total.
If a provider omits all or part of its usage block, the Gateway records
that fact in `provider_extras` and the read APIs expose
`partial_usage_llm_calls_count`, `missing_usage_llm_calls_count`,
`failed_upstream_llm_calls_count`, `usage_reporting_status`, and
`usage_estimate_confidence` so callers can distinguish a complete estimate
from a lower-confidence token/cost projection.
If the Gateway attempted an upstream provider request and the upstream
returned an error, the provider transport failed, or an ordinary Chat/Responses
HTTP-success body failed JSON decoding or required-shape validation, it records a
zero-token `llm_calls` audit row with `rate_card_hash=failed-upstream`
and `provider_extras._loom_call_status=failed`. Trial/batch debug
evidence surfaces these rows through call-status and failure-category
counts; usage/cost projections should treat them as audit evidence,
not billable provider usage.
Native Responses SSE streams without a usage-bearing event preserve the same
missing-usage metadata as JSON responses. Explicit pricing configurations leave
their monetary estimate unknown instead of pricing fabricated zero-token usage.

```
provider response
     │
     ▼
TokenUsage ──► llm_calls row
               { input_tokens, output_tokens,
                 provider_extras, rate_card_hash,
                 cost_usd snapshot }
                     │
        ┌────────────┴────────────┐
        ▼                         ▼
 trial/batch usage projection     usage/rate-card cost view
 { total_prompt_tokens,           { derived spend totals,
   total_completion_tokens,         rate-card diagnostics,
   llm_calls_count,                 optional batch drilldown }
  estimated_cost_usd,
  cost_status,
  usage_estimate_confidence }
```

Why this shape:

- **Prices change**; historical trajectories should not become
  retroactively wrong. Re-pricing yesterday's run with today's
  table is a query, not a migration.
- **Provider SDKs evolve** — new dialect-specific token counters
  appear (`cache_creation_input_tokens`, `reasoning_tokens`,
  `thoughtsTokenCount`). The `provider_extras` JSONB column
  absorbs them without a schema change.
- **Trial/batch responses stay stable** — dashboards can distinguish
  "no calls were made", "self-deployed token-only model", and
  "rate-card lookup missed" using `llm_calls_count`, token totals,
  `cost_status`, and `pricing_modes`.
- **Failed attempts stay visible without becoming spend** — failed
  upstream attempts are counted as `llm_calls` for debug evidence, but
  carry zero tokens, zero cost, `rate_card_hash=failed-upstream`, and
  explicit failure metadata in `provider_extras`. Usage projections expose
  them as `pricing_modes=["failed-upstream"]` with
  `cost_status=failed_upstream`, not as priced provider usage.
- **Cost attribution stays auditable** — the Gateway records a
  per-call `cost_usd` snapshot and `rate_card_hash` for metrics and
  diagnostics. Consumers that need fleet-wide totals should query
  `/api/v1/usage`; trial and batch detail responses expose the same
  projection fields for local debugging.
- **Usage confidence is separate from pricing mode** — `cost_status`
  says whether Loom had a price source (`estimated`,
  `not_applicable`, `price_unknown`, `failed_upstream`, `mixed`);
  `usage_estimate_confidence` says whether the provider returned complete
  token usage (`high`, `partial`, `missing`, `none`).

## Rate card shape

Both CLI mode and service mode use the same Pydantic model:

```python
class RateCardEntry:
    provider: str          # e.g. "anthropic", "openai", "local:vllm"
    model: str             # e.g. "claude-opus-4-7"
    input_per_mtok: float
    output_per_mtok: float
    cache_read_per_mtok: float
    cache_write_per_mtok: float
    currency: str          # optional metadata, defaults to "USD"
    source_url: str | None
    pricing_version: str | None
    source_model: str | None
    pricing_unit: str | None
```

Cost formula (per call):

```
cost = (input_tokens  / 1_000_000) * input_per_mtok
     + (output_tokens / 1_000_000) * output_per_mtok
     + (cache_read    / 1_000_000) * cache_read_per_mtok
     + (cache_write   / 1_000_000) * cache_write_per_mtok
```

Cache tokens come from `provider_extras`. New connection pricing subtracts
cache tokens from OpenAI/Google inclusive input totals; Anthropic counters are
separate. Missing required cache evidence or an unpriced used cache dimension
produces an unknown estimate, never an implicit zero charge.

## Where the table lives

| Mode         | Source                                            | Override                              | Lookup                |
|--------------|---------------------------------------------------|---------------------------------------|-----------------------|
| CLI          | `~/.config/loom/rate-cards.toml`                  | hand-edit the file                    | `loom_cli.rate_cards` |
| Service      | `rate_cards` table in Postgres (JSONB payload)    | `loom_service` admin endpoint         | `loom_llm_gateway.rate_card` |
| Seed for CLI | `src/loom_cli/data/default-rate-cards.toml`       | copied on first run                   | —                     |

CLI: missing rate-card row → `KeyError` with a hint to add one.
Service: missing row → `RateCardNotFoundError` (HTTP 422 from the
Gateway), so a misconfigured provider fails fast instead of silently
recording $0.

## Provider connection pricing

Provider connections expose `pricing_mode`, `custom_pricing`, `supplier_id`,
and `catalog_id`. Every connection has exactly one mode:

- `usage_only`: record tokens; monetary cost is not applicable.
- `catalog`: use one accessible supplier or team catalog; unmatched models are unknown.
- `custom`: use exact model-specific prices; unconfigured models are unknown.

There is no per-model override, cross-catalog fallback, or fuzzy model match.
Selecting a supported supplier at creation defaults to its catalog unless the
caller explicitly chooses another mode. API protocol never determines supplier
identity. Existing connections are not automatically opted into new pricing.

Supported public sources (default purchasing group):

| Supplier | Catalog ID | Authoritative source |
| --- | --- | --- |
| YibuAPI | `supplier:yibuapi` | <https://yibuapi.com/api/pricing> |
| AZ GPTPlus5 | `supplier:az-gptplus5` | <https://az.gptplus5.com/api/pricing> (page: <https://az.gptplus5.com/pricing>) |

The owner confirmed AZ GPTPlus5's `default` group for `loom-testing` and
`loom-runs`. Both suppliers publish New API token ratios: input USD/M =
`model_ratio * 2 * group_ratio`; output and cache ratios multiply this base.
Only models enabled for the selected group and representable token billing are
included. Missing cache prices remain unknown. Tiered/per-request pricing is
not approximated. No foreign-currency conversion is performed. Calls reporting
audio billing, one-hour Anthropic cache writes, or Gemini thinking/non-text
categories outside this price contract remain unpriced rather than silently
omitting those charges. Legacy configurations keep their original calculation.

The Service's background loop checks shared supplier catalogs every five minutes
and refreshes when the last attempt is at least six hours old. Database row
locking prevents concurrent replicas from publishing overlapping updates. A
bounded fetch and complete validation precede one atomic replacement. Failures
retain the last valid table and expose a stale/error indication; an empty or
invalid first update leaves prices unknown. `loom providers catalogs sync ID`
lets a platform administrator request an immediate update.

`price_catalogs` is separate from legacy global `rate_cards` so team-private
prices cannot leak through old global read endpoints. Connections using a team-private
catalog cannot be shared across teams, since call snapshots would disclose its prices. `/api/v1/price-catalogs`
lists public and current-team catalogs. Team administrators create and import
team catalogs; platform administrators synchronize public supplier catalogs.
Imports use a preview and expected revision to reject concurrent overwrites.

Every new call records its exact price basis in `_loom_price_basis`, including
model, prices, USD unit, source/catalog revision and cache overlap semantics.
Changes apply to later calls only. Usage projections show unknown prices as
`estimated_cost_usd=null`, preserve priced zero, and distinguish mixed totals as
known subtotals with unpriced-call counts. These are estimates, not invoices.
Hard monetary budgets continue to require a usable estimate.

Trial and Batch reads resolve `price_snapshots` from the calls of the Trials
being read. A provider-price hash is the digest of its basis, so the read takes
one call per unresolved hash and extracts only `_loom_price_basis`, never the
full raw `provider_extras`. Batch detail resolves original and effective
snapshots in one shared lookup (#2687).

Since migration 0182 the Gateway also copies `_loom_cost_source`,
`_loom_cost_confidence` and `_loom_usage_status` into the `llm_calls` columns
`cost_source`, `cost_confidence` and `usage_status` at insert, using `''` for an
absent key. Usage aggregates filter on those columns and fall back to
`provider_extras` only for rows written before 0182 (NULL columns), so new calls
are counted without reading the raw-log JSON. Existing rows are not backfilled;
a chunked backfill is a follow-up.

### Legacy migration and rollback

Migration 0159 adds nullable connection configuration and catalog storage without
rewriting existing connections or historical calls. A null configuration uses
the existing calculation path. Public reads expose `legacy_pricing` with the
old uniform price or namespace; unrelated PATCHes preserve it. Replacing legacy
pricing requires an explicit mode and complete applicable configuration. New
custom tables never inherit the legacy price for subsequently discovered models.

A pricing PATCH treats omission as retention and null as clearing. A mode
transition clears inactive custom/catalog configuration. New rows retain valid
legacy storage columns with usage-only semantics; older binaries cannot compute
new model-specific prices. **Do not roll back the application alone after
activating new pricing.** Export the connection configurations and catalogs,
restore the previous database backup and compatible application together, or
explicitly revert connections after reviewing the export. The migration refuses
a destructive downgrade while new configuration or catalog data exists. A
no-data migration upgrade/downgrade leaves legacy data intact.

## Usage API and CLI

`GET /api/v1/usage` is the admin/user API for totals. It supports:

- `team_id`, `user_id`, `provider_connection_id`, `model`,
  `benchmark_id`, `batch_id`, `status`, and `pricing_mode` filters.
  `pricing_mode` accepts `priced`, `tokens-only`, `price-unknown`, and
  `failed-upstream`.
- `breakdown_by=team|user|provider_connection|model|benchmark|batch|status|pricing_mode`.
- `include_batches=true` for per-batch drilldown when not using a
  breakdown dimension.
- `include_batch_family=true` with `batch_id=<main-batch-id>` to include
  linked supplemental rerun batches (`batches.rerun_of_batch_id`) in the same
  usage/cost query. This is the preferred production view after safe failed-case
  reruns because it shows the original batch and its supplemental children as
  one budget family while `include_batches=true` still lists each contributing
  batch.

`loom eval usage` exposes the same filters and prints cost status plus
usage confidence. Use `--format json` for the full API payload,
including per-batch `partial_usage_llm_calls_count` and
`missing_usage_llm_calls_count`. Use
`loom eval usage --batch-id <main-batch-id> --include-batch-family
--include-batches` to audit a production batch family after reruns.

## Local / self-hosted rates

Provider key uses a `local:<server>` prefix to match the model spec
shape (`local/<server>/<model_id>` → provider `local:<server>`):

```toml
[[entries]]
provider = "local:vllm"
model = "meta-llama/Llama-3.1-8B-Instruct"
input_per_mtok = 0.10
output_per_mtok = 0.30
cache_read_per_mtok = 0.0
cache_write_per_mtok = 0.0
```

Local trials default to **$0** if no row matches — they don't
incur a real upstream cost. Add a row to attribute internal GPU
budget; leave it absent to ignore.

For a supported supplier:

```bash
loom providers create --name testing --type openai-compatible \
  --base-url "$PROVIDER_BASE_URL" --api-key env:PROVIDER_API_KEY \
  --supplier-id az-gptplus5
loom providers catalogs list
loom providers update testing --pricing-mode usage_only
loom providers update testing --pricing-mode custom --price-file prices.csv
```

CSV columns are `model,input_usd_per_1m,output_usd_per_1m,cache_read_usd_per_1m,cache_write_usd_per_1m`.
JSON uses an array of objects with the same field names. Base input/output values
are required, finite, nonnegative USD per million tokens. Blank cache cells are
unknown; zero is valid. Duplicate models, malformed rows and unsupported columns
reject the complete import. A connection price file replaces the custom table;
omitted models become unpriced. To explicitly apply one price set to several
models, repeat `--price-model MODEL` with `--input-usd-per-1m` and
`--output-usd-per-1m` (plus optional cache rates). Numeric updates preserve
other models' configured prices; `--price-file` explicitly replaces the full table.

```bash
loom providers catalogs create --name "Negotiated rates" --price-file prices.csv
loom providers catalogs import TEAM_CATALOG_ID --price-file prices.csv --revision 1
# After reviewing the preview, repeat with --apply.
loom providers update testing --pricing-mode catalog --catalog-id TEAM_CATALOG_ID
```

The Web Provider form supports searchable model rows, numeric editing, selected
row copying, CSV/JSON preview/import, and saving custom prices as a team catalog.
Saving uses only the active mode; switching an unsaved form retains its drafts.

The local CLI vLLM helper (`--model hf:<id>` / `--model /path/`)
registers as provider `local:_auto_vllm`. Rate-card rows for that
provider attribute internal cost across all hf:/path `loom run`
invocations. This is not hosted platform inference; service-mode teams
should register their own hosted or self-hosted endpoint through provider
connections. The inline `--local-server` flag registers as `local:_inline`.

## What `provider_extras` captures

Dialect-specific counters that don't fit `{input, output}` cleanly:

| Dialect          | Extras stored verbatim                                              |
|------------------|----------------------------------------------------------------------|
| Anthropic        | `cache_creation_input_tokens`, `cache_read_input_tokens`            |
| OpenAI Chat      | `prompt_tokens_details.cached_tokens`, `completion_tokens_details.reasoning_tokens` |
| OpenAI Responses | same as Chat, plus `output_tokens_details.reasoning_tokens`         |
| Gemini           | `cachedContentTokenCount`, `thoughtsTokenCount`                     |

The `cached_input_tokens` derived property on `TokenUsage` sums every
"this read from cache" counter across dialects so cost math doesn't
need a dialect switch.

## Re-pricing historical runs

Both modes support it:

- **CLI**: edit `~/.config/loom/rate-cards.toml`, re-run any
  trial-summary projection. ATIF v1.7 cost fields are projected from
  `events.jsonl` + the current rate card on read.
- **Service**: insert a new `rate_cards` row with `effective_at` set
  to today. The lookup is `MAX(effective_at) WHERE effective_at <=
  call_time`, so a new row prices calls made *after* its
  `effective_at` and leaves earlier calls priced against the prior
  row.

## What this is NOT

- **Not a billing system.** Cost is internal accounting against
  team quotas + the `/api/v1/usage` dashboard. Loom does not invoice.
- **Not a billing system budget.** Batch-level `budget_usd` is an
  operator safety guard. `hard` budgets reject over-budget pre-run
  estimates and cancel running batches after recorded provider usage
  exceeds the cap; `soft` budgets require explicit confirmation when
  the estimate is over the cap or unpriced. This is still internal
  spend control, not invoicing or per-team currency accounting.
- **Not currency-aware.** All amounts are USD by convention; no
  conversion or per-team currency.

## See also

- [`local-llm.md`](local-llm.md) — `local:<server>` provider naming
- [`llm-gateway.md`](llm-gateway.md) — where the lookup happens in
  service mode
- [`cli-mode.md`](cli-mode.md) — where the lookup happens in CLI mode

## Web inspection and publication

Usage exports preserve the selected start/end, `--group-by day|week|month`,
team scope, and the admin batch-breakdown flag. Admin All teams omits
`--team-id`; member usage remains scoped by the authenticated team. When no
bucket has a priced cost, Usage charts token volume and explains missing
price coverage or token-only calls. Unknown costs remain unknown. Per-bucket
batch rows expand on demand.

Rate cards first show published versions and model prices. Publishing is a
separate action with advanced JSON input, a model-price preview, and a comparison
with the current card. The publish payload is `{ "id": "version", "entries": [...] }`,
not the read API's `{ "table": ... }` envelope. Example prices are illustrative.
The gateway selects the latest captured card; publishing refreshes its captured
time, and reusing an ID updates that version. Existing calls retain their cost
snapshots. The final confirmation still uses the existing admin authorization.
