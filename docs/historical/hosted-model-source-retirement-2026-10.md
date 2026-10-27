# Hosted model-source retirement — October 2026

Hosted submissions reach a model only through an OpenAI-compatible Provider
Connection (`ModelSpec.source="api"`). The hosted `hf` source (worker-launched
vLLM and HF Inference) and the dedicated `local-server` source are retired
(#2054). This page records what happened to work already accepted on those
sources, what was removed, and what stays for compatibility.

## Admission

`validate_agent_model_compat` (`src/loom_service/agent_catalog.py`) rejects
every model source other than `api`, telling the user to register an
OpenAI-compatible Provider Connection instead (#2198). Batch and Trial
submission, the effective-combination resolver, and therefore clone, rerun and
artifact reuse all call it. Nothing remaps a retired selection to another
source.

## Accepted work

No hosted path executes a retired source:

- Hosted execution is Nebius-only. The native plan compiler rejects a model
  whose source is not `api` (`api_model_required` in
  `automatic_service_execution_rejections`).
- The legacy worker, which runs only for explicit local development
  (`LOOM_LOCAL_EXECUTION=1` with `LOOM_ENV=development`), no longer launches
  vLLM (see [Removed](#removed)).

There is no migration. A user resubmits on a Provider Connection; the new Trial
follows current submission policy.

**Inventory, 2026-10-27.** Staging (`nebius.yylx.world`) was read as a platform
admin through `GET /api/v1/trials` and `GET /api/v1/batches`, across all teams:

| Check | Result |
| --- | --- |
| Trials on record (oldest 2026-09-09) | 1737 |
| `model.source = "api"` | 771 |
| No model (Oracle) | 966 |
| `model.source` `hf` or `local-server` | 0 |
| Trials `queued`, `protected-pending`, `claimed` or `running` | 0 |
| Batches not `finished` or `cancelled` | 0 |

The retired `yylx.world/{dev,staging,prod}` endpoints no longer respond. So no
accepted, queued, running or recovering work depends on a retired source, and
no historical record needs special reading beyond the retained `ModelSpec`
fields.

**Usage checks must key on `model.source`.** Every `api` record also carries the
default `hf_execution: "local-vllm"`, which means nothing unless `source` is
`hf`. Counting `hf_execution` reports every Trial as a vLLM run.

## Endpoint mappings

The only hosted code that built a `local/<server>/<model>` Gateway route is
`ModelSpec.to_gateway_model_string` for `source="local-server"`, which admission
rejects. The Gateway mappings behind it therefore had no admitted caller:
`LOOM_GW_LOCAL_YIBU_*` on the Nebius Gateway, the staging attachment's local
providers Secret, and `LOOM_GW_HUGGINGFACE_API_KEY`.

Provider Connections are database rows with their own base URL and secret.
Removing these Gateway settings does not change a connection, even one that
points at the same upstream.

## Retained

- `ModelSpec.source`, `local_server` and `hf_execution` stay parseable, so
  historical records, exports and audit reads keep working.
- The standalone CLI (`loom run --local-server`, `hf:` models, local weights,
  `loom serve`) is out of scope for #2054. It uses its own upstream client
  (`loom_cli/upstream_gateway.py`) and `loom_cli/vllm_runner.py`, not the hosted
  Gateway routes.
- Operator configuration keys still load, so existing files need no edit before
  deployment:
  - Nebius platform `model_provider_base_url` is optional and ignored; it is
    still checked to be a credential-free HTTPS URL when present.
  - Cluster `gateway_local_providers` must be empty.
  - The staging attachment's `local_providers_secret_name` stays required. The
    canonical Gateway no longer sets `LOOM_GW_LOCAL_*`, so the copied Secret is
    empty. Operators may drop the `loom-model-provider` and
    `loom-nebius-model-provider` Secrets once no rollback needs them.

## Removed

- The worker vLLM path: `loom_worker/vllm_registry.py`,
  `loom/agent/local_vllm_client.py`, the `local-vllm` branch in
  `loom_worker/trial_runner.py`, and the `enable_worker_vllm` and `vllm_*`
  worker settings. `loom_cli/vllm_runner.py` stays; the CLI uses it.
- The Gateway `local`, `huggingface` and `local-vllm` routes, the
  `LOOM_GW_LOCAL_*` parser and the `huggingface_api_key` setting. Such requests
  now get `400 unsupported provider`.
- The `LOOM_GW_LOCAL_YIBU_*` environment in the Nebius platform render and the
  development Gateway patch, and the cluster template's local-provider block.
