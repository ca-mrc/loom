# Local-LLM execution path

`loom run` can connect to an existing OpenAI-compatible server or launch vLLM
for a Hugging Face model or local weights. This is a local CLI lifecycle.
Hosted teams run their own inference endpoint and register it as a provider
connection; Loom does not host model inference for them.

The [user guide](../user-guide.md#local-llms-vllm-ollama-llamacpp-lm-studio)
provides commands and records the current local subprocess-agent provider
wiring limitation.

The optional GPU stack has remaining dependency advisories and requires a
qualified runtime migration. See [Python dependency qualification](../contributing/python-dependencies.md#optional-gpu-graph)
for its compatibility constraints and affected boundaries.

## Four entry points

| Selection | Server owner | Provider registration |
| --- | --- | --- |
| `--local-server URL --model org/model` | User | Transient `_inline` entry |
| `--model local/<server>/<model_id>` | User | Persisted `local_providers.<server>` entry |
| `--model hf:<org>/<name>` | CLI | Transient `_auto_vllm` entry |
| `--model /path/` (also `~/`, `./`, `../`) | CLI | Transient `_auto_vllm` entry |

Each resolves to `ModelSpec(provider="local", name="<server>/<model_id>")`.
`UpstreamDirectGatewayClient._call_local` splits off the registered server name,
then sends the remaining model ID to its OpenAI-compatible Chat Completions
endpoint. It records token usage and computes local cost from rate cards.
Ollama, llama.cpp and LM Studio use an existing endpoint; vLLM is the only
managed launcher.

## Model-spec parsing

[`parse_model`](../../src/loom_cli/model_spec.py) recognizes these forms:

| Spec | Parsed provider | Meaning |
| --- | --- | --- |
| `anthropic/claude-opus-4-7` | `anthropic` | Direct provider SDK |
| `local/vllm/meta-llama/Llama-3.1-8B` | `local` | Registered server name followed by model ID |
| `hf:meta-llama/Llama-3.1-8B` | `hf` | Managed vLLM launch; requires `org/name` |
| `/data/checkpoints/my-tune/` | `file` | Managed vLLM launch from local weights |

[`run_cmd.py`](../../src/loom_cli/run_cmd.py) parses every model before
`_run_one_model` configures its provider. With `--local-server`, that function
reassembles the parsed `provider/name` as the upstream model ID and registers
`_inline`. Consequently, slashless IDs are currently rejected before inline
rewriting. To use one, register the endpoint and select
`local/<server>/<model_id>`. Managed `hf:` and path specs cannot be combined
with `--local-server`.

For managed specs, `_run_one_model` calls `launch_vllm`, registers `_auto_vllm`,
and uses the model ID returned by `/v1/models`. These transient entries stay
in that model run's in-memory configuration. `loom config set` rejects server
names starting with `_`; the launcher uses these names for transient providers.

## Subprocess lifecycle

[`vllm_runner.py`](../../src/loom_cli/vllm_runner.py) owns process startup and
cleanup. `VLLMLaunchSpec` carries the model, host, starting port, GPU memory,
tensor-parallel, context-length and eager-execution settings.

1. Check that `vllm` is on `PATH`, install process-exit and SIGTERM cleanup,
   and resolve local weight paths.
2. Start `vllm serve` at the requested port, or 8234 when it is zero.
   There is no separate free-port probe. An early nonzero exit after the
   two-second settling interval retries the next port, for up to 1,000 ports;
   the implementation does not distinguish bind failures from other early exits.
3. Poll `/v1/models` for up to 300 seconds, failing when the process exits.
4. Read the first advertised model ID and return its base URL and process ID.

The subprocess inherits stdout and stderr. Health-check or model-query failures
stop the process and remove it from the registry. Shutdown attempts TERM,
waits up to 30 seconds, then uses KILL if needed.

vLLM binds to `127.0.0.1` by default. `--vllm-host 0.0.0.0` explicitly exposes
it on the LAN.

## Cleanup model

| Trigger | Behavior |
| --- | --- |
| Model run finishes or its trial gathering fails | `_run_one_model` calls `stop_one` in `finally`, unless `--keep-alive` was supplied |
| Ctrl-C | Python's `KeyboardInterrupt` follows the normal asynchronous cleanup path |
| CLI process exits | `atexit` calls `stop_all` for remaining registered processes |
| SIGTERM | The signal handler calls `stop_all`, then restores and raises the default signal behavior |
| Startup health or model query fails | `launch_vllm` stops and unregisters that process |

Per-model cleanup stops only its own process so parallel model runs keep their
servers. `--keep-alive` skips that cleanup in both single- and multiple-model
runs; process-exit cleanup still applies. Use foreground `loom serve` when a
server should outlive individual `loom run` commands.

## Concurrency

`--concurrency N` bounds simultaneous trials for each model. All trials for one
managed model share its vLLM process. Repeated `--model` selections run
sequentially by default; `--parallel-models` allows overlapping model runs.
Without `--keep-alive`, sequential execution stops each managed server before
starting the next. Parallel runs require enough GPU capacity for all selected
models; Loom does not partition GPU memory automatically.

See [multiple local model servers](multi-server-local-llm.md) for output
grouping and foreground server registration.

## Service-mode boundary

The launcher runs only in the CLI and does not submit servers to an external
scheduler. Service-mode operators can configure Gateway upstreams through
`gateway_local_providers`, which renders `LOOM_GW_LOCAL_<NAME>_BASE_URL` and
optional API-key settings. Hosted users normally register a team-owned
provider connection. Neither path starts model-serving processes.

## Verification coverage

[`test_vllm_runner.py`](../../tests/loom_cli/test_vllm_runner.py) covers dependency
checks, path resolution, command construction, early-exit port retries, health
and model probes, and process cleanup using mocked subprocesses and HTTP.
[`test_multi_model_run.py`](../../tests/loom_cli/test_multi_model_run.py) covers
output grouping and per-model cleanup, including preservation of sibling servers.
These tests do not establish live GPU or agent/provider compatibility.
