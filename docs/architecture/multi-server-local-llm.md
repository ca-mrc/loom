# Multiple Local Model Servers

CLI mode can run one or more Hugging Face or local-weight models through vLLM,
connect to an existing OpenAI-compatible server, or keep one vLLM process in
the foreground with `loom serve`.

## Foreground server

```bash
loom serve hf:meta-llama/Llama-3.1-8B-Instruct --name llama8b
```

`loom serve` launches vLLM, waits for `/v1/models`, and writes a temporary
`[local_providers.<name>]` entry containing the base URL and served model name.
It blocks until Ctrl-C or SIGTERM, then removes the config entry and stops the
process. A server exit alone currently does not stop this wait or remove its
registration.

Model specs may be `hf:<org>/<model>` or an absolute/relative path. The command
also accepts vLLM host, port, tensor-parallel, memory, context-length, and eager
execution options. Port `0` starts at 8234; an early nonzero process exit retries
a later port without distinguishing address-in-use errors from other failures.

This is a foreground lifecycle. There is no persistent daemon or separate stop
command.

## Comparing models

Repeat `--model` in one `loom run` invocation:

```bash
loom run --dataset humaneval --agent direct-completion \
  --model hf:meta-llama/Llama-3.1-8B-Instruct \
  --model /models/my-checkpoint \
  --output-dir runs/compare
```

Multiple models run sequentially by default: Loom starts a server, runs all
selected tasks, stops it, then proceeds to the next model. This bounds peak GPU
memory to one model. Output is grouped as
`<output-dir>/<model-slug>/<trial-id>/`.

`--parallel-models` allows model runs to overlap. The user must provide enough
GPU memory and device placement for all models. Per-model teardown stops only
that model's server. Multiple-model runs always stop their managed servers,
including when `--keep-alive` is supplied. For a single model, that flag retains
a healthy server after normal completion. Loom prints its PID, endpoint, log
path and stop command; output goes to `<output-dir>/vllm.log`. Setup failures
and cancellation still stop the server. `loom serve` provides a foreground
alternative for reuse across CLI invocations.

With `--local-server URL`, repeated model values are sent as upstream model
IDs to that same already-running server. Both bare IDs such as `llama3.1` and
IDs containing slashes are preserved. Managed `hf:` or path specs cannot be combined
with `--local-server`.

The local launcher exists only in CLI mode. In service mode, register a local
endpoint as an OpenAI-compatible Provider Connection. See [Local LLMs](local-llm.md) and the
[user guide](../user-guide.md#comparing-multiple-models).
