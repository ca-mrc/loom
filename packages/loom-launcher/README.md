# loom-launcher

Pluggable agent harnesses + sandbox capture utilities for [Loom](https://github.com/ca-mrc/loom) — a team platform for running LLMs on customizable tasks.

Two consumers:
- **Workers** install this package to read adapter metadata (env vars, argv, capture pattern) and construct a `SubprocessAgent`.
- **Sandboxes** run the argv built by the adapter and emit data that the adapter capture helpers can parse.

The launcher wheel itself does not bundle third-party agent CLIs. Each adapter
declares a pinned `install_script`; service-mode workers build and reuse a
content-addressed derivative of the task image for that adapter. Operators use
`loom agents audit-runtime` to check the resulting image and `loom agents
smoke-runtime` for an end-to-end platform trial before treating the runtime as
ready in `GET /api/v1/agents`.

See `docs/architecture/agent-adapter.md` in the Loom repo for the framework reference.
