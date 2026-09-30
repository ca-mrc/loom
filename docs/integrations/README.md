# Integrations

Guides for extending Loom or plugging it into external systems: registering
inference providers, authoring benchmark tasks, and consuming trial event
streams.

## Contents

- **[provider-onboarding.md](provider-onboarding.md)** — hosted third-party
  API setup (OpenAI-compatible endpoints, provider-native APIs) and
  user-managed OpenAI-compatible model servers. Provider testing, model
  refresh, and safe registration.
- **[authoring-a-task.md](authoring-a-task.md)** — `task.toml` schema,
  on-disk layout, agent/verifier choices, network policies, healthchecks,
  validation, and gotchas for benchmark task authors.
- **[live-streaming.md](live-streaming.md)** — SSE `/stream` + seq-cursor
  `/events?after_seq=N` API for real-time trajectory event consumption; the
  SPA `useTrialEventStream` hook contract. Consume this from a dashboard,
  monitor, or custom analytics tool.

## Related

- Adding a whole new benchmark adapter (not just a task): see
  [`../architecture/benchmark-adapter.md`](../architecture/benchmark-adapter.md).
- Adding a new agent harness: see
  [`../architecture/agent-adapter.md`](../architecture/agent-adapter.md) for
  local/worker adapters and
  [`../architecture/hosted-agent-harness.md`](../architecture/hosted-agent-harness.md)
  for automatic Nebius execution.
- Adding a new sandbox backend: see
  [`../architecture/driver-protocol.md`](../architecture/driver-protocol.md).
