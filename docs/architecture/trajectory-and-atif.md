# Trajectories & ATIF

Loom emits **two** trajectory artifacts per trial:

1. **`events.jsonl`** — event-sourced append-only JSONL written
   incrementally as the trial runs. In service mode the user-facing
   trajectory API reads the durable `trial_events` table first, with
   the legacy MinIO key `<team_id>/<trial_id>/events.jsonl` kept as
   the object fallback and audit copy. CLI mode writes
   `<output_dir>/<trial_id>/events.jsonl`.
2. **`atif.json`** — ATIF v1.7 projection computed from the
   trajectory at finalize. Lives at MinIO key
   `<team_id>/<trial_id>/atif.json` (or alongside events.jsonl on
   disk in CLI mode). In service mode the authenticated ATIF download
   route proxies that object first, then reprojects from durable
   `trial_events` when the object-store copy is absent.

The two coexist by design: trajectories are write-optimized (append,
no merging) and lossless; ATIF is read-optimized (one document per
trial) and projection-only.

## Event schema

Every event is one JSON object per line. Minimum shape:

```json
{
  "emitted_at": "<ISO8601 UTC>",
  "trial_id": "<uuid>",
  "step_id": "<step_name | __trial__>",
  "seq": <int>,
  "kind": "<event_kind>",
  ...
}
```

Per-trial events have a monotonic `seq`. Adapters and step runners may keep
local counters internally, but `TrajectoryWriter` is the append boundary and
normalizes every typed or raw event to the canonical trial-wide sequence before
local JSONL, object storage, and the Control Plane `trial_events` mirror see it.
This matters because the durable event table uses `(trial_id, seq)` as its
idempotency key.

### Event kinds (snake_case)

| Kind | Emitted by | Carries |
|---|---|---|
| `trial_start` | Trial.run() | task_id, agent_name, agent_mode |
| `trial_end` | Trial.run() | (terminal marker) |
| `trial_error` | Trial.run() | error_type, message, traceback |
| `trial_cancelled` | Trial.run() | cancellation_requested_at, observed_at |
| `step_start` | step_runner | step_name, instruction_excerpt |
| `step_end` | step_runner | summary, error (if failed) |
| `env_start` / `env_ready` / `env_stop` | Driver | sandbox lifecycle |
| `env_exec` | Driver | command, exit_code, duration_sec |
| `file_upload` / `file_download` | Driver | path, size, sha256 |
| `llm_call` | Gateway / direct-completion runtime | model, dialect, input_tokens, output_tokens, cost_usd snapshot, rate_card_hash, latency_ms |
| `tool_use` | Agent adapter | name, args, result_excerpt |
| `agent_thought` | Agent adapter | reasoning text |
| `verifier_start` / `verifier_end` / `verifier_check` | Verifier | check_name, passed, message |
| `network_policy_change` | Driver | from, to |
| `worker_lost_claim` | Worker | reason (heartbeat timeout, fence-bump) |

Full schema (and the per-event payload fields not shown here):
`src/loom/models/trajectory.py`. New events can be added by appending
kinds — the writer + reader are forward-compatible (unknown kinds
pass through unchanged).

## Writer (`src/loom/trajectory/writer.py`)

```python
writer = TrajectoryWriter(
    trial_id=trial_id,
    local_path=Path("/tmp/events.jsonl"),
    object_store=store,
    bucket="trajectories",
    key=f"{team_id}/{trial_id}/events.jsonl",
)
await writer.write(event)
await writer.finalize()
```

Behavior:

- **Local-first durability** — every event is appended to the local
  file synchronously before any network call. A crash mid-trial
  doesn't lose events.
- **Background flush** — flush triggers at any of: 1 MB pending bytes,
  100 pending events, or 10 seconds since last flush. The flush
  uploads pending bytes as the next multipart part.
- **5 MiB AWS-S3 mid-trial part floor** — multipart parts must be ≥
  5 MiB EXCEPT the last one. The writer buffers smaller parts until
  the threshold is met. Test code passes `min_part_bytes=0` to keep
  small-event tests deterministic.
- **Trial-wide sequence assignment** — callers can pass local event `seq`
  values, but the writer rewrites duplicates or rewinds to the next global
  trial sequence. This prevents the Control Plane mirror from dropping later
  events as `(trial_id, seq)` duplicates.
- **`finalize()`** uploads remaining bytes as the final part and
  closes the multipart upload.

Orphan handling: if a worker dies mid-trial, the local file lives but
nothing is in MinIO. The Worker's `OrphanCleanup` sweep (run at
startup) walks `<tmp>/loom-trial-*` dirs and uploads any
not-finalized trajectories. Failed orphan uploads log + skip; the
local file is preserved.

## Reader (`src/loom/trajectory/reader.py`)

`TrajectoryReader` reads typed events from a local JSONL file. It can iterate
all events, filter by kind, retain a bounded tail, or build an excerpt.

```python
from pathlib import Path
from loom.trajectory.reader import TrajectoryReader

for event in TrajectoryReader(Path("events.jsonl")).iter_all():
    ...
```

Service reads are implemented separately in `loom_service.routes.trajectory`.
The legacy `/api/v1/trials/{id}/trajectory` endpoint uses a line cursor over the
stored object. `/api/v1/trials/{id}/events?after_seq=N` and the SSE stream read
PostgreSQL `trial_events` first, with a stored-object fallback for legacy trials.
The browser's polling fallback and comparison view use the sequence endpoint,
so retained database events remain available before an object is completed or
when that copy is absent. Sequence pagination resumes after the last returned
event; an empty page is the current tail, and active trials keep polling there.

## ATIF v1.7 projection (`src/loom/trajectory/atif.py`)

Pure function: `project_to_atif(events: list[TrajectoryEvent]) ->
dict`. Reads the in-memory event list at finalize, walks once, emits
a single document.

ATIF shape (rough; canonical definition is in the projection code):

```json
{
  "schema_version": "1.7",
  "trial_id": "<uuid>",
  "task_id": "humaneval/HumanEval/0",
  "agent": {"name": "claude-code", "version": "...", "mode": "out-of-box", "model": {...}},
  "started_at": "...",
  "finished_at": "...",
  "state": "succeeded",
  "rewards": {"resolved": 1.0},
  "steps": [
    {
      "step_name": "main",
      "instruction": "...",
      "llm_calls": [
        {
          "model": "claude-opus-4-7",
          "input_tokens": ...,
          "output_tokens": ...,
          "cost_usd": ...,
          "request_params": {"status": "available", "parameters": {"temperature": 0}}
        },
        ...
      ],
      "tool_uses": [...],
      "agent_messages": [...],
      "verifier_result": {
        "rewards": {"resolved": 1.0},
        "checks": [{"name": "tb2_run_tests", "passed": true, "message": "exit=0"}]
      },
      "duration_sec": 42.5
    }
  ]
}
```

Service mode enriches the ATIF with `llm_calls` fetched from the
Gateway (via `llm_calls_fetcher` callback) before projection when the
agent did not already write gateway-backed `llm_call` trajectory
events. Synthetic `llm_call` events are billing summaries; full
request/response bodies must come from adapter/provider logs when they are
captured. CLI mode skips this step (no Gateway).
Synthetic events also carry the redacted `request_params` audit
payload from `llm_calls`. Older rows that predate this field surface
as `{"status":"unavailable_legacy","parameters":{}}` rather than
claiming settings matched.

ATIF is a pure projection — re-running it on the same events.jsonl
produces a byte-identical result modulo `emitted_at` timestamps.

## Service Output Projection

In service mode, a successful Worker finalize also updates the trial row
through the fenced Control Plane `PATCH /trials/{id}/trajectory_index`
path:

- `trials.result` stores the serialized `TrialResult`, including
  `aggregate_reward` for list and batch rollups.
- `trials.trajectory_index` stores `trajectory_uri`, `atif_uri`,
  `atif_schema_version`, and an `artifacts` array containing each
  uploaded artifact's `step_name`, bucket, object key, size, `share_status`,
  and optional safe `blocked_reason`.

`GET /api/v1/trials/{id}` reads this projection to return ready flags
and authenticated service download URLs for ATIF, trajectory, and artifacts.
`loom_service` proxies object-backed downloads from the internal MinIO/S3
endpoint to the caller, so browser and laptop clients do not need direct
object-store network access. The response owns its acquired object body even
when sending headers fails. Cancellation and disconnect drain an active read
before closing the body; normal completion and interrupted delivery also close it.
For Postgres-backed trials whose legacy object
copy is absent, `GET /api/v1/trials/{id}/trajectory/download` reconstructs an
analysis-ready JSONL body from `trial_events`, gateway `llm_calls`, and the
terminal `trials.state/result` row. The service renumbers the reconstructed
stream to a clean trial-wide sequence and uses the same synthetic `llm_call`
projection as worker finalize. `GET /api/v1/trials/{id}/atif` reprojects ATIF
from that same reconstructed stream plus `trials.result.agent` metadata. If rows
exist but the event stream or projection metadata is insufficient for ATIF, the
service returns HTTP 409 with a user-actionable message and keeps trajectory
download available. A succeeded trial should not require clients to scan object
storage, guess artifact keys, or open a separate MinIO tunnel.

Artifact share states are security metadata, not download URLs:

- `pending_scan`: legacy or not-yet-classified artifact. Treat as owner-team
  only for org-wide sharing decisions.
- `shared`: conservative scan found no secret-like content. Run Library
  download/reuse may expose it across teams through authenticated service
  routes when the parent trial or batch is also Run-Library-readable.
- `blocked`: scan matched token, provider key, secret-ref, signed URL,
  internal endpoint, cookie, CSRF, or similar secret-like material. The raw
  object remains available to the owner team for diagnostics through normal
  trial detail routes, but org-wide Run Library download/reuse must deny it and
  show only the safe `blocked_reason`.

## Trajectory ↔ ATIF in code

```python
# Worker / CLI:
writer = TrajectoryWriter(...)
async with writer:
    async for event in trial.run():
        await writer.write(event)
    await writer.finalize()

# Finalize step:
events = list(reader.stream(bucket, events_key))
atif = project_to_atif(events, llm_calls=fetched_llm_calls)
await object_store.put_object(bucket, atif_key, json.dumps(atif).encode())
```

## Common pitfalls

- **`bind_trial_context` does not nest.** The contextvars helper for
  log correlation can only have ONE binding active. Bind at the
  outermost scope (e.g. inside `Trial.run()`'s body, not inside
  per-step helpers); nesting silently drops the inner binding when
  the outer scope exits.
- **Don't write to the trajectory from inside `project_to_atif`.**
  Projection is pure; it reads events, returns a dict.
- **5 MiB part floor.** If you set `min_part_bytes=0` in production,
  multipart upload fails after the first <5 MiB flush. Only test code
  should pass 0.

## See also

- [overview.md](overview.md)
- `src/loom/trajectory/writer.py` — the writer
- `src/loom/trajectory/reader.py` — the reader
- `src/loom/trajectory/atif.py` — the projection
- `src/loom/models/trajectory.py` — event dataclasses

### Terminus model identity and generation limits

Native Terminus traces and Gateway ledger rows are reconciled against the Trial's
model identity, including provider, model name and source/routing fields. Optional
`max_input_tokens` and `max_output_tokens` are generation limits, not identity:
Gateway accounting does not persist them, so their absence in a projected event
must not invalidate the trajectory. The frozen Trial retains those limits;
identity mismatches still reject materialization, and reconciliation does not
change timeout, token usage or verifier outcomes.
