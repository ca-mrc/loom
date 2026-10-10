# Live Trial Event Streaming

Loom exposes trial trajectory events through two paginated read
paths and one push-based Server-Sent Events (SSE) stream. Together
they cover pull-mode replay (dashboards, batch analytics) and live
observation (Trial Detail viewer, custom tooling).

All three endpoints require a bearer token with the `read:own` scope
and enforce the standard team-scoping rules: admins see every trial;
team tokens only see their own team's trials.

## Endpoints

### `GET /api/v1/trials/{trial_id}/events?after_seq=N&limit=M`

Seq-cursor paginated event replay. Reads from the Postgres
`trial_events` table with a MinIO trajectory-JSONL fallback for
trials that have no rows in `trial_events` but do have a stored trajectory.

| Query param  | Default | Meaning                                                      |
|--------------|---------|--------------------------------------------------------------|
| `after_seq`  | `-1`    | Return events with `seq > after_seq`. `-1` = from the start. |
| `limit`      | `200`   | Max events returned in this page. Cap: `1000`.               |

Response:
```json
{
  "events": [
    { "seq": 0, "kind": "trial_start", "trial_id": "…", "emitted_at": "…", "…": "…" },
    { "seq": 1, "kind": "step_start", "…": "…" }
  ],
  "next_after_seq": 1
}
```

- `next_after_seq` is the seq of the last event returned, or `null`
  if the page was empty (client should re-poll with the same cursor).
- The response shape is stable across the MinIO fallback path — the
  `payload` column already carries the full typed event body.

Use this endpoint for stateless polling, batch analytics, or as the
frontend's EventSource-unavailable fallback.

### `GET /api/v1/trials/{trial_id}/stream?after_seq=N`

SSE live stream. Emits an initial replay for events matching
`after_seq`, then streams new events as they land in the
`trial_events` table via a Postgres LISTEN connection on the
`trial_events_inserted` channel. The connection closes when the
trial reaches a terminal state OR the client disconnects OR the
connection has been open for 600 s (client reconnects with the last
seen seq).

The route sets `Cache-Control: no-cache` and `X-Accel-Buffering: no`
so proxies do not buffer chunks.

**SSE contract:**

| Event kind      | Emitted when                    | Data body                                        |
|-----------------|---------------------------------|--------------------------------------------------|
| _(default)_     | A new trajectory event arrives  | Full typed event body (same shape as `/events`). |
| `complete`      | Trial reaches terminal state    | `{ "final_state": "succeeded", "last_seq": N }`  |
| `reconnect`     | 600 s connection budget hit     | `{ "reason": "max_connection_sec", "last_seq": N }` |

Every message carries an `id: <seq>` line so browser EventSource
auto-reconnect includes `Last-Event-ID` on the next attempt. The
server does not currently consume that header; clients should also
dedupe by seq on their side (the SPA hook does).

Example (bash):

```bash
curl -N -H "Authorization: Bearer $LOOM_API_TOKEN" \
  "https://loom.example.com/api/v1/trials/<trial-id>/stream"
```

### `GET /api/v1/trials/{trial_id}/trajectory?cursor=N&limit=M`

Compatibility pagination backed by the stored MinIO trajectory. Its line cursor
counts non-blank JSONL lines. Prefer `/events?after_seq=N` for seq-cursor
pagination.

## SPA integration

`web/src/hooks/useTrialEventStream.ts` wraps the browser
`EventSource` and exposes `{ events, status }`:

```tsx
const { events, status } = useTrialEventStream(trialId);
// status ∈ 'connecting' | 'open' | 'complete' | 'reconnect' | 'error'
```

The hook dedupes by event seq (browser auto-reconnect can replay
already-seen events) and closes the connection on unmount or on the
`complete` event. `TrialDetail` uses this hook by default and falls
back to `useAdaptivePolling` against `/trajectory?cursor=N` when
`status === 'error'` — appropriate for environments where corporate
proxies strip `text/event-stream`.

## Under the hood

Push semantics come from a Postgres trigger (`trial_events_notify_trigger`)
that fires `pg_notify('trial_events_inserted', '<trial_id>:<seq>')` on
every `trial_events` INSERT. The service opens one dedicated
`psycopg.AsyncConnection(autocommit=True)` LISTEN connection per SSE
stream; a per-request drain task filters notifications by the
`<trial_id>:` prefix and sets an `asyncio.Event` the outer loop
awaits with a fixed poll-interval fallback.

The subscription also owns cleanup during setup: if LISTEN registration or
the notification self-test raises, any acquired connection is closed before
the route falls back to polling. Cancellation during setup closes the connection
and propagates cancellation. Normal context exit cancels the notification drain
task and closes its connection.

The MinIO trajectory JSONL is still written for every trial by the
worker (`TrajectoryWriter`); the `trial_events` table receives the
same events via the worker's `CpEventSink` batched dual-write. In
the current path MinIO is the archive/audit-log copy and the SSE reader is
Postgres-first.
