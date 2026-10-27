# Live Trial Event Streaming

Loom exposes trial trajectory events through two paginated read
paths and one push-based Server-Sent Events (SSE) stream. Together
they cover pull-mode replay (dashboards, batch analytics) and live
observation (Trial Detail viewer, custom tooling).

All three endpoints accept a bearer token or an authenticated browser session.
Non-admin callers need the `read:own` scope and can read only their team's
trials; platform administrators can read across teams. The SPA's EventSource
uses the session cookie with `withCredentials: true`.

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
`trial_events_inserted` channel. The connection completes after the trial is
terminal and all available event pages have been drained. It also closes if the
client disconnects or the
connection has been open for 600 s (client reconnects with the last
seen seq).

The route sets `Cache-Control: no-cache` and `X-Accel-Buffering: no`
so proxies do not buffer chunks.

**SSE contract:**

| Event kind      | Emitted when                    | Data body                                        |
|-----------------|---------------------------------|--------------------------------------------------|
| _(default)_     | A new trajectory event arrives  | Full typed event body (same shape as `/events`). |
| `complete`      | Terminal Trial's event backlog is drained | `{ "final_state": "succeeded", "last_seq": N }`  |
| `reconnect`     | 600 s connection budget hit     | `{ "reason": "max_connection_sec", "last_seq": N }` |

Every trajectory event carries an `id: <seq>` line so browser EventSource
auto-reconnect includes `Last-Event-ID` on the next attempt. The
server does not currently consume that header; clients should also
dedupe by seq on their side (the SPA hook does).

Replay uses bounded pages and yields between pages. Connection deadlines and
disconnects remain effective while draining a backlog; a `reconnect` event
reports the last delivered sequence, so clients can resume with `after_seq=N`.
An invalid or nonadvancing source sequence ends the stream without claiming
completion. After LISTEN setup succeeds, disconnect, cancellation, read failure
and normal completion close the subscription.

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
`complete` event. When the server requests `reconnect`, the hook closes
that source and opens a replacement after one second, using the highest
locally accepted seq as `after_seq`. Existing events remain visible;
the control frame's `last_seq` cannot advance the cursor past received data.
Only one source or pending rollover retry is owned at a time. Ordinary
transport errors retain the browser's native EventSource retry behavior.

Changing the trial ID or API base clears events, cursor and status.
Disabling the same scope closes its source and cancels any pending retry;
reenabling resumes from its retained cursor. Cleanup and completion invalidate
callbacks from retired sources. `TrialDetail` also keys its trajectory viewer
by trial ID, so navigation already remounts that viewer.

`TrialDetail` uses this hook by default and offers the paginated
`/trajectory?cursor=N` fallback when `status === 'error'` — appropriate
for environments where corporate proxies strip `text/event-stream`.

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


## Native execution progress

Native execution now publishes **provisional live progress** through the trusted
execution controller's existing Pod-authenticated broker. The controller reports
real phase boundaries and tails only declared trajectory JSONL outputs. A bounded
background uploader flushes low-rate progress on a timer, accepts exact duplicate
retries, and stops preview delivery on persistent failure without changing the
execution deadline or discarding the durable local output.

Live progress and the canonical trajectory have different ordering. Final
materialization still owns canonical lifecycle events and Gateway accounting
reconciliation. Native preview events therefore use separate storage and never
occupy canonical `trial_events` sequence numbers or change downloads and exports.

The SSE endpoint adds named frames while canonical events are unavailable:

- `preview_reset`: `{ "source": "<lease-id>:<resource-generation>" }` starts a
  different current-attempt preview; `source: null` marks canonical takeover.
- `preview`: `{ "source": "…", "events": [...], "next_after_seq": N }` carries
  typed progress events in their own sequence space.
- Default `message` frames remain canonical events with their original cursor
  contract. Canonical events replace the displayed preview, rather than being
  merged with it by sequence number.

Clients can resume preview delivery independently with `preview_source` and
`preview_after_seq`; `after_seq` still resumes canonical events. Resume from the
last event actually accepted, not a control frame's advertised cursor. A changed
lease or resource generation resets the preview source. Legacy clients that
ignore named preview frames continue receiving the canonical stream unchanged.

Polling clients opt in with `/events?include_preview=true`, plus the same preview
cursor parameters. The existing `events` and `next_after_seq` fields remain
canonical; the additive `preview` object carries native progress, or is `null`
when unavailable or superseded by canonical data. An empty preview page does not
mean the active Trial has finished. The SPA uses this same contract when SSE is
unavailable and labels provisional progress as a live preview. If execution ends
with only partial preview evidence, it is not presented as a complete canonical
trajectory.

The ingest route checks the existing Pod identity, primary-attempt lease,
resource generation, current Trial attempt and team binding. Neither an agent nor
a task chooses another Trial, lease, credential, upstream URL, or file to tail.
Live rows use the existing Trial-event lifecycle classification and are removed
with that classified scope; canonical final output retains its existing policy.
