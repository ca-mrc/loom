# In-flight sandbox authority

Evidence: #2779. The actuator incident is #2778.

## What failed

On the November 3, 2026 rerun, both web-pod actuators stopped publishing health. A target whose health report is older than 90 seconds leaves the scheduler's ready list, so no new sandbox is placed and a separate verifier is not reserved. That admission check stays.

The same timestamp was also applied to work that already had a sandbox. Token renewal and `GET /internal/service-execution/llm-calls` both use `authorize_service_execution_peer` with `purpose=token`. After the health report went stale, a running attempt could not renew its model credential, and the execution controller's call-ledger read received HTTP 503. The pod-local broker retried that 503 for about 30 seconds. The controller's client waits 15 seconds, so the trial stored `phase=read_timeout`.

## The 15-second read

`GET /internal/loom/llm-calls` is the list of model calls this trial has already made. The controller uses it to attach each Harbor turn to the gateway row for that call. It is not a check that the batch model exists, and it is not removed.

A once-per-batch check that the selected model is present on every pool member would be a separate admission check. It does not identify which gateway call belongs to a later turn.

## Decision

- A lease that already has a pod and is `running` or `finalizing` keeps token and call-ledger authority when the only failure is a stale `health_observed_at`. The target must still be desired active, observed ready, and marked healthy.
- A sandbox that is still `creating`, and every new placement, still requires a fresh health report. The scheduler ready list is unchanged, so a separate verifier is still not reserved on a stale target.
- A transport failure of the call-ledger read is retried on the background observer, before a command is recorded, and at final reconciliation. Three attempts are made, five seconds apart. The last failure is still raised. An auth, fencing, or ambiguous-join error is not retried.

The actuator still has to keep reconciling. This decision does not place new sandboxes on a target whose actuator has stopped.
