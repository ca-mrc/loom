# Frontend Error Recovery And Safe Browser Diagnostics

This document defines how the Loom SPA contains startup, session, and render
failures without returning a blank document or exposing raw browser and API
diagnostics. It also defines the deliberately small support-report contract
behind the `WEB-*` references shown in recovery screens.

## Goals

- Keep a visible, keyboard-reachable recovery surface when startup or rendering
  fails.
- Contain a routed-page failure without taking down the surrounding Loom shell.
- Distinguish a signed-out browser from a session service that cannot be
  trusted.
- Give users safe next actions and a short support reference.
- Keep raw errors, response bodies, credentials, URLs, and stack traces out of
  rendered copy and the bounded reporter payload.

Loom does not include a telemetry backend for these reports. The browser
reporter is a local integration hook; deployments that need durable correlation
must provide and validate a separate safe transport.

## Failure containment layers

| Layer | Failure handled | Recovery behavior |
|---|---|---|
| Runtime-config bootstrap | Network, non-success HTTP, malformed JSON, or an invalid `loom-frontend-config.json` contract | Keeps React mounted and shows `Loom could not start` with fixed copy and Retry, Reload, and Home actions. |
| Root render boundary | An unexpected render failure outside the routed-content boundary | Replaces the document content with `Loom could not display this page` and offers Retry, Reload, and Home. |
| Auth-session bootstrap | `/api/v1/auth/me` cannot produce a trustworthy current session | Shows `Loom could not verify your session`; it does not render a sign-in screen for an ambiguous failure. |
| Route render boundary | A routed page or its descendants throw while the Layout shell remains healthy | Replaces only the routed content with `Loom could not display this section`; navigation to another location resets the boundary. |
| Global browser bridge | An uncaught runtime error or unhandled rejection escapes React | Redacts browser and console surfaces before emitting a bounded local report and safe browser signal. It is a diagnostic backstop, not a replacement UI. |

The root boundary wraps runtime-config bootstrap, the query client, router,
session provider, and routed application. `Layout` wraps its routed outlet in
the route boundary for both signed-in and public-onboarding shells. Expected
runtime-config and auth-session failures use explicit state machines rather
than throwing into a generic boundary, so their recovery copy remains specific.

Every full-document state, including loading and recovery, retains one
`main#main-content`. Document recovery includes a skip link. Recovery panels
move focus to the document recovery main or routed alert panel when their
reference changes, and their actions retain minimum keyboard target sizes.

## Recovery actions

Recovery actions have different semantics and must not be presented as
interchangeable:

- **Retry** starts a fresh bounded attempt without reloading the document. It
  is appropriate for runtime-config loading, auth-session loading, root
  rendering, and transient routed rendering where a fresh render can succeed.
- **Reload Loom** reloads the current document and reconstructs module, router,
  query, and session state from the deployed bundle.
- **Go to Loom home** uses the active runtime prefix: `/dev/`, `/prod/`, or
  `/`.

`React.lazy` caches a rejected module promise. A cached lazy-module rejection
therefore uses `retryPolicy="reload-required"`: its route recovery screen omits
Retry and offers only Reload and Home. A developer must not label a remount as
a retry when it will execute the same cached rejection.

## Browser report contract

Every captured failure gets a random, display-safe reference with the form
`WEB-` followed by eight uppercase hexadecimal characters. The same reference
is used by the recovery panel and its report. Separate failures get separate
references; a React development-mode replay does not intentionally create a
second report for the same captured failure.

`BrowserFailureReport` contains only:

| Field | Required | Contract |
|---|---|---|
| `referenceId` | yes | The bounded `WEB-*` reference shown to the user. |
| `kind` | yes | One of the allowlisted failure kinds below. |
| `pathname` | yes | A redacted, bounded pathname. It excludes origin, query string, and fragment. |
| `sourcePath` | no | A redacted, bounded same-origin source pathname for a browser error event. Cross-origin and invalid sources are omitted. |
| `line`, `column` | no | Bounded non-negative integer source positions. |

Allowlisted kinds are:

- `frontend-config-network`, `frontend-config-http`,
  `frontend-config-invalid`;
- `auth-session-network`, `auth-session-http`, `auth-session-invalid`;
- `root-render`, `route-render`;
- `uncaught-runtime`, `unhandled-rejection`.

`setBrowserFailureReporter()` replaces an in-memory function. The default
reporter writes the already-bounded object to the developer console only in a
Vite development build. It does not send, persist, or make the reference
searchable in server logs, metrics, tracing, or a third-party service.
Reporter exceptions are swallowed so a diagnostic integration cannot create a
second rendering failure.

Consequently, an operator must not tell a user that a `WEB-*` reference is
remotely searchable unless that deployment has explicitly installed and
validated a separate reporter adapter. Until such an adapter exists, retain the
reference alongside candidate SHA, route pathname, timestamp, and reproduction
steps in the operator-owned evidence record.

## Auth-session state machine

The browser session has four states:

| State | Meaning | UI and cache behavior |
|---|---|---|
| `loading` | A single shared `/api/v1/auth/me` attempt is unsettled. | Show `Checking your browser session…`; do not render protected or anonymous routed content yet. |
| `authenticated` | A successful response passed the allowlisted session schema and installed the user, current team, and non-empty CSRF token. | Render the authenticated Layout. A changed user, team, membership, role, scope, or platform-admin grant clears cached query data. |
| `signed-out` | The session endpoint returned exactly `401`, or an explicit logout/global unauthorized event invalidated the session. | Clear session credentials and cached query data, then expose the normal public/sign-in routing. |
| `unavailable` | The endpoint failed in a way that cannot safely prove the browser is signed out. | Clear in-memory user and CSRF state, preserve previously trusted query data while recovery is possible, and show the document recovery panel with Retry, Reload, and Home. |

Unavailable failures are classified without response details:

- `network`: the request could not obtain a response;
- `http`: a non-success response other than the exact signed-out `401`;
- `invalid`: `204`, malformed JSON, a schema mismatch, or another value that
  cannot establish a valid session.

The session loader does not read the body of a non-success response. Successful
JSON is parsed into an allowlisted `AuthMe` shape; its identity, memberships,
authorization fields, and non-empty CSRF token must match the backend contract,
while extra server fields do not flow into application session state. A retry
is generation-bound so an older in-flight response cannot overwrite a later
logout or unauthorized event. The shared query client retains only the last
trusted authorization fingerprint across provider remounts, so root recovery
cannot carry cached data into a different authorization context. A provider
with no trusted fingerprint clears unknown pre-existing cache entries before
rendering children; a same-fingerprint remount may preserve them.

The same query client also owns a serialized browser-session operation queue.
An auth read, session-producing mutation, or logout started by an old provider
must settle before a remounted provider reads `/api/v1/auth/me`; the remounted
provider then installs the authoritative cookie, team, and CSRF state. An exact
unauthorized event advances the query client's authority epoch, so a response
from an older queued or in-flight operation cannot reinstall CSRF after the
browser has entered `signed-out`.

Login completion, password login, invite acceptance, and team switching are
session-producing mutations. Their response can be lost or malformed after the
server has already changed a cookie, team, or CSRF value. Any ambiguous
network, HTTP, or invalid-response failure therefore invalidates the trusted
authorization fingerprint, in-memory identity and CSRF token, and cached query
data, then enters `unavailable`. Retry calls `/api/v1/auth/me` to reconstruct
the authoritative state. An exact `401` instead enters `signed-out`. These
mutation loaders also classify without reading non-success response bodies.

The application mount marker maps these states to browser-smoke evidence:
`authenticated` becomes `data-loom-auth-state="authenticated"`, `signed-out`
becomes `anonymous`, and `unavailable` becomes `error`. Loading remains
unsettled. Browser smoke must not reinterpret `error` as anonymous success.

## Redaction invariants

Recovery UI and reports must never include or retain:

- raw `Error` objects, messages, names, component stacks, or browser stacks;
- response bodies, parser exceptions, server-provided detail, or raw status
  text;
- cookies, CSRF tokens, bearer/provider keys, passwords, signed URLs, or other
  secret-shaped values;
- a complete raw URL, query string, URL fragment, cross-origin source, or
  arbitrary enumerable throwable fields.

User-visible messages are fixed by failure kind. Status codes may guide the
internal classification, but they are not copied into recovery text or the
bounded report. The error-event and console bridges sanitize the throwable
surface before React or the browser can serialize it; boundary state retains
only the reference ID.

Reporter transports must preserve this schema exactly or define a new,
reviewed version. They must not attach the original throwable for convenience.

## Validation expectations

Changes to this boundary must cover, as applicable:

- loading, retry success, retry failure, Reload, and route-aware Home actions;
- exact `401` signed-out behavior versus network, other HTTP, malformed, and
  invalid-session unavailable behavior;
- stale-request and React StrictMode deduplication;
- root and route containment, navigation reset, distinct failure references,
  and same-failure report deduplication;
- the `reload-required` lazy-module path without a misleading Retry action;
- one accessible `main`, a focused alert, and no raw secret/error content;
- reporter/UI reference parity and absence of console/page-error leakage in a
  real browser failure-injection check.

The focused component tests live under `web/src/__tests__/bootstrap/`,
`web/src/__tests__/components/`, and `web/src/__tests__/AuthContext.test.tsx`.
The production-build recovery matrix lives in `web/e2e/recovery.test.ts` and
runs under both `/dev` and `/prod` prefixes in desktop and mobile Chromium.
Candidate-bound browser evidence remains required for rollout acceptance; unit
tests alone do not prove production browser event behavior.

## Browser-gate and rollout interface

The frontend quality gate provides the shared production-build Playwright
server, projects, closed API router, response
ledger, accessibility checks, and fail-closed console/page-error/network
guards. This recovery work must consume that harness rather than add another
workflow, Playwright configuration, coverage gate, or aggregate status.

The recovery matrix owns only recovery scenarios and assertions: one-shot
config HTTP/invalid failures, auth-session failure, root render failure, routed
render failure, retry success, navigation reset, redaction, focus, and
basename-safe actions. Deterministic render faults are compiled into the
dedicated Playwright build only and armed by the harness before navigation;
normal production bundles must not retain the fault key or fault strings. No
URL, runtime config, live API response, or production endpoint can activate
them. Lazy-route fixtures must wire `RouteRecoveryBoundary` with
`retryPolicy="reload-required"`; ordinary route failures retain the transient
Retry policy.

Admin browser acceptance uses normal account login through
`web/scripts/admin-browser-smoke.mjs`. It remains a healthy-path check without
fault injection. The local recovery matrix continues to own injected failures;
normal-login browser acceptance does not replace it. Credentials must come from
approved environment/file secret references, and the temporary browser session
must be logged out and closed after the run.

Production route imports use `lazyRoute`: only a rejected module import becomes
a fixed-copy `LazyRouteLoadError`, without retaining its raw diagnostic. The
existing route boundary stores a boolean reload requirement alongside the safe
reference; this automatically removes Retry for the cached rejection. Ordinary
render errors still retain Retry. Location changes reset both states. The same
boundary contains failures in lazily opened help and version details.
