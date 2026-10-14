# Web presentation and diagnostics

The Loom web application presents product concepts in plain language by
default and keeps internal request fields, raw payloads, identifiers, and event
data in explicit diagnostics disclosures.

## Shared presentation components

- `HelpButton` opens the relevant in-app help topic.
- `DiagnosticPanel` renders a closed `details` disclosure for raw/internal
  data. `JsonViewer` is used inside diagnostics unless the page is an explicit
  JSON editor.
- `CopyableId` shortens long identifiers visually while preserving the full
  value and a copy action.
- `DestructiveActionDialog` provides the confirmation, pending, retry, and
  focus behavior for destructive or credential-invalidating mutations.
- `humanizeTaskFilter`, `humanizeTrialConfig`, and `humanizeFailureReason`
  return structured summaries shared across pages.

## Page behavior

### New Batch

The form uses `Task selection`, `Agent/model combinations`, `Samples per task`,
and `Advanced trial settings` rather than API field names. Readiness data from
`/api/v1/benchmarks` determines which catalog rows are selectable. The summary
shows the expansion formula:

```text
tasks × samples × combinations = planned trials
```

Internal `task_filter` and `trial_config` names are not shown when defaults are
used. Submission locks synchronously so repeated clicks cannot create duplicate
requests.

### Batch and Run Library details

Default views show the task selection, combinations, backend/provider choice,
and shared trial settings as a run plan. Raw filters, trial configuration,
combination payloads, and fan-out details remain available through diagnostics.
Fan-out failures also produce a readable alert before the raw payload.

### Monitor and Trial Detail

Monitor distinguishes planned trials, platform state, and evaluator reward.
Trial Detail renders a plain-language platform outcome, a humanized failure
reason plus its stable code, readable artifact/download labels, and summarized
timeline rows. Raw event objects are available per row under `Raw event data`.
Nebius service execution has a separate capacity and lifecycle panel: fresh
executable slots are not conflated with configured scale headroom, and Pod,
materialization, retry, source-retention, and complete-bundle states remain
visible without exposing provider target ids or raw internal errors to ordinary
users.

Monitor exposes Batches, Trials and Capacity modes. The list toolbar keeps search,
state and team visible; Advanced filters contains benchmark, agent and model
selectors, with removable chips for active values. Trial search is applied to all
authorized records before cursor pagination, including trial/task IDs and owner
names. The URL stores the cursor and previous-page history; changing a filter
resets to page one. Detail return links preserve that URL. Capacity diagnostics
remain available in a disclosure and through `?view=capacity`; unknown observations
are never converted to zero.

Trial details provide Overview, Trajectory, Artifacts and Diagnostics anchors,
with the complete bundle action next to the outcome. Terminal trials with no
trajectory explain the final absence and link to diagnostics. Compare provides
server-side trial search, task mismatch warnings, numeric metric differences,
replace/remove controls, full-detail links and continuation past 200 events.
Batch run plans recognize TaskSet sources and display the recorded purpose.

### Providers and operator pages

Provider states are presented as readiness (`Ready`, `Needs attention`, or
`Untested`) with allowed-model summaries. Settings, Admin, Rate Cards, and Usage
use product labels and short definitions; raw provider and pricing payloads are
diagnostic data.

## Destructive action contract

- Provider deletion, provider credential rotation, and TaskSet deletion require
  the exact case-sensitive target.
- Batch cancellation, token mutation, invite mutation, and registration/reset
  rejection require an explicit confirmation naming the consequence.
- While a mutation is pending, confirm, cancel, Escape, backdrop, and close
  dismissal are disabled.
- Failed requests keep the selected target and entered fields mounted and show
  one redacted retryable error.
- Dialogs close or navigate only after server-confirmed success. Busy state is
  scoped to the exact target.
- Server authorization, CSRF, audit, and conflict checks remain authoritative.

## Accessibility

Diagnostics use native disclosure semantics. Shared tabs expose tab/panel
relationships and keyboard navigation. The shared modal owns labelled title and
description relationships, initial focus, focus containment, Escape/backdrop
behavior, background inertness, body scroll locking, and final focus
restoration. Long identifiers and model names wrap without obscuring controls.

## Verification

Presentation behavior is covered by focused component, humanizer, and page
tests under `web/src/__tests__/`. The full frontend gate is:

```bash
cd web
npm test
npm run build
npm run lint
```

The cross-layer accessibility contract also runs through:

```bash
uv run pytest -q tests/ops/test_frontend_accessibility_contract.py
```

## Navigation, filters and table access

Every normal shell includes a keyboard skip link that focuses `main`, as do
loading/recovery shells. `RouteContext` supplies titles and breadcrumb links
using the router basename. Getting started links to Task sets; the TaskSet
routes remain available outside primary navigation.

Run Library keeps search visible, debounces it for 300 ms, and puts the dense
filters behind native `details`. Removable chips and Clear filters preserve
scope/team context; URL changes, refresh and back/forward remain authoritative.
The retained Pipeline artifact mode preserves its recipe/result filters.

Native tables have names and column headers. Horizontal table containers are
named keyboard-focusable regions, including tables with no row actions.
Virtual Pipeline stages retain a header row, total row count including that
header and absolute data row indexes. Decorative card separators stay subtle;
form controls and secondary buttons use a contrasting slate-500 boundary.
Focus outlines apply to every focusable element, including disclosures and
scroll regions. Reduced-motion preference suppresses transitions and animation.

`LoadingState` owns a polite status and `ErrorState` owns an alert by default.
Callers with their own live region pass `announce={false}`: authentication and
destructive-dialog alerts, plus paginated Run Library/audit loading updates.
Status pills remain visible textual labels without a live region, so polling
rows does not repeatedly announce unchanged status.

Measured token pairs: slate-500 on white is 4.76:1 and on slate-50 is 4.55:1;
indigo-600 on white is 6.29:1 and on slate-50 is 6.01:1. The field-boundary and
text assertions read computed browser colors; axe covers rendered foreground
pairs and textual status labels. These measurements are not a blanket claim
about every state or third-party artifact viewer.

### Section and member identity conventions

Page-section `Card.Header` defaults to h2 beneath the page h1. Nested sections
choose h3/h4 explicitly; artifact groups and their CLI callout are children of
the Artifacts heading. This preserves the visual style while making navigation
order understandable.

Team members may have a username without an email or display name. The team API
returns username and nullable email, and Settings uses the username when no
display name is present. Missing optional contact data must not make a valid
team response fail or leave a member unnamed.
