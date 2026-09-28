# Frontend quality gate

The required frontend gate is a repository-level prerequisite for any frontend
candidate. It is selected by `scripts/plan_ci_validations.py`, runs as
`web-checks`, and is enforced by `repository-checks`. A selected `web-checks`
result that is failed, cancelled, missing, or otherwise non-successful fails
the aggregate.

## Required layers

The gate uses a frozen `npm ci` install and requires:

1. strict TypeScript for production, unit-test, and Playwright sources without
   weakening `strict`, adding debt exclusions, or taking ownership of generated
   API declarations;
2. ESLint and Vitest with coverage floors of 80% statements, lines, and
   functions and 75% branches;
3. a successful production Vite build;
4. Chromium against that production build under a validated local `/dev` or
   `/prod` route prefix at 390x844, 768x1024, 1280x800 and 1440x900 for logged-out, user, and admin
   routes; and
5. deterministic offline OpenAPI regeneration (`npm run gen-api -- --check`); and
6. zero serious or critical axe violations plus fail-closed page-error,
   console, unhandled-error, same-origin network, asset status, and MIME
   ledgers.

The Vite default is the relative-asset build. The Playwright
server reads one validated `BrowserHarnessConfig` from `LOOM_E2E_ORIGIN` and
`LOOM_E2E_ROUTE_PREFIX`; the origin must be credential-free local HTTP and the
prefix must be exactly `/dev` or `/prod`. The default is
`http://127.0.0.1:4173/dev`. `build-browser-test.mjs` supplies that prefix to
Vite and is the only command that compiles `IS_BROWSER_TEST_BUILD` as `true`.
Normal production builds compile the constant as `false`; URL state, runtime
configuration, HTTP responses, and endpoints cannot change it.

Playwright sets `reuseExistingServer: false` for local and CI execution. Every
browser evidence run must therefore invoke `build-browser-test.mjs` and start
its own prefix server; an occupied origin fails the run instead of accepting a
stale server, an ordinary production bundle, or an SPA fallback at the config
URL. This binds local recovery evidence to the browser-test bundle that carries
the compile-time marker.

`ApiHarness.install` installs deterministic local-only responses and returns an
`ApiFixture`. Scenario-neutral `ApiOverride` rules match an exact uppercase
method and route-relative path, derive the expected status from their response,
and default to cardinality one. They support delayed JSON, arbitrary typed text
(including deliberately invalid JSON with an explicit content type), HTTP
statuses, and network failure. `ApiFixture.ledger`, `expectRequest`, and fixture
teardown enforce exact method/path/status/cardinality. Exhausted overrides and
unknown API requests fail closed. These fixtures contain synthetic identities
only and must never receive local or live credentials.

`FailureSink.expectDiagnostic` is an exact, consumed diagnostic ledger for
browser-generated console and expected same-origin network events. An
unconsumed declaration fails teardown; recovery boundary errors must not be
allowlisted. Unexpected console and page errors retain only event kind,
route-relative location, and a bounded reference while message content is
redacted. All same-origin browser assets fail on non-success status, with
script and stylesheet MIME validation. `waitForReady` accepts either a stable
locator or a caller-provided asynchronous condition, so extensions can define
their own success marker without changing the generic harness.

## Ownership boundary

`config/component-ownership.toml` and
`scripts/component_ownership.py test-paths --lane frontend` are the authority
for component/test membership when that lane query is available. The workflow
feature-detects and consumes its output; before that command lands it runs the
complete Vitest suite. It does not maintain a second copy of the owned
TypeScript test globs. This document and the workflow own only the quality
policy, specialized browser harnesses, and aggregate behavior.

## Recovery extension contract

Recovery work may consume `BrowserHarnessConfig`, `ApiHarness.install`,
`ApiOverride`, `ApiFixture`, `RequestExpectation`, `FailureSink`,
`DiagnosticExpectation`, `waitForReady`, the production-build server, axe
integration, and the console/network/error guards, then add its own recovery
scenarios and specifications. Recovery UI and error-reporting behavior do not
belong in this foundation.

Any root-render fault seam used by recovery tests must be compiled only into an
explicit test build. A URL, runtime configuration value, live response, or
endpoint must never activate it. A lazily loaded fault fixture must explicitly
declare that a reload is required; switching the fixture must not imply that an
already loaded module changed in place.

## Candidate and broker acceptance

Passing repository CI is necessary but is not staging acceptance. A protected
rollout uses this gate only after the change has merged to `dev`, the
candidate has been fixed to that merged SHA, and the rollout coordinator has
authorized the broker-owned rollout. Candidate-bound browser evidence then
extends—not replaces—the repository gate. Local or Draft-PR work must never be
inserted into, used to re-resolve, or used to replace an already fixed rollout
candidate.

See [frontend domain boundaries](frontend-domain-boundaries.md) for code generation,
ownership, aggregate catalog discovery and the local/manual acceptance boundary.

## JavaScript loading and bundle budgets (#212)

`App.tsx` lazy-loads routed pages. The managed-login route stays outside Layout
so consuming its proof is not remounted by authentication changes; the
entrypoint still scrubs the proof before any asynchronous work. Contextual help
and version details load on opening. The existing ten-minute served-build check
remains active while details are closed; the backend version request is deferred
until details open. Startup imports session/core API code directly instead of
the composed domain API, with session mutation code loaded on user action.

`npm run build` runs the marker verifier and `check-bundle-budget.mjs`. The
checker reads Vite's production manifest and recursively follows static imports,
deduplicating shared dependencies. It sums minified file bytes and each file's
Node gzip bytes (default compression); source maps are excluded. Decimal kB
means 1,000 bytes. It enforces:

- entrypoint plus all static startup dependencies: 260 kB / 85 kB gzip;
- each dynamic entry plus its static dependencies not already in startup:
  300 kB / 90 kB gzip (shared dependencies count on a cold route visit);
- every emitted JS file, even one outside the manifest: no file above 500 kB;
- at most 10% growth for either metric against `web/bundle-baseline.json`.

The startup number is the common application shell, not the sum of all requests
needed to render a particular first page: a cold Home visit also downloads its
lazy dependency closure. `dist/bundle-report.json` records both categories with
exact asset lists, plus `coldEntries` (deduplicated shell + dynamic entry static
closure). These are module-graph measurements, not a claim that deferred children
mounted by a page's default view cost zero. The route browser suite additionally
attaches `cold-route-scripts.json` with actual requested JS and compressed sizes
before reload. CI retains the production report before the browser-test build
replaces `dist`.

The original #212 startup target was 250/80 kB. The owner approved prioritizing
useful whole-frontend optimization over mechanically fitting that target. The
260/85 shell ceiling leaves room for the expanded lazy-import map and release
metadata; it does not redefine a complete cold page as just the shell. The
500 kB emitted-file limit and the 10% regression check remain unchanged.

Baseline keys use source paths instead of hashed asset names. Missing entries
fail. After reviewing the composition and user impact of an intentional change,
run a production Vite build followed by `npm run bundle:baseline`, then commit
and review that diff with the change. This explicit update can approve relative
growth but cannot waive any hard cap. Normal builds never rewrite the baseline.
No Vite warning threshold is raised. Lazy chunk URLs use compact content hashes
to reduce the startup preload map; source identities remain in the manifest.
Vite's default modulepreload behavior and polyfill are preserved.

`web/e2e/lazy-routes.test.ts` resolves actual filenames from the build manifest,
retains request evidence proving that Home does not fetch unrelated page
modules, and exercises delayed and rejected real module imports. It shares the
existing prefix server and fail-closed guards, with no production fault hook.
Run it and the route/recovery suites under both `/dev` and `/prod`. These local
checks do not establish hosted AMD64 fixed-candidate acceptance.

### Whole-frontend audit

The implementation audits all routed pages, shared widgets, polling, lists and
optional workflows. Splitting follows interaction boundaries rather than adding
a Suspense boundary to every small component:

| Area | Loading/rendering decision |
| --- | --- |
| All routed pages, authentication/onboarding, Home, guides, Settings | Load the destination page independently; preserve managed-proof scrubbing and session serialization. |
| Task Set submission | Import YAML parsing on Review submission; preserve revision checks, syntax validation and the separate confirm-upload action. |
| New batch | Load advanced fields on first expansion and retain them afterward so edits and validation focus survive collapse; load export UI on export. |
| Monitor | Load the selected batches/trials view; mount resource diagnostics only while expanded. Keep summary polling and URL filters outside these boundaries. |
| Admin access | Load tokens, teams, legacy requests and audit tabs independently; preserve existing per-tab query enablement. |
| Provider detail | Overview does not download Models, editable settings or credential dialogs; each loads when used. |
| Batch/trial diagnostics | Load the JSON tree and its CSS when raw details are opened. |
| Pipeline run and artifact detail | Load the stage drawer, eligible live preview and specialized rollout viewer only when required. Generic artifacts use the small generic renderer. |
| Library, Tasks, Benchmarks, Pipeline lists | Preserve existing server pagination, bounded pages and the large-stage-list virtualizer. |
| Usage, rates and remaining small pages | Keep existing lightweight native/SVG rendering; no chart framework or speculative memoization added. |

Existing visibility-aware polling, terminal-state stopping/backoff, bounded
artifact JSON reads, and explicit refresh to protect unsaved input remain in
place. Collapsing Monitor capacity diagnostics additionally unmounts nested
placement queries. No new dependency, manual framework chunk, warning-threshold
increase, prefetch of unrelated routes, or production deployment is required.

Browser coverage includes module request isolation, real delayed/rejected module
imports, collapsed panel expansion, retained advanced-field values, deferred
manifest validation, provider model tabs and admin tab isolation. Existing
form submission, auth, navigation, error recovery and accessibility suites remain
part of verification. Size reduction is measured; server latency and real-user
LCP/INP improvement require separately collected deployed evidence.

On the September 28 local production build, the original eager entry was
855.56 kB / 234.80 kB gzip. The final common shell is 247.96 / 79.89 kB;
the Home cold module closure is 263.83 / 85.98 kB. The largest emitted JS is
132.68 kB. Browser-test instrumentation adds a small amount: a cold user Home
visit requested 264.49 / 86.22 kB, while New batch requested 348.12 / 112.23 kB.
These complete-route figures are intentionally reported separately from the
shell ceiling. Sizes are build/platform snapshots, not LCP or INP claims.

Compared with route splitting alone, optional-content deferral reduces these
additional route static closures (minified decimal kB):

| Page | Route split only | With optional content deferred |
| --- | ---: | ---: |
| Task Set submission | 126.02 | 28.10 |
| New batch | 118.71 | 99.47 |
| Monitor | 67.35 | 43.27 |
| Admin access | 82.03 | 66.51 |
| Provider detail | 70.13 | 39.37 |
| Pipeline run | 65.73 | 57.23 |
| Pipeline artifact | 34.83 | 25.94 |

The Monitor default view and selected admin tab can add their own lazy modules;
these static-closure numbers should not be substituted for browser request
totals or summed without deduplication.

The web image uses the same `npm ci` lockfile graph as local/CI builds. Linux
native bindings for both supported architectures are pinned in that lockfile
and checked during the image build. Do not add a second unlocked `npm install`
to repair optional bindings: that silently upgraded Vite 8.0.16 to 8.3.1 and
71 dependencies, changing chunk composition and invalidating the measured
baseline. Fix missing binding declarations/lockfile entries instead. Browser
handoff tests wait for the destination viewer's content, not just its route
header, because those now load at different times.
