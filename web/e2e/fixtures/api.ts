import type { Page, Route } from "@playwright/test";

import type { BrowserHarnessConfig } from "../../scripts/browser-harness-config.mjs";

export type BrowserRole = "logged-out" | "user" | "admin";
export type FixtureStatus = number | "network-error";

export type FixtureResponse =
  | {
      kind: "json";
      status: number;
      body: unknown;
      delayMs?: number;
    }
  | {
      kind: "text";
      status: number;
      body: string;
      contentType: string;
      delayMs?: number;
    }
  | {
      kind: "network-error";
      delayMs?: number;
    };

export type ApiOverride = Readonly<{
  name: string;
  method: string;
  /** Exact route-relative path, including any expected query string. */
  path: string;
  /** Exact cardinality. Defaults to one. */
  count?: number;
  /**
   * Continue through the closed router after this exact override is consumed.
   * Without this explicit opt-in, another matching request remains an error.
   */
  fallbackToDefault?: boolean;
  response: FixtureResponse;
}>;

export type RequestExpectation = Readonly<{
  name: string;
  method: string;
  path: string;
  status: FixtureStatus;
  count: number;
}>;

export type ApiLedgerEntry = Readonly<{
  sequence: number;
  method: string;
  path: string;
  status: FixtureStatus;
  source: "override" | "default" | "unmatched";
}>;

export type InstallApiFixtureOptions = Readonly<{
  role: BrowserRole;
  harness: BrowserHarnessConfig;
  overrides?: readonly ApiOverride[];
  expectations?: readonly RequestExpectation[];
}>;

export type ApiFixture = Readonly<{
  ledger: readonly ApiLedgerEntry[];
  expectRequest: (expectation: RequestExpectation) => void;
  assertComplete: () => void;
}>;

const team = { id: "team-eai", name: "EAI", role: "owner" };

function auth(role: Exclude<BrowserRole, "logged-out">) {
  const admin = role === "admin";
  return {
    user: {
      id: admin ? "user-admin" : "user-normal",
      username: admin ? "Admin" : "User",
      email: admin ? "admin@example.test" : "user@example.test",
      display_name: admin ? "Admin User" : "Normal User",
      is_platform_admin: admin,
    },
    teams: [team],
    current_team: team,
    role: admin ? "platform_admin" : "owner",
    scopes: ["read:own", "submit", "providers:manage", "tokens:manage", "team:manage"],
    is_platform_admin: admin,
    csrf_token: "local-browser-fixture",
  };
}

const overview = {
  status: "ready",
  summary: "This team can launch model-backed evaluations.",
  team_context: {
    team_id: team.id,
    team_name: team.name,
    role: team.role,
    scopes: ["read:own", "submit", "providers:manage", "team:manage"],
    is_platform_admin: false,
    submissions_paused: false,
  },
  capabilities: {
    can_read: true,
    can_submit: true,
    can_manage_providers: true,
    can_manage_team: true,
  },
  provider_health: { total: 0, ready: 0, needs_attention: 0, untested: 0, latest: [] },
  benchmark_readiness: { total: 0, runnable: 0, needs_attention: 0, blocked: [] },
  execution_health: { configured_targets: 1, status: "observed" },
  worker_health: { active: 0, available_backends: [], has_default_backend: false },
  run_activity: {
    batches: { submitted: 0, running: 0, finished: 0, cancelled: 0 },
    trials: { queued: 0, claimed: 0, running: 0, succeeded: 0, failed: 0, cancelled: 0 },
    latest_batch: null,
  },
  next_actions: [],
};

const taskSet = {
  task_set_id: "task-set-1",
  display_name: "Browser Task Set",
  status: "ready",
  status_reason: null,
  intents: ["evaluation", "trajectory_generation"],
  manifest_intents: ["evaluation"],
  inferred_intents: ["trajectory_generation"],
  capabilities: ["evaluation", "trajectory_generation"],
  warnings: [],
  evaluation_ready: true,
  task_count: 2,
  error_summary: [],
  materialization_job_state: null,
  created_at: "2026-07-16T00:00:00Z",
};

export function monitorSummary() {
  return {
    scope: { view: "trials", team_id: team.id, benchmark_id: null, agent: null, model: null, batch_id: null, state: null },
    state_counts: {
      batches: { submitted: 0, running: 0, finished: 0, cancelled: 0 },
      trials: { queued: 0, claimed: 0, running: 0, succeeded: 0, failed: 0, cancelled: 0 },
    },
    queue: {
      queued: 0,
      claimed: 0,
      running: 0,
      waiting: 0,
      active_workers: 1,
      available_backends: ["docker"],
      has_default_backend: true,
      status: "idle",
    },
    resources: {
      aggregate: {
        current_active_slots: 1,
        active_workers: 1,
        draining_workers: 0,
        total_slots: 1,
        draining_slots: 0,
        occupied_slots: 0,
        free_slots: 1,
        running_tasks: 0,
        starting_tasks: 0,
        queued_tasks: 0,
      },
      pools: [],
    },
    failure_counts: [],
    totals: { batches: 0, trials: 0 },
  };
}

function jsonResponse(body: unknown, status = 200): FixtureResponse {
  return { kind: "json", status, body };
}

function defaultApiResponse(
  role: BrowserRole,
  method: string,
  path: string,
): FixtureResponse | null {
  if (method !== "GET") return null;
  if (path === "/v1/auth/me") {
    if (role === "logged-out") return jsonResponse({ detail: "unauthorized" }, 401);
    return jsonResponse(auth(role));
  }
  // Version details fetch this role-independent endpoint when opened.
  if (path === "/v1/version") {
    return jsonResponse({ buildRevision: null, buildTime: null });
  }
  if (path === "/v1/overview") return jsonResponse(overview);
  if (path === "/v1/monitor/summary") return jsonResponse(monitorSummary());
  if (path === "/v1/tasksets") return jsonResponse({ items: [taskSet] });
  if (path === "/v1/tasksets/task-set-1") return jsonResponse(taskSet);
  if (path === "/v1/auth/public-teams") return jsonResponse({ items: [team] });
  if (path === "/v1/auth/setup/lookup") return jsonResponse({ detail: "expired setup token" }, 404);
  if (path === "/v1/auth/reset/lookup") return jsonResponse({ detail: "expired reset token" }, 404);
  if (path.startsWith("/v1/invites/")) return jsonResponse({ detail: "expired invite" }, 404);
  if (path === `/v1/teams/${team.id}`) {
    return jsonResponse({ ...team, created_at: "2026-07-16T00:00:00Z", quota: null, members: [], user_members: [], disabled_at: null });
  }
  if (path === "/v1/admin/teams") return jsonResponse({ items: [] });
  if (path === "/v1/admin/team-registrations") return jsonResponse({ items: [] });
  if (path === "/v1/admin/registration-requests") return jsonResponse({ items: [], next_cursor: null });
  if (path === "/v1/admin/password-reset-requests") return jsonResponse({ items: [], next_cursor: null });
  if (path === "/v1/admin/audit") return jsonResponse({ items: [], next_cursor: null });
  if (path === "/v1/rate-cards") return jsonResponse({ items: [] });
  if (path === "/v1/tokens") return jsonResponse({ items: [] });
  if (path === "/v1/invites") return jsonResponse({ items: [] });
  if (path === "/v1/usage") return jsonResponse({ degraded: false, buckets: [] });
  if (path === "/v1/backends") {
    return jsonResponse({
      items: [{
        name: "nebius",
        description: "Nebius Kubernetes execution pool; scales from zero.",
        available: false,
        cold_start_available: true,
        cold_start_pools: ["nebius-cpu"],
      }],
    });
  }
  if (path === "/v1/agents") return jsonResponse({ items: [] });
  if (path.startsWith("/v1/benchmarks")) return jsonResponse({ items: [], next_cursor: null });
  if (path.startsWith("/v1/models")) return jsonResponse({ items: [] });
  if (path.startsWith("/v1/provider-connections")) return jsonResponse({ items: [] });
  if (path === "/v1/local-servers") return jsonResponse({ items: [] });
  if (path === "/v1/tasks/count") return jsonResponse({ count: 0 });
  if (path === "/v1/tasks") return jsonResponse({ items: [], next_cursor: null });
  if (path.startsWith("/v1/run-library")) return jsonResponse({ items: [], next_cursor: null });
  if (path === "/v1/batches") return jsonResponse({ items: [], next_cursor: null });
  if (path === "/v1/trials") return jsonResponse({ items: [], next_cursor: null });
  return null;
}

function runtimeConfig(harness: BrowserHarnessConfig): unknown {
  return {
    environment: harness.runtimeEnvironment,
    environmentLabel: "Local browser quality gate fixture",
    routePath: harness.routePrefix,
    apiBase: harness.routePrefix,
    apiRouteBase: harness.apiBaseURL,
  };
}

function validateExactRule(name: string, method: string, path: string, count: number): void {
  if (!name.trim()) throw new Error("fixture rule name must not be empty");
  if (method !== method.toUpperCase() || !method.trim()) {
    throw new Error(`${name}: method must be a non-empty uppercase value`);
  }
  if (!path.startsWith("/") || path.includes("#")) {
    throw new Error(`${name}: path must be route-relative and start with /`);
  }
  if (!Number.isSafeInteger(count) || count <= 0) {
    throw new Error(`${name}: count must be a positive integer`);
  }
}

function safeRuleReference(path: string): string {
  const queryIndex = path.indexOf("?");
  const pathname = queryIndex === -1 ? path : path.slice(0, queryIndex);
  return queryIndex === -1 ? pathname : `${pathname}?<redacted>`;
}

async function fulfill(route: Route, response: FixtureResponse): Promise<FixtureStatus> {
  if (response.delayMs !== undefined) {
    if (!Number.isFinite(response.delayMs) || response.delayMs < 0) {
      throw new Error("fixture delayMs must be a non-negative finite number");
    }
    await new Promise((resolve) => setTimeout(resolve, response.delayMs));
  }
  if (response.kind === "network-error") {
    await route.abort("failed");
    return "network-error";
  }
  if (response.kind === "json") {
    await route.fulfill({
      status: response.status,
      contentType: "application/json",
      body: JSON.stringify(response.body),
    });
    return response.status;
  }
  await route.fulfill({
    status: response.status,
    contentType: response.contentType,
    body: response.body,
  });
  return response.status;
}

export async function installApiFixture(
  page: Page,
  options: InstallApiFixtureOptions,
): Promise<ApiFixture> {
  const { role, harness } = options;
  const ledger: ApiLedgerEntry[] = [];
  const errors: string[] = [];
  const expectations = [...(options.expectations ?? [])];
  const overrides = (options.overrides ?? []).map((override) => {
    const count = override.count ?? 1;
    validateExactRule(override.name, override.method, override.path, count);
    return { override, count, remaining: count };
  });
  for (const expectation of expectations) {
    validateExactRule(
      expectation.name,
      expectation.method,
      expectation.path,
      expectation.count,
    );
  }

  const pattern = `${harness.baseURL}/**`;
  await page.route(pattern, async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const method = request.method();
    const relativePath = `${url.pathname.slice(harness.routePrefix.length)}${url.search}`;
    const pathname = url.pathname.slice(harness.routePrefix.length + "/api".length);
    const matchingOverrides = overrides.filter(
      ({ override }) => override.method === method && override.path === relativePath,
    );
    const available = matchingOverrides.find(({ remaining }) => remaining > 0);
    if (available) {
      available.remaining -= 1;
      const status = await fulfill(route, available.override.response);
      ledger.push({ sequence: ledger.length + 1, method, path: relativePath, status, source: "override" });
      return;
    }
    if (matchingOverrides.length > 0) {
      const fallsBackToDefault =
        matchingOverrides.length === 1 &&
        matchingOverrides[0].override.fallbackToDefault === true;
      if (!fallsBackToDefault) {
        errors.push(`override exhausted for ${method} ${safeRuleReference(relativePath)}`);
        await route.fulfill({ status: 500, contentType: "application/json", body: "{}" });
        ledger.push({ sequence: ledger.length + 1, method, path: relativePath, status: 500, source: "unmatched" });
        return;
      }
    }

    let response: FixtureResponse | null = null;
    if (relativePath === "/loom-frontend-config.json" && method === "GET") {
      response = jsonResponse(runtimeConfig(harness));
    } else if (relativePath.startsWith("/api/")) {
      response = defaultApiResponse(role, method, pathname);
    } else {
      await route.fallback();
      return;
    }

    if (response === null) {
      errors.push(`unmatched fixture request ${method} ${safeRuleReference(relativePath)}`);
      response = jsonResponse({ detail: "local fixture has no response for this request" }, 404);
      const status = await fulfill(route, response);
      ledger.push({ sequence: ledger.length + 1, method, path: relativePath, status, source: "unmatched" });
      return;
    }
    const status = await fulfill(route, response);
    ledger.push({ sequence: ledger.length + 1, method, path: relativePath, status, source: "default" });
  });

  return {
    ledger,
    expectRequest(expectation) {
      validateExactRule(
        expectation.name,
        expectation.method,
        expectation.path,
        expectation.count,
      );
      expectations.push(expectation);
    },
    assertComplete() {
      for (const { override, count, remaining } of overrides) {
        if (remaining !== 0) {
          errors.push(
            `${override.name}: expected ${count} ${override.method} ${safeRuleReference(override.path)} response(s), observed ${count - remaining}`,
          );
        }
      }
      for (const expectation of expectations) {
        const observed = ledger.filter(
          (entry) =>
            entry.method === expectation.method &&
            entry.path === expectation.path &&
            entry.status === expectation.status,
        ).length;
        if (observed !== expectation.count) {
          errors.push(
            `${expectation.name}: expected ${expectation.count} ${expectation.method} ${safeRuleReference(expectation.path)} -> ${expectation.status}, observed ${observed}`,
          );
        }
      }
      if (errors.length > 0) throw new Error(errors.join("\n"));
    },
  };
}
