import { test as base, expect, type Page, type Response } from "@playwright/test";

import {
  installApiFixture,
  type ApiFixture,
  type InstallApiFixtureOptions,
} from "./api";
import {
  readBrowserHarnessConfig,
  type BrowserHarnessConfig,
} from "../../scripts/browser-harness-config.mjs";

export type DiagnosticExpectation =
  | Readonly<{
      kind: "console";
      level: "error" | "warning";
      message: string;
      count: number;
    }>
  | Readonly<{
      kind: "requestfailed";
      /** Exact route-relative path, without a query string. */
      path: string;
      count: number;
    }>;

export type FailureSink = Readonly<{
  failures: string[];
  expectDiagnostic: (expectation: DiagnosticExpectation) => void;
}>;

export type ApiHarness = Readonly<{
  install: (
    options: Omit<InstallApiFixtureOptions, "harness">,
  ) => Promise<ApiFixture>;
}>;

export type ReadinessCondition =
  | Readonly<{
      locator: string;
      state?: "attached" | "visible";
    }>
  | Readonly<{
      check: (page: Page) => Promise<void>;
    }>;

type GuardedFixtures = {
  apiHarness: ApiHarness;
  browserHarness: BrowserHarnessConfig;
  failureSink: FailureSink;
};

type TrackedDiagnostic = {
  expectation: DiagnosticExpectation;
  reference: string;
  remaining: number;
};

const ASSET_RESOURCE_TYPES = new Set([
  "font",
  "image",
  "manifest",
  "media",
  "other",
  "script",
  "stylesheet",
  "texttrack",
]);
const MAX_FAILURE_EVIDENCE = 20;

function boundedPath(path: string): string {
  const redacted = path
    .split("/")
    .map((segment) =>
      segment.length > 64 ||
      /(authorization|bearer|credential|password|secret|token|sk-)/iu.test(segment)
        ? "<redacted>"
        : segment,
    )
    .join("/");
  return redacted.length <= 180 ? redacted : `${redacted.slice(0, 177)}...`;
}

function routeRelativePath(url: URL, harness: BrowserHarnessConfig): string {
  return url.pathname.startsWith(`${harness.routePrefix}/`)
    ? url.pathname.slice(harness.routePrefix.length)
    : url.pathname;
}

function safeLocation(rawUrl: string, harness: BrowserHarnessConfig): string {
  try {
    const url = new URL(rawUrl);
    if (url.origin !== harness.origin) return "cross-origin";
    return boundedPath(routeRelativePath(url, harness));
  } catch {
    return "unknown";
  }
}

function badAssetReason(
  response: Response,
  harness: BrowserHarnessConfig,
): string | null {
  const url = new URL(response.url());
  if (url.origin !== harness.origin) return null;
  const type = response.request().resourceType();
  if (!ASSET_RESOURCE_TYPES.has(type)) return null;
  if (!response.ok()) return `status=${response.status()} type=${type}`;
  const contentType = response.headers()["content-type"] ?? "";
  if (type === "script" && !/(java|ecma)script/u.test(contentType)) {
    return `mime type=script`;
  }
  if (type === "stylesheet" && !/text\/css/u.test(contentType)) {
    return `mime type=stylesheet`;
  }
  return null;
}

function validateDiagnostic(expectation: DiagnosticExpectation): void {
  if (!Number.isSafeInteger(expectation.count) || expectation.count <= 0) {
    throw new Error("diagnostic expectation count must be a positive integer");
  }
  if (expectation.kind === "console" && !expectation.message) {
    throw new Error("console diagnostic message must not be empty");
  }
  if (
    expectation.kind === "console" &&
    !/^Failed to load resource: the server responded with a status of \d{3} \(.+\)$/u.test(
      expectation.message,
    )
  ) {
    throw new Error(
      "console expectations are limited to exact browser-generated HTTP resource diagnostics",
    );
  }
  if (expectation.kind === "requestfailed" && !expectation.path.startsWith("/")) {
    throw new Error("requestfailed diagnostic path must start with /");
  }
}

function installGuards(
  page: Page,
  harness: BrowserHarnessConfig,
  failures: string[],
  diagnostics: TrackedDiagnostic[],
): void {
  let failureSequence = 0;
  const record = (kind: string, detail: string): void => {
    failureSequence += 1;
    if (failures.length < MAX_FAILURE_EVIDENCE) {
      failures.push(`${kind} ref=${kind}-${failureSequence} ${detail}`);
    } else if (failures.length === MAX_FAILURE_EVIDENCE) {
      failures.push("failure-overflow ref=failure-overflow additional=<redacted>");
    }
  };

  page.on("pageerror", () => {
    record("pageerror", `location=${safeLocation(page.url(), harness)} message=<redacted>`);
  });
  page.on("console", (message) => {
    if (message.type() !== "error" && message.type() !== "warning") return;
    const expected = diagnostics.find(
      (entry) =>
        entry.remaining > 0 &&
        entry.expectation.kind === "console" &&
        entry.expectation.level === message.type() &&
        entry.expectation.message === message.text(),
    );
    if (expected) {
      expected.remaining -= 1;
      return;
    }
    const location = message.location();
    record(
      `console-${message.type()}`,
      `location=${safeLocation(location.url, harness)}:${location.lineNumber}:${location.columnNumber} message=<redacted>`,
    );
  });
  page.on("requestfailed", (request) => {
    const url = new URL(request.url());
    if (url.origin !== harness.origin) return;
    const path = routeRelativePath(url, harness);
    const expected = diagnostics.find(
      (entry) =>
        entry.remaining > 0 &&
        entry.expectation.kind === "requestfailed" &&
        entry.expectation.path === path,
    );
    if (expected) {
      expected.remaining -= 1;
      return;
    }
    record("requestfailed", `path=${boundedPath(path)}`);
  });
  page.on("response", (response) => {
    const reason = badAssetReason(response, harness);
    if (reason) {
      record(
        "bad-asset-response",
        `path=${boundedPath(routeRelativePath(new URL(response.url()), harness))} ${reason}`,
      );
    }
  });
}

export async function waitForReady(
  page: Page,
  condition: ReadinessCondition,
  timeout = 10_000,
): Promise<void> {
  if ("check" in condition) {
    await condition.check(page);
    return;
  }
  await page.locator(condition.locator).waitFor({
    state: condition.state ?? "visible",
    timeout,
  });
}

export const test = base.extend<GuardedFixtures>({
  browserHarness: async ({ page }, provideFixture) => {
    // Binding to the page keeps the validated config in the same test scope.
    page.url();
    await provideFixture(readBrowserHarnessConfig());
  },
  failureSink: async ({ page, browserHarness }, provideFixture) => {
    const failures: string[] = [];
    const diagnostics: TrackedDiagnostic[] = [];
    const sink: FailureSink = {
      failures,
      expectDiagnostic(expectation) {
        validateDiagnostic(expectation);
        diagnostics.push({
          expectation,
          reference: `diagnostic-${diagnostics.length + 1}`,
          remaining: expectation.count,
        });
      },
    };
    installGuards(page, browserHarness, failures, diagnostics);
    await provideFixture(sink);
    // Chromium can deliver an expected HTTP console diagnostic just after the
    // final page assertion. Keep its exact count, but await delivery before
    // deciding that it is missing; the existing assertion timeout bounds this.
    await expect.poll(() => diagnostics
      .filter(diagnostic => diagnostic.remaining !== 0)
      .map(diagnostic =>
        `unconsumed-diagnostic ref=${diagnostic.reference} expected=${diagnostic.expectation.count} observed=${diagnostic.expectation.count - diagnostic.remaining}`,
      )).toEqual([]);
    expect(failures, failures.join("\n")).toEqual([]);
  },
  apiHarness: async ({ page, browserHarness }, provideFixture) => {
    const installed: ApiFixture[] = [];
    await provideFixture({
      async install(options) {
        if (installed.length > 0) {
          throw new Error("API fixture may be installed only once per page");
        }
        const fixture = await installApiFixture(page, {
          ...options,
          harness: browserHarness,
        });
        installed.push(fixture);
        return fixture;
      },
    });
    for (const fixture of installed) fixture.assertComplete();
  },
});

export { expect };
