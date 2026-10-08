import { readSourceInfo } from "./buildInfo";

export type FrontendEnvironment =
  | "local"
  | "development"
  | "staging"
  | "production";

export interface FrontendConfig {
  environment: FrontendEnvironment;
  environmentLabel: string;
  routePath: string;
  apiBase: string;
  apiRouteBase: string;
  /**
   * #2009: the "currently served" build identity — whatever this fetch of
   * `loom-frontend-config.json` reports right now, independent of what this
   * tab's own JS bundle already loaded (see lib/buildInfo.ts for that).
   * Optional/lenient: an older cached config or a field this build predates
   * must not fail config resolution, so missing values default to `null`
   * rather than throwing.
   */
  servedBuildRevision?: string | null;
  servedSourceRef?: string | null;
  servedBuildTime?: string | null;
  servedSourceDigest?: string | null;
}

export type FrontendConfigFailureKind = "network" | "http" | "invalid";

/**
 * A deliberately detail-free startup failure. Runtime response bodies and
 * parser errors must never become browser-visible diagnostics.
 */
export class FrontendConfigLoadError extends Error {
  readonly kind: FrontendConfigFailureKind;
  readonly status: number | null;

  constructor(kind: FrontendConfigFailureKind, status: number | null = null) {
    const message =
      kind === "network"
        ? "frontend runtime config request failed"
        : kind === "http"
          ? "frontend runtime config returned an unsuccessful status"
          : "frontend runtime config is invalid";
    super(message);
    this.name = "FrontendConfigLoadError";
    this.kind = kind;
    this.status = status;
  }
}

interface RawFrontendConfig {
  environment?: unknown;
  environmentLabel?: unknown;
  routePath?: unknown;
  apiBase?: unknown;
  apiRouteBase?: unknown;
  buildRevision?: unknown;
  sourceRef?: unknown;
  buildTime?: unknown;
  buildKind?: unknown;
  sourceDigest?: unknown;
  sourceBaseCommit?: unknown;
}

/** Lenient: a non-string, empty, or `"unknown"` placeholder all mean "not
 * reported", never a thrown error — version metadata must never block
 * startup. */
function optionalBuildString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed && trimmed !== "unknown" ? trimmed : null;
}

const STAGING_REHEARSAL_ROUTE_PATTERN =
  /^(\/staging\/rehearsal\/[0-9a-f]{24})(?:\/|$)/u;

function viteApiBase(): string {
  const env = (
    import.meta as unknown as { env: { VITE_API_BASE?: string } }
  ).env;
  return env?.VITE_API_BASE ?? "";
}

function expectString(
  value: unknown,
  field: keyof RawFrontendConfig,
): string {
  if (typeof value !== "string") {
    throw new Error(`frontend config ${field} must be a string`);
  }
  return value.trim();
}

function normalizePath(raw: string, field: string): string {
  if (raw === "" || raw === "/") return "";
  if (!raw.startsWith("/")) {
    throw new Error(`frontend config ${field} must start with /`);
  }
  return raw.replace(/\/+$/u, "");
}

function detectRoutePath(location: Pick<Location, "pathname">): string {
  const pathname = location.pathname.replace(/\/+$/u, "") || "/";
  const rehearsal = pathname.match(STAGING_REHEARSAL_ROUTE_PATTERN);
  if (rehearsal) return rehearsal[1];
  if (pathname === "/prod" || pathname.startsWith("/prod/")) return "/prod";
  if (pathname === "/staging" || pathname.startsWith("/staging/")) {
    return "/staging";
  }
  if (pathname === "/dev" || pathname.startsWith("/dev/")) return "/dev";
  return "";
}

function apiBasePath(apiBase: string, location: URL): string {
  if (apiBase === "") return "";
  if (apiBase.startsWith("http://") || apiBase.startsWith("https://")) {
    return normalizePath(new URL(apiBase).pathname, "apiBase");
  }
  return normalizePath(new URL(apiBase, location.origin).pathname, "apiBase");
}

function apiRouteBase(apiBase: string, location: URL): string {
  const base = apiBase || "/";
  const url = new URL(`${base.replace(/\/+$/u, "")}/api`, location.origin);
  return url.href.replace(/\/+$/u, "");
}

function resolveApiRouteBase(
  raw: RawFrontendConfig,
  apiBase: string,
  location: URL,
): string {
  const fallback = apiRouteBase(apiBase, location);
  if (raw.apiRouteBase === undefined || raw.apiRouteBase === null) {
    return fallback;
  }
  const value = expectString(raw.apiRouteBase, "apiRouteBase");
  if (!value) return fallback;
  const parsed = new URL(value, location.origin);
  const expectedPath = `${apiBase || ""}/api`;
  if (parsed.pathname.replace(/\/+$/u, "") !== expectedPath) {
    throw new Error(
      `frontend config apiRouteBase ${value} must match apiBase ${apiBase || "/"}`,
    );
  }
  return value.replace(/\/+$/u, "");
}

function routeMatches(routePath: string, location: URL): boolean {
  if (routePath === "") return true;
  return (
    location.pathname === routePath ||
    location.pathname.startsWith(`${routePath}/`)
  );
}

function isFrontendEnvironment(value: string): value is FrontendEnvironment {
  return ["local", "development", "staging", "production"].includes(value);
}

export function resolveFrontendConfig(
  raw: RawFrontendConfig,
  locationLike: Location | URL = window.location,
  opts: { allowRouteMismatch?: boolean } = {},
): FrontendConfig {
  const location = new URL(locationLike.href);
  const environment = expectString(raw.environment, "environment");
  if (!isFrontendEnvironment(environment)) {
    throw new Error(`frontend config environment ${environment} is not supported`);
  }
  const environmentLabel = expectString(
    raw.environmentLabel,
    "environmentLabel",
  );
  if (!environmentLabel) {
    throw new Error("frontend config environmentLabel must not be empty");
  }
  const routePath = normalizePath(
    expectString(raw.routePath ?? "", "routePath"),
    "routePath",
  );
  const apiBase = normalizePath(
    expectString(raw.apiBase ?? routePath, "apiBase"),
    "apiBase",
  );

  if (!opts.allowRouteMismatch && !routeMatches(routePath, location)) {
    throw new Error(
      `frontend config routePath ${routePath || "/"} does not match current route ${location.pathname}`,
    );
  }

  if (apiBasePath(apiBase, location) !== routePath) {
    throw new Error(
      `frontend config apiBase ${apiBase || "/"} must match routePath ${routePath || "/"}`,
    );
  }

  const detectedRoutePath = detectRoutePath(location);
  if (
    !opts.allowRouteMismatch &&
    detectedRoutePath &&
    detectedRoutePath !== routePath
  ) {
    throw new Error(
      `frontend config routePath ${routePath || "/"} does not match current route ${detectedRoutePath}`,
    );
  }

  const canonicalRoutePaths: Record<FrontendEnvironment, RegExp> = {
    local: /^$/u,
    development: /^(?:|\/dev)$/u,
    staging: /^(?:|\/staging(?:\/rehearsal\/[0-9a-f]{24})?)$/u,
    production: /^\/prod$/u,
  };
  if (!canonicalRoutePaths[environment].test(routePath)) {
    throw new Error(
      `${environment} frontend config does not use its canonical routePath`,
    );
  }
  if (environment === "production" && /beta/i.test(environmentLabel)) {
    throw new Error("production frontend label must not contain beta wording");
  }

  return {
    environment,
    environmentLabel,
    routePath,
    apiBase,
    apiRouteBase: resolveApiRouteBase(raw, apiBase, location),
    servedBuildRevision: raw.buildKind === "personal" ? null : optionalBuildString(raw.buildRevision),
    servedSourceRef: optionalBuildString(raw.sourceRef),
    servedBuildTime: optionalBuildString(raw.buildTime),
    ...(raw.buildKind === "personal" ? { servedSourceDigest: readSourceInfo(raw.buildKind, raw.sourceDigest, raw.sourceBaseCommit).sourceDigest } : {}),
  };
}

function defaultConfig(): FrontendConfig {
  return resolveFrontendConfig(
    {
      environment: "local",
      environmentLabel: "Local development",
      routePath: "",
      apiBase: viteApiBase(),
    },
    window.location,
    { allowRouteMismatch: true },
  );
}

let currentConfig: FrontendConfig = defaultConfig();
let currentLoad: Promise<FrontendConfig> | null = null;

function configUrlForLocation(location: Location): string {
  const routePath = detectRoutePath(location);
  return `${routePath}/loom-frontend-config.json`;
}

function allowLocalBuildFallback(routePath: string): boolean {
  const env = (import.meta as unknown as { env?: { DEV?: boolean } }).env;
  return routePath === "" && env?.DEV === true;
}

async function performFrontendConfigLoad(): Promise<FrontendConfig> {
  const url = configUrlForLocation(window.location);
  const routePath = detectRoutePath(window.location);
  let resp: Response;
  try {
    resp = await fetch(url, {
      cache: "no-store",
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    });
  } catch {
    if (allowLocalBuildFallback(routePath)) {
      currentConfig = defaultConfig();
      return currentConfig;
    }
    throw new FrontendConfigLoadError("network");
  }

  if (!resp.ok) {
    if (allowLocalBuildFallback(routePath)) {
      currentConfig = defaultConfig();
      return currentConfig;
    }
    // Do not read the body of an unsuccessful response. It may contain an
    // upstream error page, request details, or secret-shaped diagnostics.
    throw new FrontendConfigLoadError("http", resp.status);
  }

  try {
    const raw = (await resp.json()) as RawFrontendConfig;
    currentConfig = resolveFrontendConfig(raw, window.location);
    return currentConfig;
  } catch {
    throw new FrontendConfigLoadError("invalid");
  }
}

/**
 * Return one shared promise per startup attempt. React StrictMode may mount
 * effects twice, but a single attempt must issue exactly one config request.
 */
export function loadFrontendConfig(): Promise<FrontendConfig> {
  currentLoad ??= performFrontendConfigLoad();
  return currentLoad;
}

export function resetFrontendConfigLoad(): void {
  currentLoad = null;
}

export function getFrontendConfig(): FrontendConfig {
  return currentConfig;
}

export function getApiBase(): string {
  return currentConfig.apiBase;
}

/**
 * #2009: an independent, side-effect-free re-fetch of the "currently
 * served" build identity — used by the focus-triggered staleness check.
 * Deliberately does not touch `currentConfig`/`currentLoad`: startup
 * config resolution and update polling are separate concerns, and a
 * failed poll must never disturb the config the app is already running
 * with. Never throws; a failure just means "couldn't tell right now".
 */
export async function fetchServedBuildInfo(): Promise<{
  revision: string | null;
  sourceRef: string | null;
  buildTime: string | null;
  sourceDigest?: string | null;
} | null> {
  try {
    const resp = await fetch(configUrlForLocation(window.location), {
      cache: "no-store",
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    });
    if (!resp.ok) return null;
    const raw = (await resp.json()) as RawFrontendConfig;
    return {
      revision: raw.buildKind === "personal" ? null : optionalBuildString(raw.buildRevision),
      sourceRef: optionalBuildString(raw.sourceRef),
      buildTime: optionalBuildString(raw.buildTime),
      ...(raw.buildKind === "personal" ? { sourceDigest: readSourceInfo(raw.buildKind, raw.sourceDigest, raw.sourceBaseCommit).sourceDigest } : {}),
    };
  } catch {
    return null;
  }
}

export function setFrontendConfigForTests(config: FrontendConfig | null): void {
  currentLoad = null;
  currentConfig = config ?? defaultConfig();
}

export function frontendHomePath(
  locationLike: Pick<Location, "pathname"> = window.location,
): string {
  const pathname = locationLike.pathname.replace(/\/+$/u, "") || "/";
  const rehearsal = pathname.match(STAGING_REHEARSAL_ROUTE_PATTERN);
  if (rehearsal) return `${rehearsal[1]}/`;
  if (
    pathname === "/staging/rehearsal" ||
    pathname.startsWith("/staging/rehearsal/")
  ) {
    return "/";
  }
  if (pathname === "/staging" || pathname.startsWith("/staging/")) {
    return "/staging/";
  }
  if (pathname === "/prod" || pathname.startsWith("/prod/")) return "/prod/";
  if (pathname === "/dev" || pathname.startsWith("/dev/")) return "/dev/";
  return "/";
}
