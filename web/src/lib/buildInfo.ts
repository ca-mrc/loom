/**
 * The "loaded bundle" build identity (#2009) — Vite `define` replaces
 * `__LOOM_BUILD_REVISION__` / `__LOOM_BUILD_SOURCE_REF__` / `__LOOM_BUILD_TIME__`
 * with literal strings at build time, from deploy/Dockerfile.web's build args
 * (see vite.config.ts). This is frozen in the exact JS this browser already
 * executed: it must never be derived from a later fetch, URL, or runtime
 * config — that would make an already-open tab silently relabel itself as a
 * build it never actually loaded.
 *
 * A plain local build (`npm run build` / `vite dev` with no --build-arg)
 * leaves these empty (or, via Dockerfile.web's ARG defaults, the literal
 * string "unknown") — both resolve to `null` here, so the UI shows an
 * honest "local/unknown" instead of a fabricated identity or a broken
 * commit link to `.../commit/unknown`.
 *
 * Vite 8.0.16's dev transform can leave an unbound identifier in some
 * paths (see browserTestBuild.ts); guard with `typeof` so a missing
 * define never crashes the app shell.
 */
/** Exported for direct unit testing — `LOADED_BUILD_INFO` below is a
 * module-level constant fixed by the real Vite define, so it can't
 * otherwise exercise the "unknown" placeholder branch in a plain
 * `vitest run` (that define is always "" there, never "unknown"). */
export function readDefine(value: unknown): string | null {
  const trimmed = typeof value === "string" ? value.trim() : "";
  // deploy/Dockerfile.web's ARGs default to the literal string "unknown"
  // (not empty) when no --build-arg is passed, so a plain `docker build`
  // bakes that literal into the bundle. Treat it the same as absent,
  // matching frontendConfig.ts's optionalBuildString for the served side.
  return trimmed && trimmed !== "unknown" ? trimmed : null;
}

export interface LoadedBuildInfo {
  /** Full commit SHA, or `null` for an unstamped local build. */
  revision: string | null;
  /** e.g. `refs/heads/dev`, or `null` for an unstamped local build. */
  sourceRef: string | null;
  /** Informational only — never used for ordering or trust decisions. */
  buildTime: string | null;
  kind: "personal" | "commit" | null;
  sourceDigest: string | null;
  baseCommit: string | null;
}

export function readSourceInfo(kind: unknown, digest: unknown, base: unknown): Pick<LoadedBuildInfo, "kind" | "sourceDigest" | "baseCommit"> {
  const personal = kind === "personal";
  return {
    kind: personal ? "personal" : kind === "commit" ? "commit" : null,
    sourceDigest: personal && typeof digest === "string" && /^sha256:[0-9a-f]{64}$/.test(digest) ? digest : null,
    baseCommit: personal && typeof base === "string" && /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/.test(base) ? base : null,
  };
}

const SOURCE_INFO = readSourceInfo(
  typeof __LOOM_BUILD_KIND__ === "string" ? __LOOM_BUILD_KIND__ : "",
  typeof __LOOM_SOURCE_DIGEST__ === "string" ? __LOOM_SOURCE_DIGEST__ : "",
  typeof __LOOM_SOURCE_BASE_COMMIT__ === "string" ? __LOOM_SOURCE_BASE_COMMIT__ : "",
);

export const LOADED_BUILD_INFO: LoadedBuildInfo = {
  ...SOURCE_INFO,
  revision: SOURCE_INFO.kind === "personal" ? null : readDefine(
    typeof __LOOM_BUILD_REVISION__ === "string" ? __LOOM_BUILD_REVISION__ : "",
  ),
  sourceRef: readDefine(
    typeof __LOOM_BUILD_SOURCE_REF__ === "string" ? __LOOM_BUILD_SOURCE_REF__ : "",
  ),
  buildTime: readDefine(
    typeof __LOOM_BUILD_TIME__ === "string" ? __LOOM_BUILD_TIME__ : "",
  ),
};

/** A short, human-friendly commit for the persistent sidebar entry. */
export function shortRevision(revision: string | null): string {
  if (!revision) return "local";
  return revision.slice(0, 12);
}

const REPO_COMMIT_BASE_URL = "https://github.com/qianyi-sun/loom/commit/";

export function commitUrl(revision: string | null): string | null {
  return revision ? `${REPO_COMMIT_BASE_URL}${revision}` : null;
}
