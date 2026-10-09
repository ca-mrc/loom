import { describe, expect, it } from "vitest";

import {
  commitUrl,
  LOADED_BUILD_INFO,
  readDefine,
  shortRevision,
} from "./buildInfo";

describe("loaded build info (#2009)", () => {
  it("reports an honest local/unknown identity for the normal unit-test build", () => {
    // vite.config.ts defines __LOOM_BUILD_*__ from VITE_BUILD_* env vars,
    // which are unset for `vitest run` — the same shape a plain local
    // `npm run build`/`vite dev` produces.
    expect(LOADED_BUILD_INFO.revision).toBeNull();
    expect(LOADED_BUILD_INFO.sourceRef).toBeNull();
    expect(LOADED_BUILD_INFO.buildTime).toBeNull();
  });

  it("readDefine treats Dockerfile.web's 'unknown' ARG default as absent, not a real value", () => {
    // Regression: deploy/Dockerfile.web's `ARG LOOM_BUILD_SHA=unknown`
    // bakes the literal string "unknown" into the bundle for a plain
    // `docker build` with no --build-arg. Confirmed via a real build: the
    // compiled bundle contained `VITE_BUILD_REVISION:"unknown"`. Left
    // unfiltered, this rendered a broken `.../commit/unknown` link and
    // showed "unknown" instead of "local" in the sidebar.
    expect(readDefine("unknown")).toBeNull();
    expect(readDefine("")).toBeNull();
    expect(readDefine("   ")).toBeNull();
    expect(readDefine(undefined)).toBeNull();
    expect(readDefine("a".repeat(40))).toBe("a".repeat(40));
  });

  it("shortRevision falls back to 'local' with no revision", () => {
    expect(shortRevision(null)).toBe("local");
  });

  it("shortRevision truncates a full commit to 12 characters", () => {
    const full = "a1b2c3d4e5f6" + "7890".repeat(7);
    expect(shortRevision(full)).toBe("a1b2c3d4e5f6");
    expect(shortRevision(full)).toHaveLength(12);
  });

  it("commitUrl links to the exact commit, or null with no revision", () => {
    expect(commitUrl(null)).toBeNull();
    expect(commitUrl("a".repeat(40))).toBe(
      `https://github.com/ca-mrc/loom/commit/${"a".repeat(40)}`,
    );
  });
});
