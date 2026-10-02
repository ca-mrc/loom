/// <reference types="node" />
import { spawnSync } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";

it("the real Vite configuration freezes personal identity in executable bundle code", async () => {
  const directory = await mkdtemp(join(tmpdir(), "loom-personal-build-info-"));
  try {
    const result = spawnSync(process.execPath, ["--input-type=module", "--eval", `
      import { build } from "vite";
      import { pathToFileURL } from "node:url";
      await build({ configFile: "vite.config.ts", logLevel: "silent", build: {
        outDir: ${JSON.stringify(directory)}, emptyOutDir: true, manifest: false,
        lib: { entry: "src/lib/buildInfo.ts", formats: ["es"], fileName: () => "identity.mjs" }
      }});
      process.env.VITE_SOURCE_DIGEST = "sha256:" + "f".repeat(64);
      const { LOADED_BUILD_INFO } = await import(pathToFileURL(${JSON.stringify(join(directory, "identity.mjs"))}));
      console.log(JSON.stringify(LOADED_BUILD_INFO));
    `], { encoding: "utf8", timeout: 30_000, env: { ...process.env,
      VITE_BUILD_KIND: "personal", VITE_SOURCE_DIGEST: "sha256:" + "a".repeat(64),
      VITE_SOURCE_BASE_COMMIT: "b".repeat(40), VITE_BUILD_REVISION: "b".repeat(40),
      VITE_BUILD_SOURCE_REF: "personal", VITE_BUILD_TIME: "2026-10-02T00:00:00Z",
    } });
    expect(result.status, result.stderr).toBe(0);
    expect(JSON.parse(result.stdout.trim())).toEqual({
      revision: null, kind: "personal", sourceDigest: "sha256:" + "a".repeat(64),
      baseCommit: "b".repeat(40), sourceRef: "personal", buildTime: "2026-10-02T00:00:00Z",
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
