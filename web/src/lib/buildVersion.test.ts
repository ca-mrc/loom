import { describe, expect, it } from "vitest";

import { frontendUpdateStatus } from "./buildVersion";

describe("frontendUpdateStatus (#2009)", () => {
  it("compares personal content without inventing a Git revision", () => {
    const source = "sha256:" + "a".repeat(64);
    expect(frontendUpdateStatus(null, { revision: null, sourceDigest: source }, source).hasNewerBuild).toBe(false);
    expect(frontendUpdateStatus(null, { revision: null, sourceDigest: "sha256:" + "b".repeat(64) }, source).hasNewerBuild).toBe(true);
    expect(frontendUpdateStatus(null, { revision: "c".repeat(40) }, source).hasNewerBuild).toBe(true);
    expect(frontendUpdateStatus("c".repeat(40), { revision: null, sourceDigest: source }).hasNewerBuild).toBe(true);
    expect(frontendUpdateStatus(null, { revision: null }, source).hasNewerBuild).toBe(false);
  });
  it("reports no update when the served revision matches the loaded one", () => {
    const revision = "a".repeat(40);
    const status = frontendUpdateStatus(revision, { revision });
    expect(status.hasNewerBuild).toBe(false);
    expect(status.servedRevision).toBe(revision);
  });

  it("reports an update when the served revision differs from the loaded one", () => {
    const status = frontendUpdateStatus("a".repeat(40), {
      revision: "b".repeat(40),
    });
    expect(status.hasNewerBuild).toBe(true);
    expect(status.servedRevision).toBe("b".repeat(40));
  });

  it("never claims an update when the loaded revision is unknown (local build)", () => {
    const status = frontendUpdateStatus(null, { revision: "b".repeat(40) });
    expect(status.hasNewerBuild).toBe(false);
  });

  it("never claims an update on a failed/unknown served fetch", () => {
    expect(frontendUpdateStatus("a".repeat(40), null).hasNewerBuild).toBe(
      false,
    );
    expect(frontendUpdateStatus("a".repeat(40), undefined).hasNewerBuild).toBe(
      false,
    );
    expect(
      frontendUpdateStatus("a".repeat(40), { revision: null }).hasNewerBuild,
    ).toBe(false);
  });
});
