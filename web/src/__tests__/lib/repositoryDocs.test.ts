import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { REPOSITORY_DOCS, repositoryDocUrl, repositoryDocsVersion, type RepositoryDocId } from "../../lib/repositoryDocs";

describe("repository documentation links", () => {
  it("pins links to a full loaded-build SHA", () => {
    const revision = "abc123def4".repeat(4);
    expect(repositoryDocUrl("tasks", revision)).toBe(`https://github.com/ca-mrc/loom/blob/${revision}/docs/architecture/user-brought-tasksets.md#manifest`);
    expect(repositoryDocsVersion(revision).label).toContain("this loaded build");
  });
  it.each([null, "unknown", "abcdef", "dev", "../main", "g".repeat(40)])("explains the dev fallback for %s", (revision) => {
    expect(repositoryDocsVersion(revision).ref).toBe("dev");
    expect(repositoryDocsVersion(revision).label).toContain("docs may differ");
    expect(repositoryDocUrl("access", revision)).toContain("/blob/dev/");
  });
  it("links every topic to a real local repository heading", () => {
    for (const id of Object.keys(REPOSITORY_DOCS) as RepositoryDocId[]) {
      const doc = REPOSITORY_DOCS[id];
      const markdown = readFileSync(resolve(process.cwd(), "..", doc.path), "utf8");
      const anchors = markdown.split("\n").filter((line) => /^#{1,6} /.test(line)).map((line) => line.replace(/^#+ /, "").toLowerCase().replace(/[^\w\s-]/g, "").replace(/ /g, "-"));
      expect(anchors, `${doc.path}#${doc.anchor}`).toContain(doc.anchor);
    }
  });
});
