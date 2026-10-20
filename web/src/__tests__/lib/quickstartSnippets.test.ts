import {
  batchInspectionCommands,
  rateCardExampleJson,
  trialDownloadCommands,
  usageCommand,
} from "../../lib/quickstartSnippets";

function containsUnsafeSnippetValue(value: string): boolean {
  const patterns = [
    /\bsk-[A-Za-z0-9_-]+/i,
    /\bapi[_-]?key\s*[:=]\s*['"]?[A-Za-z0-9_-]{12,}/i,
    /\bAuthorization:\s*Bearer\s+\S+/i,
    /X-Amz-(Algorithm|Credential|Signature|Security-Token)=/i,
    /([?&]token=|[?&]signature=|[?&]X-Amz-Signature=)/i,
  ];
  return patterns.some((pattern) => pattern.test(value));
}

describe("resource commands", () => {
  it("uses the selected resource and usage scope", () => {
    expect(batchInspectionCommands("batch-1")).toEqual([
      "loom eval batch show batch-1",
      "loom eval trial list --batch-id batch-1",
    ]);
    expect(trialDownloadCommands("trial-1", "main/report.json")).toContain(
      "loom eval trial download trial-1 --kind artifact --artifact-key main/report.json --output artifact.bin",
    );
    expect(usageCommand("2026-06-01", "2026-06-30", "team-1", true)).toBe(
      "loom eval usage --start 2026-06-01 --end 2026-06-30 --group-by day --team-id team-1 --include-batches",
    );
    expect(usageCommand("2026-06-01", "2026-06-30")).not.toContain("--team-id");
  });

  it("quotes artifact names containing spaces and shell syntax", () => {
    expect(trialDownloadCommands("trial-1", "main/my report;$(touch bad).json").at(-1)).toBe(
      "loom eval trial download trial-1 --kind artifact --artifact-key 'main/my report;$(touch bad).json' --output artifact.bin",
    );
    expect(batchInspectionCommands("batch'one")[0]).toBe("loom eval batch show 'batch'\"'\"'one'");
  });

  it("keeps rate-card references free of credentials", () => {
    expect(JSON.parse(rateCardExampleJson())).toMatchObject({ provider: "openai", model: "gpt-4o-mini" });
    expect(containsUnsafeSnippetValue(rateCardExampleJson())).toBe(false);
    expect(containsUnsafeSnippetValue("sk-live-secret")).toBe(true);
    expect(containsUnsafeSnippetValue("Authorization: Bearer abc")).toBe(true);
    expect(containsUnsafeSnippetValue("--api-key env:PROVIDER_API_KEY")).toBe(false);
  });
});
