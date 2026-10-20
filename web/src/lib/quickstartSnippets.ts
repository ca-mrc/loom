import { shellQuote } from "./shellQuote";

function argument(value: string): string {
  return /^[A-Za-z0-9_./:@+-]+$/.test(value) ? value : shellQuote(value);
}

export function batchInspectionCommands(batchId: string): string[] {
  return [
    `loom eval batch show ${argument(batchId)}`,
    `loom eval trial list --batch-id ${argument(batchId)}`,
  ];
}

export function trialDownloadCommands(
  trialId: string,
  artifactKey?: string | null,
): string[] {
  const commands = [
    `loom eval trial show ${argument(trialId)}`,
    `loom eval trial download ${argument(trialId)} --kind atif --output atif.json`,
    `loom eval trial download ${argument(trialId)} --kind trajectory --output events.jsonl`,
  ];
  if (artifactKey) {
    commands.push(
      `loom eval trial download ${argument(trialId)} --kind artifact --artifact-key ${argument(artifactKey)} --output artifact.bin`,
    );
  }
  return commands;
}

export function usageCommand(
  start: string,
  end: string,
  teamId?: string,
  includeBatches = false,
  groupBy: "day" | "week" | "month" = "day",
): string {
  const trimmedTeamId = teamId?.trim();
  const teamFlag = trimmedTeamId ? ` --team-id ${argument(trimmedTeamId)}` : "";
  const batchesFlag = includeBatches ? " --include-batches" : "";
  return `loom eval usage --start ${argument(start)} --end ${argument(end)} --group-by ${groupBy}${teamFlag}${batchesFlag}`;
}

export function rateCardExampleJson(): string {
  return JSON.stringify(
    {
      provider: "openai",
      model: "gpt-4o-mini",
      input_per_mtok: 0.15,
      output_per_mtok: 0.6,
      cache_read_per_mtok: 0,
      cache_write_per_mtok: 0,
    },
    null,
    2,
  );
}
