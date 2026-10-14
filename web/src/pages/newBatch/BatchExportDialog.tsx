import { useState } from "react";
import type { CreateBatchBody } from "../../api";
import { apiBase } from "../../api/core";
import { Button } from "../../components/Button";
import CommandSnippet from "../../components/CommandSnippet";
import { Modal } from "../../components/Modal";
import { batchExport } from "./exportBatch";

export type BatchExportResult = { payload: CreateBatchBody } | { error: string };

export function BatchExportDialog({ result, onClose }: {
  result: BatchExportResult;
  onClose: () => void;
}): JSX.Element {
  const [format, setFormat] = useState<"cli" | "api">("cli");
  const commands = "payload" in result
    ? batchExport(result.payload, apiBase(), window.location.origin)
    : null;
  const downloadRequest = () => {
    if (!commands) return;
    const url = URL.createObjectURL(new Blob([commands.json + "\n"], { type: "application/json" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "batch.json";
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
  };
  return (
    <Modal open onClose={onClose} title="Export CLI / API" size="lg"
      description="A snapshot of your validated form. Exporting does not submit a batch.">
      <div className="max-h-[65vh] stack-y-4 overflow-y-auto">
        {"error" in result ? <p role="alert" className="text-sm text-red-700">{result.error}</p> : null}
        {commands ? <>
          <div className="flex gap-2" aria-label="Export format">
            <Button variant={format === "cli" ? "primary" : "secondary"} size="sm"
              aria-pressed={format === "cli"} onClick={() => setFormat("cli")}>CLI</Button>
            <Button variant={format === "api" ? "primary" : "secondary"} size="sm"
              aria-pressed={format === "api"} onClick={() => setFormat("api")}>API</Button>
          </div>
          <p className="text-sm text-slate-600">
            {format === "cli"
              ? "Set LOOM_USERNAME and LOOM_PASSWORD in your terminal environment to your approved account credentials."
              : "Set LOOM_TOKEN to a user-owned API token with access to this team. Ask a team owner or platform administrator to issue one if needed."}
            {" "}Your browser credentials are never included. Running this command creates a batch in the environment you are viewing.
          </p>
          {format === "cli" ? <p className="text-xs text-slate-500">
            Download batch.json and run the command from that folder. Requires a Loom CLI version supporting both --request-json and loom auth login --team-id. The login command selects this deployment.
          </p> : null}
          {"payload" in result ? <p className="text-sm text-slate-700">Purpose: {result.payload.purpose ?? "evaluation"} · {result.payload.combinations?.length ?? 1} agent/model combination(s). The exported request retains all selected task sources and settings.</p> : null}
          <CommandSnippet label={format === "cli" ? "CLI command" : "API request"} command={commands[format]} />
          <Button variant="secondary" size="sm" onClick={downloadRequest}>Download batch.json</Button>
          <details><summary className="cursor-pointer text-sm font-medium text-slate-700">Request JSON</summary>
            <CommandSnippet label="batch.json" command={commands.json} />
          </details>
        </> : null}
      </div>
    </Modal>
  );
}
