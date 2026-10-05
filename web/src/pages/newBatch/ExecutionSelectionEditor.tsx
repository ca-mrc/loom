import { useState } from "react";

import { Help } from "../NewBatchFields";
import type { AdvancedState } from "./advancedConfig";
import { executionSelectionDocument, parseExecutionSelection } from "./executionSelectionDocument";

export interface ExecutionSelectionEditorProps {
  advanced: AdvancedState;
  setAdv: <K extends keyof AdvancedState>(key: K, val: AdvancedState[K]) => void;
  onClose: () => void;
}

/** Edit network, verification and isolation together as one execution-selection document. */
export default function ExecutionSelectionEditor({ advanced, setAdv, onClose }: ExecutionSelectionEditorProps): JSX.Element {
  const [draft, setDraft] = useState(() => executionSelectionDocument(advanced));
  const [error, setError] = useState<string | null>(null);

  const apply = (): void => {
    const parsed = parseExecutionSelection(draft);
    if (!parsed.ok) {
      setError(parsed.error);
      return;
    }
    setAdv("networkPolicy", parsed.value.networkPolicy);
    setAdv("allowedWebsites", parsed.value.allowedWebsites);
    setAdv("verifierEnvMode", parsed.value.verifierEnvMode);
    setAdv("isolation", parsed.value.isolation);
    onClose();
  };

  return (
    <div className="max-w-lg space-y-2">
      <label className="block">
        <span className="text-sm font-medium text-slate-700">Execution config (loom.execution-selection.v1)</span>
        <textarea
          aria-label="Execution config JSON"
          className="mt-1 block min-h-40 w-full resize-y rounded-lg border border-slate-200 bg-white px-3 py-2 font-mono text-xs text-slate-800"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          spellCheck={false}
        />
      </label>
      <Help>Same document as the CLI's --execution-config. The server validates every axis together.</Help>
      {error ? (
        <p className="text-xs text-red-700" role="alert">
          {error}
        </p>
      ) : null}
      <div className="flex gap-3">
        <button
          type="button"
          className="rounded-lg bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700"
          onClick={apply}
        >
          Apply
        </button>
        <button
          type="button"
          className="rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700"
          onClick={onClose}
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
