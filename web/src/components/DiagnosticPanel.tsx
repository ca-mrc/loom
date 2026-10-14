import { useState } from "react";

import JsonViewer from "./JsonViewer";

export interface DiagnosticBlock {
  title: string;
  data: unknown;
  expanded?: boolean;
}

export interface DiagnosticPanelProps {
  blocks: DiagnosticBlock[];
  description?: string;
  title?: string;
}

export function DiagnosticPanel({
  blocks,
  description = "Raw request and internal fields for debugging, support, and API reproducibility.",
  title = "Diagnostics",
}: DiagnosticPanelProps): JSX.Element | null {
  const [open, setOpen] = useState(false);

  if (blocks.length === 0) return null;

  return (
    <details
      className="rounded-xl border border-slate-200 bg-white"
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <summary className="cursor-pointer px-5 py-3 text-sm font-semibold text-slate-900">
        {title}
      </summary>
      {open ? (
        <div className="stack-y-4 border-t border-slate-100 px-5 py-4">
          <p className="text-xs text-slate-500">{description}</p>
          {blocks.map((block) => (
            <div key={block.title}>
              <p className="mb-2 text-xs font-medium uppercase tracking-wider text-slate-500">
                {block.title}
              </p>
              <JsonViewer data={block.data} expanded={block.expanded} />
            </div>
          ))}
        </div>
      ) : null}
    </details>
  );
}
