import { useEffect, useState, type ReactNode } from "react";

import { Button } from "./Button";

export interface CommandSnippetProps {
  command: string;
  label?: string;
  helperText?: ReactNode;
}

export default function CommandSnippet({
  command,
  label = "Command",
  helperText,
}: CommandSnippetProps): JSX.Element {
  const [copyStatus, setCopyStatus] = useState<"idle" | "copied" | "error">("idle");

  useEffect(() => setCopyStatus("idle"), [command]);
  useEffect(() => {
    if (copyStatus !== "copied") return;
    const timer = window.setTimeout(() => setCopyStatus("idle"), 1500);
    return () => window.clearTimeout(timer);
  }, [copyStatus]);

  const copy = async (): Promise<void> => {
    try {
      if (!navigator.clipboard?.writeText) throw new Error("Clipboard unavailable");
      await navigator.clipboard.writeText(command);
      setCopyStatus("copied");
    } catch {
      setCopyStatus("error");
    }
  };

  return (
    <div className="stack-y-2 rounded-lg border border-slate-200 bg-slate-50 p-3">
      <div className="flex items-center justify-between gap-3">
        <p className="text-xs font-semibold uppercase tracking-wider text-slate-500">
          {label}
        </p>
        <Button
          size="sm"
          variant="secondary"
          onClick={() => void copy()}
          aria-label={`Copy ${label}`}
          title={`Copy ${label}`}
        >
          {copyStatus === "copied" ? "Copied" : "Copy"}
        </Button>
      </div>
      <pre className="whitespace-pre-wrap break-words rounded-md bg-slate-950 p-3 text-xs leading-relaxed text-slate-50">
        <code>{command}</code>
      </pre>
      {copyStatus === "error" ? (
        <p role="alert" className="text-xs text-red-700">
          Could not copy. Select and copy the text manually.
        </p>
      ) : null}
      {helperText ? (
        <div className="text-xs leading-relaxed text-slate-500">
          {helperText}
        </div>
      ) : null}
    </div>
  );
}
