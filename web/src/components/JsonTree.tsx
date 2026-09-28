/**
 * On-demand JSON tree. Uses `react-json-view-lite` for
 * the actual rendering (small bundle, no dependencies beyond React).
 * The wrapper provides a scrollable surface,
 * collapsed by default after depth 2 to keep large payloads readable.
 */
import { JsonView, defaultStyles } from "react-json-view-lite";
import "react-json-view-lite/dist/index.css";

import { cn } from "../lib/cn";

import type { JsonViewerProps } from "./JsonViewer";

export default function JsonTree({
  data,
  expanded = false,
  className,
}: JsonViewerProps): JSX.Element {
  return (
    <div
      className={cn(
        "max-h-[480px] overflow-auto rounded-lg border border-slate-200 bg-slate-50 p-3 font-mono text-xs",
        className,
      )}
    >
      <JsonView
        data={data as object}
        style={defaultStyles}
        shouldExpandNode={(level) => (expanded ? true : level < 2)}
      />
    </div>
  );
}
