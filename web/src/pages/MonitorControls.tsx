import type { View } from "./monitorPresentation";
export function SegmentedToggle({
  value,
  onChange,
}: {
  value: View | "capacity";
  onChange: (v: View | "capacity") => void;
}): JSX.Element {
  return (
    <div className="inline-flex rounded-lg border border-slate-200 bg-slate-50 p-0.5">
      {(["batches", "trials", "capacity"] as const).map((v) => {
        const active = v === value;
        return (
          <button
            key={v}
            type="button"
            onClick={() => onChange(v)}
            aria-pressed={active}
            title={
              v === "batches"
                ? "Show aggregate batch rows and their overall state."
                : v === "trials" ? "Show individual trial rows across batches." : "Inspect nodes, scheduling and shared capacity."
            }
            className={
              "rounded-md px-3 py-1 text-sm font-medium transition-colors " +
              (active ? "bg-white text-slate-900 shadow-xs" : "text-slate-600 hover:text-slate-900")
            }
          >
            {v === "batches" ? "Batches" : v === "trials" ? "Trials" : "Capacity"}
          </button>
        );
      })}
    </div>
  );
}

export function SkeletonRows({ rows = 5, cols }: { rows?: number; cols: number }): JSX.Element {
  return (
    <>
      {Array.from({ length: rows }).map((_, i) => (
        <tr key={i}>
          <td colSpan={cols} className="px-4 py-2">
            <div className="h-12 animate-pulse rounded-xl bg-slate-100" />
          </td>
        </tr>
      ))}
    </>
  );
}
