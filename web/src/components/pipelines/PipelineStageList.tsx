import { useMemo, useRef, useState } from "react";

import type { PipelineStageRunSummary } from "../../api";
import { Button } from "../Button";
import { StatusPill } from "../StatusPill";
import { useFixedVirtualRows } from "../../hooks/useFixedVirtualRows";
import { bytewiseCompare, PIPELINE_STAGE_STATE } from "../../lib/pipelinePresentation";

const ROW_HEIGHT = 44;
const HEADERS = ["Level", "Node", "Shard", "State", "Outcome", "Resource", "Attempts", "Retry"];

function sortStages(stages: PipelineStageRunSummary[]): PipelineStageRunSummary[] {
  return [...stages].sort(
    (a, b) =>
      a.topological_level - b.topological_level ||
      bytewiseCompare(a.node_key, b.node_key) ||
      bytewiseCompare(a.shard_key, b.shard_key),
  );
}
function Cell({ value }: { value: string }): JSX.Element {
  return (
    <span title={value} aria-label={value} className="block truncate">
      {value}
    </span>
  );
}

export default function PipelineStageList({
  stages,
  selectedNodeKey,
  stateFilter,
  outcomeFilter,
  page,
  hasPrevious,
  hasNext,
  onStateFilter,
  onOutcomeFilter,
  onPrevious,
  onNext,
  onOpen,
}: {
  stages: PipelineStageRunSummary[];
  selectedNodeKey: string | null;
  stateFilter: string;
  outcomeFilter: string;
  page: number;
  hasPrevious: boolean;
  hasNext: boolean;
  onStateFilter: (value: string) => void;
  onOutcomeFilter: (value: string) => void;
  onPrevious: () => void;
  onNext: () => void;
  onOpen: (stage: PipelineStageRunSummary) => void;
}): JSX.Element {
  const [focusIndex, setFocusIndex] = useState(0);
  const rows = useMemo(() => sortStages(stages), [stages]);
  const virtual = useFixedVirtualRows(rows.length, ROW_HEIGHT, 660, 12);
  const viewportRef = useRef<HTMLDivElement>(null);
  const isVirtual = rows.length > 200;
  const fields = (row: PipelineStageRunSummary): string[] => [
    String(row.topological_level),
    row.node_key,
    row.shard_key,
    PIPELINE_STAGE_STATE[row.state].label,
    row.domain_outcome ?? "—",
    row.resource_class,
    String(row.attempt_count),
    row.retry_allowed ? "Eligible" : (row.retry_ineligible_reason ?? "Ineligible"),
  ];
  const keydown = (event: React.KeyboardEvent, index: number): void => {
    if (event.key === "Enter") onOpen(rows[index]);
    else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const next = Math.max(0, Math.min(rows.length - 1, index + (event.key === "ArrowDown" ? 1 : -1)));
      setFocusIndex(next);
      if (isVirtual && viewportRef.current) viewportRef.current.scrollTop = virtual.scrollToIndex(next) + 32;
      requestAnimationFrame(() => document.querySelector<HTMLElement>(`[data-stage-row="${next}"]`)?.focus());
    }
  };
  const renderCells = (row: PipelineStageRunSummary) =>
    fields(row).map((value, index) => (
      <div key={index} role="cell" aria-colindex={index + 1} className="min-w-0 px-2 py-2">
        <Cell value={value} />
      </div>
    ));
  return (
    <div className="stack-y-3">
      <div className="grid gap-2 md:grid-cols-3">
        <label className="text-xs">
          Node key
          <input
            value={selectedNodeKey ?? ""}
            readOnly
            placeholder="All nodes"
            className="mt-1 block w-full rounded border bg-slate-50 px-2 py-1"
          />
        </label>
        <label className="text-xs">
          State
          <select
            value={stateFilter}
            onChange={(e) => onStateFilter(e.target.value)}
            className="mt-1 block w-full rounded border px-2 py-1"
          >
            <option value="">Any</option>
            {Object.keys(PIPELINE_STAGE_STATE).map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </label>
        <label className="text-xs">
          Domain outcome
          <input
            value={outcomeFilter}
            onChange={(e) => onOutcomeFilter(e.target.value.normalize("NFC"))}
            className="mt-1 block w-full rounded border px-2 py-1"
          />
        </label>
      </div>
      <p className="text-sm text-slate-600">
        Cursor page {page} · {rows.length} StageRuns on this page
      </p>
      {isVirtual ? (
        <div
          ref={viewportRef}
          role="table"
          aria-label="Pipeline stages"
          aria-colcount={8}
          aria-rowcount={rows.length + 1}
          className="h-[660px] overflow-auto border"
          onScroll={(e) => virtual.onScroll(Math.max(0, e.currentTarget.scrollTop - 32))}
        >
          <div role="rowgroup" className="sticky top-0 z-10 min-w-[960px] bg-slate-50">
            <div role="row" aria-rowindex={1} className="grid h-8 grid-cols-8">
              {HEADERS.map((label, index) => (
                <div
                  role="columnheader"
                  aria-colindex={index + 1}
                  key={label}
                  className="px-2 py-1 text-xs font-semibold"
                >
                  {label}
                </div>
              ))}
            </div>
          </div>
          <div role="rowgroup" className="relative min-w-[960px]" style={{ height: virtual.totalHeight }}>
            {virtual.rows.map((virtualRow) => {
              const row = rows[virtualRow.index];
              return (
                <div
                  key={row.id}
                  data-stage-row={virtualRow.index}
                  role="row"
                  aria-rowindex={virtualRow.index + 2}
                  tabIndex={focusIndex === virtualRow.index ? 0 : -1}
                  onKeyDown={(e) => keydown(e, virtualRow.index)}
                  onDoubleClick={() => onOpen(row)}
                  className="absolute grid w-full grid-cols-8 border-b bg-white text-xs"
                  style={{ height: virtualRow.size, transform: `translateY(${virtualRow.start}px)` }}
                >
                  {renderCells(row)}
                </div>
              );
            })}
          </div>
        </div>
      ) : (
        <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Pipeline stages scroll area">
          <table aria-label="Pipeline stages" className="min-w-full text-sm">
            <thead>
              <tr>
                {HEADERS.map((label) => (
                  <th
                    scope="col"
                    key={label}
                    className="px-2 py-2 text-left text-xs uppercase text-slate-500"
                  >
                    {label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={row.id}
                  tabIndex={0}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") onOpen(row);
                  }}
                  onClick={() => onOpen(row)}
                  className="cursor-pointer border-t hover:bg-slate-50"
                >
                  {fields(row).map((value, index) => (
                    <td key={index} className="max-w-40 px-2 py-2">
                      {index === 3 ? (
                        <StatusPill variant={PIPELINE_STAGE_STATE[row.state].variant}>{value}</StatusPill>
                      ) : (
                        <Cell value={value} />
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <div className="flex justify-end gap-2">
        <Button size="sm" disabled={!hasPrevious} onClick={onPrevious}>
          Previous
        </Button>
        <Button size="sm" disabled={!hasNext} onClick={onNext}>
          Next
        </Button>
      </div>
    </div>
  );
}
