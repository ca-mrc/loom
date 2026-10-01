import { Link } from "react-router-dom";
import { Button } from "../components/Button";
import ErrorState from "../components/ErrorState";
import { Card } from "../components/Card";
import { Input, Textarea } from "../components/Input";
import { clampInt } from "./newBatch/advancedInputs";
import { BenchmarkPicker } from "./newBatch/BenchmarkPicker";
import { type SubsetKind } from "./newBatch/formState";
import { TagFiltersCard } from "./newBatch/TagFiltersCard";
import { type BenchmarkItem } from "./newBatch/taskSources";
import { FieldLabel, Help } from "./NewBatchFields";
import { PURPOSE_OPTIONS, freshSeed } from "./newBatchState";

import type { NewBatchViewState } from "./useNewBatch";
export function NewBatchTaskSelection({
  batchPurpose,
  setBatchPurpose,
  subsetKind,
  evalTaskSets,
  selectedBenchmarks,
  setSelectedBenchmarks,
  benchmarks,
  tagSchema,
  tagFilters,
  setTagFilters,
  benchmarkIdsSorted,
  settledBenchmarkIds,
  tagQuery,
  setSubsetKind,
  subsetN,
  setSubsetN,
  subsetSeed,
  setSubsetSeed,
  explicitText,
  setExplicitText,
  parsed,
  matchedTaskCount,
  countSummary,
}: NewBatchViewState): JSX.Element {
  return (
    <Card>
      <Card.Header
        title="Task selection"
        description="Pick a purpose, then choose sources for this batch."
        actions={
          <Link
            to="/task-sets/new"
            className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent-hover"
          >
            + Submit Task Set
          </Link>
        }
      />
      <Card.Body className="min-w-0 space-y-5">
        <fieldset className="min-w-0">
          <legend className="mb-2 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Purpose
          </legend>
          <div className="grid min-w-0 grid-cols-1 gap-2 sm:grid-cols-2" role="presentation">
            {PURPOSE_OPTIONS.map((option) => {
              const selected = batchPurpose === option.value;
              return (
                <label
                  key={option.value}
                  className={
                    selected
                      ? "flex min-w-0 cursor-pointer flex-col gap-0.5 rounded-lg border border-slate-800 bg-slate-50 px-3 py-3 shadow-sm"
                      : "flex min-w-0 cursor-pointer flex-col gap-0.5 rounded-lg border border-slate-200 bg-white px-3 py-3 hover:border-slate-300 hover:bg-slate-50/80"
                  }
                >
                  <span className="flex min-w-0 items-center gap-2">
                    <input
                      type="radio"
                      name="batch-purpose"
                      value={option.value}
                      checked={selected}
                      onChange={() => setBatchPurpose(option.value)}
                      aria-label={option.radioName}
                      className="h-4 w-4 shrink-0 border-slate-300"
                    />
                    <span className="min-w-0 truncate text-sm font-semibold text-slate-900">
                      {option.title}
                    </span>
                  </span>
                  <span className="pl-6 text-xs leading-snug text-slate-500">{option.blurb}</span>
                </label>
              );
            })}
          </div>
        </fieldset>
        <fieldset
          className="block min-w-0 space-y-5"
          disabled={subsetKind === "explicit"}
          aria-label="Task sources"
        >
          {batchPurpose === "trajectory_generation" ? (
            <div className="min-w-0">
              <FieldLabel hint={subsetKind === "explicit" ? "implied by ids" : "primary"}>
                TaskSets
              </FieldLabel>
              <BenchmarkPicker
                items={(evalTaskSets.data?.items ?? [])
                  .filter((ts) => ts.status === "ready" || ts.status === "partial")
                  .map(
                    (ts) =>
                      ({
                        id: ts.task_set_id,
                        display_name: ts.display_name,
                        detail_label: `${ts.created_at.slice(0, 10)} · ${ts.task_set_id}`,
                        task_count: ts.task_count,
                        readiness_state: "ready",
                        readiness_label: ts.status,
                        selectable: true,
                      }) satisfies BenchmarkItem,
                  )}
                loading={evalTaskSets.isPending}
                loadingLabel="Loading TaskSets…"
                emptyLabel="No ready or partial TaskSets in this team yet."
                sourceKind="TaskSet"
                flat
                selected={selectedBenchmarks}
                onChange={setSelectedBenchmarks}
              />
            </div>
          ) : null}

          <div className="min-w-0">
            <FieldLabel
              hint={
                subsetKind === "explicit"
                  ? "implied by ids"
                  : batchPurpose === "evaluation"
                    ? "required"
                    : "optional"
              }
            >
              Official benchmarks
            </FieldLabel>
            <BenchmarkPicker
              items={(benchmarks.data?.items ?? []) as BenchmarkItem[]}
              loading={benchmarks.isPending}
              loadingLabel="Loading benchmarks…"
              emptyLabel="No runnable benchmarks are provisioned in this environment yet."
              sourceKind="benchmark"
              selected={selectedBenchmarks}
              onChange={setSelectedBenchmarks}
            />
            {!benchmarks.isPending && (benchmarks.data?.items.length ?? 0) === 0 ? (
              <Help>
                Ask an admin or operator to run the staging catalog provisioning step from the deployment
                runbook, then refresh this page.
              </Help>
            ) : null}
          </div>
        </fieldset>

        {subsetKind !== "explicit" && selectedBenchmarks.size > 0 ? (
          <>
            {tagQuery.isError ? <ErrorState error={tagQuery.error} /> : null}
            <TagFiltersCard
              schema={tagSchema}
              value={tagFilters}
              onChange={setTagFilters}
              loading={benchmarkIdsSorted !== settledBenchmarkIds || tagQuery.isFetching}
            />
          </>
        ) : null}

        <fieldset className="space-y-2">
          <legend className="mb-1 block text-xs font-medium uppercase tracking-wider text-slate-500">
            Subset
          </legend>
          {(
            [
              ["all", "All tasks in selected sources"],
              ["first_n", "First N by id"],
              ["last_n", "Last N by id"],
              ["random_n", "Random N (seeded)"],
              ["explicit", "Explicit task ids (paste)"],
            ] as Array<[SubsetKind, string]>
          ).map(([value, label]) => (
            <label key={value} className="flex items-center gap-2 text-sm text-slate-700">
              <input
                type="radio"
                name="subset"
                value={value}
                checked={subsetKind === value}
                onChange={() => setSubsetKind(value)}
                className="h-4 w-4 border-slate-300"
              />
              {label}
            </label>
          ))}
        </fieldset>

        {subsetKind === "first_n" || subsetKind === "last_n" || subsetKind === "random_n" ? (
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
            <label className="block max-w-xs">
              <FieldLabel>N</FieldLabel>
              <Input
                type="number"
                min={1}
                step={1}
                value={subsetN}
                onChange={(e) => setSubsetN(clampInt(e.target.value, 1, 1_000_000))}
                aria-label="Subset N"
              />
            </label>
            {subsetKind === "random_n" ? (
              <label className="block max-w-xs">
                <FieldLabel hint="0 – 2^31 - 1">Seed</FieldLabel>
                <div className="flex items-center gap-2">
                  <Input
                    type="number"
                    min={0}
                    step={1}
                    value={subsetSeed}
                    onChange={(e) => setSubsetSeed(e.target.value)}
                    aria-label="Seed"
                  />
                  <Button
                    variant="secondary"
                    size="sm"
                    onClick={() => setSubsetSeed(String(freshSeed()))}
                    title="Generate a new random seed for this random subset."
                  >
                    Reroll
                  </Button>
                </div>
              </label>
            ) : null}
          </div>
        ) : null}

        {subsetKind === "explicit" ? (
          <div className="space-y-2">
            <label className="block">
              <FieldLabel hint="one per line or any accepted format">Explicit task ids</FieldLabel>
              <Textarea
                value={explicitText}
                onChange={(e) => setExplicitText(e.target.value)}
                rows={8}
                placeholder={"HumanEval/0\nHumanEval/1\nHumanEval/2"}
                aria-label="Explicit task ids"
              />
            </label>
            <p className="text-xs">
              {parsed.error ? (
                <span className="text-red-700">{parsed.error}</span>
              ) : parsed.ids.length === 0 ? (
                <span className="text-slate-500">Paste ids above to preview.</span>
              ) : (
                <span className="text-slate-600">
                  Parsed {parsed.ids.length} id
                  {parsed.ids.length === 1 ? "" : "s"}.
                </span>
              )}
            </p>
            <details className="text-xs text-slate-600">
              <summary className="cursor-pointer font-medium text-slate-700">Accepted formats</summary>
              <ul className="ml-4 mt-2 list-disc space-y-0.5">
                <li>One id per line</li>
                <li>Comma / semicolon / pipe / tab / 2+ space separated</li>
                <li>JSON array (single or double quotes)</li>
                <li>
                  Range shorthand: <code>HumanEval/0-4</code>
                </li>
                <li>
                  Prefix shorthand: <code>HumanEval/0,1,2,3</code>
                </li>
                <li>Markdown bullets / numbered lists / single-col tables</li>
                <li>CSV with header (first column wins)</li>
                <li>Triple-backtick code fences (stripped)</li>
                <li>
                  <code>#</code> comments (rest-of-line stripped)
                </li>
                <li>
                  URL prefixes <code>/api/v1/tasks/</code> + <code>/tasks/</code>
                </li>
              </ul>
              <p className="mt-2">
                Full rules: <code>docs/user-guide.md#pasting-task-ids</code>.
              </p>
            </details>
          </div>
        ) : null}

        {subsetKind !== "explicit" ? (
          <p
            className="text-xs text-slate-500"
            role="status"
            aria-live="polite"
            aria-busy={
              selectedBenchmarks.size > 0 && (benchmarks.isPending || matchedTaskCount === undefined)
            }
          >
            {countSummary}
          </p>
        ) : null}
      </Card.Body>
    </Card>
  );
}
