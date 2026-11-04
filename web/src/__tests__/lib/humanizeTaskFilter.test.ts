import { describe, expect, it } from "vitest";

import { humanizeTaskFilter } from "../../lib/humanizeTaskFilter";

describe("humanizeTaskFilter", () => {
  it("summarizes all runnable tasks for benchmark ids", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "all", benchmark_ids: ["humaneval"] },
      { matchedTaskCount: 164 },
    );

    expect(out.primary).toBe("HumanEval / all runnable tasks / 164 tasks");
    expect(out.details).toContain("Benchmark: humaneval");
  });

  it("summarizes seeded random subsets", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "random_n", benchmark_ids: ["mbpp"], n: 25, seed: 7 },
      { matchedTaskCount: 25 },
    );

    expect(out.primary).toBe("MBPP / random 25 tasks / seed 7");
  });

  it("summarizes explicit ids without exposing raw JSON", () => {
    const out = humanizeTaskFilter(
      {
        subset_kind: "explicit",
        task_ids: ["humaneval/HumanEval/0", "humaneval/HumanEval/1"],
      },
      {},
    );

    expect(out.primary).toBe("2 explicit task IDs");
  });

  it("keeps unknown keys as diagnostics", () => {
    const out = humanizeTaskFilter(
      { subset_kind: "all", benchmark_id: "humaneval", unexpected: true },
      { matchedTaskCount: 164 },
    );

    expect(out.diagnostics).toContain("Unrecognized field: unexpected");
  });
});

it("accepts the singular task set id and lists pinned tasks", () => {
  const summary = humanizeTaskFilter({
    task_set_id: "ts/team/terminal-bench-2-1",
    task_ids: ["a", "b", "c", "d"],
  });
  expect(summary.primary).toContain("1 task set");
  expect(summary.details).toContain("Task set: ts/team/terminal-bench-2-1");
  expect(summary.details).toContain("Pinned tasks: 4");
  expect(summary.diagnostics).toEqual([]);
});

it("describes persisted TaskSet-backed plans without unknown-field warnings", () => {
  const summary = humanizeTaskFilter({ task_set_ids: ["set-one"], subset_kind: "all" }, { matchedTaskCount: 3 });
  expect(summary.primary).toContain("1 task set");
  expect(summary.primary).toContain("3 tasks");
  expect(summary.details).toContain("Task set: set-one");
  expect(summary.diagnostics).toEqual([]);
});
