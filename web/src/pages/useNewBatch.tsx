import { queryKeys } from "../api/queryKeys";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import {
  api,
  type Combination,
  type CreateBatchBody,
  type ModelEntry,
  type ProviderConnectionEntry,
} from "../api";
import { useAuth } from "../auth/useAuth";
import { findAgent } from "../components/agentModelPickerState";
import {
  buildAgentModel,
  buildProviderOverride,
  type ProviderOverride,
} from "../components/agentModelSelection";
import { useBenchmarkDiscovery } from "../hooks/useBenchmarkDiscovery";
import { agentReadinessMessage, agentServiceModeReady } from "../lib/agentReadiness";
import { parseTaskIds } from "../lib/parseTaskIds";
import {
  INITIAL_ADVANCED,
  buildAdvancedConfig,
  buildNetworkPolicyOverride,
  type AdvancedState,
  type RetryReason,
} from "./newBatch/advancedConfig";
import {
  NEBIUS_BACKEND,
  newRow,
  type BatchPurpose,
  type ComboRow,
  type SubsetKind,
} from "./newBatch/formState";
import { buildIdentityPreview } from "./newBatch/identityPreview";
import { isTaskSetId, type BenchmarkItem } from "./newBatch/taskSources";
import {
  FAN_OUT_CONFIRM_THRESHOLD,
  MAX_COMBINATIONS,
  ProviderSelectionResult,
  freshSeed,
  preflightLabel,
} from "./newBatchState";

export function useNewBatch() {
  const { currentTeamId } = useAuth();
  const [searchParams] = useSearchParams();
  const initialTaskSet = searchParams.get("taskSet");

  const [nameSuffix, setNameSuffix] = useState("");

  const [batchPurpose, setBatchPurpose] = useState<BatchPurpose>(() => initialTaskSet && isTaskSetId(initialTaskSet) ? "trajectory_generation" : "evaluation");

  const [selectedBenchmarks, setSelectedBenchmarks] = useState<Set<string>>(() => new Set(initialTaskSet && isTaskSetId(initialTaskSet) ? [initialTaskSet] : []));

  const [tagFilters, setTagFilters] = useState<Record<string, Set<string>>>({});

  const [subsetKind, setSubsetKind] = useState<SubsetKind>("all");

  const [subsetN, setSubsetN] = useState("10");

  const [subsetSeed, setSubsetSeed] = useState<string>(() => String(freshSeed()));

  const [explicitText, setExplicitText] = useState("");

  const [rows, setRows] = useState<ComboRow[]>(() => [newRow()]);

  const [advanced, setAdvanced] = useState<AdvancedState>(INITIAL_ADVANCED);

  const [confirmedLargeFanOut, setConfirmedLargeFanOut] = useState(false);

  const [budgetUsd, setBudgetUsd] = useState("");

  const [budgetPolicy, setBudgetPolicy] = useState<"none" | "soft" | "hard">("none");

  const [budgetConfirmed, setBudgetConfirmed] = useState(false);

  const [localError, setLocalError] = useState<string | null>(null);

  // Covers saving manual model ids AND creating the batch, so a second
  // click during the save cannot submit a duplicate batch (#2054).
  const [savingModels, setSavingModels] = useState(false);

  const navigate = useNavigate();

  const benchmarks = useQuery({
    queryKey: queryKeys["benchmarks"]("with-pending"),
    // New Batch is the user-facing benchmark surface. Keep pending benchmark
    // rows visible with backend readiness diagnostics so users can see the
    // supported catalog roadmap, while disabled checkboxes still prevent
    // submitting rows that need publish/repair work.
    queryFn: () => api.listBenchmarks({ limit: "200", include_empty: "true" }),
    staleTime: 5 * 60 * 1000,
  });

  const evalTaskSets = useQuery({
    queryKey: queryKeys["taskSets"]("batch-purpose", batchPurpose),
    queryFn: () => api.listTaskSets(),
    staleTime: 5 * 60 * 1000,
    enabled: batchPurpose === "trajectory_generation",
  });

  const agents = useQuery({
    queryKey: queryKeys["agents"](),
    queryFn: () => api.listAgents(),
    staleTime: 5 * 60 * 1000,
  });

  const backends = useQuery({
    queryKey: queryKeys["backends"](),
    queryFn: () => api.listBackends(),
    staleTime: 5 * 60 * 1000,
  });

  const providerConnections = useQuery({
    queryKey: queryKeys["provider-connections"](currentTeamId),
    queryFn: () => api.listProviderConnections(currentTeamId ?? undefined),
    enabled: currentTeamId !== null,
    staleTime: 5 * 60 * 1000,
  });

  const models = useQuery({
    queryKey: queryKeys["models"]("default"),
    queryFn: () => api.listModels("default"),
    staleTime: 5 * 60 * 1000,
  });

  // Drop TaskSet selections when switching to evaluation (native
  // benchmarks only). Clear skip_verifier so evaluation cannot carry it.
  useEffect(() => {
    if (batchPurpose !== "evaluation") return;
    setSelectedBenchmarks((prev) => {
      const next = new Set(Array.from(prev).filter((id) => !isTaskSetId(id)));
      return next.size === prev.size ? prev : next;
    });
    setAdvanced((prev) => (prev.skipVerifier ? { ...prev, skipVerifier: false } : prev));
  }, [batchPurpose]);

  // Parse the explicit textarea continuously so we can show the
  // "Parsed N ids" preview as the user types.
  const parsed = useMemo(() => parseTaskIds(explicitText), [explicitText]);

  const selectedSourceIdsSorted = useMemo(() => Array.from(selectedBenchmarks).sort(), [selectedBenchmarks]);

  const benchmarkIdsSorted = useMemo(
    () => selectedSourceIdsSorted.filter((id) => !isTaskSetId(id)),
    [selectedSourceIdsSorted],
  );

  const taskSetIdsSorted = useMemo(
    () => selectedSourceIdsSorted.filter(isTaskSetId),
    [selectedSourceIdsSorted],
  );

  const { query: tagQuery, settledIds: settledBenchmarkIds } = useBenchmarkDiscovery(benchmarkIdsSorted);
  const tagSchema = useMemo(() => tagQuery.data?.items ?? [], [tagQuery.data]);

  // Drop tag-filter entries whose key is no longer in the schema (the
  // user deselected its benchmark). Keeps the submit payload tight.
  useEffect(() => {
    if (benchmarkIdsSorted.length > 0 && (benchmarkIdsSorted !== settledBenchmarkIds || !tagQuery.isSuccess))
      return;
    setTagFilters((prev) => {
      const validKeys = new Set(benchmarkIdsSorted.length > 0 ? tagSchema.map((s) => s.key) : []);
      let changed = false;
      const next: Record<string, Set<string>> = {};
      for (const [k, vs] of Object.entries(prev)) {
        if (validKeys.has(k)) next[k] = vs;
        else changed = true;
      }
      return changed ? next : prev;
    });
  }, [tagSchema, benchmarkIdsSorted, settledBenchmarkIds, tagQuery.isSuccess]);

  // PR-2 backend takes the candidate count from a real query; the SPA
  // gets a "good enough" estimate by summing per-benchmark task_count
  // from /benchmarks. When tag_filters are active the estimate is a
  // pure upper bound, so we now also issue a real count via
  // POST /api/v1/tasks/count (issue #28) and prefer that when it's
  // ready. The estimate stays as the live display while the count
  // query is in flight.
  const hasTagFilter = Object.values(tagFilters).some((v) => v.size > 0);

  const tagSelectionPending =
    hasTagFilter && (benchmarkIdsSorted !== settledBenchmarkIds || !tagQuery.isSuccess);

  const sumOfSelectedTasks = useMemo(() => {
    if (!benchmarks.data || selectedBenchmarks.size === 0) {
      return undefined;
    }
    let total = 0;
    let allKnown = true;
    for (const b of benchmarks.data.items as BenchmarkItem[]) {
      if (!selectedBenchmarks.has(b.id)) continue;
      if (typeof b.task_count !== "number") {
        allKnown = false;
        break;
      }
      total += b.task_count;
    }
    if (batchPurpose === "trajectory_generation" && evalTaskSets.data) {
      for (const ts of evalTaskSets.data.items) {
        if (!selectedBenchmarks.has(ts.task_set_id)) continue;
        if (typeof ts.task_count !== "number") {
          allKnown = false;
          break;
        }
        total += ts.task_count;
      }
    } else if (
      batchPurpose === "trajectory_generation" &&
      taskSetIdsSorted.length > 0 &&
      !evalTaskSets.data
    ) {
      allKnown = false;
    }
    return allKnown ? total : undefined;
  }, [benchmarks.data, evalTaskSets.data, selectedBenchmarks, batchPurpose, taskSetIdsSorted.length]);

  // Build the same `task_filter` the submit handler would send. The
  // count endpoint returns the runnable count after stored TaskConfig
  // validation, so placeholder/unpublished rows do not make the form
  // advertise launchable trials. Keyed by the filter shape so React
  // Query dedupes and refetches only when the filter actually changes.
  const tagFiltersPayload = useMemo(() => {
    const out: Record<string, string[]> = {};
    for (const [k, vs] of Object.entries(tagFilters)) {
      if (vs.size > 0) out[k] = Array.from(vs).sort();
    }
    return out;
  }, [tagFilters]);

  const countTaskFilter = useMemo<Record<string, unknown> | null>(() => {
    if (subsetKind === "explicit") return null; // explicit ⇒ count is local
    if (selectedBenchmarks.size === 0) return null;
    const f: Record<string, unknown> = {
      subset_kind: subsetKind,
    };
    if (benchmarkIdsSorted.length > 0) f.benchmark_ids = benchmarkIdsSorted;
    if (taskSetIdsSorted.length > 0) f.task_set_ids = taskSetIdsSorted;
    if (Object.keys(tagFiltersPayload).length > 0) {
      f.tag_filters = tagFiltersPayload;
    }
    if (subsetKind !== "all") {
      const n = Number.parseInt(subsetN, 10);
      if (Number.isFinite(n) && n > 0) f.n = n;
    }
    if (subsetKind === "random_n") {
      const seed = Number.parseInt(subsetSeed, 10);
      if (Number.isFinite(seed)) f.seed = seed;
    }
    return f;
  }, [
    subsetKind,
    selectedBenchmarks.size,
    benchmarkIdsSorted,
    taskSetIdsSorted,
    tagFiltersPayload,
    subsetN,
    subsetSeed,
  ]);

  // Only issue the count call when a tag filter is active — without
  // one the estimate from sumOfSelectedTasks is exact and a network
  // round-trip is wasted. Keyed on JSON-stringified filter so the
  // cache key is stable for identical shapes.
  const exactCount = useQuery({
    queryKey: queryKeys["tasks-count"](JSON.stringify(countTaskFilter)),
    queryFn: () => api.countTasks({ task_filter: countTaskFilter ?? {} }),
    enabled: countTaskFilter !== null && hasTagFilter,
    staleTime: 30 * 1000,
  });

  const previewTaskFilter = useMemo(() => {
    if (subsetKind === "explicit") {
      return parsed.ids.length > 0 && !parsed.error
        ? { subset_kind: "explicit" as const, task_ids: parsed.ids }
        : null;
    }
    return countTaskFilter;
  }, [subsetKind, parsed.ids, parsed.error, countTaskFilter]);
  const previewNetworkPolicy = useMemo(
    () => buildNetworkPolicyOverride({
      networkPolicy: advanced.networkPolicy,
      allowedWebsites: advanced.allowedWebsites,
    }),
    [advanced.networkPolicy, advanced.allowedWebsites],
  );
  const previewAgentNames = useMemo(
    () => rows
      .map((row) => findAgent(agents.data?.items, row.picker.agentName)?.name)
      .filter((name): name is string => Boolean(name)),
    [rows, agents.data?.items],
  );
  const networkPolicyPreview = useQuery({
    queryKey: queryKeys["network-policy-preview"](
      currentTeamId,
      JSON.stringify(previewTaskFilter),
      previewNetworkPolicy.ok ? JSON.stringify(previewNetworkPolicy.value) : "invalid",
      previewAgentNames.join(","),
    ),
    queryFn: () => api.previewBatchNetworkPolicy({
      task_filter: previewTaskFilter ?? {},
      baseline_network_policy_override: previewNetworkPolicy.ok
        ? previewNetworkPolicy.value
        : null,
      agent_names: previewAgentNames,
      ...(currentTeamId ? { team_id: currentTeamId } : {}),
    }),
    enabled: currentTeamId !== null && previewTaskFilter !== null && previewNetworkPolicy.ok,
    staleTime: 30 * 1000,
  });

  const matchedTaskCount: number | undefined = (() => {
    if (subsetKind === "explicit") return parsed.ids.length;
    // When a tag filter is active and we have a real count, use it.
    // Otherwise fall back to the upper-bound estimate.
    if (hasTagFilter && exactCount.data !== undefined) {
      return exactCount.data.count;
    }
    if (sumOfSelectedTasks === undefined) return undefined;
    if (subsetKind === "all") return sumOfSelectedTasks;
    const n = Number.parseInt(subsetN, 10);
    if (!Number.isFinite(n)) return undefined;
    return Math.min(sumOfSelectedTasks, Math.max(0, n));
  })();

  const create = useMutation({
    mutationFn: (body: CreateBatchBody) => api.createBatch(body),
    onSuccess: (res) => {
      navigate(`/batches/${res.batch_id}`);
    },
  });

  const setAdv = <K extends keyof AdvancedState>(key: K, val: AdvancedState[K]): void => {
    setAdvanced((s) => ({ ...s, [key]: val }));
  };

  const toggleRetryReason = (reason: RetryReason): void => {
    setAdvanced((s) => {
      const next = new Set(s.retryOn);
      if (next.has(reason)) next.delete(reason);
      else next.add(reason);
      return { ...s, retryOn: next };
    });
  };

  const updateRow = (i: number, patch: Partial<ComboRow>): void => {
    setRows((rs) => rs.map((r, idx) => (idx === i ? { ...r, ...patch } : r)));
  };

  const addRow = (): void => {
    setRows((rs) => (rs.length >= MAX_COMBINATIONS ? rs : [...rs, newRow()]));
  };

  const removeRow = (i: number): void => {
    setRows((rs) => (rs.length <= 1 ? rs : rs.filter((_, idx) => idx !== i)));
  };

  const sumNPerTask = rows.reduce((acc, r) => {
    const n = Number.parseInt(r.nPerTask, 10);
    return acc + (Number.isFinite(n) && n > 0 ? n : 0);
  }, 0);

  const totalTrials =
    matchedTaskCount !== undefined && sumNPerTask > 0 ? matchedTaskCount * sumNPerTask : undefined;

  const showLargeFanOutConfirm = totalTrials !== undefined && totalTrials > FAN_OUT_CONFIRM_THRESHOLD;

  function buildCombinations(): { ok: true; value: Combination[] } | { ok: false; error: string } {
    const labels = new Set<string>();
    const out: Combination[] = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const selectedAgent = findAgent(agents.data?.items, r.picker.agentName);
      if (!selectedAgent) {
        return { ok: false, error: `Combination ${i + 1}: pick an agent.` };
      }
      if (!agentServiceModeReady(selectedAgent)) {
        return {
          ok: false,
          error: `Combination ${i + 1}: ${agentReadinessMessage(selectedAgent)}`,
        };
      }
      if (
        r.picker.agentVersion &&
        (selectedAgent.name !== "terminus-2" ||
          !selectedAgent.versions?.some((v) => v.agent_version === r.picker.agentVersion))
      ) {
        return {
          ok: false,
          error: `Combination ${i + 1}: choose an available agent version for Nebius Terminus-2, or use Deployment default.`,
        };
      }
      const agentModel = buildAgentModel(r.picker, selectedAgent.needs_model);
      if (selectedAgent.needs_model && !r.picker.providerConnectionId) {
        return {
          ok: false,
          error: `Combination ${i + 1}: ${selectedAgent.display_name ?? selectedAgent.name} needs a model — choose a provider connection first.`,
        };
      }
      if (selectedAgent.needs_model && agentModel === null) {
        return {
          ok: false,
          error: `Combination ${i + 1}: ${selectedAgent.display_name ?? selectedAgent.name} needs a model — pick one from the dropdown or enter an ad-hoc model ID.`,
        };
      }
      const n = Number.parseInt(r.nPerTask, 10);
      if (!Number.isFinite(n) || n < 1 || n > 100) {
        return {
          ok: false,
          error: `Combination ${i + 1}: samples per task must be between 1 and 100.`,
        };
      }
      const label = r.label.trim();
      if (label) {
        if (labels.has(label)) {
          return { ok: false, error: `Combination ${i + 1}: labels must be unique — "${label}" is repeated.` };
        }
        labels.add(label);
      }
      const combo: Combination = {
        agent_name: selectedAgent.name,
        ...(r.picker.agentVersion ? { agent_version: r.picker.agentVersion } : {}),
        agent_model: agentModel,
        n_per_task: n,
      };
      const override = buildProviderOverride(r.picker, selectedAgent.needs_model);
      if (override) {
        combo.provider_connection_id = override.provider_connection_id;
        combo.provider_model_id = override.provider_model_id;
      }
      if (label) combo.label = label;
      out.push(combo);
    }
    return { ok: true, value: out };
  }

  function buildProviderSelection(): ProviderSelectionResult {
    const overrides: Array<{ index: number; value: ProviderOverride }> = [];
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const selectedAgent = findAgent(agents.data?.items, r.picker.agentName);
      if (!selectedAgent) continue;
      const override = buildProviderOverride(r.picker, selectedAgent.needs_model);
      if (override) overrides.push({ index: i, value: override });
    }
    if (overrides.length === 0) return { ok: true, value: [] };
    if (providerConnections.isPending) {
      return {
        ok: false,
        error: "Provider connections for the active team are still loading.",
      };
    }
    if (providerConnections.isError || !providerConnections.data) {
      return {
        ok: false,
        error: "Provider connections for the active team could not be loaded. Retry before submitting.",
      };
    }
    const activeProviderIds = new Set(providerConnections.data.items.map((connection) => connection.id));
    for (const override of overrides) {
      if (!activeProviderIds.has(override.value.provider_connection_id)) {
        return {
          ok: false,
          error: `Combination ${override.index + 1}: select a provider connection for the active team.`,
        };
      }
    }
    return { ok: true, value: overrides.map((override) => override.value) };
  }

  // Shared read-only validation and payload construction for submit and export.
  // Remote writes happen only in submit, after this result succeeds.
  const buildSubmission = (): {
    ok: true; payload: CreateBatchBody; providerOverrides: ProviderOverride[];
  } | { ok: false; error: string } => {
    if (tagSelectionPending) {
      return { ok: false, error: "Wait for benchmark tags to finish loading before submitting." };
    }
    if (!currentTeamId) {
      return { ok: false, error: "Select an active team before submitting a batch." };
    }
    if (advanced.networkPolicy && networkPolicyPreview.isPending) {
      return { ok: false, error: "Wait for task network compatibility to finish loading." };
    }
    if (advanced.networkPolicy && networkPolicyPreview.data?.selected_incompatible_task_ids.length) {
      return {
        ok: false,
        error: `Task network access is incompatible with ${networkPolicyPreview.data.selected_incompatible_task_ids.length} selected task(s).`,
      };
    }
    if (subsetKind === "explicit") {
      if (parsed.error || parsed.ids.length === 0) {
        return { ok: false, error: "Paste at least one task id." };
      }
    } else {
      if (selectedBenchmarks.size === 0) {
        return { ok: false, error:
          batchPurpose === "evaluation"
            ? "Pick at least one native benchmark."
            : "Pick at least one benchmark or TaskSet.",
        };
      }
      if (subsetKind !== "all") {
        const n = Number.parseInt(subsetN, 10);
        if (!Number.isFinite(n) || n < 1) {
          return { ok: false, error: "Subset N must be a positive integer." };
        }
      }
      if (subsetKind === "random_n") {
        const seedN = Number.parseInt(subsetSeed, 10);
        if (!Number.isFinite(seedN) || seedN < 0 || seedN > 2 ** 31 - 1) {
          return { ok: false, error: "Seed must be a non-negative 32-bit integer." };
        }
      }
      if (matchedTaskCount === undefined) {
        return { ok: false, error: "Still counting matching tasks — try again in a moment." };
      }
      // Issue #28: with tag_filters active the SPA used to skip this
      // check because the local estimate was a pure upper bound; the
      // user could submit a batch that resolved to zero tasks and got
      // a confusing late 400. Now `matchedTaskCount` comes from the
      // real `/tasks/count` endpoint when `hasTagFilter`, so the gate
      // applies uniformly.
      if (matchedTaskCount === 0) {
        return { ok: false, error:
          hasTagFilter
            ? "Tag filters narrow the slate to zero tasks — adjust the filters or unselect them."
            : "No tasks match the current source selection + subset.",
        };
      }
    }

    const combos = buildCombinations();
    if (!combos.ok) {
      return { ok: false, error: combos.error };
    }
    const providerSelection = buildProviderSelection();
    if (!providerSelection.ok) {
      return { ok: false, error: providerSelection.error };
    }

    if (totalTrials !== undefined && totalTrials > FAN_OUT_CONFIRM_THRESHOLD && !confirmedLargeFanOut) {
      return { ok: false, error: `This will launch ${totalTrials} trials. Tick the confirm box below, then submit again.` };
    }

    const budgetText = budgetUsd.trim();
    const budgetValue = budgetText ? Number.parseFloat(budgetText) : undefined;
    if (budgetText && (!Number.isFinite(budgetValue) || budgetValue! < 0)) {
      return { ok: false, error: "Budget USD must be a non-negative number." };
    }

    const adv = buildAdvancedConfig(
      batchPurpose === "evaluation" ? { ...advanced, skipVerifier: false } : advanced,
    );
    if (!adv.ok) {
      return { ok: false, error: `Advanced options: ${adv.error}` };
    }

    const trial_config: Record<string, unknown> = { ...adv.value };
    const mm = trial_config.multi_model as
      | { enabled?: boolean; secondary_model?: Record<string, unknown> }
      | undefined;
    if (mm?.enabled && mm.secondary_model) {
      const first = combos.value[0]?.agent_model;
      if (first && typeof first === "object") {
        mm.secondary_model = {
          ...mm.secondary_model,
          provider: first.provider,
          ...("source" in first && first.source ? { source: first.source } : {}),
        };
        trial_config.multi_model = mm;
      }
    }
    const task_filter: CreateBatchBody["task_filter"] = {
      subset_kind: subsetKind,
    };
    if (subsetKind === "explicit") {
      task_filter.task_ids = parsed.ids;
    } else {
      if (benchmarkIdsSorted.length > 0) {
        task_filter.benchmark_ids = benchmarkIdsSorted;
      }
      if (taskSetIdsSorted.length > 0) {
        task_filter.task_set_ids = taskSetIdsSorted;
      }
      const tagPayload: Record<string, string[]> = {};
      for (const [k, vs] of Object.entries(tagFilters)) {
        if (vs.size > 0) tagPayload[k] = Array.from(vs).sort();
      }
      if (Object.keys(tagPayload).length > 0) {
        task_filter.tag_filters = tagPayload;
      }
      if (subsetKind !== "all") {
        task_filter.n = Number.parseInt(subsetN, 10);
      }
      if (subsetKind === "random_n") {
        task_filter.seed = Number.parseInt(subsetSeed, 10);
      }
    }

    const providerOverrides = providerSelection.value;

    const payload: CreateBatchBody = {
      team_id: currentTeamId,
      purpose: batchPurpose,
      task_filter,
      trial_config,
      combinations: combos.value,
    };
    const suffix = nameSuffix.trim();
    if (suffix) payload.name_suffix = suffix;
    if (budgetValue !== undefined) {
      payload.budget_usd = budgetValue;
      payload.budget_policy = budgetPolicy === "none" ? "hard" : budgetPolicy;
      payload.budget_confirmed = budgetConfirmed;
    }
    return { ok: true, payload, providerOverrides };
  };

  const isSubmitting = savingModels || create.isPending;

  const submit = async (): Promise<void> => {
    if (isSubmitting) return;
    setLocalError(null);
    const result = buildSubmission();
    if (!result.ok) {
      setLocalError(result.error);
      return;
    }
    const { payload, providerOverrides } = result;
    setSavingModels(true);
    try {
      const manualOverrides = new Map<string, ProviderOverride>();
      for (const override of providerOverrides) {
        if (!override.manual_model) continue;
        manualOverrides.set(
          `${override.provider_connection_id}\u0000${override.provider_model_id}`,
          override,
        );
      }
      for (const override of manualOverrides.values()) {
        await api.addProviderConnectionModel(override.provider_connection_id, {
          model_id: override.provider_model_id,
        });
      }
    } catch (e) {
      setSavingModels(false);
      setLocalError(e instanceof Error ? e.message : "Could not save manual model id.");
      return;
    }

    create.mutate(payload, { onSettled: () => setSavingModels(false) });
  };

  const submitButtonLabel = (() => {
    if (isSubmitting) return "Submitting…";
    if (totalTrials === undefined || totalTrials === 0) {
      return "Submit batch";
    }
    return `Submit ${totalTrials} trial${totalTrials === 1 ? "" : "s"}`;
  })();

  let countSummary = "";

  const hasTaskSetSelection = taskSetIdsSorted.length > 0;

  const sourceN = selectedBenchmarks.size;

  const sourceLabel = hasTaskSetSelection ? "source" : "benchmark";

  if (subsetKind === "explicit") {
    countSummary = "";
  } else if (selectedBenchmarks.size === 0) {
    countSummary =
      batchPurpose === "evaluation"
        ? "Pick at least one native benchmark to count matching tasks."
        : "Pick at least one benchmark or TaskSet to count matching tasks.";
  } else if (hasTagFilter && exactCount.isLoading) {
    // Real count is in flight; show the running upper-bound while we
    // wait so the page stays responsive.
    countSummary =
      sumOfSelectedTasks !== undefined
        ? `Counting tasks under tag filters (up to ${sumOfSelectedTasks})…`
        : "Counting matching tasks…";
  } else if (matchedTaskCount === undefined) {
    countSummary = "Counting matching tasks…";
  } else if (matchedTaskCount === 0) {
    countSummary = hasTagFilter
      ? "Tag filters narrow the slate to zero tasks — adjust the filters or unselect them."
      : "No runnable tasks are provisioned for the selected sources. Ask an admin or operator to run the deployment catalog provisioning step, or pick a different ready source.";
  } else if (hasTagFilter) {
    countSummary = `${matchedTaskCount} task${matchedTaskCount === 1 ? "" : "s"} match the current benchmark + tag filters.`;
  } else {
    countSummary = `${matchedTaskCount} task${matchedTaskCount === 1 ? "" : "s"} match across ${sourceN} ${sourceLabel}${sourceN === 1 ? "" : "s"}.`;
  }

  const nebiusStatus = backends.data?.items.find((b) => b.name === NEBIUS_BACKEND);

  // One review line per combination: connection, model and preflight for
  // model-backed rows; "no model" for Oracle-style agents (#2054).
  const releaseCombinationLines: string[] = rows.map((row, i) => {
    const agent = findAgent(agents.data?.items, row.picker.agentName);
    const prefix = `Combination ${i + 1}${agent ? ` (${agent.display_name ?? agent.name})` : ""}`;
    if (!agent) return `${prefix}: pick an agent.`;
    if (!agent.needs_model) return `${prefix}: no model or provider connection needed.`;
    const connection: ProviderConnectionEntry | undefined = providerConnections.data?.items.find(
      (c) => c.id === row.picker.providerConnectionId,
    );
    if (!connection) return `${prefix}: pick a provider connection before submitting.`;
    const modelName = row.picker.modelName.trim();
    if (!modelName) return `${prefix}: ${connection.name} (${connection.status}); pick a model before submitting.`;
    const model: ModelEntry | undefined = models.data?.items.find(
      (m) => m.name === modelName && m.provider_connection_id === connection.id,
    );
    const preflight = model
      ? preflightLabel(model.last_preflight_status, model.last_preflight_failure_kind)
      : row.picker.manualModel
        ? "ad-hoc, saved on submit"
        : "not preflighted";
    return `${prefix}: ${connection.name} (${connection.status}) · ${modelName} ${preflight}.`;
  });

  const releaseScopeText =
    subsetKind === "all"
      ? `Full ${hasTaskSetSelection ? "source" : "benchmark"} run: ${matchedTaskCount ?? 0} tasks selected.`
      : subsetKind === "explicit"
        ? `Explicit task-id run: ${matchedTaskCount ?? 0} tasks selected.`
        : `Subset run: ${matchedTaskCount ?? 0} tasks selected.`;

  const releaseTrialText =
    totalTrials === undefined
      ? (subsetKind === "explicit" ? "Enter valid task IDs to plan trials."
        : selectedBenchmarks.size === 0 ? "Choose a task source to plan trials."
        : "Trial count is still being calculated.")
      : `${totalTrials} trials planned.`;

  const releaseBackendText =
    nebiusStatus === undefined
      ? "Runs on Nebius (availability is not loaded yet)."
      : nebiusStatus.available
        ? "Runs on Nebius, which has a live worker."
        : nebiusStatus.cold_start_available
          ? `Runs on Nebius, which can scale from zero via ${nebiusStatus.cold_start_pools.join(", ")}.`
          : "Runs on Nebius, which has no healthy execution target right now.";

  const generatedIdentity = buildIdentityPreview({
    sourceIds: selectedSourceIdsSorted,
    tagFilters: tagFiltersPayload,
    subsetKind,
    subsetN,
    subsetSeed,
    explicitCount: parsed.ids.length,
    rows,
    suffix: nameSuffix,
  });
  return {
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
    advanced,
    networkPolicyPreview,
    setAdv,
    toggleRetryReason,
    addRow,
    rows,
    removeRow,
    updateRow,
    create,
    currentTeamId,
    releaseScopeText,
    releaseTrialText,
    releaseBackendText,
    releaseCombinationLines,
    budgetUsd,
    setBudgetUsd,
    budgetPolicy,
    setBudgetPolicy,
    setBudgetConfirmed,
    budgetConfirmed,
    nameSuffix,
    setNameSuffix,
    generatedIdentity,
    totalTrials,
    sumNPerTask,
    agents,
    showLargeFanOutConfirm,
    confirmedLargeFanOut,
    setConfirmedLargeFanOut,
    localError,
    submit,
    isSubmitting,
    buildSubmission,
    tagSelectionPending,
    submitButtonLabel,
  };
}
export type NewBatchViewState = ReturnType<typeof useNewBatch>;
