import { Input } from "../components/Input";
import type { AdvancedState } from "./newBatch/advancedConfig";
import { RETRY_REASONS, clampFloat, clampInt } from "./newBatch/advancedInputs";
import ExecutionSelectionJson from "./newBatch/ExecutionSelectionJson";
import { FieldLabel, Help } from "./NewBatchFields";

import type { NewBatchViewState } from "./useNewBatch";
export default function NewBatchAdvancedFields({ advanced, setAdv, batchPurpose, toggleRetryReason, networkPolicyPreview }: NewBatchViewState): JSX.Element {
  const policyChoice = (kind: AdvancedState["networkPolicy"]) =>
    networkPolicyPreview?.data?.choices?.find((choice) => choice.kind === kind);
  return (
    <div className="space-y-6 px-6 pb-6">
      <fieldset className="space-y-3">
        <legend className="text-sm font-semibold text-slate-700">Environment</legend>
        <label className="flex items-start gap-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={advanced.forceBuild}
            onChange={(e) => setAdv("forceBuild", e.target.checked)}
            className="mt-1 h-4 w-4 rounded border-slate-300"
          />
          <span>
            Force rebuild env image
            <Help>
              Default: off. When on, the worker rebuilds the task's docker image even if a cached layer
              exists.
            </Help>
          </span>
        </label>
        <label className="flex items-start gap-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={!advanced.deleteEnv}
            onChange={(e) => setAdv("deleteEnv", !e.target.checked)}
            className="mt-1 h-4 w-4 rounded border-slate-300"
          />
          <span>
            Keep env container after the trial finishes
            <Help>Default: off (env is deleted). Turn on for post-mortem inspection.</Help>
          </span>
        </label>
        {batchPurpose === "trajectory_generation" ? (
          <label className="flex items-start gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={advanced.skipVerifier}
              onChange={(e) => setAdv("skipVerifier", e.target.checked)}
              className="mt-1 h-4 w-4 rounded border-slate-300"
            />
            <span>
              Skip verifier
              <Help>Allowed for trajectory generation. Evaluation always runs verification.</Help>
            </span>
          </label>
        ) : (
          <p className="text-xs text-slate-500">
            Evaluation always runs the verifier; skip is unavailable.
          </p>
        )}
        <label className="block max-w-sm">
          <FieldLabel hint="default: task setting">Verifier env mode</FieldLabel>
          <select
            className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
            value={advanced.verifierEnvMode}
            onChange={(e) =>
              setAdv("verifierEnvMode", e.target.value as AdvancedState["verifierEnvMode"])
            }
          >
            <option value="">Use task default</option>
            <option value="shared">shared</option>
            <option value="separate">separate</option>
          </select>
        </label>
        <label className="block max-w-sm">
          <FieldLabel hint="default: auto">Isolation</FieldLabel>
          <select
            className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
            value={advanced.isolation}
            onChange={(e) => setAdv("isolation", e.target.value as AdvancedState["isolation"])}
          >
            <option value="">Auto (from task requirements)</option>
            <option value="container">Container sandbox</option>
            <option value="guest">VM sandbox</option>
          </select>
          <Help>
            VM sandbox runs each attempt with its own Linux kernel and grades in that attempt.
            Container sandbox cannot run tasks that require a VM sandbox.
          </Help>
        </label>
        <label className="block max-w-sm">
          <FieldLabel hint="default: task setting">Task network access</FieldLabel>
          <select
            className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
            value={advanced.networkPolicy}
            onChange={(e) =>
              setAdv("networkPolicy", e.target.value as AdvancedState["networkPolicy"])
            }
          >
            <option value="">Use task default</option>
            <option value="gateway-only" disabled={policyChoice("gateway-only")?.available === false}>Gateway only</option>
            <option value="web-allowlist" disabled={policyChoice("web-allowlist")?.available === false}>Approved websites</option>
            <option value="public-web" disabled={policyChoice("public-web")?.available === false}>Public HTTP/HTTPS</option>
          </select>
        </label>
        {advanced.networkPolicy === "web-allowlist" ? (
          <label className="block max-w-lg">
            <FieldLabel hint="one per line">Approved websites</FieldLabel>
            <textarea
              className="block min-h-24 w-full resize-y rounded-lg border border-slate-200 bg-white px-3 py-2 font-mono text-sm text-slate-800"
              value={advanced.allowedWebsites}
              onChange={(e) => setAdv("allowedWebsites", e.target.value)}
              placeholder={"https://registry.npmjs.org\nhttps://pypi.org"}
            />
            <Help>Exact public HTTP or HTTPS hostnames. Paths, ports, credentials, and wildcards are rejected.</Help>
          </label>
        ) : null}
        {networkPolicyPreview?.data?.authored_defaults?.length ? (
          <div className="text-xs text-slate-600">
            Task defaults: {networkPolicyPreview.data.authored_defaults.map((group) =>
              `${String(group.policy.kind)} (${group.task_ids.length})`,
            ).join(", ")}
          </div>
        ) : null}
        {networkPolicyPreview?.data?.widens_task_ids?.length ? (
          <p className="text-xs font-medium text-amber-700">
            This selection widens network access for {networkPolicyPreview.data.widens_task_ids.length} task(s).
          </p>
        ) : null}
        {networkPolicyPreview?.data?.selected_incompatible_task_ids?.length ? (
          <div className="text-xs text-red-700" role="alert">
            Incompatible tasks ({networkPolicyPreview.data.selected_incompatible_task_ids.length}):{" "}
            {networkPolicyPreview.data.selected_incompatible_task_ids.slice(0, 8).join(", ")}
            {networkPolicyPreview.data.selected_incompatible_task_ids.length > 8 ? "…" : ""}
          </div>
        ) : null}
        <ExecutionSelectionJson advanced={advanced} setAdv={setAdv} />
      </fieldset>

      <fieldset className="space-y-3">
        <legend className="text-sm font-semibold text-slate-700">Timeouts</legend>
        <p className="text-xs text-slate-500">
          Override = absolute seconds (blank = use task default). Multiplier scales it (1 = no change).
        </p>
        {[
          {
            label: "Agent",
            override: "overrideAgentTimeoutSec" as const,
            mult: "agentTimeoutMultiplier" as const,
          },
          {
            label: "Verifier",
            override: "overrideVerifierTimeoutSec" as const,
            mult: "verifierTimeoutMultiplier" as const,
          },
          {
            label: "Env build",
            override: "overrideEnvBuildTimeoutSec" as const,
            mult: "envBuildTimeoutMultiplier" as const,
          },
        ].map((row) => (
          <div key={row.label} className="grid grid-cols-1 gap-2 md:grid-cols-2">
            <label className="block">
              <FieldLabel>{row.label} timeout override (s)</FieldLabel>
              <Input
                type="number"
                min={0.001}
                step={1}
                value={advanced[row.override]}
                onChange={(e) => setAdv(row.override, e.target.value)}
                placeholder="task default"
              />
            </label>
            <label className="block">
              <FieldLabel>{row.label} timeout multiplier</FieldLabel>
              <Input
                type="number"
                min={0.001}
                step={0.1}
                value={advanced[row.mult]}
                onChange={(e) => setAdv(row.mult, e.target.value)}
                onBlur={() => setAdv(row.mult, clampFloat(advanced[row.mult], 0.001) || "1")}
              />
            </label>
          </div>
        ))}
      </fieldset>

      <fieldset className="space-y-3">
        <legend className="text-sm font-semibold text-slate-700">Retry on transient errors</legend>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <label className="block">
            <FieldLabel hint="1 – 20">Max attempts</FieldLabel>
            <Input
              type="number"
              min={1}
              max={20}
              step={1}
              value={advanced.maxAttempts}
              onChange={(e) => setAdv("maxAttempts", clampInt(e.target.value, 1, 20))}
              onBlur={() => setAdv("maxAttempts", clampInt(advanced.maxAttempts, 1, 20) || "1")}
            />
            <Help>Total attempts including the first. 1 = no retry.</Help>
          </label>
          <div>
            <div className="mb-1 flex items-baseline justify-between gap-2">
              <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
                Retry on
              </span>
              <div className="flex gap-2 text-xs">
                <button
                  type="button"
                  onClick={() => setAdv("retryOn", new Set(RETRY_REASONS.map((r) => r.value)))}
                  title="Retry on every listed transient failure reason."
                  className="font-medium text-accent hover:text-accent-hover"
                >
                  Select all
                </button>
                <span className="text-slate-300">·</span>
                <button
                  type="button"
                  onClick={() => setAdv("retryOn", new Set())}
                  title="Disable retry reasons so retries will not run."
                  className="font-medium text-slate-500 hover:text-slate-700"
                >
                  Clear
                </button>
              </div>
            </div>
            <div className="space-y-1">
              {RETRY_REASONS.map((r) => (
                <label key={r.value} className="flex items-center gap-2 text-sm text-slate-700">
                  <input
                    type="checkbox"
                    checked={advanced.retryOn.has(r.value)}
                    onChange={() => toggleRetryReason(r.value)}
                    className="h-4 w-4 rounded border-slate-300"
                  />
                  <span>{r.label}</span>
                </label>
              ))}
            </div>
            <Help>No boxes ticked = no retry (even if max attempts &gt; 1).</Help>
          </div>
        </div>
        <div className="space-y-2">
          <p className="text-xs text-slate-600">
            <span className="font-semibold text-slate-700">Backoff</span> is how long the runner waits
            before each retry. Sleep = min(base × multiplier<sup>attempt</sup>, max), then randomised by
            jitter (0 = exact, 1 = ±100%). Defaults (30s base, 2× per attempt, capped at 10min, 20%
            jitter) give 30s → 60s → 120s → 240s.
          </p>
          <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
            <label className="block">
              <FieldLabel>Backoff base (s)</FieldLabel>
              <Input
                type="number"
                min={0.001}
                step={1}
                value={advanced.backoffBaseSec}
                onChange={(e) => setAdv("backoffBaseSec", e.target.value)}
              />
            </label>
            <label className="block">
              <FieldLabel>Backoff max (s)</FieldLabel>
              <Input
                type="number"
                min={0.001}
                step={1}
                value={advanced.backoffMaxSec}
                onChange={(e) => setAdv("backoffMaxSec", e.target.value)}
              />
            </label>
            <label className="block">
              <FieldLabel>Backoff multiplier</FieldLabel>
              <Input
                type="number"
                min={0.001}
                step={0.1}
                value={advanced.backoffMultiplier}
                onChange={(e) => setAdv("backoffMultiplier", e.target.value)}
              />
            </label>
            <label className="block">
              <FieldLabel hint="0 – 1">Backoff jitter</FieldLabel>
              <Input
                type="number"
                min={0}
                max={1}
                step={0.05}
                value={advanced.backoffJitter}
                onChange={(e) => setAdv("backoffJitter", e.target.value)}
              />
            </label>
          </div>
        </div>
      </fieldset>

      <fieldset className="space-y-3">
        <legend className="text-sm font-semibold text-slate-700">Scheduling</legend>
        <label className="block max-w-xs">
          <FieldLabel hint="0 – 1000">Submit priority</FieldLabel>
          <Input
            type="number"
            min={0}
            max={1000}
            step={1}
            value={advanced.submitPriority}
            onChange={(e) => setAdv("submitPriority", clampInt(e.target.value, 0, 1000))}
            onBlur={() => setAdv("submitPriority", clampInt(advanced.submitPriority, 0, 1000) || "100")}
          />
          <Help>Higher = scheduled first. Default 100.</Help>
        </label>
      </fieldset>
      <fieldset className="space-y-3">
        <legend className="text-sm font-semibold text-slate-700">Multi-model (terminus-2)</legend>
        <label className="flex items-center gap-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={advanced.multiModelEnabled}
            onChange={(e) => setAdv("multiModelEnabled", e.target.checked)}
          />
          Enable student → teacher → student switch
        </label>
        {advanced.multiModelEnabled ? (
          <>
            <label className="block max-w-md">
              <FieldLabel>Teacher model name</FieldLabel>
              <Input
                value={advanced.teacherModelName}
                onChange={(e) => setAdv("teacherModelName", e.target.value)}
                placeholder="Same provider as the student model"
              />
              <Help>Secondary model on the same BYO connection. Combinations must use terminus-2.</Help>
            </label>
            <fieldset className="space-y-2">
              <legend className="text-sm text-slate-700">Mix policy</legend>
              <label className="flex items-center gap-2 text-sm text-slate-700">
                <input
                  type="radio"
                  name="multiModelPolicy"
                  checked={advanced.multiModelPolicy === "student_teacher_student"}
                  onChange={() => setAdv("multiModelPolicy", "student_teacher_student")}
                />
                Student → teacher block → student (K1/K2)
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-700">
                <input
                  type="radio"
                  name="multiModelPolicy"
                  checked={advanced.multiModelPolicy === "beta_mixture"}
                  onChange={() => setAdv("multiModelPolicy", "beta_mixture")}
                />
                Per-episode beta coin
              </label>
              <label className="flex items-center gap-2 text-sm text-slate-700">
                <input
                  type="radio"
                  name="multiModelPolicy"
                  checked={advanced.multiModelPolicy === "student_to_teacher_turns"}
                  onChange={() => setAdv("multiModelPolicy", "student_to_teacher_turns")}
                />
                Turn schedule (rising beta + latch)
              </label>
            </fieldset>
            {advanced.multiModelPolicy === "beta_mixture" ? (
              <>
                <label className="block max-w-xs">
                  <FieldLabel>Beta (P teacher)</FieldLabel>
                  <Input
                    type="number"
                    min={0}
                    max={1}
                    step="0.1"
                    value={advanced.multiModelBeta}
                    onChange={(e) => setAdv("multiModelBeta", e.target.value)}
                  />
                  <Help>Teacher drives the episode when the replay-safe hash is less than beta.</Help>
                </label>
                <label className="block max-w-md">
                  <FieldLabel>Mix seed (optional)</FieldLabel>
                  <Input
                    value={advanced.multiModelSeed}
                    onChange={(e) => setAdv("multiModelSeed", e.target.value)}
                    placeholder="Server generates one if empty"
                  />
                </label>
              </>
            ) : advanced.multiModelPolicy === "student_to_teacher_turns" ? (
              <>
                <label className="block max-w-xs">
                  <FieldLabel>Step start (call ordinal)</FieldLabel>
                  <Input
                    type="number"
                    min={2}
                    value={advanced.multiModelStepStart}
                    onChange={(e) => setAdv("multiModelStepStart", clampInt(e.target.value, 2, 1000))}
                  />
                  <Help>
                    Always student before this turn. Rising teacher probability from here until step end.
                  </Help>
                </label>
                <label className="block max-w-xs">
                  <FieldLabel>Step end (force teacher)</FieldLabel>
                  <Input
                    type="number"
                    min={3}
                    value={advanced.multiModelStepEnd}
                    onChange={(e) => setAdv("multiModelStepEnd", clampInt(e.target.value, 3, 1000))}
                  />
                  <Help>Force teacher at this turn and latch for the rest of the trial.</Help>
                </label>
                <label className="block max-w-md">
                  <FieldLabel>Mix seed (optional)</FieldLabel>
                  <Input
                    value={advanced.multiModelSeed}
                    onChange={(e) => setAdv("multiModelSeed", e.target.value)}
                    placeholder="Server generates one if empty"
                  />
                </label>
              </>
            ) : (
              <label className="block max-w-xs">
                <FieldLabel>Teacher episodes</FieldLabel>
                <Input
                  type="number"
                  min={1}
                  max={1000}
                  value={advanced.teacherEpisodes}
                  onChange={(e) => setAdv("teacherEpisodes", clampInt(e.target.value, 1, 1000))}
                />
              </label>
            )}
          </>
        ) : null}
      </fieldset>
    </div>
  );
}
