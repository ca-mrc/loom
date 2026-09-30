/** Cache roots shared by domain queries and mutation invalidation.
 * Suffix tuples retain caller types; omitting suffixes invalidates the whole domain. */
function key<const Root extends string>(root: Root) {
  return <const Parts extends readonly unknown[]>(...parts: Parts): readonly [Root, ...Parts] => [
    root,
    ...parts,
  ];
}

import { buildVersionKey } from "./buildVersionKey";
export { buildVersionKey } from "./buildVersionKey";

export const queryKeys = {
  admin: key("admin"),
  "admin-teams": key("admin-teams"),
  agents: key("agents"),
  "api-tokens": key("api-tokens"),
  backends: key("backends"),
  batch: key("batch"),
  "batch-delivery-export": key("batch-delivery-export"),
  "batch-diagnostics": key("batch-diagnostics"),
  batches: key("batches"),
  "benchmark-discovery": key("benchmark-discovery"),
  benchmarks: key("benchmarks"),
  "build-version": buildVersionKey,
  invite: key("invite"),
  invites: key("invites"),
  models: key("models"),
  "monitor-placement": key("monitor-placement"),
  "monitor-summary": key("monitor-summary"),
  overview: key("overview"),
  "password-action": key("password-action"),
  "pipeline-artifact": key("pipeline-artifact"),
  "pipeline-run": key("pipeline-run"),
  "pipeline-run-artifacts": key("pipeline-run-artifacts"),
  "pipeline-run-stages": key("pipeline-run-stages"),
  "pipeline-runs": key("pipeline-runs"),
  "pipeline-stage": key("pipeline-stage"),
  "pipeline-stage-attempts": key("pipeline-stage-attempts"),
  "provider-connections": key("provider-connections"),
  providers: key("providers"),
  "public-teams": key("public-teams"),
  "rate-cards": key("rate-cards"),
  "run-library": key("run-library"),
  "run-library-batch": key("run-library-batch"),
  "run-library-batch-diagnostics": key("run-library-batch-diagnostics"),
  "run-library-pipeline-artifacts": key("run-library-pipeline-artifacts"),
  taskSets: key("taskSets"),
  tasks: key("tasks"),
  "tasks-count": key("tasks-count"),
  team: key("team"),
  tokens: key("tokens"),
  trajectory: key("trajectory"),
  "trajectory-fallback": key("trajectory-fallback"),
  trial: key("trial"),
  trials: key("trials"),
  usage: key("usage"),
};
