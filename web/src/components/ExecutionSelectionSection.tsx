import type { components } from "../api/schema";
import { executionClassLabel, isolationLabel } from "../lib/sandbox";
import { networkPolicyLabel } from "../lib/networkPolicy";

type Requested = components["schemas"]["RequestedExecutionSelection"];
type Effective = components["schemas"]["EffectiveExecutionSelection"];

function harnessLabel(requested: Requested): string {
  const harnesses = requested.harnesses ?? (requested.harness ? [requested.harness] : []);
  if (!harnesses.length) return "—";
  return harnesses.map((item) => `${item.name} @ ${item.version ?? "default"}`).join(", ");
}

function effectiveLabel(item: Effective): string {
  const grading = item.fresh_sandbox_grading ? "graded in a fresh sandbox" : "graded in the attempt";
  const count = item.trial_count != null ? ` · ${item.trial_count} trial${item.trial_count === 1 ? "" : "s"}` : "";
  return `${executionClassLabel(item.execution_class_id)} · ${item.verification} (${grading}) · ${item.execution_class_id}${count}`;
}

/** Requested axes beside what the frozen attempt plans actually ran. */
export function ExecutionSelectionSection({
  requested,
  effective,
}: {
  requested: Requested;
  effective: Effective | Effective[] | null;
}): JSX.Element {
  const effectiveItems = effective == null ? [] : Array.isArray(effective) ? effective : [effective];
  return (
    <section aria-label="Execution selection" className="space-y-2">
      <h3 className="text-sm font-semibold text-slate-900">Execution selection</h3>
      <dl className="grid gap-2 text-sm md:grid-cols-4">
        <div>
          <dt className="text-slate-500">Harness</dt>
          <dd className="break-words">{harnessLabel(requested)}</dd>
        </div>
        <div>
          <dt className="text-slate-500">Network override</dt>
          <dd className="break-words">{networkPolicyLabel(requested.network_policy)}</dd>
        </div>
        <div>
          <dt className="text-slate-500">Verification</dt>
          <dd>{requested.verification ?? "task default"}</dd>
        </div>
        <div>
          <dt className="text-slate-500">Isolation</dt>
          <dd>{isolationLabel(requested.isolation)}</dd>
        </div>
      </dl>
      <div className="text-sm">
        <span className="text-slate-500">Effective: </span>
        {effectiveItems.length ? (
          <ul className="inline">
            {effectiveItems.map((item) => (
              <li key={`${item.execution_class_id}:${item.verification}:${item.isolation}`} className="break-words">
                {effectiveLabel(item)}
              </li>
            ))}
          </ul>
        ) : (
          <span className="text-slate-600">not compiled yet</span>
        )}
      </div>
    </section>
  );
}
