import type { components } from "../api/schema";
import { Card } from "./Card";

type Preparations = components["schemas"]["TrialDetail"]["task_environment_preparation"];

export function TaskImagePreparationCard({ preparations }: { preparations: Preparations }): JSX.Element | null {
  if (!preparations?.length) return null;
  return (
    <Card>
      <Card.Header
        title="Task environment preparation"
        description="Current shared image preparation. A later cache rebuild can change this view; the trial outcome above remains unchanged."
        headingLevel="h2"
      />
      <Card.Body className="stack-y-3">
        {preparations.map((item) => (
          <div key={item.cpu_arch} className="rounded border border-slate-200 p-3 text-sm">
            <p className="font-semibold">{item.cpu_arch} · {item.state} · {item.attempt_count} build attempts</p>
            {item.message ? <p className="mt-1">{item.message}</p> : null}
            {item.phases.length ? (
              <ul className="mt-2 stack-y-1">
                {item.phases.map((phase) => (
                  <li key={phase.name}>
                    {phase.name === "build" ? "Build / scratch cleanup" : phase.name}: {phase.state}
                    {phase.exit_code != null ? ` · exit code ${phase.exit_code}` : ""}
                  </li>
                ))}
              </ul>
            ) : null}
            {item.resources_released === true ? <p className="mt-2 text-slate-500">Build resources released</p> : null}
          </div>
        ))}
      </Card.Body>
    </Card>
  );
}
