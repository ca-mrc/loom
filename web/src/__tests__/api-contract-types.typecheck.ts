import type { components } from "../api/schema";

// Included by tsconfig.json: these contracts fail `npm run typecheck` rather
// than running a Vitest case whose type assertions are erased at runtime.
type Equal<Actual, Expected> =
  (<T>() => T extends Actual ? 1 : 2) extends
  (<T>() => T extends Expected ? 1 : 2) ? true : false;
type Assert<Matches extends true> = Matches;

type Task = components["schemas"]["Task"];
type Team = components["schemas"]["Team"];
type Trial = components["schemas"]["TrialDetail"];

export type ApiContractChecks = [
  Assert<Equal<Task["name"], string | null>>,
  Assert<Equal<Task["description"], string | null>>,
  Assert<Equal<Task["agent_name"], string | null>>,
  Assert<Equal<Task["verifier_name"], string | null>>,
  Assert<Equal<Task["step_count"], number>>,
  Assert<Equal<Team["disabled_at"], string | null | undefined>>,
  Assert<Equal<Pick<NonNullable<Trial["submitted_by_user"]>, "id" | "username" | "team_id" | "team_name">, {
    id: string;
    username: string;
    team_id?: string | null;
    team_name?: string | null;
  }>>,
  Assert<Equal<Extract<Trial["submitted_by_user"], null | undefined>, null | undefined>>,
];
