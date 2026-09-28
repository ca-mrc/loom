import type { RepositoryDocId } from "./repositoryDocs";

export interface HelpTopic {
  title: string;
  summary: string;
  steps: string[];
  docs: RepositoryDocId[];
  action: { label: string; to: string };
}

export const HELP_TOPICS = {
  quickstart: {
    title: "Start using Loom",
    summary: "Run evaluations or generate trajectories through the web, CLI, or API.",
    steps: ["Confirm your account and current team.", "Choose runnable tasks and an available agent/model combination.", "Submit a small batch, follow it in Monitor, then inspect and download the results."],
    docs: ["quickstart", "web", "cli", "purpose"],
    action: { label: "Open New batch", to: "/batches/new" },
  },
  access: {
    title: "Accounts and access",
    summary: "Web and CLI use the same approved account. API tokens belong to a user and team.",
    steps: ["Request an account or accept your team invitation, then set your password.", "Check the current team in Settings before submitting work.", "Team owners and platform administrators manage API tokens through Team access; Settings shows token management when your role allows it. Ask your administrator if you need API access."],
    docs: ["access", "api", "cli"], action: { label: "Open Settings", to: "/settings" },
  },
  providers: {
    title: "Providers and models",
    summary: "Use an existing team connection, or register a hosted API or reachable inference server.",
    steps: ["Select a connection available to your team and inspect its status.", "For a connection your team owns, test it and refresh its model list when needed. Ask the owning team to manage a shared connection.", "In New batch, choose a compatible agent and a model supported by that connection. Availability in a catalog alone does not prove a run will succeed."],
    docs: ["providers"], action: { label: "Open Providers", to: "/providers" },
  },
  tasks: {
    title: "Tasks and batch purpose",
    summary: "Choose native benchmarks for evaluation, or benchmarks and task sets for trajectory generation.",
    steps: ["Choose evaluation for scored results, or trajectory generation for agent trajectories.", "Evaluation uses native benchmark tasks with verifiers. Custom task sets are available for trajectory generation; check their readiness before submission.", "For a custom task set, upload its manifest and bundle. Open the repository schema for required fields and optional verifier or transform files."],
    docs: ["tasks", "purpose", "web"], action: { label: "Open Task sets", to: "/task-sets" },
  },
  results: {
    title: "Monitor and results",
    summary: "Follow batches and trials, inspect execution, and download available outputs.",
    steps: ["Use Monitor to filter batches or trials and inspect resource scheduling.", "Open a batch or trial to review status, reward, usage, trajectory, and diagnostics.", "Download the available artifacts or complete Trial bundle. A queued or running job may not have finalized outputs yet."],
    docs: ["results", "cli"], action: { label: "Open Monitor", to: "/monitor" },
  },
  reuse: {
    title: "Run Library and reuse",
    summary: "Find shared runs and artifacts and use their configuration as a starting point.",
    steps: ["Filter the library and inspect results and provenance.", "Download an available artifact or clone a run into your current team.", "Select your own provider connection before queuing a clone. Credentials are never copied from another run."],
    docs: ["reuse"], action: { label: "Open Run Library", to: "/library" },
  },
  pipelines: {
    title: "Pipelines",
    summary: "Inspect recipe runs, their stages, and the artifacts passed between them.",
    steps: ["Open a Pipeline run to inspect its DAG and stage status.", "Review stage outputs, domain outcomes, and budget information.", "Follow artifacts to their producer or find shared Pipeline artifacts in Run Library."],
    docs: ["pipelines"], action: { label: "Open Pipelines", to: "/pipelines" },
  },
  usage: {
    title: "Usage and cost",
    summary: "Inspect recorded model usage and cost estimates for the selected scope.",
    steps: ["Choose the time range and available team scope.", "Review token totals and breakdowns alongside cost status and confidence.", "Missing pricing is not zero cost. Use the repository guide for CLI filters and pricing behavior."],
    docs: ["usage"], action: { label: "Open Usage", to: "/usage" },
  },
  rates: {
    title: "Rate cards",
    summary: "Platform administrators publish model prices used to derive cost estimates.",
    steps: ["Review the published provider/model prices.", "Prepare a new table using the repository schema and publish it with the existing form.", "Match the connection billing namespace to its rate-card provider when they differ."],
    docs: ["rates", "usage"], action: { label: "Open Rate cards", to: "/rate-cards" },
  },
} satisfies Record<string, HelpTopic>;
export type HelpTopicId = keyof typeof HELP_TOPICS;
export const HELP_TOPIC_IDS = Object.keys(HELP_TOPICS) as HelpTopicId[];
export function isHelpTopic(value: string | null): value is HelpTopicId {
  return value !== null && Object.prototype.hasOwnProperty.call(HELP_TOPICS, value);
}
export { helpTopicForPath } from "./helpTopicForPath";
