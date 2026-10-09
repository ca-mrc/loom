import { LOADED_BUILD_INFO } from "./buildInfo";

export const REPOSITORY_DOCS = {
  quickstart: { path: "docs/user-guide.md", anchor: "choose-your-quickstart", label: "Choose your quickstart" },
  install: { path: "docs/user-guide.md", anchor: "install", label: "Install the CLI" },
  web: { path: "docs/user-guide.md", anchor: "quickstart-submit-from-the-web-app", label: "Web workflow" },
  cli: { path: "docs/user-guide.md", anchor: "quickstart-submit-from-the-cli-to-a-loom-server", label: "CLI workflow" },
  api: { path: "docs/user-guide.md", anchor: "quickstart-submit-through-the-api", label: "API workflow" },
  access: { path: "docs/user-guide.md", anchor: "web-sessions-and-teams", label: "Accounts and teams" },
  providers: { path: "docs/integrations/provider-onboarding.md", anchor: "hosted-third-party-api", label: "Provider onboarding" },
  tasks: { path: "docs/architecture/user-brought-tasksets.md", anchor: "manifest", label: "Task sets and manifest schema" },
  purpose: { path: "docs/architecture/user-brought-tasksets.md", anchor: "batch-purpose", label: "Evaluation and trajectory generation" },
  results: { path: "docs/user-guide.md", anchor: "delivery-bundles-for-release-handoff", label: "Results and delivery bundles" },
  reuse: { path: "docs/user-guide.md", anchor: "run-library", label: "Run Library and reuse" },
  pipelines: { path: "docs/user-guide.md", anchor: "official-recipe-pipelines", label: "Official recipe Pipelines" },
  usage: { path: "docs/architecture/cost-and-rate-cards.md", anchor: "usage-api-and-cli", label: "Usage API and CLI" },
  rates: { path: "docs/architecture/cost-and-rate-cards.md", anchor: "rate-card-shape", label: "Rate-card format" },
} as const;
export type RepositoryDocId = keyof typeof REPOSITORY_DOCS;

export function repositoryDocsVersion(revision: string | null = LOADED_BUILD_INFO.revision) {
  const pinned = typeof revision === "string" && /^[0-9a-f]{40}$/i.test(revision);
  const ref = pinned ? revision : "dev";
  return {
    ref,
    label: pinned
      ? `Repository documentation for this loaded build (${revision.slice(0, 12)}).`
      : "Repository documentation from dev. This local or unversioned build has no commit SHA; the docs may differ.",
  };
}

export function repositoryDocUrl(id: RepositoryDocId, revision: string | null = LOADED_BUILD_INFO.revision): string {
  const doc = REPOSITORY_DOCS[id];
  return `https://github.com/ca-mrc/loom/blob/${repositoryDocsVersion(revision).ref}/${doc.path}#${doc.anchor}`;
}
