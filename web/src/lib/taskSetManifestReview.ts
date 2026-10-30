import { parseDocument } from "yaml";

export interface TaskSetManifestReview {
  name: string;
  slug: string;
  sourceType: string;
  sourceLocator: string;
  intents: string[];
  taskName: string | null;
  verifier: string;
  maxInstances: number | null;
  taskTomlFormat: string | null;
}

function object(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown> : {};
}

/** Review declared inputs only; the server owns the complete manifest contract. */
export function reviewTaskSetManifest(text: string): TaskSetManifestReview {
  const document = parseDocument(text, { version: "1.2", schema: "core", uniqueKeys: true });
  if (document.errors.length) throw new Error(`Manifest syntax: ${document.errors[0].message}`);
  const manifest = object(document.toJS({ maxAliasCount: 20 }));
  if (manifest.apiVersion !== "loom.taskset/v1" || manifest.kind !== "UserTaskSet") {
    throw new Error("Use apiVersion: loom.taskset/v1 and kind: UserTaskSet.");
  }
  const metadata = object(manifest.metadata);
  const source = object(manifest.source);
  if (typeof metadata.name !== "string" || !metadata.name.trim()
      || typeof metadata.display_name !== "string" || !metadata.display_name.trim()) {
    throw new Error("Manifest metadata needs a name and display_name.");
  }
  if (typeof source.type !== "string" || !["hf", "git", "https", "jsonl-inline", "bundle-upload"].includes(source.type)
      || typeof source.locator !== "string" || !source.locator.trim()) {
    throw new Error("Manifest source needs a supported type and a locator.");
  }
  const intents = manifest.intents ?? ["trajectory_generation"];
  if (!Array.isArray(intents) || !intents.length || !intents.every((intent) => intent === "evaluation" || intent === "trajectory_generation")) {
    throw new Error("Manifest intents must declare evaluation or trajectory_generation.");
  }
  const task = object(object(manifest.task_template).task);
  const maxInstances = object(manifest.limits).max_instances;
  const verifier = object(manifest.verifier).type;
  return {
    name: metadata.display_name, slug: metadata.name, sourceType: source.type,
    sourceLocator: source.locator, intents,
    taskName: typeof task.name === "string" ? task.name : null,
    verifier: typeof verifier === "string" ? verifier : source.type === "bundle-upload" ? "From task bundle, if present" : "Not declared",
    maxInstances: typeof maxInstances === "number" ? maxInstances : null,
    taskTomlFormat: source.type === "bundle-upload"
      ? (typeof source.task_toml_format === "string" && source.task_toml_format.trim()
        ? source.task_toml_format.trim() : "auto")
      : null,
  };
}
