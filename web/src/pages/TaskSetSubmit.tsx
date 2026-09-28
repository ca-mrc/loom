import type { TaskSetManifestReview } from "../lib/taskSetManifestReview";
import { repositoryDocUrl } from "../lib/repositoryDocs";
import { HelpButton } from "../components/HelpButton";
import { useMutation } from "@tanstack/react-query";
import { useRef, useState } from "react";
import { useNavigate } from "react-router-dom";

import { api, type ApiError, type TaskSetSubmitResponse } from "../api";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { taskSetHref } from "../utils/taskSetLinks";

export default function TaskSetSubmit(): JSX.Element {
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);
  const [review, setReview] = useState<FormData | null>(null);
  const [manifestSummary, setManifestSummary] = useState<TaskSetManifestReview | null>(null);
  const [reviewing, setReviewing] = useState(false);
  const inputRevision = useRef(0);
  const manifestRef = useRef<HTMLInputElement>(null);
  const bundleRef = useRef<HTMLInputElement>(null);
  const verifierRef = useRef<HTMLInputElement>(null);
  const transformRef = useRef<HTMLInputElement>(null);

  const submit = useMutation({
    mutationFn: (formData: FormData) => api.submitTaskSet(formData),
    onSuccess: (res: TaskSetSubmitResponse) => {
      navigate(taskSetHref(res.task_set_id));
    },
    onError: (err: unknown) => {
      const apiErr = err as ApiError | undefined;
      setError(apiErr?.detail ?? "Submission failed. Please try again.");
    },
  });

  const handleSubmit = async (e: React.FormEvent): Promise<void> => {
    e.preventDefault();
    setError(null);
    setReview(null);
    setManifestSummary(null);
    const revision = inputRevision.current;

    const manifestFile = manifestRef.current?.files?.[0];
    if (!manifestFile) {
      setError("A manifest file is required.");
      return;
    }

    setReviewing(true);
    try {
      const [manifest, { reviewTaskSetManifest }] = await Promise.all([
        manifestFile.text(),
        import("../lib/taskSetManifestReview").catch(() => {
          throw new Error("Could not load manifest review. Reload Loom and try again.");
        }),
      ]);
      const summary = reviewTaskSetManifest(manifest);
      if (revision !== inputRevision.current) return;
      setManifestSummary(summary);
    } catch (err) {
      if (revision === inputRevision.current) setError(err instanceof Error ? err.message : "Could not read this manifest.");
      return;
    } finally {
      setReviewing(false);
    }
    const formData = new FormData();
    formData.append("manifest", manifestFile);

    const bundleFile = bundleRef.current?.files?.[0];
    if (bundleFile) formData.append("bundle", bundleFile);

    const verifierFile = verifierRef.current?.files?.[0];
    if (verifierFile) formData.append("verifier", verifierFile);

    const transformFile = transformRef.current?.files?.[0];
    if (transformFile) formData.append("transform", transformFile);

    setReview(formData);
  };

  return (
    <div className="space-y-4">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Submit Task Set</h1>
        <p className="text-sm text-slate-500">
          Upload a manifest with its task bundle or supporting scripts.
        </p>
      </header>

      <div className="rounded-lg border p-4 text-sm"><p className="mb-2">1. Prepare a YAML or JSON manifest, choosing the task source and trajectory purpose. Check the manifest schema and examples before selecting files. 2. Review the parsed manifest and selected files. Local checks cover syntax and required identity/source fields. The server validates the complete manifest and reports import errors after submission.</p><div className="flex flex-wrap items-center gap-4"><a className="text-accent underline" href={repositoryDocUrl("tasks")} target="_blank" rel="noreferrer">Manifest template and schema</a><HelpButton topic="tasks">Manifest format and task-set submission guide</HelpButton></div></div>
      <Card>
        <Card.Body>
          <form onSubmit={handleSubmit} onChange={() => { inputRevision.current += 1; setReview(null); setManifestSummary(null); }} className="space-y-5">
            <div>
              <label htmlFor="task-set-manifest" className="block text-sm font-medium text-slate-700">
                Manifest (required)
              </label>
              <input
                ref={manifestRef}
                id="task-set-manifest"
                type="file"
                accept=".yaml,.yml,.json"
                className="mt-1 block w-full text-sm text-slate-600 file:mr-3 file:rounded-md file:border file:border-slate-200 file:bg-white file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-slate-700 hover:file:bg-slate-50"
              />
              <p className="mt-1 text-xs text-slate-500">
                YAML or JSON manifest describing the task set.
              </p>
            </div>

            <div>
              <label htmlFor="task-set-bundle" className="block text-sm font-medium text-slate-700">
                Task bundle (optional)
              </label>
              <input
                ref={bundleRef}
                id="task-set-bundle"
                type="file"
                accept=".tar,.tar.gz,.tgz"
                className="mt-1 block w-full text-sm text-slate-600 file:mr-3 file:rounded-md file:border file:border-slate-200 file:bg-white file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-slate-700 hover:file:bg-slate-50"
              />
              <p className="mt-1 text-xs text-slate-500">
                Upload the .tar, .tar.gz or .tgz archive specified by your manifest.
                If it includes task verifiers, no separate verifier file is needed.
              </p>
            </div>

            <div>
              <label htmlFor="task-set-verifier" className="block text-sm font-medium text-slate-700">
                Verifier (optional)
              </label>
              <input
                ref={verifierRef}
                id="task-set-verifier"
                type="file"
                accept=".py"
                className="mt-1 block w-full text-sm text-slate-600 file:mr-3 file:rounded-md file:border file:border-slate-200 file:bg-white file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-slate-700 hover:file:bg-slate-50"
              />
              <p className="mt-1 text-xs text-slate-500">
                Python scoring script, if specified separately by your manifest.
              </p>
            </div>

            <div>
              <label htmlFor="task-set-transform" className="block text-sm font-medium text-slate-700">
                Transform (optional)
              </label>
              <input
                ref={transformRef}
                id="task-set-transform"
                type="file"
                accept=".py"
                className="mt-1 block w-full text-sm text-slate-600 file:mr-3 file:rounded-md file:border file:border-slate-200 file:bg-white file:px-3 file:py-1.5 file:text-sm file:font-medium file:text-slate-700 hover:file:bg-slate-50"
              />
              <p className="mt-1 text-xs text-slate-500">
                Optional script to transform upstream data rows before task rendering.
              </p>
            </div>

            {error ? (
              <div role="alert" className="rounded-md border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700">
                {error}
              </div>
            ) : null}

            {review ? <section className="rounded border p-3" aria-label="Submission review"><h2 className="font-medium">Review manifest before upload</h2>{manifestSummary ? <dl className="my-3 grid gap-2 text-sm sm:grid-cols-2"><div><dt className="text-slate-500">Task set</dt><dd>{manifestSummary.name} ({manifestSummary.slug})</dd></div><div><dt className="text-slate-500">Source</dt><dd>{manifestSummary.sourceType}</dd></div><div><dt className="text-slate-500">Declared purpose</dt><dd>{manifestSummary.intents.map((intent) => intent === "evaluation" ? "Evaluation" : "Trajectory generation").join(", ")}</dd></div><div><dt className="text-slate-500">Verifier</dt><dd>{manifestSummary.verifier}</dd></div><div><dt className="text-slate-500">Task template</dt><dd className="break-words">{manifestSummary.taskName ?? "From source tasks"}</dd></div><div><dt className="text-slate-500">Task limit</dt><dd>{manifestSummary.maxInstances === null ? "Server default" : `At most ${manifestSummary.maxInstances} configured instances`}; actual task count is determined during import.</dd></div></dl> : null}<p className="my-2 text-xs text-slate-500">Declared purpose and verifier are not a claim of evaluation readiness. Server validation remains authoritative.</p><ul>{Array.from(review.entries()).map(([name, value]) => <li key={name}>{name}: {value instanceof File ? `${value.name} (${value.size.toLocaleString()} bytes)` : value}</li>)}</ul><Button onClick={() => submit.mutate(review)} disabled={submit.isPending || reviewing}>Confirm upload</Button></section> : null}
            <div className="flex items-center gap-3">
              <Button
                type="submit"
                variant="primary"
                disabled={submit.isPending || reviewing}
              >
                {submit.isPending ? "Uploading..." : reviewing ? "Reading manifest…" : "Review submission"}
              </Button>
              <Button
                variant="secondary"
                onClick={() => navigate("/task-sets")}
              >
                Cancel
              </Button>
            </div>
          </form>
        </Card.Body>
      </Card>

    </div>
  );
}
