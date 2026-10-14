import { HelpButton } from "../components/HelpButton";
/**
 * /providers/new — create form. Honours ?returnTo for redirects
 * after successful create.
 */
import { useState } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";

import { Card } from "../components/Card";
import ProviderForm, {
  type ProviderFormValues,
} from "../components/providers/ProviderForm";
import { useCreateConnection } from "../hooks/providers";
import { internalReturnPath } from "../lib/internalReturnPath";

export default function ProviderCreate(): JSX.Element {
  const navigate = useNavigate();
  const [params] = useSearchParams();
  const returnTo = internalReturnPath(params.get("returnTo"));
  const [error, setError] = useState<string | null>(null);
  const create = useCreateConnection();

  const handleSubmit = async (values: ProviderFormValues) => {
    setError(null);
    try {
      const result = await create.mutateAsync(
        values as Parameters<typeof create.mutateAsync>[0],
      );
      const id = (result as { id?: string }).id;
      if (returnTo) {
        navigate(returnTo);
      } else if (id) {
        navigate(`/providers/${id}`);
      } else {
        navigate("/providers");
      }
    } catch (e: unknown) {
      const detail =
        typeof e === "object" && e !== null && "detail" in e
          ? String((e as { detail: unknown }).detail)
          : "create failed";
      setError(detail);
    }
  };

  return (
    <Card>
      <Card.Body className="stack-y-4">
        <header>
          <h1 className="text-2xl font-bold text-slate-900">
            New provider connection
          </h1>
          <p className="mt-1 text-sm text-slate-500">
            Configure a BYO model provider for batch + trial submission.
          </p>
        </header>
        <div
          aria-label="Provider setup paths"
          className="grid gap-3 md:grid-cols-2"
        >
          <section className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <h2 className="text-sm font-semibold text-slate-900">
              Third-party API
            </h2>
            <p className="mt-1 text-sm text-slate-600">
              Use this form when you already have a hosted provider URL and
              API key. Create the connection, test it, refresh models, then
              select a model in New Batch.
            </p>
            <div className="mt-3">
              <HelpButton topic="providers">Provider setup guide</HelpButton>
            </div>
          </section>
          <section className="rounded-md border border-slate-200 bg-slate-50 p-3">
            <h2 className="text-sm font-semibold text-slate-900">
              User-managed model server
            </h2>
            <p className="mt-1 text-sm text-slate-600">
              Register your existing OpenAI-compatible inference URL and API key
              with this form. The server must be reachable from Loom.
            </p>
          </section>
        </div>
        {error && (
          <div
            role="alert"
            className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-700"
          >
            {error}
          </div>
        )}
        <ProviderForm
          mode="create"
          pending={create.isPending}
          onSubmit={handleSubmit}
        />
      </Card.Body>
    </Card>
  );
}
