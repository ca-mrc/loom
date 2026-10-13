import { queryKeys } from "../../api/queryKeys";
/**
 * Models tab on /providers/:id. Refresh + Add manual + Hide/Unhide.
 * Backend returns all cached rows (no pagination); v1 fetches all and
 * provides a client-side filter input. See #167 spec.
 */
import { useQuery } from "@tanstack/react-query";
import { useState } from "react";

import { api, type ProviderConnectionModelEntry } from "../../api";
import { Button } from "../Button";
import { Card } from "../Card";
import CommandSnippet from "../CommandSnippet";
import { CommandActions } from "../CommandActions";
import { providerTestAge } from "../../lib/providerDisplay";
import { shellQuote } from "../../lib/shellQuote";
import { Input } from "../Input";
import LoadingState from "../LoadingState";
import {
  useAddManualModel,
  useHideModel,
  usePreflightModel,
  useRefreshModels,
  useUnhideModel,
} from "../../hooks/providers";
import AddManualModelModal from "./AddManualModelModal";

export type ModelsTabProps = { id: string; connectionName?: string };

function PreflightStatus({ model }: { model: ProviderConnectionModelEntry }): JSX.Element {
  if (model.last_preflight_status === "valid") {
    return (
      <div className="stack-y-1">
        <span
          className="rounded bg-emerald-50 px-2 py-1 text-xs font-medium text-emerald-700"
          title="The latest preflight call succeeded for this connection and model. This is historical verification, not a current availability guarantee."
        >
          Callable
        </span>
        <p className="text-xs text-slate-500">{providerTestAge(model.last_preflight_at)}</p>
        {model.last_preflight_http_status ? (
          <p className="text-xs text-slate-500">
            HTTP {model.last_preflight_http_status}
          </p>
        ) : null}
      </div>
    );
  }
  if (
    model.last_preflight_status === "failed" &&
    model.last_preflight_failure_kind === "inconclusive"
  ) {
    return (
      <div className="max-w-sm stack-y-1">
        <span
          className="rounded bg-amber-50 px-2 py-1 text-xs font-medium text-amber-800"
          title="The latest preflight timed out or hit a temporary upstream error. It did not confirm the model is callable, and it does not block new batches."
        >
          Inconclusive
        </span>
        <p className="text-xs text-slate-500">{providerTestAge(model.last_preflight_at)}</p>
        {model.last_preflight_error_code ? (
          <p className="text-xs font-medium text-amber-800">
            {model.last_preflight_error_code}
          </p>
        ) : null}
        {model.last_preflight_error_message ? (
          <p className="break-words text-xs text-slate-500">
            {model.last_preflight_error_message}
          </p>
        ) : null}
      </div>
    );
  }
  if (model.last_preflight_status === "failed") {
    return (
      <div className="max-w-sm stack-y-1">
        <span
          className="rounded bg-red-50 px-2 py-1 text-xs font-medium text-red-700"
          title="The latest preflight call failed. New batches using this known-failed model are blocked until it passes."
        >
          Cannot call
        </span>
        {model.last_preflight_error_code ? (
          <p className="text-xs font-medium text-red-700">
            {model.last_preflight_error_code}
          </p>
        ) : null}
        {model.last_preflight_error_message ? (
          <p className="break-words text-xs text-slate-500">
            {model.last_preflight_error_message}
          </p>
        ) : null}
      </div>
    );
  }
  return (
    <span
      className="rounded bg-slate-100 px-2 py-1 text-xs text-slate-600"
      title="Discovered or manually added, but no generation preflight has been run for this connection and model."
    >
      Not tested
    </span>
  );
}

export default function ModelsTab({ id, connectionName }: ModelsTabProps): JSX.Element {
  const [filter, setFilter] = useState("");
  const [showAdd, setShowAdd] = useState(false);
  const cliConnection = connectionName ?? id;

  const { data, isLoading, error } = useQuery({
    queryKey: queryKeys["providers"](id, "models"),
    queryFn: () => api.listProviderConnectionModels(id),
  });

  const refresh = useRefreshModels(id);
  const addManual = useAddManualModel(id);
  const preflight = usePreflightModel(id);
  const hide = useHideModel(id);
  const unhide = useUnhideModel(id);

  if (isLoading) return <LoadingState />;
  if (error) {
    return (
      <Card>
        <Card.Body>
          <p className="text-red-700">Could not load models.</p>
        </Card.Body>
      </Card>
    );
  }

  const items = (data?.items ?? []).filter((m) =>
    !filter || m.model_id.toLowerCase().includes(filter.toLowerCase()),
  );

  const isHidden = (model: ProviderConnectionModelEntry) =>
    model.visible === false || model.visibility === "hidden";

  const handleAdd = async (model: { model_id: string; display_name?: string }) => {
    await addManual.mutateAsync(model as Parameters<typeof addManual.mutateAsync>[0]);
    setShowAdd(false);
  };

  return (
    <div className="stack-y-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <Input placeholder="Filter models…" value={filter}
          onChange={(e) => setFilter(e.target.value)} className="w-full sm:w-72" />
        <div className="flex flex-wrap gap-2">
          <Button onClick={() => refresh.mutate()} disabled={refresh.isPending}>
            {refresh.isPending ? "Refreshing…" : "Refresh"}
          </Button>
          <Button variant="primary" onClick={() => setShowAdd(true)}>
            Add manual model
          </Button>
        </div>
      </div>
      {refresh.isError ? (
        <div
          className="rounded border border-red-200 bg-red-50 px-3 py-2 text-sm text-red-800"
          role="alert"
        >
          <p>
            {(refresh.error as { detail?: string } | null)?.detail
              ?? "Could not refresh models from upstream."}
          </p>
          <p className="mt-1 text-red-700">
            Use <strong>Add manual model</strong> if you know the model ID.
          </p>
        </div>
      ) : null}
      <CommandActions title="Model picker guidance" label="Model help and CLI">
        <p>
          Discovery is a cached catalog, not a generation test or proof of current availability. Refreshed visible models appear in New Batch. Hide noisy upstream
          entries, or add a manual model ID when the provider omits it from
          discovery.
        </p>
        <CommandSnippet
          label="Refresh provider model cache"
          command={`loom providers models ${shellQuote(cliConnection)} --refresh`}
        />
      </CommandActions>
      <Card>
        {items.length === 0 ? (
          <Card.Body className="text-center text-sm text-slate-500">
            No models cached. Click <strong>Refresh</strong> to fetch the upstream
            catalog, or <strong>Add manual model</strong> to register one by hand.
          </Card.Body>
        ) : (
          <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Provider models scroll area"><table aria-label="Provider models" className="min-w-full">
            <thead>
              <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wider text-slate-500">
                <th scope="col" className="px-4 py-2">Model ID</th>
                <th scope="col" className="px-4 py-2">Source</th>
                <th scope="col" className="px-4 py-2">Preflight</th>
                <th scope="col" className="px-4 py-2">Hidden</th>
                <th scope="col" className="relative px-4 py-2"><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {items.map((m) => {
                const hidden = isHidden(m);
                return (
                  <tr key={m.model_id} className="border-b border-slate-100">
                    <td className="px-4 py-3 font-mono text-sm">{m.model_id}</td>
                    <td className="px-4 py-3 text-sm text-slate-600">{m.source ?? "—"}</td>
                    <td className="px-4 py-3 text-sm">
                      <PreflightStatus model={m} />
                    </td>
                    <td className="px-4 py-3 text-sm">
                      {hidden ? (
                        <span className="rounded bg-slate-100 px-2 py-1 text-xs text-slate-600"
                          title="Hidden models don't appear in New Batch's picker">
                          hidden
                        </span>
                      ) : (<span className="text-slate-600">—</span>)}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex justify-end gap-2">
                        <Button
                          size="sm"
                          onClick={() => preflight.mutate(m.model_id)}
                          disabled={preflight.isPending}
                          title="Run one minimal generation request to confirm this connection can call the model."
                        >
                          Preflight
                        </Button>
                      {hidden ? (
                        <Button size="sm" onClick={() => unhide.mutate(m.model_id)} disabled={unhide.isPending}>
                          Unhide
                        </Button>
                      ) : (
                        <Button size="sm" onClick={() => hide.mutate(m.model_id)} disabled={hide.isPending}>
                          Hide
                        </Button>
                      )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table></div>
        )}
      </Card>
      {showAdd && (
        <AddManualModelModal pending={addManual.isPending}
          onClose={() => setShowAdd(false)} onSubmit={handleAdd} />
      )}
    </div>
  );
}
